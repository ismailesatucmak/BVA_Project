"""T2.3 — Mesafe, ücret ve süre için anomali analizi (Spark).

1. Kural vs istatistik (HAM veri): R2/R3/R4 kuralları ile global IQR ve p99,9 eşikleri
   aynı kayıtları mı yakalıyor?
2. Bağlama duyarlı anomali (TEMİZ veri, **window**): aynı güzergâhta (PU→DO bölge çifti)
   güzergâhın kendi IQR'ına göre aşırı ücret/süre. Global eşiğin kaçırdıklarını bulur.
3. Ücret–mesafe tutarlılığı: kısa mesafe + yüksek ücret.
4. Örüntü (**join** zone lookup): anomaliler hangi saat / sağlayıcı / ilçede yoğunlaşıyor?

Çıktılar (reports/results/): anomaly_rule_vs_stat.csv, anomaly_context.csv,
anomaly_consistency.csv, anomaly_rule_rates.csv, anomaly_patterns.csv, anomaly_examples.csv;
grafikler için örneklem data/samples/.

Çalıştırma:  python -m src.anomaly
"""
import logging
import sys

import pandas as pd
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from src import clean, config, quality
from src.dictionary_stats import load_raw
from src.spark_session import get_spark

logger = logging.getLogger(__name__)

# metrik → onu denetleyen temizlik kuralı
METRIC_RULES = {"trip_distance": "R2", "fare_amount": "R3", "trip_duration_min": "R4"}
CONTEXT_METRICS = {"ctx_fare": "fare_amount", "ctx_duration": "trip_duration_min"}
ROUTE = ("PULocationID", "DOLocationID")
CONTEXT_PERCENTILE_ACCURACY = 1_000  # güzergâh içi çeyrekler; binlerce güzergâh × window
SAMPLE_FRACTION = 0.01  # grafikler için ~%1 örneklem (hash tabanlı, tekrar üretilebilir)
SAMPLES_DIR = config.DATA_DIR / "samples"
EXAMPLES_PER_KIND = 5


# ---------------------------------------------------------------- yardımcılar
def overlap_counts(df: DataFrame, a: Column, b: Column) -> dict:
    row = df.agg(
        F.sum(a.cast("long")).alias("a"),
        F.sum(b.cast("long")).alias("b"),
        F.sum((a & b).cast("long")).alias("both"),
        F.sum((a & ~b).cast("long")).alias("a_only"),
        F.sum((~a & b).cast("long")).alias("b_only"),
    ).first()
    return {k: int(v or 0) for k, v in row.asDict().items()}


def global_bounds(df: DataFrame, metrics: list[str]) -> dict[str, dict[str, float]]:
    """Her metrik için Q1, Q3, IQR sınırları ve aşırı uç yüzdeliği (tek geçiş)."""
    probs = [0.25, 0.75, config.EXTREME_PERCENTILE]
    row = df.agg(*[F.percentile_approx(m, probs, 10_000).alias(m) for m in metrics]).first()
    bounds = {}
    for m in metrics:
        q1, q3, p_extreme = row[m]
        iqr = q3 - q1
        bounds[m] = {"q1": q1, "q3": q3, "iqr_low": q1 - config.IQR_K * iqr,
                     "iqr_high": q3 + config.IQR_K * iqr, "p_extreme": p_extreme}
    return bounds


def _outside_iqr(metric: str, b: dict) -> Column:
    return F.coalesce((F.col(metric) < b["iqr_low"]) | (F.col(metric) > b["iqr_high"]), F.lit(False))


def _above(metric: str, threshold: float) -> Column:
    return F.coalesce(F.col(metric) > threshold, F.lit(False))


# ------------------------------------------------- 1. kural vs istatistik (ham)
def rule_vs_stat(raw_derived: DataFrame) -> pd.DataFrame:
    bounds = global_bounds(raw_derived, list(METRIC_RULES))
    records = []
    for metric, rule in METRIC_RULES.items():
        b = bounds[metric]
        rule_flag = clean.rule_condition(rule)
        for method, stat_flag, threshold in (
            ("IQR (k=1.5)", _outside_iqr(metric, b), f"[{b['iqr_low']:.2f}, {b['iqr_high']:.2f}]"),
            (f"p{config.EXTREME_PERCENTILE * 100:g} üstü", _above(metric, b["p_extreme"]), f"> {b['p_extreme']:.2f}"),
        ):
            counts = overlap_counts(raw_derived, rule_flag, stat_flag)
            records.append({"metric": metric, "rule": rule, "stat_method": method, "stat_threshold": threshold,
                            "rule_flagged": counts["a"], "stat_flagged": counts["b"], "both": counts["both"],
                            "rule_only": counts["a_only"], "stat_only": counts["b_only"]})
    return pd.DataFrame(records)


# -------------------------------------------- 2. bağlama duyarlı (window) — temiz
def add_context_flags(df: DataFrame, min_trips: int = config.MIN_ROUTE_TRIPS) -> DataFrame:
    """Güzergâh içi IQR'a göre aşırı ücret/süre. Az yolculuklu güzergâhlar işaretlenmez."""
    route = Window.partitionBy(*ROUTE)
    out = df.withColumn("route_trips", F.count(F.lit(1)).over(route))
    for flag, metric in CONTEXT_METRICS.items():
        quartiles = F.percentile_approx(metric, [0.25, 0.5, 0.75], CONTEXT_PERCENTILE_ACCURACY).over(route)
        q = f"{metric}_route_q"
        out = out.withColumn(q, quartiles)
        q1, med, q3 = F.col(q)[0], F.col(q)[1], F.col(q)[2]
        iqr = F.greatest(q3 - q1, config.CONTEXT_MIN_IQR_FRACTION * F.abs(med))
        outlier = (F.col(metric) > q3 + config.CONTEXT_IQR_K * iqr) | (F.col(metric) < q1 - config.CONTEXT_IQR_K * iqr)
        out = out.withColumns({
            flag: F.coalesce((F.col("route_trips") >= min_trips) & outlier, F.lit(False)),
            f"{metric}_route_median": med,
        }).drop(q)
    return out


def context_summary(flagged: DataFrame, bounds: dict) -> pd.DataFrame:
    """Bağlamsal anomali (yukarı/aşağı yönlü) ile global p99,9 eşiğinin karşılaştırması."""
    records = []
    for flag, metric in CONTEXT_METRICS.items():
        counts = overlap_counts(flagged, F.col(flag), _above(metric, bounds[metric]["p_extreme"]))
        high = flagged.filter(F.col(flag) & (F.col(metric) > F.col(f"{metric}_route_median"))).count()
        records.append({"metric": metric, "context_flagged": counts["a"], "context_high": high,
                        "context_low": counts["a"] - high, "global_p999_flagged": counts["b"],
                        "both": counts["both"], "context_only": counts["a_only"], "global_only": counts["b_only"]})
    return pd.DataFrame(records)


def consistency_checks(flagged: DataFrame) -> pd.DataFrame:
    """Ücret–mesafe tutarlılığı: basit kurallar ve bağlamsal yöntemin bunları yakalayıp yakalamadığı."""
    checks = {
        f"kısa mesafe (<{config.SHORT_TRIP_MILES:g} mil) + yüksek ücret (>{config.SHORT_TRIP_HIGH_FARE:g} $)":
            (F.col("trip_distance") < config.SHORT_TRIP_MILES) & (F.col("fare_amount") > config.SHORT_TRIP_HIGH_FARE),
        f"yer tutucu ücret (0 < ücret < {config.METERED_MIN_FARE:g} $ açılış ücreti)":
            F.col("fare_amount") < config.METERED_MIN_FARE,
    }
    records = []
    for name, cond in checks.items():
        counts = overlap_counts(flagged, cond, F.col("ctx_fare"))
        flex = flagged.filter(cond & F.col("is_flex_fare")).count()
        records.append({"check": name, "rows": counts["a"], "flex_fare_rows": flex,
                        "also_context_flagged": counts["both"]})
    return pd.DataFrame(records)


def rule_rates(raw: DataFrame, zones: DataFrame) -> pd.DataFrame:
    """Her kuralın sağlayıcı ve ilçe başına tek başına yakalama oranı (%) — anomalinin kaynağı."""
    rules = [r for r in clean.RULE_IDS if r != "R9"]
    joined = raw.join(F.broadcast(zones), "PULocationID", "left")
    frames = []
    for dim in ("VendorID", "pu_borough"):
        aggs = [F.count(F.lit(1)).alias("rows")]
        aggs += [F.round(100 * F.avg(clean.rule_condition(r).cast("double")), 2).alias(r) for r in rules]
        g = joined.groupBy(F.col(dim).cast("string").alias("value")).agg(*aggs).toPandas()
        frames.append(g.assign(dimension=dim))
    return pd.concat(frames, ignore_index=True)[["dimension", "value", "rows", *rules]]


# ------------------------------------------------------ 4. örüntü (join + group)
def _zones(spark) -> DataFrame:
    zones = spark.read.option("header", True).csv(str(config.ZONE_LOOKUP_PATH))
    return zones.select(F.col("LocationID").cast("int").alias("PULocationID"),
                        F.coalesce(F.col("Borough"), F.lit("Unknown")).alias("pu_borough"))


def patterns(df: DataFrame, flag: Column, zones: DataFrame, source: str) -> pd.DataFrame:
    """Anomali oranı: saat, sağlayıcı ve biniş ilçesi kırılımı (ilçe için zone lookup join)."""
    joined = df.join(F.broadcast(zones), "PULocationID", "left").withColumn("_flag", flag)
    frames = []
    for dim in ("pickup_hour", "VendorID", "pu_borough"):
        g = (joined.groupBy(F.col(dim).cast("string").alias("value"))
             .agg(F.count(F.lit(1)).alias("rows"), F.sum(F.col("_flag").cast("long")).alias("anomalies"))
             .toPandas())
        frames.append(g.assign(dimension=dim))
    out = pd.concat(frames, ignore_index=True)
    out["rate_pct"] = (100 * out["anomalies"] / out["rows"]).round(3)
    return out.assign(source=source)[["source", "dimension", "value", "rows", "anomalies", "rate_pct"]]


def examples(flagged: DataFrame) -> pd.DataFrame:
    cols = ["tpep_pickup_datetime", "VendorID", *ROUTE, "trip_distance", "trip_duration_min", "fare_amount",
            "fare_amount_route_median", "trip_duration_min_route_median", "route_trips"]
    frames = []
    for flag, metric in CONTEXT_METRICS.items():
        # mutlak sapmaya göre sırala: medyanı ~0 olan güzergâhlarda oran anlamsız büyür
        rows = (flagged.filter(F.col(flag)).withColumn("excess", F.col(metric) - F.col(f"{metric}_route_median"))
                .orderBy(F.desc("excess")).limit(EXAMPLES_PER_KIND).select("excess", *cols).toPandas())
        frames.append(rows.assign(kind=flag))
    return pd.concat(frames, ignore_index=True)


def _sample(df: DataFrame) -> DataFrame:
    key = F.pmod(F.xxhash64("tpep_pickup_datetime", "PULocationID", "DOLocationID", "fare_amount"), F.lit(10_000))
    return df.filter(key < SAMPLE_FRACTION * 10_000)


# ------------------------------------------------------------------------ main
def run() -> None:
    spark = get_spark("anomaly")
    results = {}
    try:
        zones = _zones(spark)
        raw = quality.add_derived(load_raw(spark)).cache()
        results["anomaly_rule_vs_stat.csv"] = rule_vs_stat(raw)
        any_rule = clean.rule_condition("R1")
        for rule in clean.RULE_IDS[1:]:
            if rule != "R9":
                any_rule = any_rule | clean.rule_condition(rule)
        raw_patterns = patterns(raw, any_rule, zones, "raw: herhangi bir kural")
        results["anomaly_rule_rates.csv"] = rule_rates(raw, zones)

        clean_df = spark.read.parquet(str(config.CLEAN_DIR))
        bounds = global_bounds(clean_df, list(CONTEXT_METRICS.values()))
        flagged = add_context_flags(clean_df).cache()
        results["anomaly_context.csv"] = context_summary(flagged, bounds)
        results["anomaly_consistency.csv"] = consistency_checks(flagged)
        ctx_any = F.col("ctx_fare") | F.col("ctx_duration")
        results["anomaly_patterns.csv"] = pd.concat(
            [raw_patterns, patterns(flagged, ctx_any, zones, "clean: bağlamsal anomali")], ignore_index=True)
        results["anomaly_examples.csv"] = examples(flagged)

        SAMPLES_DIR.mkdir(parents=True, exist_ok=True)
        sample_cols = ["trip_distance", "fare_amount", "trip_duration_min", "is_flex_fare", "VendorID"]
        _sample(raw.withColumn("any_rule", any_rule)).select(*sample_cols, "any_rule") \
            .toPandas().to_parquet(SAMPLES_DIR / "raw_sample.parquet", index=False)
        _sample(flagged).select(*sample_cols, "ctx_fare", "ctx_duration", "fare_amount_route_median") \
            .toPandas().to_parquet(SAMPLES_DIR / "clean_context_sample.parquet", index=False)
    finally:
        spark.stop()

    config.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    with pd.option_context("display.width", 250, "display.max_columns", 30):
        for name, frame in results.items():
            frame.to_csv(config.RESULTS_DIR / name, index=False)
            logger.info("== %s\n%s\n", name, frame.to_string(index=False))


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
