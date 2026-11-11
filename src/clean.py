"""T2.2 + T2.4 — Kural bazlı temizlik ve temiz veri teslimi.

Kurallar (docs/cleaning_rules.md) quality.logic_checks() tanımlarını kullanır; R9 (tam kopya)
window fonksiyonuyla bulunur. Kontrollerden birine takılan kayıt silinir; Flex Fare'den gelen
null değerler tek başına silme nedeni değildir.

Her kural için üç sayı raporlanır:
  isolated   — kuralın tek başına yakaladığı kayıt
  sequential — R1→R10 sırayla uygulandığında bu kuralın sildiği (önceki kurallardan arta kalan)
  unique     — yalnızca bu kuralın yakaladığı (başka hiçbir kural yakalamıyor)

Çıktılar:
  data/clean/  Parquet, pickup_year/pickup_month ile partition'lı
  reports/results/cleaning_impact.csv, cleaning_before_after.csv

Çalıştırma:  python -m src.clean
"""
import logging
import sys

import pandas as pd
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src import config, quality
from src.dictionary_stats import load_raw
from src.spark_session import get_spark

logger = logging.getLogger(__name__)

# kural → (açıklama, kullandığı quality.logic_checks() adları). R9 ayrıca (window).
RULES: dict[str, tuple[str, tuple[str, ...]]] = {
    "R1": ("passenger_count 0 veya > 6", ("passenger_count_zero", "passenger_count_over_max")),
    "R2": ("trip_distance <= 0 veya > 100 mil", ("distance_zero_or_negative", "distance_over_max")),
    "R3": ("fare_amount <= 0 veya total_amount <= 0", ("fare_zero_or_negative", "total_zero_or_negative")),
    "R4": ("süre <= 1 dk, > 180 dk veya negatif", ("dropoff_before_pickup", "duration_under_min", "duration_over_max")),
    "R5": ("ortalama hız > 80 mph", ("speed_over_max",)),
    "R6": ("pickup 2026-01..05 dışında", ("pickup_outside_period",)),
    "R7": ("RatecodeID = 99", ("ratecode_unknown",)),
    "R8": ("PU/DO LocationID 264 veya 265", ("location_unknown",)),
    "R9": ("tam kopya kayıt (ilki tutulur)", ()),
    "R10": ("cbd_congestion_fee < 0", ("cbd_fee_negative",)),
}
RULE_IDS = tuple(RULES)
CLEAN_PARTITIONS = ("pickup_year", "pickup_month")
IMPACT_FILE = "cleaning_impact.csv"
BEFORE_AFTER_FILE = "cleaning_before_after.csv"
SUMMARY_METRICS = ("trip_distance", "fare_amount", "total_amount", "trip_duration_min", "avg_speed_mph")


def _duplicate_flag(raw_columns: list[str]):
    """Aynı 20 ham kolona sahip kayıtlardan ilki dışındakiler True (window: row_number)."""
    window = Window.partitionBy(*raw_columns).orderBy(F.lit(1))
    return F.row_number().over(window) > 1


def flag_rules(derived: DataFrame) -> DataFrame:
    """add_derived() çıktısına her kural için bir bool kolon (R1…R10) ekler."""
    raw_columns = [c for c in derived.columns if c not in quality.DERIVED_COLUMNS]
    checks = quality.logic_checks()
    flags = {}
    for rule, (_, check_names) in RULES.items():
        if rule == "R9":
            flags[rule] = _duplicate_flag(raw_columns)
            continue
        condition = F.lit(False)
        for name in check_names:
            condition = condition | checks[name]
        flags[rule] = condition
    return derived.withColumns(flags)


def rule_impact(flagged: DataFrame) -> pd.DataFrame:
    aggs = [F.count(F.lit(1)).alias("rows")]
    for i, rule in enumerate(RULE_IDS):
        earlier = [F.col(r) for r in RULE_IDS[:i]]
        others = [F.col(r) for r in RULE_IDS if r != rule]
        any_earlier = _any(earlier)
        any_other = _any(others)
        aggs += [
            F.sum(F.col(rule).cast("long")).alias(f"{rule}__isolated"),
            F.sum((F.col(rule) & ~any_earlier).cast("long")).alias(f"{rule}__sequential"),
            F.sum((F.col(rule) & ~any_other).cast("long")).alias(f"{rule}__unique"),
        ]
    aggs.append(F.sum(_any([F.col(r) for r in RULE_IDS]).cast("long")).alias("TOTAL__sequential"))
    row = flagged.agg(*aggs).first().asDict()
    total = row["rows"]
    records = [
        {
            "rule": rule,
            "description": RULES[rule][0],
            "isolated": row[f"{rule}__isolated"],
            "sequential": row[f"{rule}__sequential"],
            "unique": row[f"{rule}__unique"],
        }
        for rule in RULE_IDS
    ]
    records.append({"rule": "TOTAL", "description": "en az bir kurala takılan", "isolated": None,
                    "sequential": row["TOTAL__sequential"], "unique": None})
    impact = pd.DataFrame(records)
    impact["sequential_pct"] = (100 * impact["sequential"] / total).round(3)
    impact["isolated_pct"] = (100 * impact["isolated"] / total).round(3)
    return impact.assign(total_rows=total)


def _any(columns: list) -> F.Column:
    result = F.lit(False)
    for col in columns:
        result = result | col
    return result


def select_clean(flagged: DataFrame) -> DataFrame:
    keep = ~_any([F.col(r) for r in RULE_IDS])
    return (
        flagged.filter(keep)
        .drop(*RULE_IDS)
        .withColumns({"pickup_year": F.year("tpep_pickup_datetime"), "pickup_month": F.month("tpep_pickup_datetime")})
    )


def naive_dropna_removed(raw: DataFrame) -> int:
    """Karşılaştırma: herhangi bir null içeren satırı silen naif temizlik kaç kayıt siler?"""
    return raw.count() - raw.dropna().count()


def summary_stats(df: DataFrame, label: str) -> dict:
    aggs = [F.count(F.lit(1)).alias("rows")]
    for c in SUMMARY_METRICS:
        aggs += [F.mean(c).alias(f"{c}_mean"), F.percentile_approx(c, 0.5).alias(f"{c}_median"),
                 F.max(c).alias(f"{c}_max")]
    return {"dataset": label, **df.agg(*aggs).first().asDict()}


def run() -> None:
    spark = get_spark("clean")
    try:
        raw = load_raw(spark)
        flagged = flag_rules(quality.add_derived(raw)).cache()
        impact = rule_impact(flagged)
        naive = naive_dropna_removed(raw)
        # R9'un window'u veriyi 200 shuffle partition'ına dağıtır → ay başına 200 küçük dosya
        # (ilk denemede 1.000 dosya, 603 MB). Ay başına tek partition + zamana göre sıralama:
        # az dosya, daha iyi sıkıştırma ve zaman aralığı sorgularında row group atlama.
        cleaned = (
            select_clean(flagged)
            .repartition(*CLEAN_PARTITIONS)
            .sortWithinPartitions(*CLEAN_PARTITIONS, "tpep_pickup_datetime")
        )
        cleaned.write.mode("overwrite").partitionBy(*CLEAN_PARTITIONS).parquet(str(config.CLEAN_DIR))
        written = spark.read.parquet(str(config.CLEAN_DIR))
        before_after = pd.DataFrame([summary_stats(flagged, "raw"), summary_stats(written, "clean")])
        expected = impact.loc[impact["rule"] == "TOTAL", "total_rows"].iloc[0] - impact.loc[impact["rule"] == "TOTAL", "sequential"].iloc[0]
        if before_after.loc[1, "rows"] != expected:
            raise RuntimeError(f"Temiz veri {before_after.loc[1, 'rows']} satır, beklenen {expected}")
    finally:
        spark.stop()

    total = int(impact["total_rows"].iloc[0])
    naive_row = {"rule": "NAIVE", "description": "null içeren her satırı sil (karşılaştırma)",
                 "sequential": naive, "sequential_pct": round(100 * naive / total, 3), "total_rows": total}
    impact = pd.concat([impact, pd.DataFrame([naive_row])], ignore_index=True)
    config.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    impact.to_csv(config.RESULTS_DIR / IMPACT_FILE, index=False)
    before_after.to_csv(config.RESULTS_DIR / BEFORE_AFTER_FILE, index=False)
    with pd.option_context("display.width", 250, "display.max_columns", 30):
        logger.info("%s\n\n%s", impact.to_string(index=False), before_after.T.to_string())


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
