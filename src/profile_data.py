"""T2.1 — Ham verinin profili: dağılımlar ve mantıksal tutarsızlıklar (Spark, 19M kayıt).

Çıktılar:
  reports/results/profile_raw.csv       sayısal kolon başına null, ort., std, yüzdelikler
  reports/results/logic_checks_raw.csv  her kontrolün yakaladığı kayıt (tümü / Flex Fare / diğer)

Çalıştırma:  python -m src.profile_data
"""
import logging
import sys

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src import config, quality
from src.dictionary_stats import load_raw
from src.spark_session import get_spark

logger = logging.getLogger(__name__)

NUMERIC_COLUMNS = (
    "passenger_count", "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount",
    "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge",
    "Airport_fee", "cbd_congestion_fee", "trip_duration_min", "avg_speed_mph",
)
# percentile_approx: sıralama hatası ≈ 1/accuracy (%0,01). 100_000 ile 14 kolon × 28
# görevin özetleri 6 GB sürücü belleğini aştı ve JVM çöktü → Spark varsayılanı.
PERCENTILE_ACCURACY = 10_000
PROFILE_FILE = "profile_raw.csv"
CHECKS_FILE = "logic_checks_raw.csv"


def _pct_label(p: float) -> str:
    return f"p{p * 100:g}"


def numeric_profile(df: DataFrame) -> pd.DataFrame:
    percentiles = list(config.PROFILE_PERCENTILES)
    aggs = []
    for c in NUMERIC_COLUMNS:
        col = F.col(c).cast("double")
        aggs += [
            F.count(col).alias(f"{c}__count"),
            F.sum(col.isNull().cast("long")).alias(f"{c}__nulls"),
            F.mean(col).alias(f"{c}__mean"),
            F.stddev(col).alias(f"{c}__std"),
            F.min(col).alias(f"{c}__min"),
            F.percentile_approx(col, percentiles, PERCENTILE_ACCURACY).alias(f"{c}__pct"),
            F.max(col).alias(f"{c}__max"),
        ]
    row = df.agg(*aggs).first().asDict()
    records = []
    for c in NUMERIC_COLUMNS:
        record = {s: row[f"{c}__{s}"] for s in ("count", "nulls", "mean", "std", "min", "max")}
        pct = row[f"{c}__pct"] or [None] * len(percentiles)
        record.update({_pct_label(p): v for p, v in zip(percentiles, pct)})
        records.append({"column": c, **record})
    ordered = ["column", "count", "nulls", "mean", "std", "min", *map(_pct_label, percentiles), "max"]
    return pd.DataFrame(records)[ordered]


def check_counts(df: DataFrame) -> pd.DataFrame:
    """Her kontrol için tek başına yakalanan kayıt sayısı; Flex Fare kırılımıyla."""
    checks = quality.logic_checks()
    flagged = df.select("is_flex_fare", *[c.alias(n) for n, c in checks.items()])
    any_check = F.lit(False)
    for name in checks:
        any_check = any_check | F.col(name)
    flagged = flagged.withColumn("any_check", any_check)
    names = [*checks, "any_check"]
    per_group = (
        flagged.groupBy("is_flex_fare")
        .agg(F.count(F.lit(1)).alias("rows"), *[F.sum(F.col(n).cast("long")).alias(n) for n in names])
        .toPandas()
        .set_index("is_flex_fare")
    )
    total_rows = int(per_group["rows"].sum())
    records = []
    for name in names:
        flex = int(per_group.loc[True, name]) if True in per_group.index else 0
        other = int(per_group.loc[False, name]) if False in per_group.index else 0
        records.append(
            {
                "check": name,
                "rows": flex + other,
                "pct": round(100 * (flex + other) / total_rows, 4),
                "flex_fare_rows": flex,
                "non_flex_rows": other,
            }
        )
    return pd.DataFrame(records)


def duplicate_count(df: DataFrame) -> int:
    return df.count() - df.dropDuplicates().count()


def run() -> None:
    spark = get_spark("profile-data")
    try:
        raw = load_raw(spark)
        derived = quality.add_derived(raw).cache()
        total = derived.count()
        profile = numeric_profile(derived)
        checks = check_counts(derived)
        dups = duplicate_count(raw)
        checks = pd.concat(
            [checks, pd.DataFrame([{"check": "exact_duplicate_extra_copies", "rows": dups,
                                    "pct": round(100 * dups / total, 4)}])],
            ignore_index=True,
        )
    finally:
        spark.stop()

    config.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    profile.to_csv(config.RESULTS_DIR / PROFILE_FILE, index=False)
    checks.to_csv(config.RESULTS_DIR / CHECKS_FILE, index=False)
    with pd.option_context("display.width", 250, "display.max_columns", 20, "display.float_format", "{:,.3f}".format):
        logger.info("Toplam kayıt: %s\n\n%s\n\n%s", f"{total:,}", profile.to_string(index=False), checks.to_string(index=False))


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
