"""T1.2 — Veri sözlüğü için kolon istatistiklerini tüm ham veriden (Spark) çıkarır.

Çıktılar:
  reports/results/dictionary_stats.csv   kolon başına tip, null, min, max, distinct, örnek
  reports/results/code_value_counts.csv  kod kolonlarının değer dağılımı
  reports/results/flex_fare_check.csv    payment_type=0 ile null kolonların ilişkisi

Çalıştırma:  python -m src.dictionary_stats
"""
import logging
import sys

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src import config
from src.spark_session import get_spark

logger = logging.getLogger(__name__)

CODE_COLUMNS = ("VendorID", "RatecodeID", "payment_type", "store_and_fwd_flag")
FLEX_NULL_COLUMNS = (
    "passenger_count",
    "RatecodeID",
    "store_and_fwd_flag",
    "congestion_surcharge",
    "Airport_fee",
)


def load_raw(spark) -> DataFrame:
    paths = [str(config.RAW_DIR / config.trip_file_name(config.YEAR, m)) for m in config.MONTHS]
    return spark.read.parquet(*paths)


def column_stats(df: DataFrame) -> pd.DataFrame:
    """Tek geçişte her kolon için null, min, max ve yaklaşık distinct sayısını hesaplar."""
    aggs = []
    for c in df.columns:
        aggs += [
            F.sum(F.col(c).isNull().cast("long")).alias(f"{c}__nulls"),
            F.min(c).cast("string").alias(f"{c}__min"),
            F.max(c).cast("string").alias(f"{c}__max"),
            F.approx_count_distinct(c).alias(f"{c}__distinct"),
        ]
    row = df.agg(F.count(F.lit(1)).alias("__total"), *aggs).first().asDict()
    sample = df.dropna().first().asDict()
    total = row["__total"]

    records = [
        {
            "column": field.name,
            "spark_type": field.dataType.simpleString(),
            "null_count": row[f"{field.name}__nulls"],
            "null_pct": round(100 * row[f"{field.name}__nulls"] / total, 3),
            "min": row[f"{field.name}__min"],
            "max": row[f"{field.name}__max"],
            "approx_distinct": row[f"{field.name}__distinct"],
            "example": str(sample[field.name]),
        }
        for field in df.schema.fields
    ]
    return pd.DataFrame(records).assign(total_rows=total)


def code_value_counts(df: DataFrame) -> pd.DataFrame:
    frames = []
    for c in CODE_COLUMNS:
        # string'e çevir: null içeren int kolon pandas'ta float'a dönüşüp "1.0" görünmesin
        value = F.coalesce(F.col(c).cast("string"), F.lit("null")).alias("value")
        counts = df.groupBy(value).count().toPandas()
        frames.append(counts.assign(column=c))
    result = pd.concat(frames, ignore_index=True)[["column", "value", "count"]]
    totals = result.groupby("column")["count"].transform("sum")
    return result.assign(pct=(100 * result["count"] / totals).round(3)).sort_values(["column", "value"])


def flex_fare_check(df: DataFrame) -> pd.DataFrame:
    """payment_type=0 ile 5 kolonun birlikte null olması aynı kayıtlar mı?"""
    all_null = F.lit(True)
    for c in FLEX_NULL_COLUMNS:
        all_null = all_null & F.col(c).isNull()
    any_null = F.lit(False)
    for c in FLEX_NULL_COLUMNS:
        any_null = any_null | F.col(c).isNull()
    return (
        df.groupBy((F.col("payment_type") == 0).alias("is_payment_type_0"))
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum(all_null.cast("long")).alias("all5_null"),
            F.sum(any_null.cast("long")).alias("any_null"),
        )
        .toPandas()
    )


def run() -> None:
    spark = get_spark("dictionary-stats")
    try:
        df = load_raw(spark).cache()
        config.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        outputs = {
            "dictionary_stats.csv": column_stats(df),
            "code_value_counts.csv": code_value_counts(df),
            "flex_fare_check.csv": flex_fare_check(df),
        }
        for name, frame in outputs.items():
            frame.to_csv(config.RESULTS_DIR / name, index=False)
            logger.info("Yazıldı: %s\n%s\n", name, frame.to_string(index=False))
    finally:
        spark.stop()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
