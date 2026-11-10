"""Spark kurulum testi: Parquet/CSV yaz-oku, Python UDF, Arrow ile toPandas.

Çalıştırma (proje kökünden):  .venv\\Scripts\\python scripts\\smoke_test_spark.py
"""
import shutil
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.spark_session import get_spark  # noqa: E402

TMP_DIR = PROJECT_ROOT / "data" / "_smoke_test"


def main() -> None:
    spark = get_spark("smoke-test")
    print(f"Spark {spark.version} | Java {spark.sparkContext._jvm.System.getProperty('java.version')}")

    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType

    df = spark.range(0, 100_000).withColumn("fare", (F.rand(seed=42) * 50).cast("double"))

    parquet_path = str(TMP_DIR / "parquet")
    csv_path = str(TMP_DIR / "csv")
    df.write.mode("overwrite").parquet(parquet_path)
    df.write.mode("overwrite").option("header", True).csv(csv_path)
    print("Yazma: OK (parquet + csv)")

    n_parquet = spark.read.parquet(parquet_path).count()
    n_csv = spark.read.option("header", True).csv(csv_path).count()
    assert n_parquet == n_csv == 100_000, (n_parquet, n_csv)
    print("Okuma: OK")

    double_it = F.udf(lambda x: x * 2, IntegerType())
    total = df.limit(10).select(double_it(F.col("id").cast("int")).alias("d")).agg(F.sum("d")).first()[0]
    assert total == 90, total
    print("Python UDF (worker): OK")

    pdf = df.limit(1000).toPandas()
    assert len(pdf) == 1000
    print("toPandas (Arrow): OK")

    spark.stop()
    shutil.rmtree(TMP_DIR, ignore_errors=True)
    print("TÜM TESTLER BAŞARILI")


if __name__ == "__main__":
    main()
