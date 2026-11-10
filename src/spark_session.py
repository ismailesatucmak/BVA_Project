"""Windows'ta PySpark 3.5 için ortamı hazırlayıp SparkSession döndürür.

Yollar ortam değişkenleriyle (JAVA_HOME, HADOOP_HOME) ezilebilir;
tanımlı değilse aşağıdaki varsayılanlar kullanılır.
"""
import os
import sys
from pathlib import Path

DEFAULT_JAVA_HOME = r"C:\tools\jdk-17"
DEFAULT_HADOOP_HOME = r"C:\tools\hadoop"
DEFAULT_DRIVER_MEMORY = "6g"


def _configure_environment() -> None:
    java_home = os.environ.get("NYC_JAVA_HOME", DEFAULT_JAVA_HOME)
    hadoop_home = os.environ.get("HADOOP_HOME", DEFAULT_HADOOP_HOME)

    for name, path in (("JAVA_HOME", java_home), ("HADOOP_HOME", hadoop_home)):
        if not Path(path).exists():
            raise FileNotFoundError(f"{name} bulunamadı: {path}")

    os.environ["JAVA_HOME"] = java_home
    os.environ["HADOOP_HOME"] = hadoop_home
    hadoop_bin = str(Path(hadoop_home) / "bin")
    java_bin = str(Path(java_home) / "bin")
    os.environ["PATH"] = os.pathsep.join([java_bin, hadoop_bin, os.environ.get("PATH", "")])
    # Bilgisayar adı "_" içeriyorsa (örn. Ismail_Esat) Spark URL'si geçersiz olur → localhost kullan
    os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"
    # Spark worker'larının aynı (venv) Python'u kullanması için
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def get_spark(app_name: str = "nyc-taxi", driver_memory: str = DEFAULT_DRIVER_MEMORY):
    """Yerel modda, tüm çekirdekleri kullanan bir SparkSession döndürür."""
    _configure_environment()
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", driver_memory)
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
