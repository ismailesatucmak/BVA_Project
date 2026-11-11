"""Proje genelinde kullanılan sabitler: yollar, veri kapsamı, kaynak URL'ler."""
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
CLEAN_DIR = DATA_DIR / "clean"

# Büyük çıktılar (boyut setleri, Spark geçici dosyaları) ayrı diskte tutulur.
# Başka bir makinede NYC_DATA_ROOT ortam değişkeniyle değiştirilir.
EXTERNAL_DATA_ROOT = Path(os.environ.get("NYC_DATA_ROOT", r"D:\BVAVeri"))
PROCESSED_DIR = EXTERNAL_DATA_ROOT / "processed"
SPARK_LOCAL_DIR = EXTERNAL_DATA_ROOT / "spark_tmp"
RESULTS_DIR = PROJECT_ROOT / "reports" / "results"
FIGURES_DIR = PROJECT_ROOT / "reports" / "figures"

# Veri kapsamı: Yellow Taxi 2026 Ocak–Mayıs
YEAR = 2026
MONTHS = (1, 2, 3, 4, 5)

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
ZONE_LOOKUP_FILE = "taxi_zone_lookup.csv"
ZONE_LOOKUP_URL = f"{TLC_BASE_URL}/misc/{ZONE_LOOKUP_FILE}"
ZONE_LOOKUP_COLUMNS = ("LocationID", "Borough", "Zone", "service_zone")
ZONE_COUNT = 265

# Gerçek veriden okunan şema (2026-01..05, beş dosyada aynı)
TRIP_COLUMNS = (
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "Airport_fee",
    "cbd_congestion_fee",
)


# ---- Veri kalitesi eşikleri (T2.1 profil + T2.2 temizlik aynı değerleri kullanır)
PERIOD_START = "2026-01-01 00:00:00"  # dahil
PERIOD_END = "2026-06-01 00:00:00"  # hariç
FLEX_FARE_PAYMENT_TYPE = 0
UNKNOWN_RATECODE = 99
UNKNOWN_LOCATION_IDS = (264, 265)
MAX_PASSENGERS = 6
MAX_TRIP_MILES = 100.0
MIN_DURATION_MIN = 1.0
MAX_DURATION_MIN = 180.0
MAX_SPEED_MPH = 80.0
PROFILE_PERCENTILES = (0.001, 0.01, 0.25, 0.5, 0.75, 0.99, 0.999)


def trip_file_name(year: int, month: int) -> str:
    return f"yellow_tripdata_{year}-{month:02d}.parquet"


def trip_file_url(year: int, month: int) -> str:
    return f"{TLC_BASE_URL}/trip-data/{trip_file_name(year, month)}"
