"""Proje genelinde kullanılan sabitler: yollar, veri kapsamı, kaynak URL'ler."""
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
CLEAN_DIR = DATA_DIR / "clean"

# Büyük çıktılar (boyut setleri 5,2 GB, Spark geçici dosyaları) için kök klasör.
# Öncelik: NYC_DATA_ROOT ortam değişkeni → D:\BVAVeri (varsa; ölçümler burada alındı)
# → proje içindeki data/large (Git'e girmez). Böylece her makinede çalışır.
_ORIGINAL_DATA_ROOT = Path(r"D:\BVAVeri")


def _external_data_root() -> Path:
    if os.environ.get("NYC_DATA_ROOT"):
        return Path(os.environ["NYC_DATA_ROOT"])
    if _ORIGINAL_DATA_ROOT.exists():
        return _ORIGINAL_DATA_ROOT
    return DATA_DIR / "large"


EXTERNAL_DATA_ROOT = _external_data_root()
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

# ---- Anomali analizi (T2.3)
IQR_K = 1.5  # global IQR: [Q1 − k·IQR, Q3 + k·IQR] dışı (Tukey)
EXTREME_PERCENTILE = 0.999  # global üst eşik
CONTEXT_IQR_K = 3.0  # güzergâh içi IQR: daha sıkı → yalnızca belirgin sapmalar
MIN_ROUTE_TRIPS = 30  # bağlam için bir güzergâhta gereken en az yolculuk
# Sabit tarifeli güzergâhlarda (örn. JFK↔Manhattan) IQR≈0 olur ve küçük sapmalar bile
# anomali sayılır → IQR en az medyanın bu oranı kadar kabul edilir.
CONTEXT_MIN_IQR_FRACTION = 0.2
METERED_MIN_FARE = 3.0  # NYC taksimetre açılış ücreti; altı "yer tutucu" ücret
SHORT_TRIP_MILES = 1.0
SHORT_TRIP_HIGH_FARE = 100.0  # kısa mesafe + bu ücretin üstü = ücret–mesafe tutarsızlığı
ZONE_LOOKUP_PATH = RAW_DIR / ZONE_LOOKUP_FILE


# ---- Takım için hazır temiz veri (GitHub Release eki; dosyalar 100 MB sınırı nedeniyle depoda değil)
GITHUB_REPO = "ismailesatucmak/BVA_Project"
DATA_RELEASE_TAG = "data-v1"
CLEAN_ZIP_NAME = "nyc_taxi_clean_2026-01_05.zip"
CLEAN_ZIP_URL = f"https://github.com/{GITHUB_REPO}/releases/download/{DATA_RELEASE_TAG}/{CLEAN_ZIP_NAME}"
CLEAN_ZIP_SHA256 = "45ffa1372448c9f663eeae0752fa1814c78fdc6dd28d63d7760ec9b0c10d8652"
CLEAN_ROWS = 17_210_961


def trip_file_name(year: int, month: int) -> str:
    return f"yellow_tripdata_{year}-{month:02d}.parquet"


def trip_file_url(year: int, month: int) -> str:
    return f"{TLC_BASE_URL}/trip-data/{trip_file_name(year, month)}"
