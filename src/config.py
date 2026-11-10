"""Proje genelinde kullanılan sabitler: yollar, veri kapsamı, kaynak URL'ler."""
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
CLEAN_DIR = DATA_DIR / "clean"
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


def trip_file_name(year: int, month: int) -> str:
    return f"yellow_tripdata_{year}-{month:02d}.parquet"


def trip_file_url(year: int, month: int) -> str:
    return f"{TLC_BASE_URL}/trip-data/{trip_file_name(year, month)}"
