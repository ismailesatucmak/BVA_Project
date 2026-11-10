"""T1.1 — Ham veriyi temin eder ve doğrular.

Eksik dosyaları TLC'nin resmi adresinden indirir, var olanları atlar ve her
dosyayı doğrular (açılabiliyor mu, şema beklenen gibi mi, kayıt sayısı).

Çalıştırma:  python -m src.ingest
"""
import logging
import sys
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import requests

from src import config

logger = logging.getLogger(__name__)

DOWNLOAD_TIMEOUT_SEC = 60
CHUNK_BYTES = 1024 * 1024


class IngestError(Exception):
    """İndirme veya doğrulama başarısız olduğunda fırlatılır."""


@dataclass(frozen=True)
class Source:
    file_name: str
    url: str
    is_trip_data: bool


def expected_sources() -> list[Source]:
    trips = [
        Source(config.trip_file_name(config.YEAR, m), config.trip_file_url(config.YEAR, m), True)
        for m in config.MONTHS
    ]
    zones = Source(config.ZONE_LOOKUP_FILE, config.ZONE_LOOKUP_URL, False)
    return [*trips, zones]


def _download(url: str, dest: Path) -> None:
    """Önce .part dosyasına yazar, bitince yeniden adlandırır (yarım dosya kalmaz)."""
    part = dest.with_name(dest.name + ".part")
    try:
        with requests.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT_SEC) as resp:
            resp.raise_for_status()
            with open(part, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=CHUNK_BYTES):
                    fh.write(chunk)
        part.replace(dest)
    except requests.RequestException as exc:
        part.unlink(missing_ok=True)
        raise IngestError(f"{dest.name} indirilemedi ({url}): {exc}") from exc


def ensure_file(url: str, dest: Path) -> bool:
    """Dosya yoksa indirir. İndirme yapıldıysa True, zaten varsa False döner."""
    if dest.exists():
        return False
    dest.parent.mkdir(parents=True, exist_ok=True)
    logger.info("İndiriliyor: %s", url)
    _download(url, dest)
    return True


def verify_trip_file(path: Path) -> int:
    """Parquet dosyasını açar, kolonları kontrol eder, kayıt sayısını döndürür."""
    try:
        pf = pq.ParquetFile(path)
    except Exception as exc:  # pyarrow bozuk dosyada farklı hata tipleri fırlatabilir
        raise IngestError(f"{path.name} okunamadı: {exc}") from exc

    actual = set(pf.schema_arrow.names)
    missing = [c for c in config.TRIP_COLUMNS if c not in actual]
    extra = sorted(actual - set(config.TRIP_COLUMNS))
    if missing:
        raise IngestError(f"{path.name} eksik kolon: {missing}")
    if extra:
        logger.warning("%s beklenmeyen kolon içeriyor: %s", path.name, extra)
    return pf.metadata.num_rows


def verify_zone_lookup(path: Path) -> int:
    """Zone lookup CSV'sinin kolonlarını ve satır sayısını kontrol eder."""
    try:
        zones = pd.read_csv(path)
    except Exception as exc:
        raise IngestError(f"{path.name} okunamadı: {exc}") from exc

    missing = [c for c in config.ZONE_LOOKUP_COLUMNS if c not in zones.columns]
    if missing:
        raise IngestError(f"{path.name} eksik kolon: {missing}")
    if len(zones) != config.ZONE_COUNT:
        raise IngestError(f"{path.name}: {config.ZONE_COUNT} bölge bekleniyordu, {len(zones)} bulundu")
    return len(zones)


def run(raw_dir: Path = config.RAW_DIR) -> int:
    """Tüm kaynakları temin edip doğrular. Toplam trip kayıt sayısını döndürür."""
    total_rows = 0
    for src in expected_sources():
        path = raw_dir / src.file_name
        downloaded = ensure_file(src.url, path)
        rows = verify_trip_file(path) if src.is_trip_data else verify_zone_lookup(path)
        if src.is_trip_data:
            total_rows += rows
        size_mb = path.stat().st_size / 1024**2
        status = "indirildi" if downloaded else "mevcut"
        logger.info("%-34s %9s  %12s kayıt  %7.2f MB", src.file_name, status, f"{rows:,}", size_mb)
    logger.info("Toplam trip kaydı: %s", f"{total_rows:,}")
    return total_rows


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        run()
    except IngestError as exc:
        logger.error("HATA: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
