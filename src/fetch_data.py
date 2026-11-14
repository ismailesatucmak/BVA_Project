"""Takım için: hazır temiz veriyi (data/clean) GitHub Release'den indirir.

Temiz veri 479 MB ve tek dosyası GitHub'ın 100 MB sınırını aştığı için depoda değil,
Release eki olarak duruyor. Bu script zip'i indirir, SHA-256 ile doğrular, data/clean'e
açar ve satır sayısını kontrol eder. Kendi temizliğinizi üretmek isterseniz bunun yerine:
    python -m src.ingest  →  python -m src.clean

Çalıştırma:  python -m src.fetch_data
"""
import hashlib
import logging
import shutil
import sys
import zipfile
from pathlib import Path

import pyarrow.dataset as ds

from src import config
from src.ingest import IngestError, ensure_file

logger = logging.getLogger(__name__)
HASH_CHUNK = 1 << 20


class FetchError(Exception):
    """İndirilen temiz veri doğrulanamadığında fırlatılır."""


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(HASH_CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest()


def count_rows(clean_dir: Path) -> int:
    return ds.dataset(clean_dir, format="parquet", partitioning="hive").count_rows()


def install_clean(zip_path: Path, target: Path, expected_sha256: str, expected_rows: int) -> int:
    """Zip'i doğrulayıp target'a açar. Hata olursa target oluşturulmaz."""
    if target.exists():
        raise FetchError(f"{target} zaten var; yeniden kurmak için önce silin")
    actual = sha256_of(zip_path)
    if actual != expected_sha256:
        raise FetchError(f"SHA-256 uyuşmuyor (beklenen {expected_sha256[:12]}…, gelen {actual[:12]}…); dosya bozuk")
    staging = target.with_name(target.name + "_tmp")
    shutil.rmtree(staging, ignore_errors=True)
    try:
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(staging)
        rows = count_rows(staging)
        if rows != expected_rows:
            raise FetchError(f"{rows:,} satır bulundu, {expected_rows:,} bekleniyordu")
        staging.replace(target)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return rows


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    zip_path = config.DATA_DIR / "downloads" / config.CLEAN_ZIP_NAME
    try:
        if ensure_file(config.CLEAN_ZIP_URL, zip_path):
            logger.info("İndirildi: %s", zip_path)
        rows = install_clean(zip_path, config.CLEAN_DIR, config.CLEAN_ZIP_SHA256, config.CLEAN_ROWS)
    except (FetchError, IngestError) as exc:
        logger.error("HATA: %s", exc)
        return 1
    logger.info("Hazır: %s (%s satır). Zip silinebilir: %s", config.CLEAN_DIR, f"{rows:,}", zip_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
