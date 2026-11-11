"""T1.3 — Boyut setlerini üretir ve her birini 4 formatta yazar.

Örnekleme: her satırın 20 kolonunun xxhash64'ünden [0, 1) aralığında bir anahtar
üretilir; k boyutlu set = anahtarı k/N'den küçük satırlar. Böylece
  * sonuç makineden / çekirdek sayısından bağımsızdır (rand(seed) değildir),
  * setler iç içedir: 100K ⊂ 1M ⊂ 5M ⊂ 10M ⊂ full → okuma farkı sadece boyuttan gelir.
Satır sayıları hedefe yaklaşıktır (±%0,1); gerçek sayılar manifest'e yazılır.

Her boyut × format tek dosyadır (coalesce(1)); pandas tek çağrıyla okuyabilsin ve
bellek ölçümü birleştirme (concat) kopyasıyla bozulmasın. coalesce satır sırasını
korur → tüm codec'ler aynı sırayı görür, karşılaştırma adil olur.

Çalıştırma:  python -m src.build_sizes [--sizes 100k 1m ...] [--force]
"""
import argparse
import logging
import shutil
import sys
import time
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src import config
from src.spark_session import get_spark

logger = logging.getLogger(__name__)

SIZES: dict[str, int | None] = {
    "100k": 100_000,
    "1m": 1_000_000,
    "5m": 5_000_000,
    "10m": 10_000_000,
    "full": None,
}
# format adı → (Spark formatı, sıkıştırma codec'i)
FORMATS: dict[str, tuple[str, str]] = {
    "csv": ("csv", "none"),
    "parquet_snappy": ("parquet", "snappy"),
    "parquet_gzip": ("parquet", "gzip"),
    "parquet_zstd": ("parquet", "zstd"),
}
OUTPUT_STEM = "yellow"
HASH_BUCKETS = 10**9
PARQUET_BLOCK_BYTES = 128 * 1024 * 1024  # tüm codec'lerde aynı row group hedefi
CSV_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"
SAMPLE_KEY = "_sample_key"
MANIFEST_FILE = "size_sets_manifest.csv"


class BuildError(Exception):
    """Boyut seti üretimi veya doğrulaması başarısız olduğunda fırlatılır."""


def sample_threshold(target_rows: int | None, total_rows: int) -> float:
    if target_rows is None:
        return 1.0
    return min(1.0, target_rows / total_rows)


def target_path(processed_dir: Path, size: str, fmt: str) -> Path:
    ext = FORMATS[fmt][0]
    return processed_dir / f"size_{size}" / fmt / f"{OUTPUT_STEM}.{ext}"


def with_sample_key(df: DataFrame) -> DataFrame:
    """Satır içeriğinden türetilen, makineden bağımsız [0, 1) anahtarı ekler."""
    row_hash = F.xxhash64(*[F.col(c) for c in df.columns])
    return df.withColumn(SAMPLE_KEY, F.pmod(row_hash, F.lit(HASH_BUCKETS)) / HASH_BUCKETS)


def select_size(keyed: DataFrame, threshold: float) -> DataFrame:
    subset = keyed if threshold >= 1.0 else keyed.filter(F.col(SAMPLE_KEY) < threshold)
    return subset.drop(SAMPLE_KEY)


def finalize_single_file(spark_out: Path, dest: Path) -> None:
    """Spark'ın yazdığı tek part dosyasını sabit isme taşır, artıkları siler."""
    parts = [p for p in spark_out.glob("part-*") if not p.name.endswith(".crc")]
    if len(parts) != 1:
        raise BuildError(f"{spark_out}: 1 part dosyası bekleniyordu, {len(parts)} bulundu")
    parts[0].replace(dest)
    shutil.rmtree(spark_out)


def write_single_file(df: DataFrame, fmt: str, dest: Path) -> None:
    spark_format, codec = FORMATS[fmt]
    spark_out = dest.parent / "_spark_tmp"
    if spark_out.exists():
        shutil.rmtree(spark_out)
    dest.parent.mkdir(parents=True, exist_ok=True)

    writer = df.coalesce(1).write.mode("overwrite").option("compression", codec)
    if spark_format == "csv":
        writer = writer.option("header", True).option("timestampNTZFormat", CSV_TIMESTAMP_FORMAT)
    else:
        writer = writer.option("parquet.block.size", PARQUET_BLOCK_BYTES)
    writer.format(spark_format).save(str(spark_out.resolve()))
    finalize_single_file(spark_out, dest)


def count_rows(path: Path) -> int:
    if path.suffix == ".parquet":
        return pq.ParquetFile(path).metadata.num_rows
    with open(path, "rb") as fh:
        newlines = sum(chunk.count(b"\n") for chunk in iter(lambda: fh.read(1 << 24), b""))
    return newlines - 1  # başlık satırı


def file_info(path: Path, size: str, fmt: str) -> dict:
    info = {
        "size": size,
        "target_rows": SIZES[size],
        "format": fmt,
        "codec": FORMATS[fmt][1],
        "rows": count_rows(path),
        "bytes": path.stat().st_size,
        "mb": round(path.stat().st_size / 1024**2, 2),
        "row_groups": None,
    }
    if path.suffix == ".parquet":
        info["row_groups"] = pq.ParquetFile(path).metadata.num_row_groups
    return info


def build_manifest(processed_dir: Path) -> pd.DataFrame:
    records = [
        file_info(target_path(processed_dir, size, fmt), size, fmt)
        for size in SIZES
        for fmt in FORMATS
        if target_path(processed_dir, size, fmt).exists()
    ]
    # NaN içeren tamsayı kolonlar float'a dönmesin (100000.0 yerine 100000)
    return pd.DataFrame(records).astype({"target_rows": "Int64", "row_groups": "Int64"})


def check_consistency(manifest: pd.DataFrame) -> None:
    """Aynı boyuttaki tüm formatlar aynı sayıda satır içermeli."""
    per_size = manifest.groupby("size")["rows"].nunique()
    bad = per_size[per_size > 1].index.tolist()
    if bad:
        raise BuildError(f"Formatlar arasında satır sayısı farklı: {bad}")


def build(sizes: list[str], force: bool) -> pd.DataFrame:
    spark = get_spark("build-sizes")
    try:
        # as_uri() boşluğu %20 yapar ve Hadoop bunu çözmez → düz yol kullanılır
        raw_paths = [str(config.RAW_DIR / config.trip_file_name(config.YEAR, m)) for m in config.MONTHS]
        raw = spark.read.parquet(*raw_paths)
        total = raw.count()
        keyed = with_sample_key(raw)
        logger.info("Ham veri: %s kayıt", f"{total:,}")

        for size in sizes:
            subset = select_size(keyed, sample_threshold(SIZES[size], total))
            for fmt in FORMATS:
                dest = target_path(config.PROCESSED_DIR, size, fmt)
                if dest.exists() and not force:
                    logger.info("%-5s %-15s mevcut, atlandı", size, fmt)
                    continue
                started = time.perf_counter()
                write_single_file(subset, fmt, dest)
                logger.info(
                    "%-5s %-15s yazıldı  %8.1f MB  %6.1f sn",
                    size,
                    fmt,
                    dest.stat().st_size / 1024**2,
                    time.perf_counter() - started,
                )
    finally:
        spark.stop()

    manifest = build_manifest(config.PROCESSED_DIR)
    check_consistency(manifest)
    config.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    manifest.to_csv(config.RESULTS_DIR / MANIFEST_FILE, index=False)
    return manifest


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--sizes", nargs="+", choices=list(SIZES), default=list(SIZES))
    parser.add_argument("--force", action="store_true", help="var olan dosyaları yeniden yaz")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = parse_args(sys.argv[1:] if argv is None else argv)
    try:
        manifest = build(args.sizes, args.force)
    except BuildError as exc:
        logger.error("HATA: %s", exc)
        return 1
    logger.info("\n%s", manifest.to_string(index=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
