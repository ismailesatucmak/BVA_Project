"""S5 — Sentetik ölçekleme: 19M ham boyut setinin k kopyası (ör. ×3 = 57M kayıt).

Proje kuralı: "Veri yeterince büyük değilse sentetik olarak ölçeklendirilmeli." 19M kayıt
gereksinimi zaten aşıyor; bu set yalnızca S5'in 19M'de **ekstrapolasyonla** verdiği
sonuçları (pandas'ın bellek eğimi, Spark'ın kesişim noktaları) ölçümle sınamak için üretilir.

Üretim kuralları:
  * Kaynak: Öğrenci 1'in ``size_full`` Parquet (snappy) dosyası; şema ve kolonlar aynen korunur.
  * Kopyalar **art arda bloklar** hâlinde yazılır (kopya 1'in tüm satırları, sonra kopya 2 ...).
    Satır satır çoğaltma (her satırın hemen yanında kopyası) Parquet'in RLE/dictionary
    kodlamasını yapay olarak iyileştirir ve okumayı gerçekçi olmayan biçimde hızlandırırdı.
  * CSV ve Parquet, S1 ile aynı yazıcıyla (``build_sizes.write_single_file``) yazılır.
  * Doğrulama: satır sayısı = k × kaynak. S5 benchmark'ında her işlemin checksum'ı
    (sayım/toplam) tam olarak k × 19M sonucudur; window_top3'te ilk 3 artık eşit tutarlı
    kopyalardan oluşur (sıralama eşitliği), toplam yine tanımlıdır.

Çıktı: ``<PROCESSED_DIR>/size_x3/{csv,parquet_snappy}/yellow.*`` +
``reports/results/s5_synthetic_manifest.csv``.

Çalıştırma:  python -m src.scale_synthetic [--factor 3] [--force]
"""
from __future__ import annotations

import argparse
import logging
import sys
import time

import pandas as pd

from src import config
from src.build_sizes import count_rows, target_path, write_single_file

logger = logging.getLogger(__name__)

SOURCE_SIZE = "full"
FORMATS = ("csv", "parquet_snappy")
DEFAULT_FACTOR = 3
MANIFEST_FILE = "s5_synthetic_manifest.csv"


def size_label(factor: int) -> str:
    return f"x{factor}"


def replicate(df, factor: int):
    """k kopyayı art arda (blok blok) birleştirir; satır sırası kopya içinde korunur."""
    if factor < 1:
        raise ValueError("factor >= 1 olmalı")
    out = df
    for _ in range(factor - 1):
        out = out.unionAll(df)
    return out


def build(factor: int = DEFAULT_FACTOR, force: bool = False) -> pd.DataFrame:
    from src.spark_session import get_spark

    label = size_label(factor)
    source = target_path(config.PROCESSED_DIR, SOURCE_SIZE, "parquet_snappy")
    if not source.exists():
        raise FileNotFoundError(f"Kaynak yok: {source} → önce python -m src.build_sizes")
    source_rows = count_rows(source)

    spark = get_spark("s5-synthetic")
    try:
        synthetic = replicate(spark.read.parquet(str(source)), factor)
        for fmt in FORMATS:
            dest = target_path(config.PROCESSED_DIR, label, fmt)
            if dest.exists() and not force:
                logger.info("%s %-15s mevcut, atlandı", label, fmt)
                continue
            started = time.perf_counter()
            write_single_file(synthetic, fmt, dest)
            logger.info("%s %-15s yazıldı %8.1f MB %6.1f sn", label, fmt,
                        dest.stat().st_size / 1024**2, time.perf_counter() - started)
    finally:
        spark.stop()

    records = []
    for fmt in FORMATS:
        path = target_path(config.PROCESSED_DIR, label, fmt)
        src_path = target_path(config.PROCESSED_DIR, SOURCE_SIZE, fmt)
        rows = count_rows(path)
        if rows != factor * source_rows:
            raise ValueError(f"{path}: {rows} satır, beklenen {factor * source_rows}")
        records.append({"size": label, "factor": factor, "format": fmt, "rows": rows,
                        "mb": round(path.stat().st_size / 1024**2, 2),
                        "source_mb": round(src_path.stat().st_size / 1024**2, 2),
                        "size_ratio": round(path.stat().st_size / src_path.stat().st_size, 3)})
    manifest = pd.DataFrame(records)
    config.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    manifest.to_csv(config.RESULTS_DIR / MANIFEST_FILE, index=False)
    return manifest


LAYOUT_SIZE = "full_rg16"
LAYOUT_BLOCK_BYTES = 16 * 1024 * 1024


def build_layout_copy(force: bool = False) -> dict:
    """19M Parquet'in küçük row group'lu (16 MB) kopyası: dosya düzeni kontrol deneyi.

    S1'in tek dosyalık Parquet'i 128 MB row group'la yazıldı → 19M = 3–4 row group →
    Spark dosyayı 3 bölümle okur (20 çekirdeğin 3'ü). Aynı satırlar, aynı sıra, aynı codec;
    yalnızca row group boyutu küçültülür. Fark tamamen dosya düzeninden gelir.
    """
    import pyarrow.parquet as pq

    from src.spark_session import get_spark

    source = target_path(config.PROCESSED_DIR, SOURCE_SIZE, "parquet_snappy")
    dest = target_path(config.PROCESSED_DIR, LAYOUT_SIZE, "parquet_snappy")
    if not dest.exists() or force:
        spark = get_spark("s5-layout")
        try:
            spark_out = dest.parent / "_spark_tmp"
            dest.parent.mkdir(parents=True, exist_ok=True)
            (spark.read.parquet(str(source)).coalesce(1).write.mode("overwrite")
             .option("compression", "snappy").option("parquet.block.size", LAYOUT_BLOCK_BYTES)
             .parquet(str(spark_out.resolve())))
            from src.build_sizes import finalize_single_file
            finalize_single_file(spark_out, dest)
        finally:
            spark.stop()
    info = {"size": LAYOUT_SIZE, "rows": count_rows(dest), "source_rows": count_rows(source),
            "row_groups": pq.ParquetFile(dest).metadata.num_row_groups,
            "source_row_groups": pq.ParquetFile(source).metadata.num_row_groups,
            "mb": round(dest.stat().st_size / 1024**2, 2)}
    if info["rows"] != info["source_rows"]:
        raise ValueError(f"{dest}: satır sayısı farklı {info}")
    return info


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [S5] %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--factor", type=int, default=DEFAULT_FACTOR)
    parser.add_argument("--layout", action="store_true",
                        help="yalnızca 19M Parquet'in 16 MB row group'lu kopyasını üret")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    if args.layout:
        logger.info("%s", build_layout_copy(args.force))
        return 0
    logger.info("\n%s", build(args.factor, args.force).to_string(index=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
