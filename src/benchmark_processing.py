"""S5 — İşleme performansı: pandas vs PySpark × CSV vs Parquet × 5 veri boyutu.

Öğrenci 1 (``benchmark_io``) yalnızca OKUMA'yı ölçtü. Bu modül aynı boyut setleri
(100K, 1M, 5M, 10M, 19M; ``build_sizes``) üzerinde **işleme** süresini ve belleğini ölçer:

  * ``groupby_hour``  — aggregation: biniş saatine göre yolculuk sayısı, ort. mesafe,
                        ort. ücret, toplam gelir (24 grup)
  * ``join_borough``  — join: yolculuklar ⋈ TLC bölge tablosu (265 satır), ilçeye göre özet
  * ``window_top3``   — window: her biniş bölgesinde en yüksek tutarlı 3 yolculuk
                        (``row_number() over (partition by PULocationID order by total_amount desc)``)

Ölçüm kuralları (S1 ile aynı ilkeler):
  * Her (motor × boyut × format) ayrı bir alt süreçte; pandas ve Spark aynı anda çalışmaz.
  * Her işlem 1 ısınma + 3 tekrar; ortalama ± std. Sıcak işletim sistemi önbelleği koşulu.
  * İki motor da aynı 6 kolonu okur ve aynı tipli tabloyu görür (CSV'de tarih parse
    edilir; Spark CSV'ye Parquet şeması verilir).
  * pandas: veri bir kez belleğe yüklenir (``load``; S1'in okuma ölçümüyle aynı iş, burada
    yalnızca uçtan uca süreyi aynı makinede hesaplamak için ölçülür), işlemler bellekteki
    tabloda çalışır.
    Spark: tembel değerlendirme → her işlem dosyadan okuyarak çalışır; sonuç küçük olduğu
    için ``collect()`` ile tetiklenir (``count()`` kısayolu yok). Oturum açılışı (``startup``)
    ayrı ölçülür, işlem süresine dahil değildir.
  * Bellek: süreç ağacının (Python + JVM) tepe RSS'i, ``src.mem_probe.PeakRSS``.
  * Doğruluk: her işlemin sonucu satır sayısı + sağlama toplamıyla (checksum) kaydedilir;
    iki motor ve iki format aynı sonucu vermek zorundadır (``check_consistency``).

Çalıştırma:  python -m src.benchmark_processing [--sizes 100k 1m] [--engines pandas spark]
             [--formats csv parquet_snappy] [--force]
"""
from __future__ import annotations

import argparse
import gc
import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
import psutil

from src import config
from src.build_sizes import SIZES, target_path

logger = logging.getLogger(__name__)

ENGINES = ("pandas", "spark")
FORMATS = ("csv", "parquet_snappy")
OPERATIONS = ("groupby_hour", "join_borough", "window_top3")
SETUP_STEP = {"pandas": "load", "spark": "startup"}  # uçtan uca süreye eklenen hazırlık adımı
COLUMNS = ("tpep_pickup_datetime", "PULocationID", "payment_type",
           "trip_distance", "fare_amount", "total_amount")
TOP_N = 3
REPEATS = 3
WORKER_TIMEOUT_SEC = 3600
RESULT_PREFIX = "S5_RESULT "
RAW_FILE = "s5_processing_raw.csv"
SUMMARY_FILE = "s5_processing.csv"
PANDAS_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
SPARK_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"
KEY = ["engine", "size", "format", "op"]
CHECKSUM_RTOL = 1e-6


class ConsistencyError(Exception):
    """Motorlar/formatlar aynı işlem için farklı sonuç verdiğinde fırlatılır."""


# ------------------------------------------------------------------ sonuç özetleri
def checksum(result: pd.DataFrame, value_col: str) -> tuple[int, float]:
    """(satır sayısı, value_col toplamı) — motorlar arası karşılaştırma için."""
    return len(result), float(result[value_col].sum())


# -------------------------------------------------------------------- pandas
def pandas_load(path: Path) -> pd.DataFrame:
    columns = list(COLUMNS)
    if path.suffix == ".parquet":
        return pd.read_parquet(path, columns=columns)
    return pd.read_csv(path, usecols=columns, parse_dates=["tpep_pickup_datetime"],
                       date_format=PANDAS_DATE_FORMAT)


def pandas_zones(path: Path = config.ZONE_LOOKUP_PATH) -> pd.DataFrame:
    # "N/A" ilçesi gerçek bir etikettir (265 = Outside of NYC); NaN'a çevrilmesin
    return pd.read_csv(path, keep_default_na=False)


def pandas_groupby_hour(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby(df["tpep_pickup_datetime"].dt.hour.rename("hour"))
        .agg(trips=("PULocationID", "size"), avg_distance=("trip_distance", "mean"),
             avg_fare=("fare_amount", "mean"), revenue=("total_amount", "sum"))
        .reset_index()
    )


def pandas_join_borough(df: pd.DataFrame, zones: pd.DataFrame) -> pd.DataFrame:
    joined = df[["PULocationID", "total_amount"]].merge(
        zones[["LocationID", "Borough"]], left_on="PULocationID", right_on="LocationID", how="inner"
    )
    return (
        joined.groupby("Borough", dropna=False)
        .agg(trips=("total_amount", "size"), avg_total=("total_amount", "mean"))
        .reset_index()
    )


def pandas_window_top3(df: pd.DataFrame) -> pd.DataFrame:
    ranked = df[["PULocationID", "total_amount"]].sort_values(
        ["PULocationID", "total_amount"], ascending=[True, False], kind="stable"
    )
    return ranked.groupby("PULocationID", sort=False).head(TOP_N)


def pandas_ops(zones: pd.DataFrame):
    """işlem adı → (fonksiyon(df) → sonuç, checksum kolonu)."""
    return {
        "groupby_hour": (pandas_groupby_hour, "trips"),
        "join_borough": (lambda df: pandas_join_borough(df, zones), "trips"),
        "window_top3": (pandas_window_top3, "total_amount"),
    }


# --------------------------------------------------------------------- spark
def spark_frame(spark, path: Path, fmt: str, schema):
    if fmt == "csv":
        df = (spark.read.option("header", True)
              .option("timestampNTZFormat", SPARK_TIMESTAMP_FORMAT)
              .schema(schema).csv(str(path)))
    else:
        df = spark.read.parquet(str(path))
    return df.select(*COLUMNS)


def spark_zones(spark, path: Path = config.ZONE_LOOKUP_PATH):
    return spark.read.option("header", True).option("inferSchema", True).csv(str(path))


def spark_groupby_hour(df):
    from pyspark.sql import functions as F

    return df.groupBy(F.hour("tpep_pickup_datetime").alias("hour")).agg(
        F.count(F.lit(1)).alias("trips"), F.avg("trip_distance").alias("avg_distance"),
        F.avg("fare_amount").alias("avg_fare"), F.sum("total_amount").alias("revenue"),
    )


def spark_join_borough(df, zones):
    from pyspark.sql import functions as F

    joined = df.select("PULocationID", "total_amount").join(
        zones.select("LocationID", "Borough"), df["PULocationID"] == zones["LocationID"], "inner"
    )
    return joined.groupBy("Borough").agg(
        F.count(F.lit(1)).alias("trips"), F.avg("total_amount").alias("avg_total")
    )


def spark_window_top3(df):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    w = Window.partitionBy("PULocationID").orderBy(F.desc("total_amount"))
    return (df.select("PULocationID", "total_amount")
            .withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") <= TOP_N))


def spark_ops(zones):
    return {
        "groupby_hour": (spark_groupby_hour, "trips"),
        "join_borough": (lambda df: spark_join_borough(df, zones), "trips"),
        "window_top3": (spark_window_top3, "total_amount"),
    }


# -------------------------------------------------------------------- worker
def _timed(fn, repeats: int = REPEATS):
    """1 ısınma + repeats ölçüm. Her ölçüm: (saniye, tepe RSS, başlangıç RSS, sonuç)."""
    from src.mem_probe import PeakRSS

    runs = []
    for rep in range(repeats + 1):
        gc.collect()
        with PeakRSS() as probe:
            started = time.perf_counter()
            result = fn()
            seconds = time.perf_counter() - started
        if rep:  # rep 0 = ısınma, atılır
            runs.append((rep, seconds, probe.peak_total_mb, probe.start_total_mb, result))
        else:
            del result
    return runs


def _record(op, rep, seconds, peak, start, rows=None, result_rows=None, check=None):
    return {"op": op, "rep": rep, "seconds": seconds, "peak_rss_mb": peak,
            "rss_before_mb": start, "rows": rows, "result_rows": result_rows, "checksum": check}


def run_pandas_worker(size: str, fmt: str) -> list[dict]:
    path = target_path(config.PROCESSED_DIR, size, fmt)
    zones = pandas_zones()
    records = []

    holder: dict[str, pd.DataFrame] = {}

    def load():
        holder.pop("df", None)  # eski kopya bellekte kalmasın
        gc.collect()
        holder["df"] = pandas_load(path)
        return None

    for rep, sec, peak, start, _ in _timed(load):
        records.append(_record("load", rep, sec, peak, start, rows=len(holder["df"])))

    df = holder["df"]
    for op, (fn, value_col) in pandas_ops(zones).items():
        for rep, sec, peak, start, result in _timed(lambda: fn(df)):
            n, s = checksum(result, value_col)
            records.append(_record(op, rep, sec, peak, start, rows=len(df), result_rows=n, check=s))
    return records


def run_spark_worker(size: str, fmt: str) -> list[dict]:
    from src.mem_probe import PeakRSS
    from src.spark_session import get_spark

    with PeakRSS() as probe:
        started = time.perf_counter()
        spark = get_spark(f"s5-{size}-{fmt}")
        spark.range(1).count()
        startup = time.perf_counter() - started
    records = [_record("startup", 1, startup, probe.peak_total_mb, probe.start_total_mb)]
    try:
        schema = spark.read.parquet(str(target_path(config.PROCESSED_DIR, size, "parquet_snappy"))).schema
        path = target_path(config.PROCESSED_DIR, size, fmt)
        zones = spark_zones(spark).cache()
        zones.count()
        rows = spark_frame(spark, path, fmt, schema).count()  # doğrulama, süreye dahil değil
        for op, (fn, value_col) in spark_ops(zones).items():
            def run():
                return fn(spark_frame(spark, path, fmt, schema)).toPandas()

            for rep, sec, peak, start, result in _timed(run):
                n, s = checksum(result, value_col)
                records.append(_record(op, rep, sec, peak, start, rows=rows, result_rows=n, check=s))
    finally:
        spark.stop()
    return records


def worker_main(args: argparse.Namespace) -> int:
    fn = run_pandas_worker if args.worker == "pandas" else run_spark_worker
    for rec in fn(args.size, args.format):
        print(RESULT_PREFIX + json.dumps(rec), flush=True)
    return 0


# -------------------------------------------------------------- orchestrator
def parse_worker_output(stdout: str) -> list[dict]:
    return [json.loads(line[len(RESULT_PREFIX):]) for line in stdout.splitlines()
            if line.startswith(RESULT_PREFIX)]


def _run_worker(engine: str, size: str, fmt: str) -> tuple[list[dict], str | None]:
    cmd = [sys.executable, "-m", "src.benchmark_processing", "--worker", engine,
           "--size", size, "--format", fmt]
    env = {**os.environ, "PYTHONIOENCODING": "utf-8"}
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8",
                              errors="replace", timeout=WORKER_TIMEOUT_SEC,
                              cwd=config.PROJECT_ROOT, env=env)
    except subprocess.TimeoutExpired:
        return [], "timeout"
    results = parse_worker_output(proc.stdout)
    if proc.returncode != 0 or not results:
        tail = (proc.stderr.strip().splitlines() or ["(stderr boş)"])[-1]
        return results, f"error: {tail[:300]}"
    return results, None


def summarize(raw: pd.DataFrame) -> pd.DataFrame:
    ok = raw[raw["status"] == "ok"]
    summary = ok.groupby(KEY, sort=False).agg(
        rows=("rows", "first"),
        seconds_mean=("seconds", "mean"),
        seconds_std=("seconds", "std"),
        peak_rss_mb=("peak_rss_mb", "max"),
        rss_before_mb=("rss_before_mb", "min"),
        result_rows=("result_rows", "first"),
        checksum=("checksum", "first"),
        n_ok=("rep", "count"),
    ).reset_index()
    summary["throughput_rows_s"] = summary["rows"] / summary["seconds_mean"]
    failed = raw[raw["status"] != "ok"].groupby(["engine", "size", "format"]).size()
    summary["n_failed"] = [int(failed.get((r.engine, r.size, r.format), 0))
                           for r in summary.itertuples()]
    return summary


def check_consistency(summary: pd.DataFrame) -> list[str]:
    """Aynı (boyut, işlem) için tüm motor × format sonuçları aynı olmalı."""
    problems = []
    ops = summary[summary["op"].isin(OPERATIONS)]
    for (size, op), grp in ops.groupby(["size", "op"]):
        if grp["result_rows"].nunique() > 1:
            problems.append(f"{size}/{op}: sonuç satır sayısı farklı {sorted(grp['result_rows'].unique())}")
        ref = grp["checksum"].iloc[0]
        tol = CHECKSUM_RTOL * max(1.0, abs(ref))
        if ((grp["checksum"] - ref).abs() > tol).any():
            problems.append(f"{size}/{op}: checksum farklı {grp['checksum'].tolist()}")
    return problems


def end_to_end(summary: pd.DataFrame) -> pd.DataFrame:
    """Motor × boyut × format için hazırlık (load / startup) + 3 işlemin toplam süresi."""
    rows = []
    for (engine, size, fmt), grp in summary.groupby(["engine", "size", "format"], sort=False):
        by_op = grp.set_index("op")["seconds_mean"]
        if not set(OPERATIONS) <= set(by_op.index):
            continue
        setup = float(by_op.get(SETUP_STEP[engine], 0.0))
        ops = float(by_op[list(OPERATIONS)].sum())
        rows.append({"engine": engine, "size": size, "format": fmt,
                     "rows": int(grp["rows"].dropna().iloc[0]),
                     "setup_s": setup, "ops_s": ops, "total_s": setup + ops,
                     "peak_rss_mb": float(grp["peak_rss_mb"].max())})
    return pd.DataFrame(rows)


def _load_raw(path: Path) -> pd.DataFrame:
    return pd.read_csv(path) if path.exists() else pd.DataFrame(columns=["engine", "size", "format", "op", "rep", "status"])


def _save(raw: pd.DataFrame) -> pd.DataFrame:
    config.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    raw.to_csv(config.RESULTS_DIR / RAW_FILE, index=False)
    summary = summarize(raw)
    total_ram_mb = psutil.virtual_memory().total / 1024**2
    summary["peak_pct_ram"] = (100 * summary["peak_rss_mb"] / total_ram_mb).round(1)
    summary.round(4).to_csv(config.RESULTS_DIR / SUMMARY_FILE, index=False)
    return summary


def _done(raw: pd.DataFrame, engine: str, size: str, fmt: str) -> bool:
    if raw.empty:
        return False
    sel = raw[(raw["engine"] == engine) & (raw["size"] == size) & (raw["format"] == fmt)
              & (raw["status"] == "ok")]
    return set(OPERATIONS) <= set(sel["op"])


def orchestrate(sizes, engines, formats, force: bool = False) -> pd.DataFrame:
    raw_path = config.RESULTS_DIR / RAW_FILE
    raw = _load_raw(raw_path)
    summary = summarize(raw) if not raw.empty else pd.DataFrame()
    for size in sizes:
        for fmt in formats:
            for engine in engines:
                if not force and _done(raw, engine, size, fmt):
                    logger.info("%-6s %-5s %-15s tamam, atlandı", engine, size, fmt)
                    continue
                stale = (raw["engine"] == engine) & (raw["size"] == size) & (raw["format"] == fmt)
                raw = raw[~stale]
                logger.info("%-6s %-5s %-15s ölçülüyor...", engine, size, fmt)
                results, error = _run_worker(engine, size, fmt)
                new = pd.DataFrame([{"engine": engine, "size": size, "format": fmt,
                                     "status": "ok", **r} for r in results])
                if error:
                    logger.warning("  BAŞARISIZ: %s", error)
                    new = pd.concat([new, pd.DataFrame([{"engine": engine, "size": size,
                                     "format": fmt, "op": "worker", "rep": 0, "status": error}])],
                                    ignore_index=True)
                raw = pd.concat([raw, new], ignore_index=True)
                summary = _save(raw)  # her kombinasyondan sonra kaydet (kesintiye dayanıklı)
                for r in summary[(summary["engine"] == engine) & (summary["size"] == size)
                                 & (summary["format"] == fmt)].itertuples():
                    logger.info("  %-13s %8.3f sn  tepe %7.0f MB", r.op, r.seconds_mean, r.peak_rss_mb)
    return summary


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--sizes", nargs="+", choices=list(SIZES), default=list(SIZES))
    parser.add_argument("--engines", nargs="+", choices=ENGINES, default=list(ENGINES))
    parser.add_argument("--formats", nargs="+", choices=FORMATS, default=list(FORMATS))
    parser.add_argument("--force", action="store_true", help="tamamlanmış kombinasyonları da yeniden ölç")
    parser.add_argument("--worker", choices=ENGINES, help=argparse.SUPPRESS)
    parser.add_argument("--size", choices=list(SIZES), help=argparse.SUPPRESS)
    parser.add_argument("--format", choices=FORMATS, help=argparse.SUPPRESS)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    if args.worker:
        return worker_main(args)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [S5] %(message)s")
    summary = orchestrate(args.sizes, args.engines, args.formats, force=args.force)
    problems = check_consistency(summary)
    if problems:
        for p in problems:
            logger.error("TUTARSIZLIK: %s", p)
        return 1
    logger.info("Tüm motor × format sonuçları tutarlı.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
