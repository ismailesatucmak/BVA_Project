"""T1.4 — Boyut × format için okuma süresi ve bellek ölçümü (pandas vs Spark).

Ölçüm kuralları (adil karşılaştırma için):
  * Her okuma aynı tipli tabloyu üretir: pandas CSV'de tarih kolonları parse edilir;
    Spark CSV'ye Parquet'in şeması verilir (ayrıca bir kez inferSchema maliyeti ölçülür).
  * Spark okuması `noop` yazımıyla tetiklenir — `count()` Parquet'te veriyi okumaz,
    metadata'dan cevap verir.
  * pandas her ölçümde yeni bir süreçte çalışır → tepe bellek (peak working set) temizdir.
    Spark her boyut × format için ayrı süreçte, pandas ile aynı anda çalışmaz.
  * Her kombinasyonda 1 ısınma okuması atılır → ölçümler "sıcak işletim sistemi önbelleği"
    koşulundadır.
  * Her okumanın satır sayısı manifest ile karşılaştırılır.
Kapsam yalnızca OKUMA'dır; groupby/join gibi işleme süreleri Öğrenci 5'in alanıdır.

Çalıştırma:  python -m src.benchmark_io [--sizes 100k 1m] [--engines pandas spark]
"""
import argparse
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
from src.build_sizes import FORMATS, MANIFEST_FILE, SIZES, target_path

logger = logging.getLogger(__name__)

SELECTED_COLUMNS = ("tpep_pickup_datetime", "trip_distance", "fare_amount")
TIMESTAMP_COLUMNS = ("tpep_pickup_datetime", "tpep_dropoff_datetime")
PANDAS_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
SPARK_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"
MODES = ("all", "3cols")
SPARK_INFER_MODE = "all_infer"  # yalnızca Spark + CSV: şema çıkarımının maliyeti
ENGINES = ("pandas", "spark")
REPEATS = 3
WORKER_TIMEOUT_SEC = 900
RESULT_PREFIX = "BENCH_RESULT "
RAW_FILE = "io_benchmark_raw.csv"
SUMMARY_FILE = "io_benchmark.csv"
MB = 1024**2
KEY = ["engine", "size", "format", "mode"]


class BenchmarkError(Exception):
    """Ölçüm geçersiz olduğunda (örn. satır sayısı uyuşmazlığı) fırlatılır."""


# ---------------------------------------------------------------- pandas worker
def read_pandas(path: Path, mode: str) -> pd.DataFrame:
    columns = list(SELECTED_COLUMNS) if mode == "3cols" else None
    if path.suffix == ".parquet":
        return pd.read_parquet(path, columns=columns)
    dates = [c for c in TIMESTAMP_COLUMNS if columns is None or c in columns]
    return pd.read_csv(path, usecols=columns, parse_dates=dates, date_format=PANDAS_DATE_FORMAT)


def _working_set_mb(peak: bool) -> float:
    info = psutil.Process().memory_info()
    value = getattr(info, "peak_wset", info.rss) if peak else getattr(info, "wset", info.rss)
    return value / MB


def measure_pandas(path: Path, mode: str) -> dict:
    baseline_mb = _working_set_mb(peak=False)
    started = time.perf_counter()
    df = read_pandas(path, mode)
    seconds = time.perf_counter() - started
    peak_mb = _working_set_mb(peak=True) - baseline_mb
    frame_mb = df.memory_usage(deep=True).sum() / MB  # süre ölçümünün dışında
    return {"seconds": seconds, "rows": len(df), "frame_mb": frame_mb, "peak_mb": peak_mb}


# ----------------------------------------------------------------- spark worker
def _spark_frame(spark, path: Path, fmt: str, mode: str, schema):
    if fmt != "csv":
        df = spark.read.parquet(str(path))
    else:
        reader = spark.read.option("header", True).option("timestampNTZFormat", SPARK_TIMESTAMP_FORMAT)
        reader = reader.option("inferSchema", True) if mode == SPARK_INFER_MODE else reader.schema(schema)
        df = reader.csv(str(path))
    return df.select(*SELECTED_COLUMNS) if mode == "3cols" else df


def _spark_modes(fmt: str) -> tuple[str, ...]:
    return (*MODES, SPARK_INFER_MODE) if fmt == "csv" else MODES


def run_spark_worker(size: str, fmt: str) -> list[dict]:
    from src.spark_session import get_spark

    spark = get_spark(f"bench-{size}-{fmt}")
    results = []
    try:
        schema = spark.read.parquet(str(target_path(config.PROCESSED_DIR, size, "parquet_snappy"))).schema
        path = target_path(config.PROCESSED_DIR, size, fmt)
        for mode in _spark_modes(fmt):
            rows = _spark_frame(spark, path, fmt, mode, schema).count()  # doğrulama, süreye dahil değil
            for rep in range(REPEATS + 1):  # rep 0 = ısınma, atılır
                # okuyucu kurulumu süreye dahil: inferSchema veriyi .csv() çağrısında tarar
                started = time.perf_counter()
                df = _spark_frame(spark, path, fmt, mode, schema)
                df.write.format("noop").mode("overwrite").save()
                seconds = time.perf_counter() - started
                if rep:
                    partitions = df.rdd.getNumPartitions()
                    results.append({"mode": mode, "rep": rep, "seconds": seconds, "rows": rows, "partitions": partitions})
    finally:
        spark.stop()
    return results


def worker_main(args: argparse.Namespace) -> int:
    if args.worker == "pandas":
        path = target_path(config.PROCESSED_DIR, args.size, args.format)
        results = [measure_pandas(path, args.mode)]
    else:
        results = run_spark_worker(args.size, args.format)
    for result in results:
        print(RESULT_PREFIX + json.dumps(result), flush=True)
    return 0


# ------------------------------------------------------------------ orchestrator
def parse_worker_output(stdout: str) -> list[dict]:
    return [json.loads(line[len(RESULT_PREFIX):]) for line in stdout.splitlines() if line.startswith(RESULT_PREFIX)]


def _run_worker(worker_args: list[str]) -> tuple[list[dict], str | None]:
    """Worker'ı ayrı süreçte çalıştırır. (sonuçlar, hata mesajı) döndürür."""
    cmd = [sys.executable, "-m", "src.benchmark_io", "--worker", *worker_args]
    env = {**os.environ, "PYTHONIOENCODING": "utf-8"}
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, encoding="utf-8", errors="replace",
            timeout=WORKER_TIMEOUT_SEC, cwd=config.PROJECT_ROOT, env=env,
        )
    except subprocess.TimeoutExpired:
        return [], "timeout"
    results = parse_worker_output(proc.stdout)
    if proc.returncode != 0 or not results:
        tail = (proc.stderr.strip().splitlines() or ["(stderr boş)"])[-1]
        return [], f"error: {tail[:300]}"
    return results, None


def _failure(engine: str, size: str, fmt: str, mode: str, rep: int, status: str) -> dict:
    return {"engine": engine, "size": size, "format": fmt, "mode": mode, "rep": rep, "status": status}


def bench_pandas(size: str, fmt: str, mode: str) -> list[dict]:
    args = ["pandas", "--size", size, "--format", fmt, "--mode", mode]
    _run_worker(args)  # ısınma: dosyayı işletim sistemi önbelleğine alır, sonuç atılır
    records = []
    for rep in range(1, REPEATS + 1):
        results, error = _run_worker(args)
        if error:
            records.append(_failure("pandas", size, fmt, mode, rep, error))
            if error == "timeout":
                break  # bir sonraki tekrar da aynı sonucu verir, zaman kaybı
            continue
        records.append({"engine": "pandas", "size": size, "format": fmt, "mode": mode, "rep": rep, "status": "ok", **results[0]})
    return records


def bench_spark(size: str, fmt: str) -> list[dict]:
    results, error = _run_worker(["spark", "--size", size, "--format", fmt])
    if error:
        return [_failure("spark", size, fmt, mode, 1, error) for mode in _spark_modes(fmt)]
    return [{"engine": "spark", "size": size, "format": fmt, "status": "ok", **r} for r in results]


def completed_combos(raw: pd.DataFrame) -> set[tuple]:
    if raw.empty:
        return set()
    ok = raw[raw["status"] == "ok"].groupby(KEY)["rep"].nunique()
    return set(ok[ok >= REPEATS].index)


def check_rows(raw: pd.DataFrame, expected: dict[tuple[str, str], int]) -> None:
    ok = raw[raw["status"] == "ok"]
    bad = ok[[r.rows != expected[(r.size, r.format)] for r in ok.itertuples()]]
    if not bad.empty:
        combos = sorted(set(zip(bad["size"], bad["format"], bad["engine"])))
        raise BenchmarkError(f"Satır sayısı manifest ile uyuşmuyor: {combos}")


METRIC_COLUMNS = ("seconds", "rows", "frame_mb", "peak_mb", "partitions")


def summarize(raw: pd.DataFrame) -> pd.DataFrame:
    # pandas sonuçlarında partitions, Spark sonuçlarında bellek kolonları yoktur
    raw = raw.reindex(columns=[*raw.columns, *[c for c in METRIC_COLUMNS if c not in raw.columns]])
    ok = raw[raw["status"] == "ok"]
    summary = ok.groupby(KEY, sort=False).agg(
        seconds_mean=("seconds", "mean"),
        seconds_std=("seconds", "std"),
        rows=("rows", "first"),
        frame_mb=("frame_mb", "mean"),
        peak_mb=("peak_mb", "max"),
        partitions=("partitions", "first"),
        n_ok=("rep", "count"),
    )
    failed = raw[raw["status"] != "ok"].groupby(KEY).size().rename("n_failed")
    summary = summary.join(failed, how="outer").reset_index()
    summary["n_failed"] = summary["n_failed"].fillna(0).astype(int)
    summary["n_ok"] = summary["n_ok"].fillna(0).astype(int)
    return summary


def _load_raw(path: Path) -> pd.DataFrame:
    return pd.read_csv(path) if path.exists() else pd.DataFrame(columns=[*KEY, "rep", "status"])


def _save(raw: pd.DataFrame, manifest: pd.DataFrame) -> None:
    raw.to_csv(config.RESULTS_DIR / RAW_FILE, index=False)
    disk = manifest[["size", "format", "mb"]].rename(columns={"mb": "disk_mb"})
    total_ram_mb = psutil.virtual_memory().total / MB
    summary = summarize(raw).merge(disk, on=["size", "format"], how="left")
    summary["peak_pct_ram"] = (100 * summary["peak_mb"] / total_ram_mb).round(1)
    summary.round(3).to_csv(config.RESULTS_DIR / SUMMARY_FILE, index=False)


def orchestrate(sizes: list[str], engines: list[str]) -> pd.DataFrame:
    manifest = pd.read_csv(config.RESULTS_DIR / MANIFEST_FILE)
    expected = {(r.size, r.format): r.rows for r in manifest.itertuples()}
    raw_path = config.RESULTS_DIR / RAW_FILE
    raw = _load_raw(raw_path)

    for size in sizes:
        for fmt in FORMATS:
            for engine in engines:
                modes = _spark_modes(fmt) if engine == "spark" else MODES
                done = completed_combos(raw)
                if all((engine, size, fmt, m) in done for m in modes):
                    logger.info("%-6s %-5s %-15s tamam, atlandı", engine, size, fmt)
                    continue
                # tamamlanmamış eski kayıtları temizle, kombinasyonu baştan ölç
                stale = (raw["engine"] == engine) & (raw["size"] == size) & (raw["format"] == fmt)
                raw = raw[~stale]
                if engine == "spark":
                    new = bench_spark(size, fmt)
                else:
                    new = [rec for mode in modes for rec in bench_pandas(size, fmt, mode)]
                raw = pd.concat([raw, pd.DataFrame(new)], ignore_index=True)
                _save(raw, manifest)  # her kombinasyondan sonra kaydet (kesintiye dayanıklı)
                _log_combo(pd.DataFrame(new))

    check_rows(raw, expected)
    return summarize(raw)


def _log_combo(new: pd.DataFrame) -> None:
    for (engine, size, fmt, mode), grp in new.groupby(KEY, sort=False):
        ok = grp[grp["status"] == "ok"]
        if ok.empty:
            logger.info("%-6s %-5s %-15s %-9s BAŞARISIZ: %s", engine, size, fmt, mode, grp["status"].iloc[0])
            continue
        extra = f"peak {ok['peak_mb'].max():8.0f} MB" if "peak_mb" in ok and ok["peak_mb"].notna().any() else f"{int(ok['partitions'].iloc[0])} partition"
        logger.info("%-6s %-5s %-15s %-9s %7.2f sn  %s", engine, size, fmt, mode, ok["seconds"].mean(), extra)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--sizes", nargs="+", choices=list(SIZES), default=list(SIZES))
    parser.add_argument("--engines", nargs="+", choices=ENGINES, default=list(ENGINES))
    parser.add_argument("--worker", choices=ENGINES, help=argparse.SUPPRESS)
    parser.add_argument("--size", choices=list(SIZES), help=argparse.SUPPRESS)
    parser.add_argument("--format", choices=list(FORMATS), help=argparse.SUPPRESS)
    parser.add_argument("--mode", choices=MODES, help=argparse.SUPPRESS)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    if args.worker:
        return worker_main(args)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        orchestrate(args.sizes, args.engines)
    except BenchmarkError as exc:
        logger.error("HATA: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
