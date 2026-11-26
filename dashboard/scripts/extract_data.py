"""Build the dashboard's data files (results, browser samples, zone lookup, zone map).

Outputs (all under ``dashboard/src/data/``, bundled into the offline build):
  results/<name>.json      every ``reports/results/*.csv`` as {columns, rows}; numbers are parsed
                           from the CSV text without rounding.
  samples/clean_sample.json  5,000 cleaned trips (deterministic hash sample) — simulations of
                           aggregation, window/join and ML run on this in the browser.
  samples/raw_sample.json    5,000 raw trips (same method) — cleaning-rule simulation.
  zones.json               TLC taxi zone lookup (265 rows).
  geo/taxi_zones.json      taxi zone polygons (GeoJSON, EPSG:2263 feet, simplified) built from
                           the official TLC shapefile zip (``--zones-zip``).

Usage (repository root):
    python dashboard/scripts/extract_data.py --data-root C:/BVA/data --zones-zip <taxi_zones.zip>
"""
from __future__ import annotations

import argparse
import io
import json
import math
import struct
import sys
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.dataset as ds

ROOT = Path(__file__).resolve().parents[2]
RESULTS_DIR = ROOT / "reports" / "results"
OUT = ROOT / "dashboard" / "src" / "data"
SAMPLE_ROWS = 5_000
SAMPLE_SALT = "bva-dashboard"
TRAIN_RATIO = 0.8
SIMPLIFY_FEET = 40.0

CLEAN_COLUMNS = ["tpep_pickup_datetime", "pickup_hour", "pickup_dow", "PULocationID", "DOLocationID",
                 "trip_distance", "fare_amount", "tip_amount", "total_amount", "payment_type",
                 "trip_duration_min", "extra", "mta_tax", "tolls_amount", "improvement_surcharge",
                 "cbd_congestion_fee"]
RAW_COLUMNS = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance",
               "fare_amount", "total_amount", "RatecodeID", "PULocationID", "DOLocationID",
               "cbd_congestion_fee", "payment_type"]


# ----------------------------------------------------------------- results CSV
def _to_number(text: str):
    if text == "":
        return None
    try:
        value = int(text)
        return value
    except ValueError:
        pass
    try:
        value = float(text)
    except ValueError:
        return text
    return value if math.isfinite(value) else text


def export_results() -> list[str]:
    target = OUT / "results"
    target.mkdir(parents=True, exist_ok=True)
    names = []
    for csv_path in sorted(RESULTS_DIR.glob("*.csv")):
        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
        rows = [[_to_number(v) for v in row] for row in df.itertuples(index=False, name=None)]
        doc = {"source": f"reports/results/{csv_path.name}", "columns": list(df.columns), "rows": rows}
        (target / f"{csv_path.stem}.json").write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")
        names.append(csv_path.stem)
    return names


# ----------------------------------------------------------------- samples
def hash_sample(df: pd.DataFrame, n: int) -> pd.DataFrame:
    """Rows with the n smallest content hashes: deterministic, order-independent, uniform."""
    keys = pd.util.hash_pandas_object(df, index=False, hash_key=SAMPLE_SALT.ljust(16)[:16])
    return df.iloc[np.argsort(keys.to_numpy(), kind="stable")[:n]].reset_index(drop=True)


def columnar(df: pd.DataFrame) -> dict:
    data = {}
    for col in df.columns:
        s = df[col]
        if pd.api.types.is_datetime64_any_dtype(s):
            data[col] = s.dt.strftime("%Y-%m-%d %H:%M:%S").tolist()
        elif pd.api.types.is_float_dtype(s):
            data[col] = [None if pd.isna(v) else float(v) for v in s]
        elif pd.api.types.is_integer_dtype(s):
            data[col] = [None if pd.isna(v) else int(v) for v in s]
        elif pd.api.types.is_bool_dtype(s):
            data[col] = [bool(v) for v in s]
        else:
            data[col] = [None if pd.isna(v) else v for v in s]
    return {"rows": len(df), "columns": list(df.columns), "data": data}


def export_samples(data_root: Path) -> dict:
    target = OUT / "samples"
    target.mkdir(parents=True, exist_ok=True)

    clean = ds.dataset(data_root / "clean", format="parquet", partitioning="hive")
    clean_df = clean.to_table(columns=CLEAN_COLUMNS).to_pandas()
    clean_total = len(clean_df)
    clean_s = hash_sample(clean_df, SAMPLE_ROWS)
    del clean_df
    split_key = pd.util.hash_pandas_object(clean_s, index=False, hash_key="bva-train-split.".ljust(16)[:16])
    clean_s["is_train"] = (split_key.to_numpy() % 1000) < TRAIN_RATIO * 1000
    doc = columnar(clean_s)
    doc.update(source="data/clean", population=clean_total, method="hash_sample")
    (target / "clean_sample.json").write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")

    raw = ds.dataset(sorted((data_root / "raw").glob("yellow_tripdata_*.parquet")), format="parquet")
    raw_df = raw.to_table(columns=RAW_COLUMNS).to_pandas()
    raw_total = len(raw_df)
    raw_s = hash_sample(raw_df, SAMPLE_ROWS)
    del raw_df
    seconds = (raw_s["tpep_dropoff_datetime"] - raw_s["tpep_pickup_datetime"]).dt.total_seconds()
    raw_s["trip_duration_min"] = seconds / 60
    raw_s["avg_speed_mph"] = np.where(raw_s["trip_duration_min"] > 0,
                                      raw_s["trip_distance"] / (raw_s["trip_duration_min"] / 60), np.nan)
    doc = columnar(raw_s)
    doc.update(source="data/raw (TLC 2026-01..05)", population=raw_total, method="hash_sample")
    (target / "raw_sample.json").write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")
    return {"clean_population": clean_total, "raw_population": raw_total}


def export_zones(data_root: Path) -> None:
    zones = pd.read_csv(data_root / "raw" / "taxi_zone_lookup.csv", keep_default_na=False)
    (OUT / "zones.json").write_text(json.dumps(zones.to_dict(orient="records"), ensure_ascii=False),
                                    encoding="utf-8")


# ----------------------------------------------------------------- shapefile → GeoJSON
def _read_dbf(raw: bytes) -> list[dict]:
    n_records, header_len, record_len = struct.unpack("<IHH", raw[4:12])
    fields, pos = [], 32
    while raw[pos] != 0x0D:
        name = raw[pos:pos + 11].split(b"\x00")[0].decode("ascii")
        ftype, flen = chr(raw[pos + 11]), raw[pos + 16]
        fields.append((name, ftype, flen))
        pos += 32
    records = []
    for i in range(n_records):
        off = header_len + i * record_len + 1  # 1 byte deletion flag
        rec = {}
        for name, ftype, flen in fields:
            text = raw[off:off + flen].decode("utf-8", errors="replace").strip()
            off += flen
            rec[name] = (float(text) if ftype in "NF" and text else text)
        records.append(rec)
    return records


def _read_shp(raw: bytes) -> list[list[list[tuple[float, float]]]]:
    shapes, pos = [], 100
    while pos < len(raw):
        _, content_words = struct.unpack(">ii", raw[pos:pos + 8])
        content = raw[pos + 8:pos + 8 + content_words * 2]
        pos += 8 + content_words * 2
        shape_type = struct.unpack("<i", content[:4])[0]
        if shape_type == 0:
            shapes.append([])
            continue
        num_parts, num_points = struct.unpack("<ii", content[36:44])
        parts = list(struct.unpack(f"<{num_parts}i", content[44:44 + 4 * num_parts]))
        pts_off = 44 + 4 * num_parts
        pts = struct.unpack(f"<{2 * num_points}d", content[pts_off:pts_off + 16 * num_points])
        points = list(zip(pts[0::2], pts[1::2]))
        bounds = parts + [num_points]
        shapes.append([points[bounds[k]:bounds[k + 1]] for k in range(num_parts)])
    return shapes


def _simplify(points: list[tuple[float, float]], tol: float) -> list[tuple[float, float]]:
    """Douglas–Peucker (iterative)."""
    if len(points) < 5:
        return points
    keep = [False] * len(points)
    keep[0] = keep[-1] = True
    stack = [(0, len(points) - 1)]
    while stack:
        a, b = stack.pop()
        (x1, y1), (x2, y2) = points[a], points[b]
        dx, dy = x2 - x1, y2 - y1
        norm = math.hypot(dx, dy) or 1e-9
        best, idx = -1.0, -1
        for i in range(a + 1, b):
            x0, y0 = points[i]
            d = abs(dy * x0 - dx * y0 + x2 * y1 - y2 * x1) / norm
            if d > best:
                best, idx = d, i
        if best > tol and idx > 0:
            keep[idx] = True
            stack += [(a, idx), (idx, b)]
    out = [p for p, k in zip(points, keep) if k]
    return out if len(out) >= 4 else points


def _signed_area(ring) -> float:
    return sum(x1 * y2 - x2 * y1 for (x1, y1), (x2, y2) in zip(ring, ring[1:] + ring[:1])) / 2


def export_geo(zones_zip: Path) -> int:
    with zipfile.ZipFile(zones_zip) as zf:
        names = {Path(n).suffix: n for n in zf.namelist()}
        shapes = _read_shp(zf.read(names[".shp"]))
        records = _read_dbf(zf.read(names[".dbf"]))
    by_id: dict[int, dict] = {}
    for rings, rec in zip(shapes, records):
        loc = int(rec["LocationID"])
        entry = by_id.setdefault(loc, {"zone": rec["zone"], "borough": rec["borough"], "polygons": []})
        for ring in rings:
            simple = [(round(x), round(y)) for x, y in _simplify(ring, SIMPLIFY_FEET)]
            if _signed_area(ring) < 0:  # shapefile outer ring = clockwise
                entry["polygons"].append([simple])
            elif entry["polygons"]:
                entry["polygons"][-1].append(simple)
            else:
                entry["polygons"].append([simple])
    features = [{"type": "Feature", "id": loc,
                 "properties": {"LocationID": loc, "zone": e["zone"], "borough": e["borough"]},
                 "geometry": {"type": "MultiPolygon",
                              "coordinates": [[[list(p) for p in ring] for ring in poly] for poly in e["polygons"]]}}
                for loc, e in sorted(by_id.items())]
    geo = {"type": "FeatureCollection", "crs": "EPSG:2263 (NY State Plane Long Island, feet)",
           "source": "TLC taxi_zones.zip (d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip)",
           "features": features}
    (OUT / "geo").mkdir(parents=True, exist_ok=True)
    (OUT / "geo" / "taxi_zones.json").write_text(json.dumps(geo, separators=(",", ":")), encoding="utf-8")
    return len(features)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--data-root", type=Path, default=ROOT / "data")
    parser.add_argument("--zones-zip", type=Path)
    args = parser.parse_args(argv)
    print("results:", ", ".join(export_results()))
    print("samples:", export_samples(args.data_root))
    export_zones(args.data_root)
    if args.zones_zip:
        print("zone polygons:", export_geo(args.zones_zip))
    return 0


if __name__ == "__main__":
    sys.exit(main())
