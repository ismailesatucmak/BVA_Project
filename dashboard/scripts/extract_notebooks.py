"""Extract every cell of every project notebook into JSON for the presentation dashboard.

Reads the saved notebooks (never executes them) with nbformat and writes, per notebook,
``dashboard/src/data/notebooks/<slug>.json`` plus an ``index.json`` inventory. Image outputs
(PNG/SVG/JPEG, base64) are written as files under ``dashboard/public/nb-images/`` so the
built dashboard serves them offline.

Usage (from the repository root):
    python dashboard/scripts/extract_notebooks.py
"""
from __future__ import annotations

import base64
import json
import re
import sys
from pathlib import Path

import nbformat

ROOT = Path(__file__).resolve().parents[2]
NOTEBOOK_DIR = ROOT / "notebooks"
DASHBOARD = ROOT / "dashboard"
OUT_DIR = DASHBOARD / "src" / "data" / "notebooks"
IMAGE_DIR = DASHBOARD / "public" / "nb-images"
IMAGE_URL = "nb-images"

ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
HEADING = re.compile(r"^(#{1,4})\s+(.+?)\s*$", re.M)
IMAGE_TYPES = {"image/png": "png", "image/jpeg": "jpg", "image/svg+xml": "svg"}


def slug_of(path: Path) -> str:
    return path.stem


def text_of(value) -> str:
    return "".join(value) if isinstance(value, list) else (value or "")


def save_image(data: str, mime: str, name: str) -> str:
    ext = IMAGE_TYPES[mime]
    target = IMAGE_DIR / f"{name}.{ext}"
    if mime == "image/svg+xml":
        target.write_text(text_of(data), encoding="utf-8")
    else:
        target.write_bytes(base64.b64decode(text_of(data)))
    return f"{IMAGE_URL}/{target.name}"


def parse_tables(html: str) -> list[dict]:
    """HTML tabloları → {header: [[...]], rows: [[...]]}. Hücre metinleri olduğu gibi korunur."""
    from bs4 import BeautifulSoup

    tables = []
    for table in BeautifulSoup(html, "html.parser").find_all("table"):
        header, rows = [], []
        thead = table.find("thead")
        if thead:
            for tr in thead.find_all("tr"):
                header.append([{"text": c.get_text(strip=True), "colspan": int(c.get("colspan", 1))}
                               for c in tr.find_all(["th", "td"])])
        body = table.find("tbody") or table
        for tr in body.find_all("tr"):
            if thead and tr.find_parent("thead"):
                continue
            rows.append([{"text": c.get_text(strip=True), "th": c.name == "th"} for c in tr.find_all(["th", "td"])])
        tables.append({"header": header, "rows": rows})
    return tables


def convert_output(out, slug: str, cell_idx: int, out_idx: int) -> dict:
    kind = out.get("output_type")
    if kind == "stream":
        return {"type": "stream", "name": out.get("name", "stdout"), "text": text_of(out.get("text"))}
    if kind == "error":
        tb = "\n".join(ANSI.sub("", line) for line in out.get("traceback", []))
        return {"type": "error", "ename": out.get("ename"), "evalue": out.get("evalue"), "traceback": tb}
    # execute_result / display_data
    data = out.get("data", {})
    item: dict = {"type": kind, "data": {}}
    for mime, value in data.items():
        if mime in IMAGE_TYPES:
            item["data"][mime] = save_image(value, mime, f"{slug}_c{cell_idx:02d}_o{out_idx}")
        elif mime in ("text/plain", "text/html", "text/markdown", "text/latex"):
            item["data"][mime] = text_of(value)
        elif mime == "application/json":
            item["data"][mime] = value
        else:  # widget state etc. — record that it existed, keep it lossless as text
            item["data"][mime] = text_of(value) if isinstance(value, (str, list)) else json.dumps(value)
    html = item["data"].get("text/html", "")
    if "<table" in html:
        item["tables"] = parse_tables(html)
    if out.get("execution_count") is not None:
        item["execution_count"] = out["execution_count"]
    return item


def classify(outputs: list[dict]) -> dict:
    stats = {"images": 0, "tables": 0, "markdown": 0, "stdout": 0, "errors": 0, "text": 0, "iframes": 0}
    for o in outputs:
        if o["type"] == "stream":
            stats["stdout"] += 1
        elif o["type"] == "error":
            stats["errors"] += 1
        else:
            d = o["data"]
            if any(m in d for m in IMAGE_TYPES):
                stats["images"] += 1
            elif "text/html" in d and "<table" in d["text/html"]:
                stats["tables"] += 1
            elif "text/html" in d and "<iframe" in d["text/html"]:
                stats["iframes"] += 1
            elif "text/markdown" in d:
                stats["markdown"] += 1
            elif "text/plain" in d:
                stats["text"] += 1
    return stats


def extract(path: Path) -> dict:
    nb = nbformat.read(path, as_version=4)
    slug = slug_of(path)
    cells = []
    totals = {"cells": 0, "markdown_cells": 0, "code_cells": 0, "images": 0, "tables": 0,
              "markdown": 0, "stdout": 0, "errors": 0, "text": 0, "iframes": 0, "unexecuted": 0}
    headings = []
    for i, cell in enumerate(nb.cells):
        source = text_of(cell.source)
        entry = {"index": i, "cell_type": cell.cell_type, "source": source}
        totals["cells"] += 1
        attachments = cell.get("attachments") or {}
        if attachments:  # markdown'a gömülü görseller (attachment:<ad>) → dosya
            entry["attachments"] = {}
            for k, (name, bundle) in enumerate(attachments.items()):
                mime = next((m for m in IMAGE_TYPES if m in bundle), None)
                if mime:
                    entry["attachments"][name] = save_image(bundle[mime], mime, f"{slug}_c{i:02d}_att{k}")
                    totals["images"] += 1
        if cell.cell_type == "markdown":
            totals["markdown_cells"] += 1
            for level, title in HEADING.findall(source):
                headings.append({"cell": i, "level": len(level), "title": title.strip()})
        elif cell.cell_type == "code":
            totals["code_cells"] += 1
            outputs = [convert_output(o, slug, i, j) for j, o in enumerate(cell.get("outputs", []))]
            entry["execution_count"] = cell.get("execution_count")
            entry["outputs"] = outputs
            if source.strip() and cell.get("execution_count") is None:
                totals["unexecuted"] += 1
            for k, v in classify(outputs).items():
                totals[k] += v
        cells.append(entry)
    return {"slug": slug, "file": f"notebooks/{path.name}", "nbformat": nb.nbformat,
            "kernel": nb.metadata.get("kernelspec", {}).get("display_name"),
            "headings": headings, "stats": totals, "cells": cells}


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    IMAGE_DIR.mkdir(parents=True, exist_ok=True)
    for old in list(OUT_DIR.glob("*.json")) + list(IMAGE_DIR.glob("*")):
        old.unlink()
    paths = sorted(NOTEBOOK_DIR.glob("*.ipynb"))
    if not paths:
        print(f"Notebook bulunamadı: {NOTEBOOK_DIR}", file=sys.stderr)
        return 1
    index = []
    for path in paths:
        doc = extract(path)
        (OUT_DIR / f"{doc['slug']}.json").write_text(json.dumps(doc, ensure_ascii=False, indent=1), encoding="utf-8")
        index.append({"slug": doc["slug"], "file": doc["file"], "title": next(
            (h["title"] for h in doc["headings"] if h["level"] == 1), doc["slug"]),
            "stats": doc["stats"], "headings": doc["headings"]})
        print(f"{doc['slug']:<40} {doc['stats']}")
    (OUT_DIR / "index.json").write_text(json.dumps(index, ensure_ascii=False, indent=1), encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
