import json
import re

from src import dashboard


def test_build_writes_html_with_embedded_json(tmp_path):
    path = dashboard.build(tmp_path)
    text = path.read_text(encoding="utf-8")
    assert "__" not in re.sub(r"__init__|__name__", "", text.split("<script>")[0])  # tüm yer tutucular dolu
    payload = re.search(r'<script id="data" type="application/json">(.*?)</script>', text, re.S).group(1)
    assert isinstance(json.loads(payload), list)


def test_table_html_escapes_values():
    import pandas as pd

    out = dashboard.table_html(pd.DataFrame({"a": ["<b>"]}), {"a": "A"})
    assert "&lt;b&gt;" in out and "<b>" not in out


def test_number_formatting_turkish():
    assert dashboard._fmt_int(17210961) == "17.210.961"
    assert dashboard._fmt(3.25, 2) == "3,25"
