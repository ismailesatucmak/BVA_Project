import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src import config, ingest


def _write_trip_parquet(path, columns=config.TRIP_COLUMNS, rows=3):
    table = pa.table({c: list(range(rows)) for c in columns})
    pq.write_table(table, path)


def _write_zone_csv(path, rows=config.ZONE_COUNT):
    lines = [",".join(config.ZONE_LOOKUP_COLUMNS)]
    lines += [f"{i},B,Z,S" for i in range(1, rows + 1)]
    path.write_text("\n".join(lines), encoding="utf-8")


class _FakeResponse:
    def __init__(self, chunks, status=200):
        self._chunks = chunks
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise ingest.requests.HTTPError(f"HTTP {self.status_code}")

    def iter_content(self, chunk_size):
        for chunk in self._chunks:
            if isinstance(chunk, Exception):
                raise chunk
            yield chunk

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_expected_sources_cover_all_months_and_zone_lookup():
    names = [s.file_name for s in ingest.expected_sources()]
    assert names == [config.trip_file_name(config.YEAR, m) for m in config.MONTHS] + [
        config.ZONE_LOOKUP_FILE
    ]


def test_verify_trip_file_returns_row_count(tmp_path):
    path = tmp_path / "t.parquet"
    _write_trip_parquet(path, rows=5)
    assert ingest.verify_trip_file(path) == 5


def test_verify_trip_file_rejects_missing_column(tmp_path):
    path = tmp_path / "t.parquet"
    _write_trip_parquet(path, columns=config.TRIP_COLUMNS[:-1])
    with pytest.raises(ingest.IngestError, match="cbd_congestion_fee"):
        ingest.verify_trip_file(path)


def test_verify_trip_file_rejects_corrupt_file(tmp_path):
    path = tmp_path / "t.parquet"
    path.write_bytes(b"not a parquet file")
    with pytest.raises(ingest.IngestError, match="okunamadı"):
        ingest.verify_trip_file(path)


def test_verify_zone_lookup_accepts_valid_file(tmp_path):
    path = tmp_path / "z.csv"
    _write_zone_csv(path)
    assert ingest.verify_zone_lookup(path) == config.ZONE_COUNT


def test_verify_zone_lookup_rejects_wrong_row_count(tmp_path):
    path = tmp_path / "z.csv"
    _write_zone_csv(path, rows=10)
    with pytest.raises(ingest.IngestError, match="265"):
        ingest.verify_zone_lookup(path)


def test_ensure_file_skips_download_when_present(tmp_path, monkeypatch):
    path = tmp_path / "exists.parquet"
    path.write_bytes(b"x")
    monkeypatch.setattr(ingest.requests, "get", lambda *a, **k: pytest.fail("indirmemeli"))
    assert ingest.ensure_file("http://x", path) is False


def test_download_writes_file_atomically(tmp_path, monkeypatch):
    path = tmp_path / "new.parquet"
    monkeypatch.setattr(ingest.requests, "get", lambda *a, **k: _FakeResponse([b"ab", b"cd"]))
    assert ingest.ensure_file("http://x", path) is True
    assert path.read_bytes() == b"abcd"
    assert not path.with_name(path.name + ".part").exists()


def test_download_failure_leaves_no_partial_file(tmp_path, monkeypatch):
    path = tmp_path / "new.parquet"
    broken = _FakeResponse([b"ab", ingest.requests.ConnectionError("koptu")])
    monkeypatch.setattr(ingest.requests, "get", lambda *a, **k: broken)
    with pytest.raises(ingest.IngestError, match="indirilemedi"):
        ingest.ensure_file("http://x", path)
    assert not path.exists()
    assert not path.with_name(path.name + ".part").exists()


def test_download_http_error_is_reported(tmp_path, monkeypatch):
    path = tmp_path / "new.parquet"
    monkeypatch.setattr(ingest.requests, "get", lambda *a, **k: _FakeResponse([], status=404))
    with pytest.raises(ingest.IngestError, match="404"):
        ingest.ensure_file("http://x", path)
