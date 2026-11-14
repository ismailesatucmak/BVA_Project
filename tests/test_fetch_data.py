import hashlib
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src import fetch_data as fd


@pytest.fixture
def clean_zip(tmp_path):
    src = tmp_path / "src" / "pickup_year=2026" / "pickup_month=1"
    src.mkdir(parents=True)
    pq.write_table(pa.table({"x": [1, 2, 3]}), src / "part-0.parquet")
    zpath = tmp_path / "clean.zip"
    with zipfile.ZipFile(zpath, "w") as zf:
        zf.write(src / "part-0.parquet", "pickup_year=2026/pickup_month=1/part-0.parquet")
    return zpath, hashlib.sha256(zpath.read_bytes()).hexdigest()


def test_sha256_of_file(clean_zip):
    path, digest = clean_zip
    assert fd.sha256_of(path) == digest


def test_extract_verifies_checksum_and_rows(clean_zip, tmp_path):
    path, digest = clean_zip
    target = tmp_path / "clean"
    assert fd.install_clean(path, target, expected_sha256=digest, expected_rows=3) == 3
    assert (target / "pickup_year=2026" / "pickup_month=1" / "part-0.parquet").exists()


def test_checksum_mismatch_is_rejected(clean_zip, tmp_path):
    path, _ = clean_zip
    with pytest.raises(fd.FetchError, match="SHA-256"):
        fd.install_clean(path, tmp_path / "clean", expected_sha256="0" * 64, expected_rows=3)
    assert not (tmp_path / "clean").exists()


def test_row_count_mismatch_is_rejected(clean_zip, tmp_path):
    path, digest = clean_zip
    with pytest.raises(fd.FetchError, match="satır"):
        fd.install_clean(path, tmp_path / "clean", expected_sha256=digest, expected_rows=99)
    assert not (tmp_path / "clean").exists()


def test_existing_target_is_not_overwritten(clean_zip, tmp_path):
    path, digest = clean_zip
    target = tmp_path / "clean"
    target.mkdir()
    with pytest.raises(fd.FetchError, match="zaten var"):
        fd.install_clean(path, target, expected_sha256=digest, expected_rows=3)
