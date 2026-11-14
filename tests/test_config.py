from pathlib import Path

from src import config


def test_env_var_wins(monkeypatch, tmp_path):
    monkeypatch.setenv("NYC_DATA_ROOT", str(tmp_path))
    assert config._external_data_root() == tmp_path


def test_falls_back_to_project_folder_when_original_missing(monkeypatch, tmp_path):
    monkeypatch.delenv("NYC_DATA_ROOT", raising=False)
    monkeypatch.setattr(config, "_ORIGINAL_DATA_ROOT", tmp_path / "yok")
    assert config._external_data_root() == config.DATA_DIR / "large"


def test_uses_original_root_when_present(monkeypatch, tmp_path):
    monkeypatch.delenv("NYC_DATA_ROOT", raising=False)
    monkeypatch.setattr(config, "_ORIGINAL_DATA_ROOT", Path(tmp_path))
    assert config._external_data_root() == tmp_path
