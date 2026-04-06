from pathlib import Path

from crawl_news import PROJECT_ROOT, build_runtime_paths


PATH_ENV_VARS = [
    "NEWSCRAWLER_DATA_DIR",
    "NEWSCRAWLER_DB_PATH",
    "NEWSCRAWLER_LEGACY_DB_PATH",
    "NEWSCRAWLER_RAW_DIR",
    "NEWSCRAWLER_RUNS_DIR",
    "NEWSCRAWLER_AISTOCK_EXPORT_DIR",
    "NEWSCRAWLER_TICKER_EXPORTS_DIR",
]


def test_build_runtime_paths_resolves_relative_data_dir_from_script_location(monkeypatch):
    for name in PATH_ENV_VARS:
        monkeypatch.delenv(name, raising=False)

    paths = build_runtime_paths(data_dir="linux-data")
    expected_root = PROJECT_ROOT / "linux-data"

    assert paths["DATA_DIR"] == expected_root
    assert paths["DB_PATH"] == expected_root / "news.db"
    assert paths["LEGACY_DB_PATH"] == expected_root / "amd_news.db"
    assert paths["RAW_DIR"] == expected_root / "raw"
    assert paths["RUNS_DIR"] == expected_root / "runs"
    assert paths["AISTOCK_EXPORT_DIR"] == expected_root / "aistock"
    assert paths["TICKER_EXPORTS_DIR"] == expected_root / "aistock" / "by_ticker"


def test_build_runtime_paths_scopes_relative_overrides_to_data_dir(monkeypatch, tmp_path):
    for name in PATH_ENV_VARS:
        monkeypatch.delenv(name, raising=False)

    data_root = tmp_path / "runtime"
    monkeypatch.setenv("NEWSCRAWLER_DB_PATH", "sqlite/news.db")
    monkeypatch.setenv("NEWSCRAWLER_RUNS_DIR", "artifacts/runs")
    monkeypatch.setenv("NEWSCRAWLER_AISTOCK_EXPORT_DIR", "exports")
    monkeypatch.setenv("NEWSCRAWLER_TICKER_EXPORTS_DIR", "stable/by_ticker")

    paths = build_runtime_paths(data_dir=data_root)

    assert paths["DATA_DIR"] == data_root.resolve()
    assert paths["DB_PATH"] == data_root / "sqlite" / "news.db"
    assert paths["RUNS_DIR"] == data_root / "artifacts" / "runs"
    assert paths["AISTOCK_EXPORT_DIR"] == data_root / "exports"
    assert paths["TICKER_EXPORTS_DIR"] == data_root / "exports" / "stable" / "by_ticker"
