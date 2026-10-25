from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class AppConfig:
    program: str = "BD"
    source_root: str = "https://download.bls.gov/pub/time.series"
    data_dir: Path = Path("data")
    user_agent: str = (
        "Mozilla/5.0 (X11; Linux x86_64; BLS-Corpus-Research/1.0; "
        "+https://www.bls.gov/data/) bls-corpus/0.1"
    )
    chunk_rows: int = 65536
    concurrency: int = 2
    min_interval_s: float = 0.35
    connect_timeout_s: float = 15.0
    read_timeout_s: float = 120.0
    max_retries: int = 5
    backoff_base_s: float = 1.0
    backoff_max_s: float = 60.0
    output_format: str = "parquet"
    api_enabled: bool = False
    api_base_url: str = "https://api.bls.gov/publicAPI/v2/timeseries/data"
    api_key_env: str = "BLS_API_KEY"
    api_sample_size: int = 25

    @property
    def program_dir(self) -> str:
        """LABSTAT directory names are the lower-case survey abbreviation."""
        return self.program.lower()

    @property
    def program_url(self) -> str:
        return f"{self.source_root.rstrip('/')}/{self.program_dir}/"

    @property
    def raw_root(self) -> Path:
        return self.data_dir / "raw" / self.program_dir

    @property
    def processed_root(self) -> Path:
        return self.data_dir / "processed"

    @property
    def manifest_root(self) -> Path:
        return self.data_dir / "manifests"


def load_config(path: str | Path | None = None) -> AppConfig:
    cfg = AppConfig()
    candidates = [Path(path)] if path else [Path("config/default.toml")]
    for candidate in candidates:
        if candidate and candidate.exists():
            raw = tomllib.loads(candidate.read_text(encoding="utf-8"))
            core, dl, out, api = (raw.get(k, {}) for k in ("core", "download", "output", "api_validation"))
            cfg.program = core.get("program", cfg.program)
            cfg.source_root = core.get("source_root", cfg.source_root)
            cfg.data_dir = Path(core.get("data_dir", str(cfg.data_dir)))
            cfg.user_agent = core.get("user_agent", cfg.user_agent)
            cfg.chunk_rows = int(core.get("chunk_rows", cfg.chunk_rows))
            cfg.concurrency = int(dl.get("concurrency", cfg.concurrency))
            cfg.min_interval_s = float(dl.get("min_interval_s", cfg.min_interval_s))
            cfg.connect_timeout_s = float(dl.get("connect_timeout_s", cfg.connect_timeout_s))
            cfg.read_timeout_s = float(dl.get("read_timeout_s", cfg.read_timeout_s))
            cfg.max_retries = int(dl.get("max_retries", cfg.max_retries))
            cfg.backoff_base_s = float(dl.get("backoff_base_s", cfg.backoff_base_s))
            cfg.backoff_max_s = float(dl.get("backoff_max_s", cfg.backoff_max_s))
            cfg.output_format = out.get("format", cfg.output_format)
            cfg.api_base_url = api.get("base_url", cfg.api_base_url)
            cfg.api_key_env = api.get("api_key_env", cfg.api_key_env)
            cfg.api_sample_size = int(api.get("sample_size", cfg.api_sample_size))
            break
    return cfg
