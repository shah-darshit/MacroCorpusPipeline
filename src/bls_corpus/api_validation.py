"""Optional API spot-checking. Never used by the bulk workflow; bulk flat
files remain the source of truth and API values never overwrite them.
Unregistered API use is rate-limited by BLS, so sampling is small and paced.
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass, field

import httpx
import pyarrow.parquet as pq
from pathlib import Path


@dataclass
class ApiComparison:
    series_checked: int = 0
    observations_compared: int = 0
    matches: int = 0
    discrepancies: list[dict] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def sample_series(catalog_path: Path, sample_size: int, seed: int = 42) -> list[str]:
    import random

    ids = pq.read_table(catalog_path, columns=["series_id"]).column("series_id").to_pylist()
    rng = random.Random(seed)
    return rng.sample(ids, min(sample_size, len(ids)))


def compare_with_api(catalog_path: Path, observations_path: Path, cfg,
                     sample_size: int = 25) -> ApiComparison:
    out = ApiComparison()
    series_ids = sample_series(catalog_path, sample_size)
    # Load local observations for sampled series only (streamed filter).
    wanted = set(series_ids)
    local: dict[tuple[str, int, str], float | None] = {}
    pf = pq.ParquetFile(observations_path)
    for batch in pf.iter_batches(batch_size=262144):
        cols = batch.to_pylist()
        for r in cols:
            if r["series_id"] in wanted:
                local[(r["series_id"], r["year"], r["period"])] = r["value_numeric"]
    key = os.environ.get(cfg.api_key_env)
    headers = {"User-Agent": cfg.user_agent}
    with httpx.Client(headers=headers, timeout=30.0) as client:
        for sid in series_ids:
            url = f"{cfg.api_base_url}/{sid}"
            params = {"registrationkey": key} if key else {}
            try:
                resp = client.get(url, params=params)
                resp.raise_for_status()
                payload = resp.json()
            except Exception as exc:  # noqa: BLE001 - reported, not raised
                out.errors.append(f"{sid}: {type(exc).__name__}: {exc}")
                continue
            out.series_checked += 1
            for s in payload.get("Results", {}).get("series", []):
                for obs in s.get("data", []):
                    k = (sid, int(obs["year"]), obs["period"])
                    if k not in local:
                        continue
                    out.observations_compared += 1
                    try:
                        api_val = float(str(obs["value"]).replace(",", ""))
                    except ValueError:
                        api_val = None
                    if api_val == local[k]:
                        out.matches += 1
                    elif len(out.discrepancies) < 100:
                        out.discrepancies.append({
                            "series_id": sid, "year": k[1], "period": k[2],
                            "bulk_value": local[k], "api_value": api_val,
                            "note": "possible revision/formatting/vintage difference; bulk value retained"})
            time.sleep(0.5)  # respect unregistered quota
    return out
