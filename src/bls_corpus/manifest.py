"""Snapshot manifest: the auditable record of one acquisition run."""
from __future__ import annotations

import json
import os
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from . import __version__
from .discovery import DiscoveryResult


@dataclass
class FileRecord:
    relative_path: str
    source_url: str
    file_role: str = "unclassified"
    required: bool = False
    remote_size: int | None = None
    local_size: int | None = None
    last_modified: str | None = None
    etag: str | None = None
    sha256: str | None = None
    download_status: str = "pending"  # pending|downloaded|skipped_not_modified|skipped_verified|failed
    parse_status: str = "pending"     # pending|parsed|failed|not_applicable
    attempts: int = 0
    error_message: str | None = None


@dataclass
class Manifest:
    snapshot_id: str
    program_id: str
    source_url: str
    run_id: str
    created_utc: str
    discovery: dict = field(default_factory=dict)
    files: list[FileRecord] = field(default_factory=list)
    downloader_version: str = __version__
    git_commit: str | None = None
    config_summary: dict = field(default_factory=dict)
    notes: list[str] = field(default_factory=lambda: [
        "Snapshot is a sequential crawl, not an atomic upstream transaction; "
        "per-file Last-Modified/ETag/SHA-256 are recorded for audit."
    ])

    def record_for(self, relative_path: str) -> FileRecord | None:
        for r in self.files:
            if r.relative_path == relative_path:
                return r
        return None

    def counts(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for r in self.files:
            out[r.download_status] = out.get(r.download_status, 0) + 1
        return out

    def to_dict(self) -> dict:
        d = asdict(self)
        return d

    def save(self, path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(self.to_dict(), indent=2), encoding="utf-8")
        os.replace(tmp, path)
        return path

    @classmethod
    def load(cls, path: Path) -> "Manifest":
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        files = [FileRecord(**f) for f in raw.pop("files", [])]
        known = {f.name for f in cls.__dataclass_fields__.values()}  # noqa: SLF001
        raw = {k: v for k, v in raw.items() if k in known}
        return cls(files=files, **raw)


def git_commit() -> str | None:
    import subprocess

    try:
        out = subprocess.run(["git", "rev-parse", "--short", "HEAD"],
                             capture_output=True, text=True, timeout=5)
        return out.stdout.strip() or None
    except Exception:
        return None


def new_manifest(discovery: DiscoveryResult, snapshot_id: str,
                 config_summary: dict | None = None) -> Manifest:
    now = datetime.now(timezone.utc)
    manifest = Manifest(
        snapshot_id=snapshot_id,
        program_id=discovery.program,
        source_url=discovery.source_url,
        run_id=f"run-{now.strftime('%Y%m%dT%H%M%SZ')}-{os.getpid()}",
        created_utc=now.isoformat(),
        discovery=discovery.to_dict(),
        git_commit=git_commit(),
        config_summary=config_summary or {},
    )
    for f in discovery.files:
        if f.is_dir:
            continue
        manifest.files.append(FileRecord(
            relative_path=f.relative_path,
            source_url=f.source_url,
            file_role=f.file_role,
            required=f.required,
            remote_size=f.remote_size,
            last_modified=f.listing_last_modified,
        ))
    print(f"[manifest] snapshot={snapshot_id} files={len(manifest.files)}", file=sys.stderr)
    return manifest


def latest_manifest(manifest_root: Path, program: str) -> Path | None:
    candidates = sorted(manifest_root.glob(f"{program.lower()}-*.json"))
    return candidates[-1] if candidates else None
