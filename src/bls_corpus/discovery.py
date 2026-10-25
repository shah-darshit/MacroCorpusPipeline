"""Remote discovery: parse the official LABSTAT (IIS) directory listing.

Classification uses filename patterns *plus* the documented LABSTAT layout
(see docs/source_inventory.md). Anything not matching a known pattern is kept
and reported as ``unclassified`` -- never silently dropped.
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

_LINE_RE = re.compile(
    r"(?P<date>\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}\s*[AP]M)\s+"
    r"(?P<size>&lt;dir&gt;|\d+)\s+"
    r'<A HREF="(?P<href>[^"]+)">(?P<name>[^<]+)</A>',
    re.IGNORECASE,
)

# Suffixes (after "<prog>.") that are code->label mapping files in BD.
# Generic LABSTAT programs use the same convention (xx.industry, xx.area, ...),
# so any two-part file that is not catalog/data/doc/footnote is treated as a
# mapping candidate and verified by header sniffing when parsed.
_KNOWN_ROLES: list[tuple[re.Pattern[str], str, bool]] = [
    (re.compile(r"\.series$"), "series_catalog", True),
    (re.compile(r"\.data\.\d+\..+$"), "observations", True),
    (re.compile(r"\.footnote$"), "footnotes", True),
    (re.compile(r"\.txt$"), "documentation", False),
    (re.compile(r"\.contacts$"), "documentation", False),
]


@dataclass
class FileEntry:
    name: str
    relative_path: str
    source_url: str
    remote_size: int | None = None
    listing_last_modified: str | None = None
    is_dir: bool = False
    file_role: str = "unclassified"
    required: bool = False

    def classify(self, program_dir: str) -> None:
        if self.is_dir:
            self.file_role = "directory"
            return
        for pattern, role, required in _KNOWN_ROLES:
            if pattern.search(self.name):
                self.file_role, self.required = role, required
                return
        # "<prog>.<something>" single-suffix files are mapping/reference files
        # in the LABSTAT convention (bd.industry, bd.state, ...).
        if re.fullmatch(rf"{re.escape(program_dir)}\.[a-z0-9_]+", self.name):
            self.file_role, self.required = "mapping", True
            return
        self.file_role, self.required = "unclassified", False


@dataclass
class DiscoveryResult:
    program: str
    source_url: str
    fetched_at: str
    listing_sha256: str
    files: list[FileEntry] = field(default_factory=list)

    @property
    def required_missing(self) -> list[str]:
        roles = {f.file_role for f in self.files if not f.is_dir}
        missing = []
        if "series_catalog" not in roles:
            missing.append("series_catalog (*.series)")
        if "observations" not in roles:
            missing.append("observations (*.data.*)")
        return missing

    def to_dict(self) -> dict:
        return {
            "program": self.program,
            "source_url": self.source_url,
            "fetched_at": self.fetched_at,
            "listing_sha256": self.listing_sha256,
            "files": [asdict(f) for f in self.files],
        }


def parse_listing(html: str, program: str, source_url: str) -> DiscoveryResult:
    program_dir = program.lower()
    files: list[FileEntry] = []
    for m in _LINE_RE.finditer(html):
        name = m.group("name").strip()
        if name.startswith("[To Parent"):
            continue
        is_dir = m.group("size") == "&lt;dir&gt;"
        entry = FileEntry(
            name=name,
            relative_path=name,
            source_url=f"{source_url.rstrip('/')}/{name}",
            remote_size=None if is_dir else int(m.group("size")),
            listing_last_modified=m.group("date").strip(),
            is_dir=is_dir,
        )
        entry.classify(program_dir)
        files.append(entry)
    files.sort(key=lambda f: f.name)
    return DiscoveryResult(
        program=program.upper(),
        source_url=source_url,
        fetched_at=datetime.now(timezone.utc).isoformat(),
        listing_sha256=hashlib.sha256(html.encode()).hexdigest(),
        files=files,
    )


def discover_remote(program: str, source_url: str, user_agent: str,
                    timeout_s: float = 30.0, transport=None) -> DiscoveryResult:
    import httpx

    headers = {"User-Agent": user_agent, "Accept-Encoding": "identity"}
    with httpx.Client(headers=headers, timeout=timeout_s, transport=transport,
                      follow_redirects=True) as client:
        resp = client.get(source_url)
        resp.raise_for_status()
        html = resp.text
    result = parse_listing(html, program, source_url)
    if result.required_missing:
        raise DiscoveryError(
            f"Required LABSTAT file(s) missing for {program} at {source_url}: "
            + ", ".join(result.required_missing)
        )
    return result


class DiscoveryError(RuntimeError):
    pass


def snapshot_id_for(discovery: DiscoveryResult) -> str:
    """Deterministic snapshot id: newest listing mtime + short listing hash.

    Changes iff the upstream listing changes; documents (but cannot eliminate)
    the non-atomicity of a multi-file crawl.
    """
    stamps = []
    for f in discovery.files:
        if f.listing_last_modified:
            try:
                stamps.append(datetime.strptime(f.listing_last_modified, "%m/%d/%Y %I:%M %p"))
            except ValueError:
                pass
    newest = max(stamps).strftime("%Y%m%dT%H%M%SZ") if stamps else "unknown-time"
    return f"{discovery.program.lower()}-{newest}-{discovery.listing_sha256[:8]}"
