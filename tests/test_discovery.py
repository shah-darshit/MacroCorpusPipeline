import pytest

from bls_corpus.discovery import DiscoveryError, discover_remote, parse_listing, snapshot_id_for

SAMPLE = """<html><head><title>download.bls.gov - /pub/time.series/bd/</title></head><body>
<pre><A HREF="/pub/time.series/">[To Parent Directory]</A><br><br>
 7/29/2026 10:00 AM    197581980 <A HREF="/pub/time.series/bd/bd.data.0.Current">bd.data.0.Current</A><br>
 7/29/2026 10:00 AM      5611545 <A HREF="/pub/time.series/bd/bd.series">bd.series</A><br>
 7/29/2026 10:00 AM          153 <A HREF="/pub/time.series/bd/bd.footnote">bd.footnote</A><br>
 7/29/2026 10:00 AM         4452 <A HREF="/pub/time.series/bd/bd.industry">bd.industry</A><br>
 7/27/2022  2:05 PM        17479 <A HREF="/pub/time.series/bd/bd.txt">bd.txt</A><br>
 7/29/2026 10:00 AM          999 <A HREF="/pub/time.series/bd/bd.mystery">bd.mystery</A><br>
</pre></body></html>"""


def test_parse_listing_classifies_and_records_unknown():
    d = parse_listing(SAMPLE, "BD", "https://download.bls.gov/pub/time.series/bd/")
    roles = {f.name: (f.file_role, f.required) for f in d.files}
    assert roles["bd.series"] == ("series_catalog", True)
    assert roles["bd.data.0.Current"] == ("observations", True)
    assert roles["bd.industry"] == ("mapping", True)
    assert roles["bd.txt"] == ("documentation", False)
    assert roles["bd.mystery"] == ("mapping", True)  # LABSTAT <prog>.<x> convention
    assert d.files[0].remote_size is not None
    assert d.required_missing == []


def test_unrecognized_auxiliary_file_is_kept():
    html = SAMPLE.replace("bd.mystery", "strange-file.bin")
    d = parse_listing(html, "BD", "https://x/bd/")
    roles = {f.name: f.file_role for f in d.files}
    assert roles["strange-file.bin"] == "unclassified"


def test_missing_required_fails_visibly():
    html = "<pre> 7/29/2026 10:00 AM  153 <A HREF=\"/bd/bd.footnote\">bd.footnote</A></pre>"
    d = parse_listing(html, "BD", "https://x/bd/")
    assert "series_catalog (*.series)" in d.required_missing[0]


def test_snapshot_id_deterministic():
    a = parse_listing(SAMPLE, "BD", "https://x/bd/")
    b = parse_listing(SAMPLE, "BD", "https://x/bd/")
    assert snapshot_id_for(a) == snapshot_id_for(b)
    assert snapshot_id_for(a).startswith("bd-20260729")


def test_discover_remote_missing_required_raises():
    import httpx
    transport = httpx.MockTransport(lambda r: httpx.Response(200, text="<pre></pre>"))
    with pytest.raises(DiscoveryError):
        discover_remote("BD", "https://x/bd/", "test-agent", transport=transport)
