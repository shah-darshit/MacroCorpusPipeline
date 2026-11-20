import pytest

from bls_corpus.pipeline import (
    DiscoveryError, discover_remote, list_catalog_prefixes, parse_listing,
)

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


def test_list_catalog_prefixes_skips_non_survey_dirs():
    html = """<pre>
 1/1/2026 10:00 AM        &lt;dir&gt; <A HREF="/pub/time.series/cu/">cu</A><br>
 1/1/2026 10:00 AM        &lt;dir&gt; <A HREF="/pub/time.series/ws/">ws</A><br>
 1/1/2026 10:00 AM        &lt;dir&gt; <A HREF="/pub/time.series/pf/">pf</A><br>
 1/1/2026 10:00 AM        &lt;dir&gt; <A HREF="/pub/time.series/compressed/">compressed</A><br>
 1/1/2026 10:00 AM        5929 <A HREF="/pub/time.series/overview.txt">overview.txt</A><br>
</pre>"""
    assert list_catalog_prefixes(html) == ["cu", "ws"]


def test_discover_remote_missing_required_raises():
    import httpx
    transport = httpx.MockTransport(lambda r: httpx.Response(200, text="<pre></pre>"))
    with pytest.raises(DiscoveryError):
        discover_remote("BD", "https://x/bd/", "test-agent", transport=transport)


def test_listing_retries_429_and_honors_retry_after():
    import httpx
    from bls_corpus.pipeline import fetch_listing
    calls, waits = [], []

    def handler(request):
        calls.append(request)
        if len(calls) < 3:
            return httpx.Response(429, headers={"Retry-After": "7"})
        return httpx.Response(200, text="<pre>ok</pre>")

    html = fetch_listing("https://x/bd/", "test-agent", transport=httpx.MockTransport(handler),
                         sleep=waits.append)
    assert html == "<pre>ok</pre>"
    assert len(calls) == 3 and waits == pytest.approx([7.0, 7.0], abs=0.1)


def test_listing_gives_up_with_throttled_error():
    import httpx
    from bls_corpus.pipeline import ThrottledError, fetch_listing
    waits = []
    transport = httpx.MockTransport(lambda r: httpx.Response(429))
    with pytest.raises(ThrottledError):
        fetch_listing("https://x/bd/", "test-agent", transport=transport,
                      max_retries=2, sleep=waits.append)
    assert len(waits) == 2 and waits[1] > waits[0]


def test_listing_404_is_not_retried():
    import httpx
    from bls_corpus.pipeline import fetch_listing
    waits = []
    transport = httpx.MockTransport(lambda r: httpx.Response(404))
    with pytest.raises(httpx.HTTPStatusError):
        fetch_listing("https://x/zz/", "test-agent", transport=transport, sleep=waits.append)
    assert waits == []


def test_listing_throttle_holds_back_the_shared_pacer():
    import httpx
    from bls_corpus.pipeline import RequestPacer, fetch_listing
    clock = {"t": 0.0}
    sent = []

    def sleep(s):
        clock["t"] += s

    def handler(request):
        sent.append(clock["t"])
        if len(sent) == 1:
            return httpx.Response(429, headers={"Retry-After": "30"})
        return httpx.Response(200, text="<pre>ok</pre>")

    pacer = RequestPacer(0.5, 10.0, sleep=sleep, clock=lambda: clock["t"])
    fetch_listing("https://x/bd/", "test-agent", transport=httpx.MockTransport(handler),
                  pacer=pacer)
    assert sent == [0.0, 30.0]
    pacer.wait()  # the next request anywhere in the run is spaced by the raised interval
    assert clock["t"] >= 30.0 + 0.5
