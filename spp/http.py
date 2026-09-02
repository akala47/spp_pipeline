"""HTTP extraction — the Selenium replacement.

The old scrapers drove a headless browser to click a download link, which cost
seconds, wrote to /tmp, and failed silently when the click missed. Every SPP
file we need is a plain GET behind a stable URL, so this module is a pooled
`requests` session with bounded retries and one meaningful distinction:

    FileNotPublished   -> SPP hasn't posted this interval yet (404/403).
                          Not an error; the caller skips or retries later.
    requests exceptions -> we are broken (DNS, TLS, 5xx after retries).

Keeping those apart is what makes the failure-rate metric worth alerting on.
Retries cover transient 5xx and connection resets only; a 404 is returned
immediately rather than burning the retry budget on a file that does not exist.
"""
from __future__ import annotations

import threading
from typing import Iterable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import settings
from .logging_setup import get_logger

log = get_logger("spp.http")

USER_AGENT = "spp-pipeline/2.0 (+data-eng)"

_local = threading.local()


class FileNotPublished(Exception):
    """SPP has not posted the requested file (yet)."""


def session() -> requests.Session:
    """One pooled session per thread; connection reuse is most of the speedup."""
    existing = getattr(_local, "session", None)
    if existing is not None:
        return existing
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    retry = Retry(
        total=settings.http_retries,
        connect=settings.http_retries,
        read=settings.http_retries,
        status=settings.http_retries,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=8, pool_maxsize=8)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    _local.session = s
    return s


def fetch_csv(url: str) -> bytes:
    """GET a CSV. Raises FileNotPublished on 404/403, HTTPError on anything else."""
    resp = session().get(url, timeout=settings.http_timeout)
    if resp.status_code in (403, 404):
        log.warning("file not published",
                    extra={"event": "extract.not_published",
                           "url": url, "status": resp.status_code})
        raise FileNotPublished(url)
    resp.raise_for_status()
    body = resp.content
    if not body.strip():
        raise FileNotPublished(f"{url} (empty body)")
    log.info("fetched", extra={"event": "extract.ok", "url": url,
                               "bytes": len(body)})
    return body


def probe(urls: Iterable[str]) -> dict[str, int]:
    """HEAD each URL and return {url: status}. Used by the availability sensor."""
    out: dict[str, int] = {}
    for url in urls:
        try:
            resp = session().head(url, timeout=settings.http_timeout,
                                  allow_redirects=True)
            out[url] = resp.status_code
        except requests.RequestException as exc:
            log.warning("probe failed",
                        extra={"event": "extract.probe_error",
                               "url": url, "error": str(exc)})
            out[url] = 0
    return out


def close() -> None:
    """Release the thread's session. Airflow tasks call this in a finally block."""
    existing = getattr(_local, "session", None)
    if existing is not None:
        existing.close()
        _local.session = None
