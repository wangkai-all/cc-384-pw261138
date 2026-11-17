"""Small stdlib-only helpers shared by the services.

The demo deliberately has no third-party runtime dependency.  Every component is
an ordinary HTTP service so it can be stopped, restarted and probed
independently.
"""

from __future__ import annotations

import json
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


# ---------------------------------------------------------------------------
# Time / ids
# ---------------------------------------------------------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def epoch_now() -> float:
    return time.time()


_BATCH_SEQ = re.compile(r"(\d+)$")


def batch_sequence(name: str) -> int:
    """Order business batches such as BIZ-2026-11-07.

    Falls back to 0 for names without a numeric suffix.  Callers always use the
    pair (sequence, name) so the ordering remains deterministic.
    """
    match = _BATCH_SEQ.search(name or "")
    return int(match.group(1)) if match else 0


def batch_sort_key(name: str) -> tuple[int, str]:
    return (batch_sequence(name), name or "")


# ---------------------------------------------------------------------------
# JSON HTTP helpers
# ---------------------------------------------------------------------------

class UpstreamError(Exception):
    """A remote service was reachable but answered non-2xx."""

    def __init__(self, url: str, status: int, payload: Any | None = None,
                 message: str = "") -> None:
        self.url = url
        self.status = status
        self.payload = payload
        self.message = message or f"upstream returned HTTP {status}"
        super().__init__(self.message)


class UpstreamUnavailable(UpstreamError):
    """A remote service could not be reached / timed out / spoke garbage."""

    def __init__(self, url: str, message: str) -> None:
        super().__init__(url, 0, None, message)


def http_json(method: str, url: str, payload: Any | None = None,
              headers: dict[str, str] | None = None,
              timeout: float = 2.0) -> tuple[int, Any, dict[str, str]]:
    """Perform one real HTTP request and decode a JSON response.

    Returns (status, parsed_body, response_headers).  Raises UpstreamError for
    HTTP errors (including 4xx/5xx) and UpstreamUnavailable for transport
    failures so callers can keep a structured distinction.
    """
    body = None
    request_headers = {"Accept": "application/json"}
    if headers:
        request_headers.update(headers)
    if payload is not None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request_headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=body, method=method,
                                     headers=request_headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            parsed = json.loads(raw.decode("utf-8")) if raw else None
            return response.status, parsed, dict(response.headers.items())
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        try:
            parsed_body = json.loads(raw.decode("utf-8")) if raw else None
        except (ValueError, UnicodeDecodeError):
            parsed_body = {"raw": raw.decode("utf-8", "replace")}
        raise UpstreamError(url, exc.code, parsed_body,
                            f"upstream returned HTTP {exc.code}") from exc
    except (urllib.error.URLError, TimeoutError, ConnectionError, OSError,
            ValueError) as exc:
        raise UpstreamUnavailable(url, f"{type(exc).__name__}: {exc}") from exc


def append_query(url: str, **params: Any) -> str:
    query = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
    if not query:
        return url
    return url + ("&" if "?" in url else "?") + query


class JsonRequestHandler(BaseHTTPRequestHandler):
    """Base class with JSON request/response conveniences."""

    server_version = "CrossdomainDemo/1.0"
    verbose = False

    # -- logging -----------------------------------------------------------
    def log_message(self, fmt: str, *args: Any) -> None:  # noqa: D401
        if self.verbose:
            super().log_message(fmt, *args)

    def log_error(self, fmt: str, *args: Any) -> None:
        # Keep infrastructure noise out of the demo; handlers return JSON errors.
        if self.verbose:
            super().log_error(fmt, *args)

    # -- responses ---------------------------------------------------------
    def send_json(self, status: int, payload: Any,
                  extra_headers: dict[str, str] | None = None) -> None:
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        if extra_headers:
            for key, value in extra_headers.items():
                self.send_header(key, str(value))
        self.end_headers()
        try:
            self.wfile.write(raw)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def send_error_json(self, status: int, code: str, message: str,
                        **details: Any) -> None:
        payload = {"error": code, "message": message, "status": status}
        payload.update(details)
        self.send_json(status, payload)

    # -- requests ----------------------------------------------------------
    def read_json(self, max_bytes: int = 4 * 1024 * 1024) -> Any:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0
        if length < 0 or length > max_bytes:
            raise ValueError("request body too large")
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as exc:
            raise ValueError(f"invalid JSON body: {exc}") from exc
        if not isinstance(parsed, dict):
            # Every API in this demo takes an object; rejecting other shapes
            # here yields a 400 instead of an AttributeError deep in a handler.
            raise ValueError("request body must be a JSON object")
        return parsed

    # -- routing -----------------------------------------------------------
    def path_parts(self) -> list[str]:
        path = urllib.parse.urlparse(self.path).path
        return [urllib.parse.unquote(part) for part in path.strip("/").split("/") if part]

    def query_params(self) -> dict[str, list[str]]:
        return urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)

    def query_one(self, name: str, default: str | None = None) -> str | None:
        values = self.query_params().get(name)
        return values[0] if values else default

    def header_or_query(self, name: str, query_name: str | None = None) -> str | None:
        value = self.headers.get(name)
        if value:
            return value.strip()
        return self.query_one(query_name or name.lower())


class ServiceHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def serve_in_thread(server: ServiceHTTPServer) -> threading.Thread:
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return thread
