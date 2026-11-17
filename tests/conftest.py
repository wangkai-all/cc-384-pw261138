"""Shared fixtures: real in-process HTTP services backed by temporary databases."""

from __future__ import annotations

import sys
import threading
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from crossdomain.coordinator import (SnapshotCoordinator, create_coordinator_server)  # noqa: E402
from crossdomain.domain_service import create_domain_server  # noqa: E402
from crossdomain.seed import ensure_seed  # noqa: E402


class ServiceHandle:
    def __init__(self, server, domain: str | None = None) -> None:
        self.server = server
        self.domain = domain
        self.port = server.server_address[1]
        self.thread = threading.Thread(target=server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


@pytest.fixture()
def fleet(tmp_path):
    """Two domain services + coordinator, each on its own ephemeral port."""
    seeds = ensure_seed(tmp_path / "data")
    handles = []
    try:
        orders = ServiceHandle(create_domain_server(
            "orders", tmp_path / "data" / "orders.sqlite3", seeds["orders"]))
        settlement = ServiceHandle(create_domain_server(
            "settlement", tmp_path / "data" / "settlement.sqlite3",
            seeds["settlement"]))
        handles += [orders, settlement]
        coordinator = ServiceHandle(create_coordinator_server(
            orders.url, settlement.url))
        handles.append(coordinator)
        yield {
            "orders": orders, "settlement": settlement,
            "coordinator": coordinator, "data": tmp_path,
            "coord": coordinator.server.coordinator,
        }
    finally:
        for handle in reversed(handles):
            handle.stop()


@pytest.fixture()
def two_stores(tmp_path):
    """Stores without HTTP for fast unit tests of business rules."""
    from crossdomain.domain_service import DomainStore
    seeds = ensure_seed(tmp_path / "data")
    orders = DomainStore("orders", tmp_path / "data" / "orders.sqlite3",
                         seeds["orders"])
    settlement = DomainStore("settlement",
                             tmp_path / "data" / "settlement.sqlite3",
                             seeds["settlement"])
    return orders, settlement


@pytest.fixture()
def coordinator_for_urls():
    def build(orders_url: str, settlement_url: str) -> SnapshotCoordinator:
        return SnapshotCoordinator({"orders": orders_url,
                                    "settlement": settlement_url})
    return build
