#!/usr/bin/env python3
"""Shared pytest fixtures for the Kagecal test harness.

Fixtures are grouped by the layer they support:

* ``filled_in_calendar`` / ``calendar``  -> in-memory + persistence unit tests
* ``persistence_factory``                -> persistence durability / restore tests
* ``bare_server``                        -> Server RPC-handler unit tests (no sockets)
* ``live_server`` / ``client_to``        -> end-to-end integration tests
"""

import logging
import selectors
import threading
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from DistributedCalendar.Calendar import Calendar
from DistributedCalendar.PersistantCalendar import PersistantCalendar
from DistributedCalendar.Server import Server, ServerMode
from DistributedCalendar.Client import Client

from utils import create_event


@pytest.fixture(autouse=True)
def _silence_logging():
    """Keep the chatty module loggers from flooding test output."""
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)


# --------------------------------------------------------------------------- #
# Calendar / persistence fixtures
# --------------------------------------------------------------------------- #
@pytest.fixture
def filled_in_calendar() -> Calendar:
    """An in-memory calendar pre-populated with three distinct events."""
    calendar = Calendar()
    calendar.create(**create_event().__dict__)
    calendar.create(**create_event(name="new").__dict__)
    calendar.create(**create_event(name="new1").__dict__)
    return calendar


@pytest.fixture
def calendar(mocker, tmp_path):
    """A ``PersistantCalendar`` whose on-disk files live directly in ``tmp_path``.

    ``_restore`` is stubbed during construction so the constructor does not touch
    the real working directory; afterwards every path attribute is repointed at
    ``tmp_path`` and a fresh, empty in-memory calendar is installed.
    """
    mocker.patch.object(Path, "cwd", return_value=tmp_path)
    restore = mocker.patch.object(PersistantCalendar, "_restore", return_value=None)
    pc = PersistantCalendar()
    mocker.stop(restore)

    pc.CKPT_PATH = str(tmp_path / "calendar.ckpt")
    pc.NEW_CKPT_PATH = str(tmp_path / "calendar.ckpt.new")
    pc.TXN_LOG_PATH = str(tmp_path / "calendar.txns")
    pc.UPDATE_PATH = str(tmp_path / "calendar.update")
    pc.NEW_UPDATE_PATH = str(tmp_path / "calendar.update.new")
    pc.txn_log_file.close()
    pc.txn_log_file = open(pc.TXN_LOG_PATH, "ab")
    pc.calendar = Calendar()
    pc._logical_clock = 0
    pc.txns_logged = 0
    return pc


@pytest.fixture
def persistence_factory(tmp_path, monkeypatch):
    """Factory that builds *real* ``PersistantCalendar`` instances rooted at a
    private temp dir. Useful for exercising durability: build one, mutate it,
    then build a second over the same files to assert recovery."""
    monkeypatch.chdir(tmp_path)
    created = []

    def _make(name: str = "cal") -> PersistantCalendar:
        pc = PersistantCalendar(f"{name}.ckpt", f"{name}.txns", f"{name}.update")
        created.append(pc)
        return pc

    yield _make

    for pc in created:
        try:
            pc.txn_log_file.close()
        except Exception:
            pass


# --------------------------------------------------------------------------- #
# Server fixtures
# --------------------------------------------------------------------------- #
@pytest.fixture
def bare_server() -> Server:
    """A ``Server`` instance built without running ``__init__``.

    No sockets, threads, or persistence are created. ``persistence`` is a
    ``MagicMock`` so RPC handlers can be tested in complete isolation.
    """
    srv = object.__new__(Server)
    srv.log = logging.getLogger("test.server")
    srv.persistence = MagicMock()
    srv.calendar_ident = "cal"
    srv.peer_ident = "peer"
    srv.host, srv.port = "10.0.0.1", 5000
    srv.leader_host, srv.leader_port = "10.0.0.2", 6000
    srv.mode = ServerMode.FOLLOWER
    srv.mode_lock = threading.Lock()
    srv.calendar_lock = threading.Lock()
    srv.coordinate = False
    # Attributes that Server.__del__ touches, so GC of this bare instance is clean.
    srv.stop = threading.Event()
    srv.threads = []
    srv.sock_selector = selectors.DefaultSelector()
    srv.RPC_METHODS = {
        "create": srv._create,
        "delete": srv._delete,
        "modify": srv._modify,
        "who_is_leader": srv._who_is_leader,
        "sync": srv._sync,
        "coordinate": srv._coordinate,
        "election": srv._election,
    }
    return srv


@pytest.fixture
def live_server(tmp_path, monkeypatch, mocker) -> Server:
    """A real ``Server`` bound to an ephemeral port, serving in a background
    thread, in LEADER mode. The ND catalog daemon is stubbed out so no traffic
    leaves the machine."""
    monkeypatch.chdir(tmp_path)
    mocker.patch.object(Server, "_name_server", lambda self: None)

    srv = Server(
        calendar_ident="itest",
        peer_ident="leader",
        ckpt_path="l.ckpt",
        txn_path="l.txn",
        update_path="l.update",
        leader_host="",
        leader_port=0,
    )
    srv.set_mode(ServerMode.LEADER)
    srv.leader_host, srv.leader_port = srv.host, srv.port

    stop = threading.Event()

    def _loop():
        while not stop.is_set():
            srv.serve()

    thread = threading.Thread(target=_loop, daemon=True)
    thread.start()

    yield srv

    stop.set()
    thread.join(timeout=2)

    # Server.__del__ mis-iterates its selector map and raises during GC, which
    # surfaces as a noisy PytestUnraisableExceptionWarning. Close the real
    # sockets here and swap in an empty selector so GC stays quiet.
    try:
        for key in list(srv.sock_selector.get_map().values()):
            srv._close_socket(key.fileobj)
        srv.sock_selector.close()
    except Exception:
        pass
    srv.sock_selector = selectors.DefaultSelector()


@pytest.fixture
def client_to(live_server):
    """Factory that returns ``Client`` instances connected to ``live_server``."""
    clients = []

    def _connect(peer: str = "client") -> Client:
        c = Client(peer, live_server.host, live_server.port, "127.0.0.1", 0)
        clients.append(c)
        return c

    yield _connect

    for c in clients:
        try:
            c._socket_close()
        except Exception:
            pass
