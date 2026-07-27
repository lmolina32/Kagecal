#!/usr/bin/env python3
"""End-to-end integration tests.

A real ``Server`` (in LEADER mode, catalog daemon stubbed) serves on an
ephemeral port in a background thread, driven by a real ``Client`` over a
loopback TCP socket. These exercise the full RPC framing + dispatch + persistence
path. Marked ``integration`` so they can be skipped with ``-m "not integration"``.
"""

import pytest

from DistributedCalendar.Server import ServerMode
from DistributedCalendar.PersistantCalendar import PersistantCalendar

pytestmark = pytest.mark.integration


def test_create_then_sync_round_trip(client_to, live_server):
    client = client_to()
    ident = client.create("meeting", 100, 200, None, None, None)
    assert ident is not None

    events, clock = client.sync()
    assert ident in events
    assert clock >= 1
    assert events[ident].name == "meeting"


def test_modify_replaces_event(client_to):
    client = client_to()
    ident = client.create("draft", 100, 200, None, None, None)
    new_ident = client.modify(ident, "final", 100, 200, None, None, None)
    assert new_ident is not None

    events, _ = client.sync()
    assert new_ident in events
    assert ident not in events
    assert events[new_ident].name == "final"


def test_delete_removes_event(client_to):
    client = client_to()
    ident = client.create("temp", 100, 200, None, None, None)
    client.delete(ident)

    events, _ = client.sync()
    assert ident not in events


def test_who_is_leader_reports_self(client_to, live_server):
    client = client_to()
    assert client.who_is_leader() == (live_server.host, live_server.port)


def test_logical_clock_advances_with_writes(client_to):
    client = client_to()
    _, clock_before = client.sync()
    client.create("e1", 1, 2, None, None, None)
    client.create("e2", 3, 4, None, None, None)
    _, clock_after = client.sync()
    assert clock_after > clock_before


def test_follower_redirects_writes(client_to, live_server):
    live_server.set_mode(ServerMode.FOLLOWER)
    client = client_to()
    # In follower mode a write is answered with a redirect, so the stub returns None.
    assert client.create("x", 1, 2, None, None, None) is None


def test_state_survives_restart(client_to, live_server, tmp_path):
    """After writes, a brand-new persistence instance over the same files
    recovers the committed events by replaying the transaction log."""
    client = client_to()
    ident = client.create("durable", 100, 200, None, None, None)
    assert ident is not None

    # live_server chdir'd into tmp_path and uses these file names.
    recovered = PersistantCalendar("l.ckpt", "l.txn", "l.update")
    try:
        assert ident in recovered.list_events()
    finally:
        recovered.txn_log_file.close()
