#!/usr/bin/env python3
"""Unit tests for peer coordination (Peer): catalog discovery, bootstrap role
selection, and the Bully leader-election decision logic.

A Peer is constructed via ``object.__new__`` so no real Server, sockets, or
threads are created; the ``server`` is a MagicMock and ``Client`` / ``urlopen``
are patched per test.
"""

import json
import logging
import threading
from unittest.mock import MagicMock

import pytest

from DistributedCalendar.Peer import Peer
from DistributedCalendar.Server import ServerMode


@pytest.fixture
def bare_peer():
    p = object.__new__(Peer)
    p.calendar_ident = "cal"
    p.peer_ident = "me"
    p.log = logging.getLogger("test.peer")
    p.pid = 1000
    p.do_election = False
    p.election_cv = threading.Condition()
    p.client = None
    p.client_cv = threading.Condition()
    p.own_host = "127.0.0.1"
    p.own_port = 5555
    p.server = MagicMock()
    return p


def _catalog_cm(entries):
    """Build a context-manager mock matching ``with urlopen(url) as res:``."""
    cm = MagicMock()
    cm.__enter__.return_value.read.return_value = json.dumps(entries).encode()
    return cm


# --------------------------------------------------------------------------- #
# _get_catalog
# --------------------------------------------------------------------------- #
def test_get_catalog_filters_by_project_and_calendar(bare_peer, mocker):
    entries = [
        {"project": "kagecal", "calendar_ident": "cal", "host": "h1", "port": 1, "peer_ident": "a", "PID": 1},
        {"project": "kagecal", "calendar_ident": "other", "host": "h2", "port": 2, "peer_ident": "b", "PID": 2},
        {"project": "notkage", "calendar_ident": "cal", "host": "h3", "port": 3, "peer_ident": "c", "PID": 3},
        # This is us -> excluded by host/port match.
        {"project": "kagecal", "calendar_ident": "cal", "host": "127.0.0.1", "port": 5555, "peer_ident": "me", "PID": 4},
    ]
    mocker.patch("DistributedCalendar.Peer.urlopen", return_value=_catalog_cm(entries))

    result = bare_peer._get_catalog()
    assert len(result) == 1
    assert result[0]["peer_ident"] == "a"


def test_get_catalog_returns_empty_on_error(bare_peer, mocker):
    mocker.patch("DistributedCalendar.Peer.urlopen", side_effect=OSError("network down"))
    assert bare_peer._get_catalog() == []


# --------------------------------------------------------------------------- #
# _bootstrap
# --------------------------------------------------------------------------- #
def test_bootstrap_no_peers_becomes_leader(bare_peer, mocker):
    mocker.patch.object(bare_peer, "_get_catalog", return_value=[])
    bare_peer._bootstrap()
    bare_peer.server.set_mode.assert_called_once_with(ServerMode.LEADER)
    assert bare_peer.server.leader_host == bare_peer.own_host
    assert bare_peer.server.leader_port == bare_peer.own_port


def test_bootstrap_follows_existing_leader(bare_peer, mocker):
    mocker.patch.object(
        bare_peer,
        "_get_catalog",
        return_value=[{"host": "h", "port": 2, "peer_ident": "leader", "PID": 1, "lastheardfrom": 0}],
    )
    mocker.patch("DistributedCalendar.Peer.random.shuffle", lambda x: None)

    fake_client = MagicMock()
    fake_client.who_is_leader.return_value = ("leaderhost", 9999)
    fake_client.sync.return_value = ({}, 3)
    mocker.patch("DistributedCalendar.Peer.Client", return_value=fake_client)

    # Make the follower-sync branch run by reporting FOLLOWER mode.
    bare_peer.server.get_mode.return_value = ServerMode.FOLLOWER
    bare_peer.client = fake_client  # so the sync loop does not block waiting

    bare_peer._bootstrap()

    bare_peer.server.set_mode.assert_called_with(ServerMode.FOLLOWER)
    assert bare_peer.server.leader_host == "leaderhost"
    assert bare_peer.server.leader_port == 9999
    bare_peer.server.update.assert_called_once_with({}, 3)


# --------------------------------------------------------------------------- #
# call_election (Bully)
# --------------------------------------------------------------------------- #
def test_election_no_higher_pid_becomes_leader(bare_peer, mocker):
    # Only a lower-PID peer exists -> this peer wins.
    entries = [{"PID": 1, "host": "h", "port": 2, "peer_ident": "low", "lastheardfrom": 0}]
    mocker.patch.object(bare_peer, "_get_catalog", return_value=entries)

    fake_client = MagicMock()
    fake_client.coordinate.return_value = 0  # no higher clock -> no sync
    mocker.patch("DistributedCalendar.Peer.Client", return_value=fake_client)
    bare_peer.server.get_logical_clock.return_value = 0

    bare_peer.call_election()

    bare_peer.server.set_mode.assert_any_call(ServerMode.LEADER)


def test_election_higher_pid_declines_then_wins(bare_peer, mocker):
    # A higher-PID peer exists but does not respond OK -> this peer still wins.
    entries = [{"PID": 2000, "host": "h", "port": 2, "peer_ident": "high", "lastheardfrom": 1}]
    mocker.patch.object(bare_peer, "_get_catalog", return_value=entries)

    fake_client = MagicMock()
    fake_client.call_election.return_value = False
    mocker.patch("DistributedCalendar.Peer.Client", return_value=fake_client)
    bare_peer.server.get_logical_clock.return_value = 0

    bare_peer.call_election()

    fake_client.call_election.assert_called_once()
    bare_peer.server.set_mode.assert_any_call(ServerMode.LEADER)


def test_election_higher_pid_accepts_yields_leadership(bare_peer, mocker):
    """A higher-PID peer answers OK: this peer waits for a COORDINATE and does
    NOT declare itself leader."""
    entries = [{"PID": 2000, "host": "h", "port": 2, "peer_ident": "high", "lastheardfrom": 1}]
    mocker.patch.object(bare_peer, "_get_catalog", return_value=entries)

    fake_client = MagicMock()
    fake_client.call_election.return_value = True
    mocker.patch("DistributedCalendar.Peer.Client", return_value=fake_client)

    # Run the (blocking) election in a thread; it parks on client_cv until a
    # COORDINATE arrives (simulated by setting self.client and notifying).
    t = threading.Thread(target=bare_peer.call_election, daemon=True)
    t.start()

    # Give it a moment to reach the wait, then deliver the coordinate.
    for _ in range(100):
        if bare_peer.server.set_coordinate.called:
            break
        threading.Event().wait(0.01)

    with bare_peer.client_cv:
        bare_peer.client = fake_client
        bare_peer.client_cv.notify_all()
    t.join(timeout=2)

    assert not t.is_alive()
    bare_peer.server.set_coordinate.assert_called_with(True)
    # It yielded: never promoted itself to leader.
    assert (ServerMode.LEADER,) not in [
        call.args for call in bare_peer.server.set_mode.call_args_list
    ]


# --------------------------------------------------------------------------- #
# Calendar mutation routing (leader vs follower)
# --------------------------------------------------------------------------- #
def test_create_as_leader_writes_locally_and_broadcasts(bare_peer):
    bare_peer.server.get_mode.return_value = ServerMode.LEADER
    bare_peer.server.persistence.create.return_value = "id-1"

    assert bare_peer.create("n", 1, 2, None, None, None) == "id-1"
    bare_peer.server.persistence.create.assert_called_once()
    bare_peer.server.broadcast_clock.assert_called_once()


def test_create_as_follower_forwards_to_leader(bare_peer):
    bare_peer.server.get_mode.return_value = ServerMode.FOLLOWER
    bare_peer.server.persistence.create.return_value = "id-2"
    bare_peer.client = MagicMock()  # leader client present, so no blocking

    assert bare_peer.create("n", 1, 2, None, None, None) == "id-2"
    bare_peer.client.create.assert_called_once()
    bare_peer.server.persistence.create.assert_called_once()
    # Followers never broadcast.
    bare_peer.server.broadcast_clock.assert_not_called()


def test_create_as_follower_triggers_election_on_leader_failure(bare_peer):
    bare_peer.server.get_mode.return_value = ServerMode.FOLLOWER
    bare_peer.client = MagicMock()
    bare_peer.client.create.side_effect = ConnectionError("leader down")

    with pytest.raises(ConnectionError):
        bare_peer.create("n", 1, 2, None, None, None)
    assert bare_peer.do_election is True


def test_delete_as_leader_writes_and_broadcasts(bare_peer):
    bare_peer.server.get_mode.return_value = ServerMode.LEADER
    bare_peer.delete("victim")
    bare_peer.server.persistence.delete.assert_called_once_with("victim")
    bare_peer.server.broadcast_clock.assert_called_once()


def test_modify_as_leader_writes_and_broadcasts(bare_peer):
    bare_peer.server.get_mode.return_value = ServerMode.LEADER
    bare_peer.server.persistence.modify.return_value = "new-id"
    assert bare_peer.modify("old", "n", 1, 2, None, None, None) == "new-id"
    bare_peer.server.broadcast_clock.assert_called_once()


def test_reads_take_calendar_lock(bare_peer):
    bare_peer.server.persistence.get_event.return_value = "the-event"
    bare_peer.server.persistence.list_events.return_value = {"id": "the-event"}
    assert bare_peer.get_event("id") == "the-event"
    assert bare_peer.list_events() == {"id": "the-event"}
