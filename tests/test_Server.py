#!/usr/bin/env python3
"""Unit tests for the RPC server (Server).

These exercise the handler methods and the dispatch logic in ``_handle_rpc`` /
``_handle_broadcast`` using the ``bare_server`` fixture (a Server built without
sockets/threads, with a mocked persistence layer).
"""

import json

import pytest

from utils import unframe_message
from DistributedCalendar.Server import ServerMode, ServerFlags


class FakeClientSock:
    """Captures bytes written by Server when acking an RPC."""

    def __init__(self, peer=("1.2.3.4", 1111)):
        self.sent = b""
        self._peer = peer

    def getpeername(self):
        return self._peer

    def sendall(self, data):
        self.sent += data


class FakeReceiver:
    """A datagram socket stand-in returning a single payload from recvfrom."""

    def __init__(self, data):
        self._data = data

    def recvfrom(self, _maxlen):
        return self._data, ("broadcast-src", 0)


# --------------------------------------------------------------------------- #
# RPC handlers
# --------------------------------------------------------------------------- #
def test_create_success(bare_server):
    bare_server.persistence.create.return_value = "ident-1"
    resp, flags = bare_server._create("create", {"name": "n", "start": 1, "end": 2})
    assert resp == {"method": "create", "status": "success", "ident": "ident-1"}
    assert flags == ServerFlags.NONE


def test_create_raises_when_rejected(bare_server):
    bare_server.persistence.create.return_value = None
    with pytest.raises(ValueError):
        bare_server._create("create", {"name": "n", "start": 2, "end": 1})


def test_delete_success(bare_server):
    resp, flags = bare_server._delete("delete", {"ident": "x"})
    assert resp == {"method": "delete", "status": "success"}
    assert flags == ServerFlags.NONE
    bare_server.persistence.delete.assert_called_once_with(ident="x")


def test_modify_success(bare_server):
    bare_server.persistence.modify.return_value = "new-id"
    resp, _ = bare_server._modify("modify", {"ident": "old", "name": "n", "start": 1, "end": 2})
    assert resp["status"] == "success"
    assert resp["ident"] == "new-id"


def test_modify_raises_when_rejected(bare_server):
    bare_server.persistence.modify.return_value = None
    with pytest.raises(ValueError):
        bare_server._modify("modify", {"ident": "old", "name": "n", "start": 1, "end": 2})


def test_who_is_leader_as_leader(bare_server):
    bare_server.mode = ServerMode.LEADER
    resp, _ = bare_server._who_is_leader("who_is_leader", {})
    assert resp["host"] == bare_server.host
    assert resp["port"] == bare_server.port


def test_who_is_leader_as_follower(bare_server):
    bare_server.mode = ServerMode.FOLLOWER
    resp, _ = bare_server._who_is_leader("who_is_leader", {})
    assert resp["host"] == bare_server.leader_host
    assert resp["port"] == bare_server.leader_port


def test_sync_returns_state(bare_server):
    events = {"id": "event"}
    bare_server.persistence.list_events.return_value = events
    bare_server.persistence.get_logical_clock.return_value = 9
    resp, _ = bare_server._sync("sync", {})
    assert resp["calendar"] == events
    assert resp["logical_clock"] == 9


def test_coordinate_demotes_to_follower(bare_server):
    bare_server.mode = ServerMode.LEADER
    bare_server.persistence.get_logical_clock.return_value = 3
    resp, flags = bare_server._coordinate("coordinate", {"host": "h", "port": 7})
    assert bare_server.mode == ServerMode.FOLLOWER
    assert bare_server.leader_host == "h"
    assert bare_server.leader_port == 7
    assert resp["logical_clock"] == 3
    assert flags == ServerFlags.NEW_LEADER


def test_election_sets_do_election_flag(bare_server):
    resp, flags = bare_server._election("election", {})
    assert resp["status"] == "success"
    assert flags == ServerFlags.DO_ELECTION


# --------------------------------------------------------------------------- #
# _handle_rpc dispatch
# --------------------------------------------------------------------------- #
def _rpc(method, params=None, peer="caller"):
    return {"method": method, "peer_ident": peer, "params": params or {}}


def test_follower_redirects_write(bare_server, mocker):
    bare_server.mode = ServerMode.FOLLOWER
    mocker.patch.object(bare_server, "_get_rpc", return_value=_rpc("create"))
    sock = FakeClientSock()
    bare_server._handle_rpc(sock)
    resp = unframe_message(sock.sent)
    assert resp["status"] == "redirect"
    assert resp["host"] == bare_server.leader_host
    assert resp["port"] == bare_server.leader_port


def test_follower_answers_who_is_leader(bare_server, mocker):
    bare_server.mode = ServerMode.FOLLOWER
    mocker.patch.object(bare_server, "_get_rpc", return_value=_rpc("who_is_leader"))
    sock = FakeClientSock()
    bare_server._handle_rpc(sock)
    resp = unframe_message(sock.sent)
    assert resp["status"] == "success"
    assert resp["host"] == bare_server.leader_host


def test_leader_create_acks_and_broadcasts(bare_server, mocker):
    bare_server.mode = ServerMode.LEADER
    bare_server.persistence.create.return_value = "abc"
    mocker.patch.object(
        bare_server,
        "_get_rpc",
        return_value=_rpc(
            "create",
            {"name": "n", "start": 1, "end": 2, "description": None, "location": None, "repeats": None},
        ),
    )
    bcast = mocker.patch.object(bare_server, "broadcast_clock")
    sock = FakeClientSock()
    bare_server._handle_rpc(sock)
    resp = unframe_message(sock.sent)
    assert resp["status"] == "success"
    assert resp["ident"] == "abc"
    bcast.assert_called_once()


def test_leader_create_failure_acks_failure(bare_server, mocker):
    bare_server.mode = ServerMode.LEADER
    bare_server.persistence.create.return_value = None  # -> handler raises ValueError
    mocker.patch.object(bare_server, "_get_rpc", return_value=_rpc("create", {"name": "n", "start": 2, "end": 1}))
    mocker.patch.object(bare_server, "broadcast_clock")
    sock = FakeClientSock()
    bare_server._handle_rpc(sock)
    resp = unframe_message(sock.sent)
    assert resp["status"] == "failure"
    assert "error" in resp


def test_coordinate_gate_blocks_other_methods(bare_server, mocker):
    bare_server.coordinate = True
    mocker.patch.object(bare_server, "_get_rpc", return_value=_rpc("create"))
    sock = FakeClientSock()
    bare_server._handle_rpc(sock)
    assert unframe_message(sock.sent)["status"] == "coordinate"


def test_coordinate_gate_allows_coordinate(bare_server, mocker):
    bare_server.coordinate = True
    bare_server.persistence.get_logical_clock.return_value = 4
    mocker.patch.object(
        bare_server, "_get_rpc", return_value=_rpc("coordinate", {"host": "h", "port": 8})
    )
    sock = FakeClientSock()
    flags = bare_server._handle_rpc(sock)
    resp = unframe_message(sock.sent)
    assert resp["status"] == "success"
    assert resp["logical_clock"] == 4
    assert bare_server.coordinate is False
    assert flags & ServerFlags.NEW_LEADER


def test_handle_rpc_drops_socket_on_bad_request(bare_server, mocker):
    mocker.patch.object(bare_server, "_get_rpc", side_effect=ValueError("torn"))
    sel = mocker.MagicMock()
    sel.get_map.return_value = {}
    bare_server.sock_selector = sel
    mocker.patch.object(bare_server, "_close_socket")
    sock = FakeClientSock()
    flags = bare_server._handle_rpc(sock)
    assert flags == ServerFlags.NONE
    bare_server.sock_selector.unregister.assert_called_once_with(sock)


# --------------------------------------------------------------------------- #
# _handle_broadcast
# --------------------------------------------------------------------------- #
def _broadcast(server, **overrides):
    msg = {
        "calendar_ident": server.calendar_ident,
        "host": server.leader_host,
        "port": server.leader_port,
        "logical_clock": 5,
    }
    msg.update(overrides)
    return FakeReceiver(json.dumps(msg).encode())


def test_broadcast_from_leader_higher_clock_requests_sync(bare_server):
    bare_server.persistence.get_logical_clock.return_value = 1
    assert bare_server._handle_broadcast(_broadcast(bare_server)) == ServerFlags.DO_SYNC


def test_broadcast_from_leader_equal_clock_is_noop(bare_server):
    bare_server.persistence.get_logical_clock.return_value = 5
    assert bare_server._handle_broadcast(_broadcast(bare_server)) == 0


def test_broadcast_wrong_calendar_ignored(bare_server):
    bare_server.persistence.get_logical_clock.return_value = 0
    recv = _broadcast(bare_server, calendar_ident="some-other-calendar")
    assert bare_server._handle_broadcast(recv) == 0


def test_broadcast_from_self_ignored(bare_server):
    bare_server.persistence.get_logical_clock.return_value = 0
    recv = _broadcast(bare_server, host=bare_server.host, port=bare_server.port)
    assert bare_server._handle_broadcast(recv) == 0


def test_broadcast_from_unknown_node_ignored(bare_server):
    bare_server.persistence.get_logical_clock.return_value = 0
    recv = _broadcast(bare_server, host="9.9.9.9", port=42)
    assert bare_server._handle_broadcast(recv) == 0


def test_broadcast_invalid_json_ignored(bare_server):
    assert bare_server._handle_broadcast(FakeReceiver(b"not json")) == 0


# --------------------------------------------------------------------------- #
# State accessors
# --------------------------------------------------------------------------- #
def test_set_and_get_mode(bare_server):
    bare_server.set_mode(ServerMode.LEADER)
    assert bare_server.get_mode() == ServerMode.LEADER


def test_set_coordinate(bare_server):
    bare_server.set_coordinate(True)
    assert bare_server.coordinate is True
    bare_server.set_coordinate(False)
    assert bare_server.coordinate is False
