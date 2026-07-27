#!/usr/bin/env python3
"""Unit tests for the RPC client stub (Client).

The wire protocol is ``<length>\\n<pickled-payload>``. These tests use a fake
socket to exercise framing/parsing in ``_send`` and patch ``_send`` to verify
the higher-level RPC stubs build the right messages and interpret responses.
"""

import logging
import pickle

import pytest

from utils import create_event, frame_message
from DistributedCalendar.Client import Client


class FakeSocket:
    """Minimal stand-in for a connected socket.

    ``recv`` yields the supplied chunks in order (to exercise partial reads) and
    returns ``b""`` once exhausted, mimicking a closed connection.
    """

    def __init__(self, recv_chunks=()):
        self.sent = b""
        self._chunks = list(recv_chunks)
        self.closed = False

    def sendall(self, data):
        self.sent += data

    def recv(self, _bufsize):
        if self._chunks:
            return self._chunks.pop(0)
        return b""

    def shutdown(self, _how):
        pass

    def close(self):
        self.closed = True


def make_client(sock=None) -> Client:
    """Build a Client without running __init__ (no real connection)."""
    c = object.__new__(Client)
    c.peer_ident = "tester"
    c.target_host, c.target_port = "leader", 9000
    c.own_host, c.own_port = "self", 1234
    c.log = logging.getLogger("test.client")
    c.socket = sock
    return c


def response_bytes(payload: dict, split=False):
    framed = frame_message(pickle.dumps(payload))
    if not split:
        return [framed]
    # Split header from body to force multiple recv() calls.
    nl = framed.index(b"\n")
    return [framed[: nl + 1], framed[nl + 1 :]]


# --------------------------------------------------------------------------- #
# _send framing
# --------------------------------------------------------------------------- #
def test_send_round_trips_payload():
    sock = FakeSocket(response_bytes({"status": "success", "ident": "abc"}))
    client = make_client(sock)
    resp = client._send({"method": "create", "peer_ident": "tester", "params": {}})
    assert resp == {"status": "success", "ident": "abc"}
    # The request was framed with a length header.
    assert b"\n" in sock.sent


def test_send_handles_chunked_response():
    sock = FakeSocket(response_bytes({"status": "success"}, split=True))
    client = make_client(sock)
    assert client._send({"method": "x", "peer_ident": "t", "params": {}}) == {
        "status": "success"
    }


def test_send_raises_when_peer_closes_before_response():
    sock = FakeSocket(recv_chunks=[])  # recv immediately returns b""
    client = make_client(sock)
    with pytest.raises(ConnectionError):
        client._send({"method": "x", "peer_ident": "t", "params": {}})


def test_send_raises_when_sendall_fails():
    class BrokenSocket(FakeSocket):
        def sendall(self, data):
            raise OSError("broken pipe")

    client = make_client(BrokenSocket())
    with pytest.raises(ConnectionError):
        client._send({"method": "x", "peer_ident": "t", "params": {}})


# --------------------------------------------------------------------------- #
# RPC stubs (with _send patched)
# --------------------------------------------------------------------------- #
def test_create_returns_ident_on_success(mocker):
    client = make_client()
    mocker.patch.object(
        client, "_send", return_value={"status": "success", "ident": "id1"}
    )
    assert client.create("n", 1, 2, None, None, None) == "id1"


def test_create_returns_none_on_failure(mocker):
    client = make_client()
    mocker.patch.object(client, "_send", return_value={"status": "failure"})
    assert client.create("n", 1, 2, None, None, None) is None


def test_create_builds_expected_message(mocker):
    client = make_client()
    send = mocker.patch.object(
        client, "_send", return_value={"status": "success", "ident": "id1"}
    )
    client.create("standup", 10, 20, "desc", "loc", None)
    msg = send.call_args.args[0]
    assert msg["method"] == "create"
    assert msg["peer_ident"] == "tester"
    assert msg["params"]["name"] == "standup"
    assert msg["params"]["start"] == 10
    assert msg["params"]["location"] == "loc"


def test_modify_returns_ident_on_success(mocker):
    client = make_client()
    mocker.patch.object(
        client, "_send", return_value={"status": "success", "ident": "new"}
    )
    assert client.modify("old", "n", 1, 2, None, None, None) == "new"


def test_modify_returns_none_on_failure(mocker):
    client = make_client()
    mocker.patch.object(client, "_send", return_value={"status": "failure"})
    assert client.modify("old", "n", 1, 2, None, None, None) is None


def test_delete_sends_ident(mocker):
    client = make_client()
    send = mocker.patch.object(client, "_send", return_value={"status": "success"})
    assert client.delete("id1") is None
    assert send.call_args.args[0]["params"] == {"ident": "id1"}


def test_who_is_leader_returns_endpoint(mocker):
    client = make_client()
    mocker.patch.object(
        client, "_send", return_value={"status": "success", "host": "h", "port": 7}
    )
    assert client.who_is_leader() == ("h", 7)


def test_sync_returns_calendar_and_clock(mocker):
    client = make_client()
    events = {"id": create_event()}
    mocker.patch.object(
        client, "_send", return_value={"calendar": events, "logical_clock": 5}
    )
    cal, clock = client.sync()
    assert cal == events
    assert clock == 5


def test_coordinate_returns_clock(mocker):
    client = make_client()
    mocker.patch.object(client, "_send", return_value={"logical_clock": 11})
    assert client.coordinate() == 11


def test_coordinate_swallows_connection_error(mocker):
    client = make_client()
    mocker.patch.object(client, "_send", side_effect=ConnectionError)
    assert client.coordinate() == 0


def test_call_election_true_on_ok(mocker):
    client = make_client()
    mocker.patch.object(client, "_send", return_value={"status": "success"})
    assert client.call_election() is True


def test_call_election_false_on_connection_error(mocker):
    client = make_client()
    mocker.patch.object(client, "_send", side_effect=ConnectionError)
    assert client.call_election() is False


# --------------------------------------------------------------------------- #
# Socket lifecycle
# --------------------------------------------------------------------------- #
def test_create_socket_retries_then_raises(mocker):
    """Every connect attempt fails -> ConnectionError after MAX_RETRIES."""
    fake = mocker.MagicMock()
    fake.connect.side_effect = OSError("refused")
    mocker.patch("DistributedCalendar.Client.socket.socket", return_value=fake)
    mocker.patch("DistributedCalendar.Client.time.sleep")
    mocker.patch("DistributedCalendar.Client.random.randint", return_value=0)

    client = make_client()
    with pytest.raises(ConnectionError):
        client._create_socket()
    assert fake.connect.call_count == Client.MAX_RETRIES


def test_create_socket_succeeds_on_first_try(mocker):
    fake = mocker.MagicMock()
    fake.connect.return_value = None
    mocker.patch("DistributedCalendar.Client.socket.socket", return_value=fake)

    client = make_client()
    client._create_socket()
    fake.connect.assert_called_once_with((client.target_host, client.target_port))


def test_context_manager_closes_socket():
    sock = FakeSocket()
    client = make_client(sock)
    with client as c:
        assert c is client
    assert sock.closed is True
    assert client.socket is None
