#!/usr/bin/env python3
"""Unit tests for the durable persistence layer (PersistantCalendar).

Checkpoints and updates are pickled as a ``(events, logical_clock)`` tuple; the
transaction log is a stream of ``!I``-length-prefixed pickled ``Transaction``
records. These tests drive ``_restore`` / ``_log`` / ``_checkpoint`` directly.
"""

import pickle
import struct

from unittest.mock import MagicMock, mock_open

from utils import create_event, frame_transaction
from DistributedCalendar import Calendar
from DistributedCalendar import PersistantCalendar, Transaction


# --------------------------------------------------------------------------- #
# Construction
# --------------------------------------------------------------------------- #
def test_init_calls_restore(mocker):
    mock_restore = mocker.patch.object(
        PersistantCalendar, "_restore", return_value=MagicMock()
    )
    mocker.patch("builtins.open", mock_open())
    PersistantCalendar()
    mock_restore.assert_called_once()


def test_init_opens_txn_log(mocker):
    mocker.patch.object(PersistantCalendar, "_restore", return_value=MagicMock())
    mock_file = mocker.patch("builtins.open", mock_open())
    p = PersistantCalendar()
    mock_file.assert_called_once_with(p.TXN_LOG_PATH, "ab")


def test_init_txns_logged_starts_at_zero(mocker):
    mocker.patch.object(PersistantCalendar, "_restore", return_value=MagicMock())
    mocker.patch("builtins.open", mock_open())
    pht = PersistantCalendar()
    assert pht.txns_logged == 0


# --------------------------------------------------------------------------- #
# Write path: create / delete / modify update the log + logical clock
# --------------------------------------------------------------------------- #
def test_create_appends_txn_and_bumps_clock(calendar):
    ident = calendar.create(**create_event().__dict__)
    assert ident == create_event().hash()
    assert calendar.get_logical_clock() == 1
    assert calendar.txns_logged == 1
    assert ident in calendar.list_events()


def test_create_malformed_returns_none(calendar):
    assert calendar.create(**create_event(end=0).__dict__) is None
    # Logical clock still advances (the source increments before the None check).
    assert calendar.get_logical_clock() == 1
    assert calendar.txns_logged == 0


def test_delete_appends_txn(calendar):
    ident = calendar.create(**create_event().__dict__)
    calendar.delete(ident)
    assert ident not in calendar.list_events()
    assert calendar.txns_logged == 2


def test_modify_appends_txn(calendar):
    ident = calendar.create(**create_event().__dict__)
    new_ident = calendar.modify(ident, **create_event(name="renamed").__dict__)
    assert new_ident == create_event(name="renamed").hash()
    assert new_ident in calendar.list_events()
    assert ident not in calendar.list_events()


# --------------------------------------------------------------------------- #
# Transaction log reading
# --------------------------------------------------------------------------- #
def test_read_transaction(calendar, tmp_path) -> None:
    txn_log = tmp_path / "calendar.txns"
    txns = []
    with txn_log.open("wb") as f:
        for i in range(10):
            event = create_event(start=i)
            txn = Transaction("create", event.hash(), event)
            txns.append(txn)
            f.write(frame_transaction(txn))

    with txn_log.open("rb") as f:
        for i, txn in enumerate(calendar._read_transactions(f)):
            assert txn.method == txns[i].method
            assert txn.identifier == txns[i].identifier
            assert txn.event == txns[i].event


def test_read_transaction_skips_truncated_tail(calendar, tmp_path) -> None:
    """A partially-written trailing record (torn write) is skipped, not raised."""
    txn_log = tmp_path / "calendar.txns"
    event = create_event()
    good = frame_transaction(Transaction("create", event.hash(), event))
    with txn_log.open("wb") as f:
        f.write(good)
        f.write(struct.pack("!I", 9999))  # header promising bytes that never arrive
        f.write(b"partial")

    with txn_log.open("rb") as f:
        recovered = list(calendar._read_transactions(f))
    assert len(recovered) == 1


# --------------------------------------------------------------------------- #
# Restore
# --------------------------------------------------------------------------- #
def test_restore_from_checkpoint(calendar, tmp_path) -> None:
    ckpt = tmp_path / "calendar.ckpt"
    c = Calendar()
    for i in range(10):
        c.create(**create_event(start=i).__dict__)
    ckpt.write_bytes(pickle.dumps((c.events, 7)))

    calendar._restore()
    assert len(calendar.calendar.events) == 10
    assert calendar.get_logical_clock() == 7


def test_restore_prefers_update_over_checkpoint(calendar, tmp_path) -> None:
    """When an update file is present, restore loads it (and ignores the log)."""
    ckpt = tmp_path / "calendar.ckpt"
    update = tmp_path / "calendar.update"
    txn_log = tmp_path / "calendar.txns"

    ckpt.write_bytes(pickle.dumps(({}, 0)))

    update_cal = Calendar()
    update_cal.create(**create_event(name="from_update").__dict__)
    update.write_bytes(pickle.dumps((update_cal.events, 42)))

    # A stale txn the update should cause us to ignore.
    stale = create_event(name="stale")
    txn_log.write_bytes(frame_transaction(Transaction("create", stale.hash(), stale)))

    calendar._restore()
    assert calendar.get_logical_clock() == 42
    assert create_event(name="from_update").hash() in calendar.calendar.events
    assert stale.hash() not in calendar.calendar.events
    # The update file is consumed once applied.
    assert not update.exists()


def test_restore_replays_creates(calendar, tmp_path) -> None:
    txn_log = tmp_path / "calendar.txns"
    idents = []
    with txn_log.open("wb") as f:
        for i in range(10):
            event = create_event(start=i)
            idents.append(event.hash())
            f.write(frame_transaction(Transaction("create", event.hash(), event)))

    calendar._restore()
    assert len(calendar.calendar.events) == 10
    assert calendar.txns_logged == 10
    for ident in idents:
        assert ident in calendar.calendar.events


def test_restore_replays_creates_and_deletes(calendar, tmp_path) -> None:
    txn_log = tmp_path / "calendar.txns"
    idents = []
    with txn_log.open("wb") as f:
        for i in range(10):
            event = create_event(start=i)
            idents.append(event.hash())
            f.write(frame_transaction(Transaction("create", event.hash(), event)))
        for i in range(4):
            f.write(frame_transaction(Transaction("delete", idents[i], None)))

    calendar._restore()
    assert len(calendar.calendar.events) == 6
    assert calendar.txns_logged == 14
    for ident in idents[4:]:
        assert ident in calendar.calendar.events


def test_restore_replays_create_delete_modify(calendar, tmp_path) -> None:
    txn_log = tmp_path / "calendar.txns"
    idents, modifies = [], []
    with txn_log.open("wb") as f:
        for i in range(10):
            event = create_event(start=i)
            idents.append(event.hash())
            f.write(frame_transaction(Transaction("create", event.hash(), event)))
        for i in range(4):
            f.write(frame_transaction(Transaction("delete", idents[i], None)))
        for i in range(4, 10):
            event = create_event(start=10 + i)
            modifies.append(event.hash())
            f.write(frame_transaction(Transaction("modify", idents[i], event)))

    calendar._restore()
    assert len(calendar.calendar.events) == 6
    assert calendar.txns_logged == 20
    for ident in idents:
        assert ident not in calendar.calendar.events
    for ident in modifies:
        assert ident in calendar.calendar.events


def test_restore_recovers_stale_new_checkpoint(calendar, tmp_path, mocker) -> None:
    """A leftover ``.ckpt.new`` from a crash mid-checkpoint is removed and a fresh
    checkpoint is taken."""
    new_ckpt = tmp_path / "calendar.ckpt.new"
    new_ckpt.touch()

    chk = mocker.patch.object(PersistantCalendar, "_checkpoint", return_value=None)
    calendar._restore()
    chk.assert_called_once()
    assert not new_ckpt.exists()


# --------------------------------------------------------------------------- #
# Logging + checkpointing
# --------------------------------------------------------------------------- #
def test_log_writes_framed_transaction(calendar, tmp_path, mocker) -> None:
    txn_log = tmp_path / "calendar.txns"
    event = create_event()
    txn = Transaction("create", event.hash(), event)
    calendar._log(txn)

    assert calendar.txns_logged == 1
    assert txn_log.read_bytes() == frame_transaction(txn)

    # Crossing CKPT_THRESHOLD triggers a checkpoint.
    calendar.txns_logged = calendar.CKPT_THRESHOLD - 1
    chk = mocker.patch.object(PersistantCalendar, "_checkpoint", return_value=None)
    calendar._log(txn)
    chk.assert_called_once()


def test_checkpoint_writes_tuple_and_truncates_log(calendar, tmp_path) -> None:
    txn_log = tmp_path / "calendar.txns"
    with txn_log.open("wb") as f:
        for i in range(10):
            event = create_event(start=i)
            f.write(frame_transaction(Transaction("create", event.hash(), event)))

    c = Calendar()
    for i in range(10):
        c.create(**create_event(start=i).__dict__)
    calendar.calendar = c
    calendar._logical_clock = 99

    ckpt = tmp_path / "calendar.ckpt"
    new_ckpt = tmp_path / "calendar.ckpt.new"
    ckpt.write_bytes(b"stale...")

    calendar._checkpoint()

    assert ckpt.read_bytes() != b"stale..."
    assert txn_log.read_bytes() == b""
    assert not new_ckpt.exists()
    assert calendar.txns_logged == 0

    events, clock = pickle.loads(ckpt.read_bytes())
    assert clock == 99
    assert events == c.events
