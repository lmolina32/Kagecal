#!/usr/bin/env python3

import pytest
import pickle
import struct
from unittest.mock import MagicMock, mock_open, patch

from utils import create_event, create_repeat
from DistributedCalendar.Calendar import Calendar
from DistributedCalendar.PersistantCalendar import PersistantHashTable, Transaction


def test_init_calls_restore(mocker):
    mock_restore = mocker.patch.object(
        PersistantHashTable, "_restore", return_value=MagicMock()
    )
    mocker.patch("builtins.open", mock_open())
    PersistantHashTable()
    mock_restore.assert_called_once()


def test_init_opens_txn_log(mocker):
    mocker.patch.object(PersistantHashTable, "_restore", return_value=MagicMock())
    mock_file = mocker.patch("builtins.open", mock_open())
    p = PersistantHashTable()
    mock_file.assert_called_once_with(p.TXN_LOG_PATH, "ab")


def test_init_txns_logged_starts_at_zero(mocker):
    mocker.patch.object(PersistantHashTable, "_restore", return_value=MagicMock())
    mocker.patch("builtins.open", mock_open())

    pht = PersistantHashTable()

    assert pht.txns_logged == 0


def test_peresistent_read_transaction(calendar, tmp_path) -> None:
    txn_log = tmp_path / "calendar.txns"
    txn_log.touch
    txns = []
    with txn_log.open("wb") as f:
        for i in range(10):
            event = create_event(start=i)
            txn = Transaction("create", hash(event), event)
            txns.append(txn)
            txn = pickle.dumps(txn, protocol=pickle.HIGHEST_PROTOCOL)
            header = struct.pack("!I", len(txn))
            f.write(header + txn)

    with txn_log.open("rb") as f:
        i = 0
        for txn in calendar._read_transactions(f):
            assert txn.method == txns[i].method
            assert txn.identifier == txns[i].identifier
            assert txn.event == txns[i].event
            i += 1


def test_persistent_restore_checkpoint_logic(calendar, tmp_path) -> None:
    ckpt = tmp_path / "calendar.ckpt"
    ckpt.touch()

    # assert that checkpoint reads in the file correctly
    c = Calendar()
    for i in range(10):
        c.create(**create_event(start=i).__dict__)
    assert len(c.events) == 10

    ckpt.write_bytes(pickle.dumps(c.events))
    restored_calendar = calendar._restore()
    assert len(restored_calendar.events) == 10


def test_persistent_restore_new_checkpoint_logic(calendar, tmp_path, mocker) -> None:
    new_ckpt = tmp_path / "calendar.ckpt.new"
    new_ckpt.touch()

    # assert that new_checkpoint reads calles _checkpoint
    m = mocker.patch.object(
        PersistantHashTable, "_checkpoint", return_value=MagicMock()
    )
    calendar._restore()
    m.assert_called_once()
    mocker.stop(m)
    assert new_ckpt.exists() is not True


def test_persistent_restore_transaction_creates_logic(calendar, tmp_path) -> None:
    txn_log = tmp_path / "calendar.txns"
    idents = []
    txn_log.touch()
    # assert that txn logs work
    with txn_log.open("wb") as f:
        for i in range(10):
            event = create_event(start=i)
            idents.append(hash(event))
            txn = pickle.dumps(Transaction("create", hash(event), event))
            header = struct.pack("!I", len(txn))
            f.write(header + txn)

    restored_calendar = calendar._restore()
    assert len(restored_calendar.events) == 10
    assert calendar.txns_logged == 10
    for i in range(len(idents)):
        assert idents[i] in restored_calendar.events


def test_persistent_restore_transaction_creates_deletes_logic(
    calendar, tmp_path
) -> None:
    txn_log = tmp_path / "calendar.txns"
    idents = []
    txn_log.touch()
    # assert that txn logs work
    with txn_log.open("wb") as f:
        for i in range(10):
            event = create_event(start=i)
            idents.append(hash(event))
            txn = pickle.dumps(Transaction("create", hash(event), event))
            header = struct.pack("!I", len(txn))
            f.write(header + txn)

        for i in range(4):
            txn = pickle.dumps(Transaction("delete", idents[i], None))
            header = struct.pack("!I", len(txn))
            f.write(header + txn)

    restored_calendar = calendar._restore()
    assert len(restored_calendar.events) == 6
    assert calendar.txns_logged == 14

    for i in range(4, len(idents)):
        assert idents[i] in restored_calendar.events
    txn_log.unlink()


def test_persistent_restore_transaction_CDM_logic(calendar, tmp_path):
    "CDM -> create, delete, modify"
    txn_log = tmp_path / "calendar.txns"
    idents = []
    modifies = []
    txn_log.touch()
    # assert that txn logs work
    with txn_log.open("wb") as f:
        for i in range(10):
            event = create_event(start=i)
            idents.append(hash(event))
            txn = pickle.dumps(Transaction("create", hash(event), event))
            header = struct.pack("!I", len(txn))
            f.write(header + txn)
        for i in range(4):
            txn = pickle.dumps(Transaction("delete", idents[i], None))
            header = struct.pack("!I", len(txn))
            f.write(header + txn)
        for i in range(4, len(idents)):
            event = create_event(start=10 + i)
            modifies.append(hash(event))
            txn = pickle.dumps(Transaction("modify", idents[i], event))
            header = struct.pack("!I", len(txn))
            f.write(header + txn)

    restored_calendar = calendar._restore()
    assert len(restored_calendar.events) == 6
    assert calendar.txns_logged == 20

    for i in range(len(idents)):
        assert idents[i] not in restored_calendar.events

    for i in range(len(modifies)):
        assert modifies[i] in restored_calendar.events
    txn_log.unlink()


def test_persistent_log(calendar, tmp_path, mocker) -> None:
    txn_log = tmp_path / "calendar.txns"
    event = create_event()
    txn = Transaction("create", hash(event), event)
    calendar._log(txn)

    pickled_txn = pickle.dumps(txn, protocol=pickle.HIGHEST_PROTOCOL)
    header = struct.pack("!I", len(pickled_txn))
    txn_bytes = header + pickled_txn
    assert calendar.txns_logged == 1
    assert txn_log.read_bytes() == txn_bytes

    # assert compaction
    calendar.txns_logged = calendar.CKPT_THRESHOLD - 1
    m = mocker.patch.object(
        PersistantHashTable, "_checkpoint", return_value=MagicMock()
    )
    calendar._log(txn)
    m.assert_called_once()


def test_persistent_checkpoint(calendar, tmp_path) -> None:
    txn_log = tmp_path / "calendar.txns"
    with txn_log.open("wb") as f:
        for i in range(10):
            event = create_event(start=i)
            txn = pickle.dumps(Transaction("create", hash(event), event))
            header = struct.pack("!I", len(txn))
            f.write(header + txn)

    assert txn_log.read_bytes() is not None

    c = Calendar()
    for i in range(10):
        c.create(**create_event(start=i).__dict__)

    calendar.calendar = c
    new_ckpt = tmp_path / "calendar.ckpt.new"
    ckpt = tmp_path / "calendar.ckpt"
    ckpt.touch()
    ckpt.write_bytes(b"data...")

    calendar._checkpoint()
    assert ckpt.read_bytes() != b"data..."
    assert txn_log.read_bytes() == b""
    assert new_ckpt.exists() is False
    assert calendar.txns_logged == 0

    with ckpt.open("rb") as f:
        new_c = pickle.load(f)
        for id, event in new_c.items():
            assert id in c.events
            old_event = c.events[id]
            assert old_event == event
