#!/usr/bin/env python3

import pytest
import pickle
import struct
from unittest.mock import MagicMock, mock_open, patch

from utils import create_event, create_repeat
from DistributedCalendar.Calendar import Calendar
from DistributedCalendar.PersistantCalendar import PersistantHashTable, Transaction


@pytest.fixture
def calendar(mocker, tmp_path):
    txn_log = tmp_path / "calendar.txns"
    ckpt = tmp_path / "calendar.ckpt"
    new_ckpt = tmp_path / "calendar.new.ckpt"

    mocker.patch.object(PersistantHashTable, "CKPT_PATH", str(ckpt))
    mocker.patch.object(PersistantHashTable, "NEW_CKPT_PATH", str(new_ckpt))
    mocker.patch.object(PersistantHashTable, "TXN_LOG_PATH", str(txn_log))
    calendar = PersistantHashTable()
    calendar.calendar = Calendar()
    return calendar


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
    PersistantHashTable()
    mock_file.assert_called_once_with(PersistantHashTable.TXN_LOG_PATH, "ab")


def test_init_txns_logged_starts_at_zero(mocker):
    mocker.patch.object(PersistantHashTable, "_restore", return_value=MagicMock())
    mocker.patch("builtins.open", mock_open())

    pht = PersistantHashTable()

    assert pht.txns_logged == 0


def test_persistent_restore_checkpoint_logic(calendar, tmp_path, mocker) -> None:
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
    new_ckpt = tmp_path / "calendar.new.ckpt"
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


def test_persistent_log(calendar, tmp_path) -> None:
    pass


def test_persistent_checkpoint(calendar, tmp_path) -> None:
    pass
