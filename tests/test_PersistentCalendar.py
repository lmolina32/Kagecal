#!/usr/bin/env python3

import pytest
from unittest.mock import MagicMock, mock_open

from DistributedCalendar.Calendar import Calendar
from DistributedCalendar.PersistantCalendar import PersistantHashTable


@pytest.fixture
def calendar(mocker, tmp_path):
    txn_log = tmp_path / "calendar.txns"
    ckpt = tmp_path / "calendar.ckpt"
    new_ckpt = tmp_path / "calendar.new.ckpt"
    txn_log.touch()
    ckpt.touch()
    new_ckpt()

    return_cal = Calendar()
    mocker.patch.object(PersistantHashTable, "_restore", return_value=return_cal)
    mocker.patch.object(PersistantHashTable, "CKPT_PATH", str(ckpt))
    mocker.patch.object(PersistantHashTable, "NEW_CKPT_PATH", str(new_ckpt))
    mocker.patch.object(PersistantHashTable, "TXN_LOG_PATH", str(txn_log))
    calendar = PersistantHashTable()
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
