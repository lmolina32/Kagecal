#!/usr/bin/env python3

import pytest
from unittest.mock import MagicMock

from pathlib import Path
from DistributedCalendar.Calendar import Calendar
from DistributedCalendar.PersistantCalendar import PersistantHashTable
from utils import create_event


@pytest.fixture
def filled_in_calendar() -> Calendar:
    calendar = Calendar()
    calendar.create(**create_event().__dict__)
    calendar.create(**create_event(name="new").__dict__)
    calendar.create(**create_event(name="new1").__dict__)
    return calendar


@pytest.fixture
def calendar(mocker, tmp_path):
    restore_mock = mocker.patch.object(
        PersistantHashTable, "_restore", return_value=MagicMock()
    )
    mocker.patch.object(Path, "cwd", return_value=tmp_path)
    calendar = PersistantHashTable()
    mocker.stop(restore_mock)
    calendar.CKPT_PATH = str(tmp_path / "calendar.ckpt")
    calendar.TXN_LOG_PATH = str(tmp_path / "calendar.txns")
    calendar.NEW_CKPT_PATH = str(tmp_path / "calendar.ckpt.new")
    calendar.txn_log_file.close()
    calendar.txn_log_file = open(calendar.TXN_LOG_PATH, "ab")
    calendar.calendar = Calendar()
    return calendar
