#!/usr/bin/env python3

import pytest

from DistributedCalendar.Calendar import Calendar
from DistributedCalendar.PersistantCalendar import PersistantHashTable
from utils import create_event


@pytest.fixture
def filled_in_calendar() -> Calendar:
    calendar = Calendar()
    ident1 = calendar.create(**create_event().__dict__)
    ident2 = calendar.create(**create_event(name="new").__dict__)
    ident3 = calendar.create(**create_event(name="new1").__dict__)
    return calendar


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
