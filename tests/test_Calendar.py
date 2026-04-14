#!/usr/bin/env python3

import pytest
from datetime import datetime, timezone, timedelta

from DistributedCalendar.Calendar import Calendar, Repeats, Event, Day


def test_repeats_eq():
    now_utc = int(datetime.now(timezone.utc).timestamp())
    one_hour_later_utc = int(
        (datetime.now(timezone.utc) + timedelta(hours=1)).timestamp()
    )
    two_hours_later_utc = int(
        (datetime.now(timezone.utc) + timedelta(hours=2)).timestamp()
    )
    repeats1 = Repeats(Day.SUNDAY, now_utc, one_hour_later_utc)
    repeats2 = Repeats(Day.SUNDAY, now_utc, one_hour_later_utc)
    assert (repeats1 == repeats2) is True

    repeats3 = Repeats(Day.SUNDAY, now_utc, two_hours_later_utc)
    assert (repeats2 != repeats3) is True
    assert (repeats1 == repeats3) is False
