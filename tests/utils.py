#!/usr/bin/env python3

from typing import Optional
from datetime import datetime, timezone, timedelta

from DistributedCalendar.Calendar import Repeats, Event, Day


def create_repeat(day: Day = Day.SUNDAY, hour: int = 1) -> Repeats:
    now_utc = int(datetime.now(timezone.utc).timestamp())
    one_hour_later_utc = int(
        (datetime.now(timezone.utc) + timedelta(hours=hour)).timestamp()
    )
    return Repeats(day, now_utc, one_hour_later_utc)


def create_event(
    name: str = "progress_report",
    start: int = 1713045600,
    end: int = 1713045601,
    description: Optional[str] = None,
    location: Optional[str] = None,
    repeats: Optional[Repeats] = None,
) -> Event:
    return Event(
        name=name,
        start=start,
        end=end,
        description=description,
        location=location,
        repeats=repeats,
    )
