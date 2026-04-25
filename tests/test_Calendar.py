#!/usr/bin/env python3

import pytest

from utils import create_event, create_repeat
from DistributedCalendar.Calendar import Calendar, Day


def test_repeats_eq() -> None:
    repeats1 = create_repeat()
    repeats2 = create_repeat()
    assert (repeats1 == repeats2) is True

    repeats3 = create_repeat(hour=2)
    assert (repeats2 != repeats3) is True
    assert (repeats1 == repeats3) is False

    repeats4 = create_repeat(day=Day.MONDAY)
    assert (repeats1 == repeats4) is False
    assert (repeats2 == repeats4) is False


def test_repeats_hash() -> None:
    repeats1 = create_repeat()
    repeats2 = create_repeat()
    repeats3 = create_repeat(hour=2)
    assert hash(repeats1) == hash(repeats2)
    assert hash(repeats1) != hash(repeats3)
    assert hash(repeats2) != hash(repeats3)


def test_event_eq() -> None:
    event1 = create_event()
    event2 = create_event()
    event3 = create_event(name="progress_update")
    event4 = create_event(name="progress_upate", start=1)
    assert (event1 == event2) is True
    assert (event2 == event3) is False
    assert (event3 == event4) is False
    assert (event1 != event4) is True
    assert (event2 != event4) is True
    assert (event3 != event4) is True


def test_event_hash() -> None:
    event1 = create_event()
    event2 = create_event()
    event3 = create_event(name="progress_update")
    event4 = create_event(name="progress_upate", start=1)
    assert hash(event1) == hash(event2)
    assert hash(event3) != hash(event2)
    assert hash(event4) != hash(event2)
    assert hash(event1) != hash(event3)
    assert hash(event1) != hash(event4)


def test_calendar_create() -> None:
    calendar = Calendar()
    ident1 = calendar.create(**create_event().__dict__)
    assert len(calendar.events) == 1
    assert ident1 == hash(create_event())
    # should be the same event
    ident2 = calendar.create(**create_event().__dict__)
    assert len(calendar.events) == 1
    assert ident1 == ident2
    ## create new event
    ident3 = calendar.create(**create_event(name="progress report").__dict__)
    assert len(calendar.events) == 2
    # event hash should be the same as well as event
    assert calendar.events[ident1] == create_event()

    ident4 = calendar.create(**create_event(end=0).__dict__)
    assert ident4 == None


def test_calendar_delete(filled_in_calendar) -> None:
    ident1, ident2, ident3 = filled_in_calendar.events.keys()
    assert len(filled_in_calendar.events) == 3
    filled_in_calendar.delete(ident1)
    assert len(filled_in_calendar.events) == 2
    filled_in_calendar.delete(ident2)
    assert len(filled_in_calendar.events) == 1
    filled_in_calendar.delete(ident2)
    assert len(filled_in_calendar.events) == 1
    filled_in_calendar.delete(ident3)
    assert len(filled_in_calendar.events) == 0


def test_calendar_modify(filled_in_calendar) -> None:
    ident1, ident2, ident3 = filled_in_calendar.events.keys()
    assert len(filled_in_calendar.events) == 3
    new_event1 = create_event(name="new_event1")
    new_event2 = create_event(name="new_event2")
    new_event3 = create_event(name="new_event3")
    new_ident1 = filled_in_calendar.modify(ident=ident1, **new_event1.__dict__)
    assert len(filled_in_calendar.events) == 3
    assert filled_in_calendar.events[new_ident1] == new_event1
    assert new_ident1 == hash(new_event1)

    new_ident2 = filled_in_calendar.modify(ident=ident2, **new_event2.__dict__)
    assert len(filled_in_calendar.events) == 3
    assert filled_in_calendar.events[new_ident2] == new_event2
    assert new_ident2 == hash(new_event2)

    new_ident3 = filled_in_calendar.modify(ident=ident3, **new_event3.__dict__)
    assert len(filled_in_calendar.events) == 3
    assert filled_in_calendar.events[new_ident3] == new_event3
    assert new_ident3 == hash(new_event3)


def test_validate_event() -> None:
    c = Calendar()
    event = create_event()
    event.validate_event()

    event = create_event(start=1, end=1)
    event.validate_event()

    event = create_event(name=f"{"-"*(1<<10)}")
    event.validate_event()

    event = create_event(description=f"{"-"*(1<<13)}")
    event.validate_event()

    with pytest.raises(ValueError):
        event = create_event(end=1)
        event.validate_event()

        event = create_event(name=f"{"-"*(1<<10)}-")
        event.validate_event()

        event = create_event(description=f"{"-"*(1<<13)}-")
        event.validate_event()

        repeats = create_repeat(hour=-1)
        event.validate_event()

        repeats = create_repeat(hour=0)
        event = create_event(repeats=repeats)
        event.validate_event()
