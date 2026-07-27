#!/usr/bin/env python3
"""Unit tests for the in-memory data model (Event / Repeats / Calendar).

Note on identity: the project does *not* rely on Python's builtin ``hash()`` for
events (the dataclasses define a custom ``__eq__`` and are therefore unhashable).
The canonical identifier is ``Event.hash()``, a SHA-256 hexdigest string, which
is also what ``Calendar.create`` returns.
"""

import pytest

from utils import create_event, create_repeat
from DistributedCalendar.Calendar import Calendar, Event, Day


# --------------------------------------------------------------------------- #
# Repeats
# --------------------------------------------------------------------------- #
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


def test_repeats_not_equal_to_other_types() -> None:
    assert (create_repeat() == "not a repeat") is False


def test_event_hash_depends_on_repeats() -> None:
    """Events that differ only by their repeat rule hash differently, and equal
    repeat rules produce equal event identifiers."""
    r1 = create_repeat()
    r2 = create_repeat()
    r3 = create_repeat(hour=2)
    assert create_event(repeats=r1).hash() == create_event(repeats=r2).hash()
    assert create_event(repeats=r1).hash() != create_event(repeats=r3).hash()


# --------------------------------------------------------------------------- #
# Event
# --------------------------------------------------------------------------- #
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


def test_event_not_equal_to_other_types() -> None:
    assert (create_event() == 42) is False


def test_event_hash_is_stable_hexdigest() -> None:
    event1 = create_event()
    event2 = create_event()
    event3 = create_event(name="progress_update")
    event4 = create_event(name="progress_upate", start=1)

    # Deterministic, equal events -> equal identifiers.
    assert event1.hash() == event2.hash()
    # Distinct events -> distinct identifiers.
    assert event3.hash() != event2.hash()
    assert event4.hash() != event2.hash()
    assert event1.hash() != event3.hash()
    assert event1.hash() != event4.hash()

    # The identifier is a 64-char SHA-256 hexdigest.
    digest = event1.hash()
    assert isinstance(digest, str)
    assert len(digest) == 64
    int(digest, 16)  # raises ValueError if not valid hex


def test_event_is_unhashable() -> None:
    """The dataclass overrides __eq__ without __hash__, so events are unhashable;
    callers must use Event.hash() instead of the builtin hash()."""
    with pytest.raises(TypeError):
        hash(create_event())


# --------------------------------------------------------------------------- #
# Calendar
# --------------------------------------------------------------------------- #
def test_calendar_create() -> None:
    calendar = Calendar()
    ident1 = calendar.create(**create_event().__dict__)
    assert len(calendar.events) == 1
    assert ident1 == create_event().hash()

    # Creating an identical event is idempotent (same identifier, no growth).
    ident2 = calendar.create(**create_event().__dict__)
    assert len(calendar.events) == 1
    assert ident1 == ident2

    # A distinct event is added under a new identifier.
    calendar.create(**create_event(name="progress report").__dict__)
    assert len(calendar.events) == 2
    assert calendar.events[ident1] == create_event()

    # A malformed event (end before start) is rejected.
    ident_bad = calendar.create(**create_event(end=0).__dict__)
    assert ident_bad is None
    assert len(calendar.events) == 2


def test_calendar_delete(filled_in_calendar) -> None:
    ident1, ident2, ident3 = filled_in_calendar.events.keys()
    assert len(filled_in_calendar.events) == 3
    filled_in_calendar.delete(ident1)
    assert len(filled_in_calendar.events) == 2
    filled_in_calendar.delete(ident2)
    assert len(filled_in_calendar.events) == 1
    # Deleting a non-existent id is a no-op.
    filled_in_calendar.delete(ident2)
    assert len(filled_in_calendar.events) == 1
    filled_in_calendar.delete(ident3)
    assert len(filled_in_calendar.events) == 0


def test_calendar_modify(filled_in_calendar) -> None:
    ident1, ident2, ident3 = filled_in_calendar.events.keys()
    assert len(filled_in_calendar.events) == 3

    new_event1 = create_event(name="new_event1")
    new_ident1 = filled_in_calendar.modify(ident=ident1, **new_event1.__dict__)
    assert len(filled_in_calendar.events) == 3
    assert filled_in_calendar.events[new_ident1] == new_event1
    assert new_ident1 == new_event1.hash()
    assert ident1 not in filled_in_calendar.events


def test_calendar_modify_missing_returns_none() -> None:
    calendar = Calendar()
    assert calendar.modify(ident="nope", **create_event().__dict__) is None


def test_calendar_modify_malformed_keeps_original(filled_in_calendar) -> None:
    ident1 = next(iter(filled_in_calendar.events))
    # end < start -> validation fails -> modify is a no-op and original survives.
    result = filled_in_calendar.modify(ident=ident1, **create_event(end=0).__dict__)
    assert result is None
    assert ident1 in filled_in_calendar.events


def test_calendar_get_and_list(filled_in_calendar) -> None:
    ident1 = next(iter(filled_in_calendar.events))
    assert isinstance(filled_in_calendar.get_event(ident1), Event)
    assert filled_in_calendar.get_event("missing") is None

    listing = filled_in_calendar.list_events()
    assert len(listing) == 3
    # list_events returns a copy: mutating it must not affect the calendar.
    listing.clear()
    assert len(filled_in_calendar.events) == 3


# --------------------------------------------------------------------------- #
# Event.validate_event
# --------------------------------------------------------------------------- #
def test_validate_event_accepts_valid_boundaries() -> None:
    create_event().validate_event()
    create_event(start=1, end=1).validate_event()  # start == end is allowed
    create_event(name="-" * (1 << 10)).validate_event()  # exactly 1 KiB name
    create_event(description="-" * (1 << 13)).validate_event()  # exactly 8 KiB
    create_event(location="-" * (1 << 10)).validate_event()


def test_validate_event_rejects_end_before_start() -> None:
    with pytest.raises(ValueError):
        create_event(end=1).validate_event()


def test_validate_event_rejects_oversized_name() -> None:
    with pytest.raises(ValueError):
        create_event(name="-" * ((1 << 10) + 1)).validate_event()


def test_validate_event_rejects_oversized_description() -> None:
    with pytest.raises(ValueError):
        create_event(description="-" * ((1 << 13) + 1)).validate_event()


def test_validate_event_rejects_oversized_location() -> None:
    with pytest.raises(ValueError):
        create_event(location="-" * ((1 << 10) + 1)).validate_event()


def test_validate_event_rejects_backwards_repeat_window() -> None:
    repeats = create_repeat()
    repeats.repeats_starting, repeats.repeats_until = (
        repeats.repeats_until + 10,
        repeats.repeats_until,
    )
    with pytest.raises(ValueError):
        create_event(repeats=repeats).validate_event()
