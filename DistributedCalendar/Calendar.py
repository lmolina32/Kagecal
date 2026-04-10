from dataclasses import dataclass
from enum import Enum
from typing import Optional
import time


class Day(Enum):
    SUNDAY = 1
    MONDAY = 2
    TUESDAY = 3
    WEDNESDAY = 4
    THURSDAY = 5
    FRIDAY = 6
    SATURDAY = 7


@dataclass
class Repeats:
    repeats_every: set[Day]
    repeats_starting: int
    repeats_until: int

    def __eq__(self, other):
        if not isinstance(other, Event):
            return NotImplemented
        return (
            self.repeats_every == other.repeats_every
            and self.repeats_starting == other.repeats_starting
            and self.repeats_until == other.repeats_until
        )

    def __hash__(self):
        # Hash based on the same attributes used for equality
        return hash((self.repeats_every, self.repeats_starting, self.repeats_until))


@dataclass
class Event:
    name: str
    start: int
    end: int
    description: Optional[str] = None
    location: Optional[str] = None
    repeats: Optional[Repeats] = None

    def __eq__(self, other):
        if not isinstance(other, Event):
            return NotImplemented
        return (
            self.name == other.name
            and self.start == other.start
            and self.end == other.end
            and self.description == other.description
            and self.location == other.location
            and self.repeats == other.repeats
        )

    def __hash__(self):
        # Hash based on the same attributes used for equality
        return hash(
            (
                self.name,
                self.start,
                self.end,
                self.description,
                self.location,
                self.repeats,
            )
        )


class Calendar:

    def __init__(self):
        events: dict[int, Event] = {}

    def create(
        self,
        name: str,
        start: int,
        end: int,
        description: Optional[str] = None,
        location: Optional[str] = None,
        repeats: Optional[Repeats] = None,
    ) -> Optional[int]:
        """Creates an Event, assigns it a unique identifier, and adds it to the calendar. If the event metadata is malformed, does nothing. Returns the identifer for the event."""

        event = Event(name, start, end, description, location, repeats)

        if not self._validate_event(event):
            return None

        identifier = hash(event)
        self.events[identifier] = event

        return identifier

    def delete(self, ident: int) -> None:
        """Deletes an event with a given identifier from the calendar, regardless of whether or not the event exists."""
        if id in self.events:
            del self.events[ident]

    def modify(
        self,
        ident: int,
        name: str,
        start: int,
        end: int,
        description: Optional[str] = None,
        location: Optional[str] = None,
        repeats: Optional[Repeats] = None,
    ) -> Optional[int]:
        """Modifies an event with a given identifier. If the event doesn't exist, or if the event metadata is malformed, does nothing. Returns the new identifier for the event."""
        if id not in self.events:
            return None
        new_ident = self.create(name, stard, end, description, location, repeats)
        if new_ident is None:
            return None
        del self.events[ident]
        return new_ident

    def _validate_event(event: Event) -> bool:
        """Checks if an event is consistent with the following invariants:
        - Start time is less than or equal to end time
        - name is bounded at 1KiB
        - description is bounded at 8KiB
        - location is bounded at 1KiB
        - Repeats.repeats_starting is less than or equal to Repeats.repeats_until
        """
        if (
            event.start > event.end
            or len(event.name) > (1 << 10)
            or len(event.description) > (1 << 13)
            or len(event.location) > (1 << 10)
            or (
                events.repeats
                and event.repeats.repeats_starting > event.repeats.repeats_until
            )
        ):
            return False

        return True
