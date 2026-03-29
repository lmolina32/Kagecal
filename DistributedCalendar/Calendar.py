from dataclasses import dataclass
from enum import Enum
from typing import Optional
import time


class Calendar:

    def __init__(self):

        # Used to generate unique identifiers for each event.
        self.identifier_counter = 0

        events = {}
        pass

    def create(
        self,
        name: str,
        start: int,
        end: int,
        description: Optional[str] = None,
        location: Optional[str] = None,
        repeats: Optional[Repeats] = None,
    ) -> None:
        """Creates an Event, assigns it a unique identifier, and adds it to the calendar. If the event metadata is malformed, does nothing."""

        # TODO: How will we genearate a unique identifier for each event? Just use a monotonic counter.
        identifier = self.identifier_counter
        self.identifier_counter += 1

        event = Event(name, start, end, description, location, repeats)
        events[identifier] = event

        pass

    def delete(self, id: str) -> None:
        """Deletes an event with a given identifier from the calendar, regardless of whether or not the event exists."""
        pass

    def modify() -> None:
        """Modifies an event with a given identifier. If the event doesn't exist, or if the event metadata is malformed, does nothing."""
        pass

    def _validate_event(event: Event) -> bool:
        """Checks if an event is consistent with the following invariants:
        - Start time is less than or equal to end time
        - name is bounded at 1KiB
        - description is bounded at 8KiB
        - location is bounded at 1KiB
        - Repeats.repeats_starting is less than or equal to Repeats.repeats_until
        """


@dataclass
class Event:
    name: str
    start: int
    end: int
    description: Optional[str] = None
    location: Optional[str] = None
    repeats: Optional[Repeats] = None


@dataclass
class Repeats:
    repeats_every: set[Day]
    repeats_starting: int
    repeats_until: int


class Day(Enum):
    SUNDAY = 1
    MONDAY = 2
    TUESDAY = 3
    WEDNESDAY = 4
    THURSDAY = 5
    FRIDAY = 6
    SATURDAY = 7
