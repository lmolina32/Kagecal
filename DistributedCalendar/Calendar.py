import time
import hashlib
import pickle
from enum import Enum
from typing import Optional
from dataclasses import dataclass


# TODO: This would be more efficient as a bitset
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

    def __eq__(self, other) -> bool:
        if not isinstance(other, Repeats):
            return NotImplemented
        return (
            self.repeats_every == other.repeats_every
            and self.repeats_starting == other.repeats_starting
            and self.repeats_until == other.repeats_until
        )


@dataclass
class Event:
    name: str
    start: int
    end: int
    description: Optional[str] = None
    location: Optional[str] = None
    repeats: Optional[Repeats] = None

    def __eq__(self, other) -> bool:
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

    def hash(self) -> str:
        # Hash based on the same attributes used for equality
        hash_obj = hashlib.sha256(
            pickle.dumps(
                (
                    self.name,
                    self.start,
                    self.end,
                    self.description,
                    self.location,
                    self.repeats,
                )
            )
        )
        return hash_obj.hexdigest()

    def validate_event(self):
        """Checks if an event is consistent with the following invariants:
        - Start time is less than or equal to end time
        - name is bounded at 1KiB
        - description is bounded at 8KiB
        - location is bounded at 1KiB
        - Repeats.repeats_starting is less than or equal to Repeats.repeats_until
        """

        if self.start > self.end:
            raise ValueError("End time cannot be before start time.")
        if len(self.name) > (1 << 10):
            raise ValueError("Event name must be less than 1K characters.")
        if self.description and len(self.description) > (1 << 13):
            raise ValueError("Event description must be less than 8K characters.")
        if self.location and len(self.location) > (1 << 10):
            raise ValueError("Event location must be less than 1K characters.")
        if self.repeats and self.repeats.repeats_starting > self.repeats.repeats_until:
            raise ValueError("Repeat end date cannot be before repeat start date.")


class Calendar:

    def __init__(self) -> None:
        self.events: dict[str, Event] = {}

    def create(
        self,
        name: str,
        start: int,
        end: int,
        description: Optional[str] = None,
        location: Optional[str] = None,
        repeats: Optional[Repeats] = None,
    ) -> Optional[str]:
        """Creates an Event, assigns it a unique identifier, and adds it to the calendar. If the event metadata is malformed, does nothing. Returns the identifer for the event."""

        event = Event(name, start, end, description, location, repeats)

        try:
            event.validate_event()
        except ValueError:
            return None

        identifier = event.hash()
        self.events[identifier] = event
        return identifier

    def delete(self, ident: str) -> None:
        """Deletes an event with a given identifier from the calendar, regardless of whether or not the event exists."""
        if ident in self.events:
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
    ) -> Optional[str]:
        """Modifies an event with a given identifier. If the event doesn't exist, or if the event metadata is malformed, does nothing. Returns the new identifier for the event."""
        if ident not in self.events:
            return None
        new_ident = self.create(name, start, end, description, location, repeats)
        if new_ident is None:
            return None
        del self.events[ident]
        return new_ident

    def get_event(self, ident: str) -> Optional[Event]:
        """Retrives an event with a given identifier from the calendar, regardless of whether or not the event exists"""
        return self.events.get(ident, None)

    def list_events(self) -> dict[str, Event]:
        """Retrives all events in the calendar"""
        return dict(self.events)

    def search_events(self):
        """Searches calendar for events that match description"""
        ...
