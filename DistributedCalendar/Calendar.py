from dataclasses import dataclass
from enum import Enum
from typing import Optional
import time


class Calendar:
    def __init__(self):

        # We need some data structure to hold events... it is okay if events overlap... so it should be okay to have

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
        pass

    def delete(self, id: str) -> None:
        """Deletes an event with a given identifier from the calendar, regardless of whether or not the event exists."""
        pass

    def modify() -> None:
        """Modifies an event with a given identifier. If the event doesn't exist, or if the event metadata is malformed, does nothing."""
        pass


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
