#!/usr/bin/env python3
"""Helpers shared across the Kagecal test suite."""

import pickle
import struct
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


def frame_message(payload: bytes) -> bytes:
    r"""Wrap a payload using the ``<length>\n<payload>`` framing used by the
    Client/Server RPC channel."""
    return str(len(payload)).encode() + b"\n" + payload


def unframe_message(data: bytes):
    """Inverse of :func:`frame_message`: strip the length header and unpickle."""
    delim = data.index(b"\n")
    size = int(data[:delim].decode())
    payload = data[delim + 1 : delim + 1 + size]
    return pickle.loads(payload)


def frame_transaction(txn) -> bytes:
    """Wrap a transaction using the ``!I`` (4-byte big-endian length) framing
    used by the persistent transaction log."""
    pickled = pickle.dumps(txn, protocol=pickle.HIGHEST_PROTOCOL)
    return struct.pack("!I", len(pickled)) + pickled
