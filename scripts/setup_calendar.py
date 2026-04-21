#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
import string
import random
import pickle
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Tuple

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from DistributedCalendar.Client import Client
from DistributedCalendar.Calendar import Event, Repeats, Day


def create_ckpt_events(filename: str, output_dir=str, num_events=None) -> None:
    root = Path(PROJECT_ROOT)
    if output_dir != "data":
        data_dir = root / output_dir
    else:
        data_dir = root / "data"

    data_dir.mkdir(parents=True, exist_ok=True)

    file_path = data_dir / filename

    ids, events = generate_random_events(num_events=num_events)
    calendar_events = {event_id: event for event_id, event in zip(ids, events)}

    file_path.write_bytes(
        pickle.dumps(calendar_events, protocol=pickle.HIGHEST_PROTOCOL)
    )


def generate_random_string(length: int = 10) -> str:
    chars = string.ascii_letters + string.digits + "_" + " "
    return (
        "".join(random.choice(chars) for _ in range(length)).strip()
        or f"test_event{length}"
    )


def generate_random_events(num_events=None) -> Tuple[List, List]:
    if num_events is None:
        num_events = random.randint(5, 20)

    events = []
    ids = []
    for i in range(num_events):
        current_time = int(datetime.now(timezone.utc).timestamp())
        event = Event(
            name=f"test_event{i}",
            start=current_time - 1000 * (i + 1),
            end=current_time + 1000 * (i + 1),
            description=generate_random_string(1 << 5),
            location=f"Sorin Hall, Room {i + 1}",
            repeats=None,
        )
        events.append(event)
        ids.append(hash(event))

    return ids, events


def send_random_events(host: str, port: int, num_events=None) -> None:
    ids, events = generate_random_events(num_events=num_events)
    with Client(client_name="", host=host, port=port) as client:
        for i in range(len(events)):
            ids.append(client.create(**events[i].__dict__))
    print(
        f"Sent {len(ids)}, Event ids:\n"
        f"{''.join('\t' + str(event_id) + '\n' for event_id in ids)}"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True, help="host name of peer", type=str)
    parser.add_argument("--port", required=True, help="port number of peer", type=int)
    parser.add_argument(
        "--number-events",
        help="specify the number of events to create",
        type=int,
        default=None,
    )
    parser.add_argument(
        "--create-ckpt",
        help="Instead of sending events over network create ckpt in data directory",
        type=bool,
        default=False,
    )
    parser.add_argument(
        "--peer-name",
        help="peer name to construct ckpt file e.g (calendar_<project_name>_<peer_name>.txn)",
        type=str,
        default="",
    )
    parser.add_argument(
        "--project-name",
        help="project name to construct ckpt file e.g (calendar_<project_name>_<peer_name>.txn)",
        type=str,
        default="",
    )
    parser.add_argument(
        "--output-dir", help="were to write ckpoint file", type=str, default="data"
    )
    args = parser.parse_args()

    if args.create_ckpt and not args.peer_name:
        parser.error("--create-ckpt requires --peer-name to create file")

    if args.create_ckpt and not args.project_name:
        parser.error("--create-ckpt requires --project-name to create file")

    if args.create_ckpt:
        create_ckpt_events(
            f"calendar_{args.project_name}_{args.peer_name}.ckpt",
            args.output_dir,
            args.number_events,
        )
    else:
        send_random_events(args.host, args.port, args.number_events)


if __name__ == "__main__":
    main()
