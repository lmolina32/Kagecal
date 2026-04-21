#!/usr/bin/env python3

import os
import sys
import argparse
import string
import random
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from DistributedCalendar.Client import Client
from DistributedCalendar.Calendar import Event, Repeats, Day


def generate_random_string(length: int = 10) -> str:
    chars = string.ascii_letters + string.digits + "_" + " "
    return (
        "".join(random.choice(chars) for _ in range(length)).strip()
        or f"test_event{length}"
    )


def send_random_events(host: str, port: int, num_events=None) -> None:
    if num_events is None:
        num_events = random.randint(5, 20)

    ids = []

    with Client(client_name="", host=host, port=port) as client:
        for i in range(num_events):
            current_time = int(datetime.now(timezone.utc).timestamp())
            ids.append(
                client.create(
                    name=f"test_event{i}",
                    start=current_time - 1000 * (i + 1),
                    end=current_time + 1000 * (i + 1),
                    description=generate_random_string(1 << 5),
                    location=f"Sorin Hall, Room {i + 1}",
                )
            )

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
    args = parser.parse_args()

    send_random_events(args.host, args.port, args.number_events)


if __name__ == "__main__":
    main()
