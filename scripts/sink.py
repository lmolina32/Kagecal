#!/usr/bin/env python3

import os
import sys
import argparse
import time
import threading
from pathlib import Path

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from DistributedCalendar.Peer import Peer


def parse_args() -> argparse.Namespace:
    """parse command line prompts"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--events",
        default=10,
        type=int,
        help="This is the number of events you want the leader peer to wait for before terminating",
    )
    parser.add_argument(
        "--output-dir",
        default="results",
        type=str,
        help="where the results should be written to",
    )
    parser.add_argument(
        "--calendar-ident",
        default="test_calendar",
        type=str,
        help="calendar identifier for testing purposes",
    )
    parser.add_argument(
        "--peer-ident",
        default="test_leader",
        type=str,
        help="peer identifier for testin",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_path = Path(PROJECT_ROOT) / args.output_dir
    output_path.mkdir(parents=True, exist_ok=True)

    # run leader peer
    # TODO: should this just forever then get killed don't really see no point lol
    peer = Peer(args.calendar_ident, args.peer_ident)
    while True:
        pass
        # print("sleeping for 60 seconds")
        # time.sleep(60)
        # with peer.server.calendar_lock:
        #     events = peer.server.persistence.list_events()
        #     if events == args.events:
        #         print(f"The {events} was reached the peer is now shutting down")
        #         break


if __name__ == "__main__":
    main()
