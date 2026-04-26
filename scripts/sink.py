#!/usr/bin/env python3

import os
import sys
import argparse
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
    peer = Peer(args.calendar_ident, args.peer_ident)
    peer.startup()
    # TODO: is this just a file that calls the peer._serve method? 
    # TODO: figure out how to do performance testing when the peer is fully worked out 
    peer._serve()


if __name__ == "__main__":
    main()
