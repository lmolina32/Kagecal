#!/usr/bin/env

import sys
import argparse

from DistributedCalendar.Peer import Peer


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--calendar-project",
        help="The specific calendar you intend to subscribe to",
        required=True,
    )
    parser.add_argument(
        "--peer-name", help="the peer name you want to register as", required=True
    )
    args = parser.parse_args()

    peer = Peer(calendar_name=args.calendar_project, peer_name=args.peer_name)
    peer.run()


if __name__ == "__main__":
    main()
