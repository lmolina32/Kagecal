#!/usr/bin/env python3

import os
import sys
import json
import time
import argparse
import threading
import statistics
from pathlib import Path
from multiprocessing import Pool
from setup_calendar import generate_random_events

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from DistributedCalendar.Peer import Peer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--peers", default=5, type=int, help="number of peers to spawn")
    parser.add_argument(
        "--calendar-ident",
        default="test_calendar",
        type=str,
        help="calendar identifier for testing purposes",
    )
    parser.add_argument(
        "--events", default=10, type=int, help="number of random events to send"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="results",
        help="where the results should be written to",
    )
    return parser.parse_args()


def run_peer(peer_name: str, calendar_ident: str, events: int, output: Path):
    """Creates a test peer that geneartes X random events to send to the leader. It times the creation of each event and computes test statistics that are saved in output as <peer_name>_<events>_results.json"""
    output_path = output / f"{peer_name}_results.json"
    _, list_of_events = generate_random_events(events)
    event_times = []
    # TODO: ensure this is the actual behavior of a follower thread
    peer = Peer(calendar_ident=calendar_ident, peer_ident=peer_name)
    peer.startup()
    serve_thread = threading.Thread(target=peer._serve, daemon=True)

    for event in list_of_events:
        # TODO: once create fully functioning pass in event
        # TODO: will create return once calendar is sinked?
        #       if not, call down to peers calendar and check for consistent event
        start = time.perf_counter_ns()
        peer.create()
        end = time.perf_counter_ns() - start
        event_times.append(end)

    peer.pause_server.set()
    serve_thread.join()

    results = {
        "events": events,
        "times": event_times,
        "mean": statistics.mean(event_times),
        "std": statistics.stdev(event_times),
    }

    output_path.write_text(json.dumps(results, indent=4))


def main() -> None:
    args = parse_args()
    output_path = Path(PROJECT_ROOT) / args.output_dir
    output_path.mkdir(parents=True, exist_ok=True)
    with Pool(processes=args.peers) as pool:
        pool.starmap(
            run_peer,
            [
                (f"test_peer_{i}", args.calendar_ident, args.events, output_path)
                for i in range(args.peers)
            ],
        )
    print("All peers succesfully terminated")


if __name__ == "__main__":
    main()
