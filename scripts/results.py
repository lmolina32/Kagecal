#!/usr/bin/env python3

import os
import sys
import json
import argparse
from pathlib import Path

# TODO: some point create a config for this
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-dir",
        type=str,
        default="results",
        help="where the results should be written to",
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        default="results",
        help="where the results should be written to",
    )
    return parser.parse_args()


def aggregate_data(input_path: Path) -> dict:
    """aggregates data from results collected in spawn.py and sink.py from files in input_path"""
    data = {}
    for fp in input_path.iterdir():
        if not fp.is_file():
            continue

        if fp.suffix.lower() != ".json":
            continue

        parts = fp.stem.split("_")
        if parts[-1] != "results":
            continue

        with fp.open() as f:
            file_data = json.load(f)

        events = int(file_data["events"])
        if events not in data:
            data[events] = {"means": [], "stds": []}
        data[events]["means"].append(file_data["mean"])
        data[events]["stds"].append(file_data["std"])

    return data


def plot_data(data: dict, output: Path) -> None:
    """plots the data into figures and saves them to ouput"""


def main() -> None:
    args = parse_args()
    input_path = Path(PROJECT_ROOT) / args.input_dir
    output_path = Path(PROJECT_ROOT) / args.output_dir
    output_path.mkdir(parents=True, exist_ok=True)
    data = aggregate_data(input_path)
    plot_data(data, output_path)


if __name__ == "__main__":
    main()
