#!/usr/bin/env python3

import os
import json
import argparse
import statistics
import matplotlib.pyplot as plt
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
    parser.add_argument(
        "--plot", type=str, default="latency", help="(latency|throughput)"
    )
    return parser.parse_args()


def aggregate_data_latency(input_path: Path) -> dict:
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
            try:
                file_data = json.load(f)
            except json.JSONDecodeError:
                continue

        if "events" not in file_data:
            continue

        # the key is the number of peers in the system
        events = 256 // int(file_data["events"])
        if events not in data:
            data[events] = {"means": [], "stds": []}
        data[events]["means"].append(file_data["mean"])
        data[events]["stds"].append(file_data["std"])

    return data


def aggregate_data_throughput(input_path: Path) -> dict:
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
            try:
                file_data = json.load(f)
            except json.JSONDecodeError:
                continue

        if "peers" not in file_data:
            continue

        data[file_data["peers"]] = file_data["time"]
    return data


def plot_data_latency(data: dict, output: Path) -> None:
    """plots the data into figures and saves them to ouput"""
    x_axis = [2**i for i in range(len(data))]
    y_axis = [
        statistics.mean(times_data["means"]) / 1e9 for times_data in data.values()
    ]  # ms -> s

    plt.figure(figsize=(10, 6), dpi=100)
    plt.plot(
        x_axis,
        y_axis,
        marker="o",
        linestyle="-",
        color="#007acc",
        linewidth=2,
        markersize=8,
        label="System Latency",
    )
    plt.show()

    plt.title(
        "System Latency vs. Node Count for 256 transactions",
        fontsize=16,
        fontweight="bold",
        pad=20,
    )
    plt.xticks(x_axis)
    plt.ylim(0, 1)
    plt.xlabel("Number of Nodes", fontsize=12)
    plt.ylabel("Latency (seconds)", fontsize=12)

    plt.grid(True, linestyle="--", alpha=0.7)
    plt.legend(loc="upper left", fontsize=11)
    plt.tight_layout()

    plt.savefig(output / "latency_graph.png")


def plot_data_throughput(data: dict, output: Path) -> None:
    """plots the data into figures and saves them to ouput"""
    x_axis = [2**i for i in range(len(data))]
    y_axis = [time for time in data.values()]
    plt.figure(figsize=(10, 6), dpi=100)
    plt.plot(
        x_axis,
        y_axis,
        marker="o",
        linestyle="-",
        color="#007acc",
        linewidth=2,
        markersize=8,
        label="System Throughput",
    )
    plt.show()

    plt.title(
        "System Throughput vs. Node Count for 256 transactions",
        fontsize=16,
        fontweight="bold",
        pad=20,
    )
    plt.xticks(x_axis)
    plt.ylim(0, 1.0)
    plt.xlabel("Number of Nodes", fontsize=12)
    plt.ylabel("Throughput (transactions/seconds)", fontsize=12)
    plt.grid(True, linestyle="--", alpha=0.7)
    plt.legend(loc="upper left", fontsize=11)
    plt.tight_layout()

    plt.savefig(output / "throughput_graph.png")


def main() -> None:
    args = parse_args()
    input_path = Path(PROJECT_ROOT) / args.input_dir
    output_path = Path(PROJECT_ROOT) / args.output_dir
    output_path.mkdir(parents=True, exist_ok=True)
    if args.plot == "latency":
        data = aggregate_data_latency(input_path)
        plot_data_latency(data, output_path)
    else:
        data = aggregate_data_throughput(input_path)
        plot_data_throughput(data, output_path)


if __name__ == "__main__":
    main()
