import argparse
import logging
import mmap
import time
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool as Pool
from pathlib import Path
from typing import Tuple

import numpy as np

from utils.constants import INT_BYTES


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i",
        "--input_file",
        type=Path,
        required=True,
        help="file with big integers"
    )
    parser.add_argument(
        "--allow_multithreading",
        default=False,
        action="store_true",
        help="allow to calculate sum, max and min in multithreading way.",
    )
    args = parser.parse_args()
    return args


def _dummy_process_file(filename: Path) -> Tuple[float, int, int, int]:
    start = time.time()
    size = filename.stat().st_size
    num_ints = size // INT_BYTES
    logging.info(f"Starting reading from file {filename}...")
    with open(filename, "rb") as file:
        data = np.fromfile(file, dtype=">u4", count=num_ints)

    logging.info("Starting calculations...")
    _sum = np.sum(data)
    _min = np.min(data)
    _max = np.max(data)
    _time = time.time() - start
    return _time, _sum, _min, _max


def _process_group(
        group: np.ndarray
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    return np.sum(group, dtype=np.uint64), np.min(group), np.max(group)


def _multithreading_process_file(
        filename: Path
) -> Tuple[float, int, int, int]:
    file_size = filename.stat().st_size
    ints_amount = file_size // INT_BYTES
    device_amount = cpu_count()
    chunk_size = ints_amount // device_amount
    logging.info(f"Cpus amount is {device_amount}")

    logging.info(f"Starting reading from file {filename}...")
    start = time.time()
    with open(filename, "rb") as file:
        mmapped_data = mmap.mmap(
            file.fileno(),
            length=0,
            access=mmap.ACCESS_READ
        )
        arr = np.frombuffer(mmapped_data, dtype=">u4")

    logging.info("Starting calculations...")
    with Pool(device_amount) as pool:
        chunks = [
            arr[i * chunk_size:(i + 1) * chunk_size]
            for i in range(device_amount)
        ]
        assert (
            len(chunks) == device_amount
        ), ("Device amount does not match with number of chunks!"
            f"{len(chunks)} chunks vs {device_amount} cpus.")
        results = pool.map(_process_group, chunks)

    _sum = sum(result[0] for result in results)
    _min = min(result[1] for result in results)
    _max = max(result[2] for result in results)
    return time.time() - start, _sum, _min, _max


def main():
    logging.basicConfig(level=logging.INFO)
    args = _parse_args()
    if args.allow_multithreading:
        _time, _sum, _min, _max = _multithreading_process_file(args.input_file)
    else:
        _time, _sum, _min, _max = _dummy_process_file(args.input_file)

    threading_state = "turned on" if args.allow_multithreading else "turned off"
    logging.info(
        f"Sum is {_sum}.\n"
        f"Max is {_max}.\nMin is {_min}.\n"
        f"Processing time is {_time:.2f}s.\n"
        f"Multithreading was {threading_state}."
    )


if __name__ == "__main__":
    main()
