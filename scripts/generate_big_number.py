import argparse
import logging
import time

import numpy as np

from utils.constants import INTS_IN_GB, MAX_VALUE


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s",
        "--size",
        type=int,
        default=2,
        help="desired size of output file."
    )
    parser.add_argument(
        "-o",
        "--output_file",
        default="big_numbers_binary.bin",
        help="output file."
    )
    parser.add_argument(
        "--batching",
        default=False,
        action='store_true',
        help=("allow to write in file not whole array of numbers"
              "but batch by batch.")
    )
    args = parser.parse_args()
    return args


def generate_big_numbers_file(
    desired_size: int = 2,
    output_file: str = "big_numbers_binary.bin",
    batching: bool = False
) -> float:
    """Generate binary file containing some big endians.

    Args:
        desired_size (int): Desired size of binary file. Defaults: 2.
        output_file (str, optional): Name of output file.
            Defaults to "big_numbers_binary.bin".
        batching (bool, optional): Write whole array in file or batch by batch.
            Defaults to False.

    Returns:
        float: time taken to generate such file.
    """
    start = time.time()
    with open(output_file, "wb") as file:
        if batching:
            for i in range(desired_size):
                one_gb_ints = np.random.randint(
                    low=0,
                    high=MAX_VALUE,
                    size=INTS_IN_GB,
                    dtype=np.uint32,
                )
                # unfortunatelly, ability to use not native byteorder
                # is depricated and will cause ValueError in the future.
                one_gb_ints = one_gb_ints.newbyteorder()
                assert (
                    one_gb_ints.dtype.byteorder == ">"
                ), "Result array somehow is not big endian"
                file.write(one_gb_ints.tobytes())
                logging.info(f"wrote {i+1} GB/{desired_size} GB.")
        else:
            one_gb_ints = np.random.randint(
                    low=0,
                    high=MAX_VALUE,
                    size=INTS_IN_GB * desired_size,
                    dtype=np.uint32,
                )
            one_gb_ints = one_gb_ints.newbyteorder()
            assert (
                    one_gb_ints.dtype.byteorder == ">"
                ), "Result array somehow is not big endian"
            file.write(one_gb_ints.tobytes())
            logging.info(f"wrote {desired_size} GB.")
    finish = time.time()
    return finish - start


def main():
    logging.basicConfig(level=logging.INFO)
    args = _parse_args()
    _time = generate_big_numbers_file(
        desired_size=args.size,
        output_file=args.output_file,
        batching=args.batching
    )
    batching_state = "turned on" if args.batching else "turned off"
    logging.info(
        f"{args.size} GB file {args.output_file} "
        f"was written for {_time:.2f} s.\n"
        f"Batching was {batching_state}."
    )


if __name__ == "__main__":
    main()
