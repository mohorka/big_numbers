import numpy as np
MAX_VALUE = 2**32
with open("output.bin", "wb") as file:
    big_ints = np.random.randint(
                    low=0,
                    high=MAX_VALUE,
                    size=5000,
                    dtype=np.uint32,
                )
    big_ints = big_ints.newbyteorder()
    assert (
        big_ints.dtype.byteorder == ">"
        ), "Result array somehow is not big endian"
    file.write(big_ints.tobytes())