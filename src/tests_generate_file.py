import time
import os
from gen_sample_data.generate_files import generate_file, delete_file


def test_generate_file_sync():
    time_start = time.perf_counter()

    n_files = 10000

    for i in range(n_files):
        file = generate_file("tests/test_files", f"test_file_{i}.bin", 1024 * 2)
        delete_file(file)

    print(f"Time elapsed for {n_files} files: {time.perf_counter() - time_start}")


def main():
    test_generate_file_sync()


if __name__ == "__main__":
    main()
