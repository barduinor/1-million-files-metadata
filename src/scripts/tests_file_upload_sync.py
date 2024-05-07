import time
import os
from gen_sample_data.generate_files import generate_file, delete_file
from box_utils.box_client_ccg import ConfigCCG, get_ccg_user_client
from box_utils.box_uploads import box_upload_file


def test_file_upload_sync():
    time_start = time.perf_counter()
    config = ConfigCCG()
    client = get_ccg_user_client(config, config.ccg_user_id)
    folder_id = config.folder_id

    n_files = 10

    for i in range(n_files):
        file_name = generate_file("tests/test_files", f"test_file_{i}.bin", 1024 * 2)

        file = box_upload_file(client, folder_id, file_name)
        client.files.delete_file_by_id(file.id)
        delete_file(file_name)

    print(f"Time elapsed for {n_files} files: {time.perf_counter() - time_start}")


def main():
    test_file_upload_sync()


if __name__ == "__main__":
    main()
