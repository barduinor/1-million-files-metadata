import time
import os
import asyncio
from gen_sample_data.generate_files import generate_file_async, delete_file_async_aio
from box_utils.box_client_ccg import ConfigCCG, get_ccg_user_client
from box_utils.box_uploads import box_upload_file_async, box_delete_file_async


async def test_file_upload_async():
    time_start = time.perf_counter()
    config = ConfigCCG()
    client = get_ccg_user_client(config, config.ccg_user_id)
    folder_id = config.folder_id

    n_files = 10

    for i in range(n_files):
        file_name = await generate_file_async("tests/test_files", f"test_file_{i}.bin", 1024 * 2)

        file = await box_upload_file_async(client, folder_id, file_name)
        await box_delete_file_async(client, file.id)
        await delete_file_async_aio(file_name)

    print(f"Time elapsed for {n_files} files: {time.perf_counter() - time_start}")


async def main():
    await test_file_upload_async()


if __name__ == "__main__":
    asyncio.run(main())
