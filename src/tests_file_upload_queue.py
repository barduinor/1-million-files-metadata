import time
import os
import asyncio
import uuid
from asyncio import Queue
from box_sdk_gen import BoxClient
from gen_sample_data.generate_files import generate_file_async, delete_file_async_aio
from box_utils.box_client_ccg import ConfigCCG, get_ccg_user_client
from box_utils.box_uploads import box_upload_file_async, box_delete_file_async


async def upload_worker(worker_id, queue: Queue, client: BoxClient, folder_id: str):
    while True:
        file_path, file_name = await queue.get()

        local_file = await generate_file_async(file_path, file_name, 1024 * 2)
        file = await box_upload_file_async(client, folder_id, local_file)
        # await box_delete_file_async(client, file.id)
        await delete_file_async_aio(local_file)
        queue.task_done()
        print(f"Worker id: {worker_id} uploaded {file_name}")


async def main():
    time_start = time.monotonic()
    config = ConfigCCG()
    client = get_ccg_user_client(config, config.ccg_user_id)
    folder_id = config.folder_id

    n_files = 10
    n_workers = 3

    queue = asyncio.Queue()

    for i in range(n_files):
        file_name = "tests/test_files", f"test_file_{i}_{uuid.uuid4()}.bin"
        # print(file_name)
        queue.put_nowait(file_name)

    tasks = []
    for i in range(n_workers):
        tasks.append(asyncio.create_task(upload_worker(f"Worker-{i}", queue, client, folder_id)))

    await queue.join()
    print(f"Time elapsed for {n_files} files: {time.monotonic() - time_start}")

    # Cancel our worker tasks.
    for task in tasks:
        task.cancel()

    # Wait until all worker tasks are cancelled.
    await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
