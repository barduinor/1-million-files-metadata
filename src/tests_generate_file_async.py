import asyncio
import aiofiles.os
import time
import os
from gen_sample_data.generate_files import (
    generate_file_async,
    delete_file_async,
    delete_file,
    delete_file_async_aio,
    generate_file_aio,
)


async def test_generate_file_sync():
    time_start = time.perf_counter()

    n_files = 10000

    for i in range(n_files):
        file = await generate_file_aio("tests/test_files", f"test_file_{i}.bin", 1024 * 2)
        # await delete_file_async_aio(file)
        await aiofiles.os.remove(file)

    print(f"Time elapsed for {n_files} files: {time.perf_counter() - time_start}")


async def main():
    await test_generate_file_sync()


if __name__ == "__main__":
    asyncio.run(main())
