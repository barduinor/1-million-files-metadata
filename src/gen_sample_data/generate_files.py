import os
import asyncio
import aiofiles
import aiofiles.os


def generate_file(path: str, file_name, size: int):

    # create a test file with random bytes
    test_file = os.path.join(path, file_name)
    with open(test_file, "wb") as file:
        file.write(os.urandom(size))

    return test_file


async def generate_file_aio(path: str, file_name, size: int):

    # create a test file with random bytes
    test_file = os.path.join(path, file_name)
    async with aiofiles.open(test_file, "wb") as file:
        await file.write(os.urandom(size))

    return test_file


async def generate_file_async(path: str, file_name, size: int):
    return generate_file(path, file_name, size)


def delete_file(file):
    os.remove(file)


async def delete_file_async(file):
    await delete_file(file)


async def delete_file_async_aio(file):
    await aiofiles.os.remove(file)


def log_last_process(log_file_path, data):
    with open(log_file_path, mode="w") as f:
        f.write(data)


async def log_last_process_async(log_file_path, data):
    await log_last_process(log_file_path, data)


async def log_last_process_aio(log_file_path, data):
    async with aiofiles.open(log_file_path, mode="w") as f:
        await f.write(data)


def append_log(log_file, line):
    with open(log_file, mode="a") as f:
        f.write(line + "\n")


async def append_log_aio(log_file, line):
    async with aiofiles.open(log_file, mode="a") as f:
        await f.write(line + "\n")
