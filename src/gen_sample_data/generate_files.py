import os
import asyncio
import aiofiles
import aiofiles.os


def generate_file(path: str, file_name, size: int):

    # create path if does not exist
    if not os.path.exists(path):
        os.makedirs(path)

    # create a test file with random bytes
    test_file = os.path.join(path, file_name)
    with open(test_file, "wb") as file:
        file.write(os.urandom(size))

    return test_file


async def generate_file_aio(path: str, file_name, size: int):
    # create path if does not exist
    if not await aiofiles.os.path.exists(path):
        aiofiles.os.makedirs(path)

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
