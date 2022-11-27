"""
GradeSens - External Source package - Async pool

Simple-minded execution pool for any number of async tasks, but limiting the
maximum number of concurrent tasks active at the same time.
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, GradeSens AG"


import asyncio


class AsyncConcurrentPool:
    """
    Simple-minded execution pool for any number of async tasks, but limiting
    the maximum number of concurrent tasks active at the same time.
    """

    def __init__(self, concurrency):
        self.semaphore = asyncio.Semaphore(concurrency)

    def schedule(self, coroutine):
        return self.__coroutine_wrapper(coroutine)

    def schedule_task(self, coroutine):
        return asyncio.create_task(self.schedule(coroutine))

    async def __coroutine_wrapper(self, coroutine):
        async with self.semaphore:
            return await coroutine
