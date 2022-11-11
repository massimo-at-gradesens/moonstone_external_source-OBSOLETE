"""
GradeSens - External Source package - Async pool

Simple-minded execution pool for any number of async tasks, but limiting the
maximum number of concurrent tasks active at the same time.
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, Gradesens AG"


import asyncio


class AsyncConcurrentPool:
    """
    Simple-minded execution pool for any number of async tasks, but limiting
    the maximum number of concurrent tasks active at the same time.
    """

    def __init__(self, concurrency):
        self.semaphore = asyncio.Semaphore(concurrency)

    def schedule(self, task):
        return self.__task_wrapper(task)

    async def __task_wrapper(self, task):
        async with self.semaphore:
            return await task
