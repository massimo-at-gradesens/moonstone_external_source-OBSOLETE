"""
GradeSens - External Source package - External sources

The external sources are the main entry point of the External Source package.
They provide access to the actual core functionality of the package:
customizable support to retrieve measurement data from external sources.
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, Gradesens AG"


import asyncio
from datetime import datetime, timedelta
from typing import Type, Union

from .async_concurrent_pool import AsyncConcurrentPool
from .backend_driver import BackendDriver, HttpBackendDriver
from .error import Error


class ExternalSource:
    """
    Retrieve measurement data from one or more external sources via concurrent
    requests.
    The total number of concurrent requests active at the same is limited via
    an ``AsyncConcurrentPool``
    """

    def __init__(
        self,
        concurrency=10,
        backend_driver: Union[
            Type[BackendDriver], BackendDriver
        ] = HttpBackendDriver,
        **kwargs,
    ):
        if concurrency is None:
            concurrency = 1
        else:
            concurrency = max(1, concurrency)
        self.request_task_pool = AsyncConcurrentPool(concurrency)

        if isinstance(backend_driver, type):
            self.backend_driver = backend_driver(**kwargs)
        else:
            assert len(kwargs) == 0
            self.backend_driver = backend_driver

    def get_data(self, *args, **kwargs):
        asyncio.run(self.async_get_data(*args, **kwargs))

    DEFAULT_TIME_MARGIN = timedelta(minutes=5)

    async def async_get_data(
        self,
        start_time: datetime,
        end_time: datetime,
        time_margin: timedelta = DEFAULT_TIME_MARGIN,
        begin_time_margin: Union[timedelta, None] = None,
        end_time_margin: Union[timedelta, None] = None,
    ):
        if self.__is_naive_time(start_time):
            raise Error(f"Start time is not timezone-aware: {start_time}")
        if self.__is_naive_time(end_time):
            raise Error(f"End time is not timezone-aware: {end_time}")

        begin_time_margin = self.__first_non_none(
            begin_time_margin, time_margin
        )
        end_time_margin = self.__first_non_none(end_time_margin, time_margin)
        request_tasks = []

        # for node in nodes:
        #     request_tasks.append(
        #         self.request_task_pool.schedule(
        #             self.backend_driver.process(aaaaaa)
        #         )
        #     )

        results = await asyncio.gather(*request_tasks)
        return results

    @staticmethod
    def __is_naive_time(time):
        if time.tzinfo is None:
            return True
        if time.tzinfo.utcoffset(time) is None:
            return True
        return False

    @staticmethod
    def __first_non_null(*values):
        try:
            return next(filter(lambda value: value is not None, values))
        except StopIteration:
            raise Error("Non null value expected") from None
