"""
GradeSens - External Source package - External sources

The external sources are the main entry point of the External Source package.
They provide access to the actual core functionality of the package:
customizable support to retrieve measurement data from external sources.
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, Gradesens AG"


import asyncio
from collections import OrderedDict
from datetime import datetime, timedelta
from typing import Iterable, Type, Union

from .async_concurrent_pool import AsyncConcurrentPool
from .backend_driver import BackendDriver, HTTPBackendDriver
from .configuration import MachineConfiguration, MeasurementConfiguration
from .error import Error
from .io_driver import IODriver


class ExternalSource:
    """
    Retrieve measurement data from one or more external sources via concurrent
    requests.
    The total number of concurrent requests active at the same is limited via
    an ``AsyncConcurrentPool``
    """

    DEFAULT_TIME_MARGIN = timedelta(minutes=5)

    def __init__(
        self,
        io_driver: IODriver,
        request_task_pool: Union[AsyncConcurrentPool, int, None] = 10,
        time_margin: timedelta = DEFAULT_TIME_MARGIN,
        start_time_margin: Union[timedelta, None] = None,
        end_time_margin: Union[timedelta, None] = None,
        backend_driver: Union[
            Type[BackendDriver], BackendDriver
        ] = HTTPBackendDriver,
        **kwargs,
    ):
        self.io_driver = io_driver

        if request_task_pool is None:
            request_task_pool = 1
        if isinstance(request_task_pool, int):
            request_task_pool = max(1, request_task_pool)
            request_task_pool = AsyncConcurrentPool(request_task_pool)
        self.request_task_pool = request_task_pool

        self.time_margin = time_margin
        self.start_time_margin = start_time_margin
        self.end_time_margin = end_time_margin

        if isinstance(backend_driver, type):
            self.backend_driver = backend_driver(**kwargs)
        else:
            assert len(kwargs) == 0
            self.backend_driver = backend_driver

    def get_data(self, *args, **kwargs):
        asyncio.run(self.async_get_data(*args, **kwargs))

    async def async_get_data(
        self,
        *,
        start_time: datetime,
        end_time: datetime,
        machine_identifier: MachineConfiguration.Identifier,
        measurements_identifiers: Union[
            None, Iterable[MeasurementConfiguration.Identifier]
        ] = None,
        time_margin: Union[timedelta, None] = None,
        start_time_margin: Union[timedelta, None] = None,
        end_time_margin: Union[timedelta, None] = None,
    ):
        time_margin = self.__first_non_none(time_margin, self.time_margin)

        start_time_margin = self.__first_non_none(
            start_time_margin,
            self.start_time_margin,
            time_margin,
        )
        end_time_margin = self.__first_non_none(
            end_time_margin,
            self.end_time_margin,
            time_margin,
        )

        machine_configuration = (
            await self.io_driver.machine_configurations.get(machine_identifier)
        )
        resolver = machine_configuration.get_setting_resolver(self.io_driver)
        settings = await resolver.get_settings(
            measurements_identifiers=measurements_identifiers,
        )

        # Use an OrderedDict to guarantee order repeatability, so as to
        # guarantee that identifiers and results can be correctly zipped
        # together after asyncio.gather()
        request_tasks = OrderedDict()

        for measurement_identifier, measurement_settings in settings.items():
            request_tasks[
                measurement_identifier
            ] = self.request_task_pool.schedule(
                self.backend_driver.process(measurement_settings)
            )

        results = await asyncio.gather(*request_tasks.values())
        return {
            measurement_identifier: result
            for measurement_identifier, result in zip(
                request_tasks.keys(), results
            )
        }

    @staticmethod
    def __first_non_null(*values):
        try:
            return next(filter(lambda value: value is not None, values))
        except StopIteration:
            raise Error("Non null value expected") from None
