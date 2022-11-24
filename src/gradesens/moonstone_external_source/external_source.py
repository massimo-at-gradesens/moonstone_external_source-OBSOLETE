"""
GradeSens - External Source package - External sources

The external sources are the main entry point of the External Source package.
They provide access to the actual core functionality of the package:
customizable support to retrieve measurement data from external sources.
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, GradeSens AG"


import asyncio
from datetime import datetime
from typing import Any, Dict, Iterable, List, Tuple

from .configuration import MachineConfiguration
from .io_manager import IOManager


class ResultEntry:
    def __init__(
        self,
        timestamp: datetime,
        value: Any,
    ):
        self.timestamp = timestamp
        self.value = value


class ResultRow:
    def __init__(
        self,
        timestamp: datetime,
        values: List[ResultEntry],
    ):
        self.timestamp = timestamp
        self.values = values


class Result:
    def __init__(
        self,
        timestamps_and_values: Iterable[Tuple[datetime, Dict[str, Any]]],
    ):
        timestamps_and_values = list(timestamps_and_values)

        self.headers = None
        self.rows = []

        headers_map = {}

        if len(timestamps_and_values):
            return

        self.headers = []
        _, values = timestamps_and_values[0]
        for key in values.keys():
            self.headers.append(key)
            headers_map[key] = len(headers_map)

        for timestamp, values in timestamps_and_values:
            value_list = [None] * len(self.headers)
            for key, value in values.items():
                value_list[headers_map[key]] = ResultEntry(**value)
            self.rows.append(ResultRow(timestamp=timestamp, values=value_list))


class ExternalSource:
    """
    Retrieve measurement data from one or more external sources via concurrent
    requests.
    The total number of concurrent requests active at the same is limited via
    an ``AsyncConcurrentPool``
    """

    async def async_get_data(
        self,
        *,
        client_session: "IOManager.ClientSession",
        timestamps: Iterable[datetime],
        machine_id: MachineConfiguration.Id,
    ) -> Result:
        machine_configuration = (
            await self.client_session.machine_configurations.get(machine_id)
        )

        timestamps = list(timestamps)

        request_tasks = []
        task_pool = client_session.task_pool
        for timestamp in timestamps:
            request_tasks.append(
                task_pool.schedule(
                    machine_configuration.fetch_result(timestamp=timestamp)
                )
            )

        results = await asyncio.gather(*request_tasks.values())

        result = Result(zip(timestamps, results))
        return result
