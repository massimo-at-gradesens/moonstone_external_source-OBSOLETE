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

    def __str__(self):
        return f"{self.value}@{self.timestamp}"


class ResultRow(list):
    def __init__(
        self,
        timestamp: datetime,
        values: List[ResultEntry],
    ):
        self.timestamp = timestamp
        super().__init__(values)

    def __str__(self):
        return ", ".join(map(str, [self.timestamp] + list(self)))


class Result(list):
    def __init__(
        self,
        timestamps_and_values: Iterable[Tuple[datetime, Dict[str, Any]]],
    ):
        super().__init__()

        timestamps_and_values = list(timestamps_and_values)

        self.headers = None

        headers_map = {}

        if len(timestamps_and_values) == 0:
            return

        self.headers = []
        _, values = timestamps_and_values[0]
        for key in values.keys():
            if isinstance(key, (list, tuple)):
                key = key[0]
            self.headers.append(key)
            headers_map[key] = len(headers_map)

        for timestamp, values in timestamps_and_values:
            value_list = [None] * len(self.headers)
            for key, value in values.items():
                if isinstance(value, (list, tuple)):
                    value = value[0]
                value_list[headers_map[key]] = ResultEntry(**value)
            self.append(ResultRow(timestamp=timestamp, values=value_list))

    def __str__(self):
        rows = "\n".join(map(str, self))
        headers = ", ".join(map(str, ["@timestamp"] + self.headers))
        return f"HEADERS: {headers}\n" "DATA:\n" + rows


class ExternalSourceSession:
    """
    Retrieve measurement data from one or more external sources via concurrent
    requests.
    The total number of concurrent requests active at the same is limited via
    an ``AsyncConcurrentPool``
    """

    def __init__(
        self,
        client_session: "IOManager.ClientSession",
    ):
        self.client_session = client_session

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def close(self):
        pass

    async def get_data(
        self,
        *,
        machine_id: MachineConfiguration.Id,
        timestamps: Iterable[datetime],
    ) -> Result:
        machine_configuration = (
            await self.client_session.machine_configurations.get(machine_id)
        )

        timestamps = list(timestamps)

        request_tasks = []
        task_pool = self.client_session.task_pool
        for timestamp in timestamps:
            request_tasks.append(
                task_pool.schedule(
                    machine_configuration.fetch_result(
                        client_session=self.client_session,
                        timestamp=timestamp,
                    )
                )
            )

        results = await asyncio.gather(*request_tasks)

        result = Result(zip(timestamps, results))
        return result
