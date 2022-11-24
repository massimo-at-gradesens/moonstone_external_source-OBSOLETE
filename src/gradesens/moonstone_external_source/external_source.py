"""
GradeSens - External Source package - External sources

The external sources are the main entry point of the External Source package.
They provide access to the actual core functionality of the package:
customizable support to retrieve measurement data from external sources.
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, GradeSens AG"


import asyncio
import json
from collections import OrderedDict
from datetime import datetime
from typing import Any, Dict, Iterable, List

from .backend_driver import BackendDriver
from .configuration import MachineConfiguration, MeasurementConfiguration
from .error import Error, HTTPResponseError
from .io_manager import IOManager


class ExternalSource:
    """
    Retrieve measurement data from one or more external sources via concurrent
    requests.
    The total number of concurrent requests active at the same is limited via
    an ``AsyncConcurrentPool``
    """

    class Result:
        def __init__(
            self,
            timestamp: datetime,
            value: Any,
        ):
            self.timestamp = timestamp
            self.value = value

    ResultListType = List[Result]
    ResultsType = Dict[MeasurementConfiguration.Id, ResultListType]

    async def async_get_data(
        self,
        *,
        client_session: "IOManager.ClientSession",
        timestamps: Iterable[datetime],
        machine_id: MachineConfiguration.Id,
    ) -> ResultsType:
        machine_configuration = (
            await self.client_session.machine_configurations.get(machine_id)
        )
        print(machine_configuration)

        # for timestamp in timestamps:

        for timestamp in timestamps:
            # settings = await resolver.get_aggregated_settings(
            #     start_time=timestamp - start_time_margin,
            #     end_time=timestamp + end_time_margin,
            # )

            # Use an OrderedDict to guarantee order repeatability, so as to
            # guarantee that ids and results can be correctly zipped
            # together after asyncio.gather()
            request_tasks = OrderedDict()

            # for measurement_id, measurement_settings in settings.items():
            #     request_tasks[
            #         measurement_id
            #     ] = self.request_task_pool.schedule(
            #         self.backend_driver.process(measurement_settings)
            #     )

        responses = await asyncio.gather(*request_tasks.values())
        return responses

        # return {
        #     measurement_id: self.process_response(
        #         response=response,
        #         settings=measurement_settings[measurement_id],
        #     )
        #     for measurement_id, response in zip(
        #         request_tasks.keys(), responses
        #     )
        # }

    CONTENT_TYPE_HANDLERS = {"application/json": json.loads}

    def process_response(
        self,
        response: BackendDriver.Response,
        start_time: datetime,
        end_time: datetime,
        settings: MeasurementConfiguration.InterpolatedSettings,
    ):
        content_type = response.headers["content-type"]

        try:
            content_type_handler = self.CONTENT_TYPE_HANDLERS[content_type]
        except KeyError:
            valid_content_type_handlers = ", ".join(
                self.CONTENT_TYPE_HANDLERS.keys()
            )
            raise HTTPResponseError(
                f"Unable to process content-type {content_type}.\n"
                f"Supported content-types: {valid_content_type_handlers}"
            )
        result = content_type_handler(response.text)
        if not isinstance(result, list):
            result = [result]

    def process_single_result(
        self,
        raw_result,
        settings: MeasurementConfiguration.InterpolatedSettings,
    ):
        pass

    @staticmethod
    def __first_non_null(*values):
        try:
            return next(filter(lambda value: value is not None, values))
        except StopIteration:
            raise Error("Non null value expected") from None
