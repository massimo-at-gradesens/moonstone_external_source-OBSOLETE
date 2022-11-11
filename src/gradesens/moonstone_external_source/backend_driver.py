"""
GradeSens - External Source package - Backend drivers

Backend drivers are responsible for formatting and performing requests to the
target remote sources.
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, Gradesens AG"


import abc
import asyncio
import json
from http import HTTPStatus
from typing import Callable, Dict, Sequence, Tuple, Union

import aiohttp

from .configuration import MachineConfiguration, MeasurementConfiguration
from .error import Error, HttpError


class BackendDriver(abc.ABC):
    """
    Abstract base class for all backend drivers.

    Backend drivers are responsible for formatting and performing requests to
    the target remote sources.
    """

    def __init__(self, *, settings: MeasurementConfiguration.SettingsType):
        self.settings = settings

    @abc.abstractclassmethod
    async def process(self, params: dict) -> Dict[str, str]:
        pass


class HttpRequestProcessor:
    """
    An HTTP request processor takes care of applying input parameters over
    user-specified patterns to create a corresponding URL and query string
    """

    def get_url(self, params: Dict[str, str]) -> str:
        """
        Return the URL for the user-specified parameters
        """
        try:
            return self.url_pattern.format(**params)
        except Exception as excp:
            raise Error(
                f"Unable to expand URL pattern {self.url_pattern!r}"
                f" with params {params}:\n"
                f"   {excp}"
            ) from None

    def get_query_string_params(
        self, params: Dict[str, str]
    ) -> Sequence[Tuple[str, str]]:
        """
        Return the query string parameters for the user-specified parameters
        """
        try:
            return self.query_string_patterns.apply(params)
        except Exception as excp:
            raise Error(
                "Unable to expand query string patterns"
                f" {self.query_string_patterns.patterns}"
                f" with params {params}:\n"
                f"   {excp}"
            ) from None


class HttpBackendDriver(BackendDriver):
    """
    HTTP backend driver.
    """

    def __init__(
        self,
        *,
        settings: MachineConfiguration.SettingsType,
        response_decoder: Callable[
            [Union[str, bytes]],
            Dict[str, str],
        ] = json.loads,
        max_attempts: int = 1,
        attempt_delay: float = 0.5,
    ):
        self.request_processor = HttpRequestProcessor()
        self.response_decoder = response_decoder
        self.max_attempts = max_attempts
        self.attempt_delay = attempt_delay

    async def process(self, params: dict) -> Dict[str, str]:
        url = self.request_processor.get_url(params)
        query_string_params = self.request_processpr.get_query_string_params(
            params
        )

        async with aiohttp.ClientSession() as session:
            status = None
            remaining_attempts = self.max_attempts
            while True:
                async with session.get(
                    url, params=query_string_params
                ) as resp:
                    status = resp.status
                    text = await resp.text()

                    if status == HTTPStatus.OK:
                        return self.response_decoder(text)

                    remaining_attempts -= 1
                    if remaining_attempts <= 0:
                        raise HttpError(
                            f"HTTP request to {url!r} failed"
                            f" after {self.max_attempts} attempts",
                            status=status,
                        )
                    await asyncio.sleep(self.attempt_delay)
