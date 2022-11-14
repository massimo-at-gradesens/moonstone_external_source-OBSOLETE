"""
GradeSens - External Source package - Backend drivers

Backend drivers are responsible for formatting and performing requests to the
target remote sources.
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, Gradesens AG"


import abc
import asyncio
from http import HTTPStatus
from typing import Dict

import aiohttp

from .error import HTTPError


class BackendDriver(abc.ABC):
    """
    Abstract base class for all backend drivers.

    Backend drivers are responsible for formatting and performing requests to
    the target remote sources.
    """

    @abc.abstractclassmethod
    async def process(
        self,
        url: str,
        headers: Dict[str, str] = {},
        query_string: Dict[str, str] = {},
    ) -> str:
        pass


class HTTPBackendDriver(BackendDriver):
    """
    HTTP backend driver.
    """

    def __init__(
        self,
        *,
        max_attempts: int = 1,
        attempt_delay: float = 0.5,
    ):
        self.max_attempts = max_attempts
        self.attempt_delay = attempt_delay

    async def process(
        self,
        url: str,
        headers: Dict[str, str] = {},
        query_string_params: Dict[str, str] = {},
    ) -> str:

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
                        raise HTTPError(
                            f"HTTP request to {url!r} failed"
                            f" after {self.max_attempts} attempts",
                            status=status,
                        )
                    await asyncio.sleep(self.attempt_delay)
