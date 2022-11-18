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
from typing import Any, Dict, Union

import aiohttp
from multidict import CIMultiDict

from .error import HTTPError, HTTPResponseError


class BackendDriver(abc.ABC):
    """
    Abstract base class for all backend drivers.

    Backend drivers are responsible for formatting and performing requests to
    the target remote sources.
    """

    class Response:
        def __init__(
            self,
            data: Any,
            headers: CIMultiDict,
        ):
            self.data = data
            self.headers = headers

    @abc.abstractclassmethod
    async def request(
        self,
        url: str,
        headers: Dict[str, str] = {},
        query_string: Dict[str, str] = {},
        data: Union[Any, None] = None,
    ) -> Response:
        pass

    CONTENT_TYPE_HANDLERS = {"application/json": json.loads}

    async def get_raw_result(self, *args, **kwargs) -> Response:
        response = await self.request(*args, **kwargs)
        content_type = response.headers["content-type"].lower()

        try:
            content_type_handler = self.CONTENT_TYPE_HANDLERS[content_type]
        except KeyError:
            valid_content_type_handlers = ", ".join(
                self.CONTENT_TYPE_HANDLERS.keys()
            )
            raise HTTPResponseError(
                f"Unable to process content-type {content_type}.\n"
                f"Supported content-types: {valid_content_type_handlers}"
            ) from None
        response.data = content_type_handler(response.data)
        return response


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
        # aiohttp.ClientSession() must be created in an async function:
        # delay its creation to the first call of request
        self.client_sessions = None

    async def request(
        self,
        url: str,
        headers: Dict[str, str] = {},
        query_string_params: Dict[str, str] = {},
        data: Union[Any, None] = None,
    ) -> BackendDriver.Response:
        """
        Default implementation of HTTP request driver.
        By default a GET operation is performed.
        If ``data`` is not ``None``, a POST is performed instead.
        """

        # Create a single client session once.
        # See recommendation against opening a client session per request
        # at https://docs.aiohttp.org/\
        #   en/stable/client_quickstart.html#make-a-request .
        # Also the comment about letting client sessions live forever at
        # https://github.com/\
        #   aio-libs/aiohttp/issues/789#issuecomment-186333636
        if self.client_session is None:
            self.client_sessions = aiohttp.ClientSession()
        if self.client_session.closed:
            raise HTTPError("HTTP client session unexpectedly closed")

        status = None
        remaining_attempts = self.max_attempts
        while True:
            if data is None:
                request = self.client_session.get(
                    url, params=query_string_params
                )
            else:
                request = self.client_session.post(
                    url, params=query_string_params, data=data
                )
            async with request as response:
                status = response.status

                if status == HTTPStatus.OK:
                    text = await response.text()
                    return self.Response(
                        data=text,
                        headers=response.headers,
                    )

                remaining_attempts -= 1
                if remaining_attempts <= 0:
                    raise HTTPError(
                        f"HTTP request to {url!r} failed"
                        f" after {self.max_attempts} attempts",
                        status=status,
                    )
                await asyncio.sleep(self.attempt_delay)
