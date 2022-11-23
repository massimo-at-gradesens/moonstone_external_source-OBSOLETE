"""
GradeSens - External Source package - Backend drivers

Backend drivers are responsible for formatting and performing requests to the
target remote sources.
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, GradeSens AG"


import abc
import asyncio
import json
from http import HTTPStatus
from typing import Any, Dict, Optional

import aiohttp
from multidict import CIMultiDict

from .error import BackendError, HTTPError, HTTPResponseError


class BackendDriver:
    """
    Base class for all backend drivers.

    The whole backend-specific customization is not to be provided directly by
    overriding this class' methods, but rather doing by doing it within its
    :class:`.ClientSession` inner class.
    """

    CONTENT_TYPE_HANDLERS = {"application/json": json.loads}

    def __init__(
        self,
        *,
        max_attempts: int = 1,
        attempt_delay: float = 0.5,
    ):
        self.max_attempts = max_attempts
        self.attempt_delay = attempt_delay

    class Response:
        """
        Generic response for client operations
        """

        def __init__(
            self,
            headers: CIMultiDict,
            data: Any,
        ):
            self.headers = headers
            self.data = data

    class ClientSession(abc.ABC):
        """
        Abstract base class for all backend drivers's context managers.

        Derived context manaegers are responsible for formatting and performing
        requests to the target remote sources, and extract the corresponding
        output results.
        """

        def __init__(
            self,
            backend_driver: "BackendDriver",
        ):
            self.backend_driver = backend_driver

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            await self.close()

        async def close(self):
            pass

        async def execute(self, *args, **kwargs) -> "BackendDriver.Response":
            max_attempts = self.backend_driver.max_attempts
            remaining_attempts = max_attempts
            while True:
                try:
                    response = await self.execute_raw(*args, **kwargs)
                except BackendError as err:
                    remaining_attempts -= 1
                    if remaining_attempts <= 0:
                        if max_attempts > 1:
                            err.message = (
                                err.message
                                + f" (after {max_attempts} attempts)"
                            )
                        raise
                else:
                    break
                await asyncio.sleep(self.backend_driver.attempt_delay)

            content_type = response.headers["content-type"].lower()
            content_type = set(
                comp.strip() for comp in content_type.split(";")
            )
            content_type_handlers = []
            for key, value in BackendDriver.CONTENT_TYPE_HANDLERS.items():
                if key in content_type:
                    content_type_handlers.append(value)
            if len(content_type_handlers) != 1:
                valid_content_type_handlers = ", ".join(
                    BackendDriver.CONTENT_TYPE_HANDLERS.keys()
                )
                raise HTTPResponseError(
                    f"Unable to process content-type {content_type}.\n"
                    f"Supported content-types: {valid_content_type_handlers}"
                ) from None
            content_type_handler = content_type_handlers[0]

            response.data = content_type_handler(response.data)
            return response

        @abc.abstractclassmethod
        async def execute_raw(
            self,
            url: str,
            headers: Dict[str, str] = {},
            query_string: Dict[str, str] = {},
            data: Optional[Any] = None,
        ) -> "BackendDriver.Response":
            """
            THE method enabling executing backend requests.

            To be implemented by derived classes.
            """

    def client_session(self, *args, **kwargs):
        """
        Return the client session context manager, which is the actual entry
        point to perform any backend transaction.

        The returned client sessions is a backend-specific specialization
        derived from :class:`.ClientSession`, customized within
        backend-specific classes derived from this class
        :class:`BackendDriver`.
        """

        return self.ClientSession(
            *args,
            **kwargs,
            backend_driver=self,
        )


class AsyncHTTPBackendDriver(BackendDriver):
    """
    Async HTTP backend driver.

    This :class:`BackendDriver` specialization is based on
    `AIOHTTP package <https://docs.aiohttp.org/en/stable/>`_
    """

    class ClientSession(BackendDriver.ClientSession):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.aiohttp_client_session = aiohttp.ClientSession()

        async def close(self):
            await self.aiohttp_client_session.close()

        async def execute_raw(
            self,
            url: str,
            headers: Dict[str, str] = {},
            query_string: Dict[str, str] = {},
            data: Optional[Any] = None,
            request_type: str = "GET",
        ) -> BackendDriver.Response:
            """
            Default implementation of HTTP request driver.
            By default a GET operation is performed.
            If ``data`` is not ``None``, a POST is performed instead.
            """

            status = None
            request_kwargs = dict(
                url=url,
            )
            if headers:
                request_kwargs.update(headers=headers)
            if query_string:
                request_kwargs.update(params=query_string)
            if data is not None:
                request_kwargs.update(data=data)
            try:
                request_func = {
                    "GET": self.aiohttp_client_session.get,
                    "PUT": self.aiohttp_client_session.put,
                    "POST": self.aiohttp_client_session.post,
                }[request_type.upper()]
            except KeyError:
                raise HTTPError(
                    f"Unsupported or invalid request type {request_type}"
                )
            async with request_func(**request_kwargs) as response:
                status = response.status

                if status == HTTPStatus.OK:
                    text = await response.text()
                    return BackendDriver.Response(
                        data=text,
                        headers=response.headers,
                    )

                raise HTTPError(
                    f"HTTP request to {url!r} failed",
                    status=status,
                )
