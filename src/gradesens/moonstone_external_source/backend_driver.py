"""
GradeSens - External Source package - backend drivers

Backend drivers are responsible for formatting and performing requests to the
target remote sources.
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, Gradesens AG"


import abc
import asyncio
import json
from http import HTTPStatus
from typing import Callable, Dict, Iterable, Sequence, Tuple, Union

import aiohttp

from .error import Error, HttpError


class KeyValuePatterns:
    """
    A list of ``[key_pattern, value_pattern]`` entries, each of them applied
    via ``str.format()`` over an input dictionary of ``[key, value]`` entries,
    to produce a ``Backend``-specific output list of ``[key, value]`` to
    be used in the actual backend request.
    """

    Patterns = Union[
        Iterable[Tuple[str, str]],
        dict,
    ]

    def __init__(self, patterns: Patterns):
        if isinstance(patterns, dict):
            patterns = patterns.items()
        self.patterns = tuple(patterns)

    def apply(self, params: Dict[str, str]) -> Sequence[Tuple[str, str]]:
        return tuple(
            (key_pattern.format(**params), value_pattern.format(**params))
            for key_pattern, value_pattern in self.patterns
        )


class BackendDriver(abc.ABC):
    """
    Abstract base class for all backend drivers.

    Backend drivers are responsible for formatting and performing requests to
    the target remote sources.
    """

    @abc.abstractclassmethod
    async def process(self, params: dict) -> Dict[str, str]:
        pass


class HttpRequestProcessor:
    """
    An HTTP request processor takes care of applying input parameters over
    user-specified patterns to create a corresponding URL and query string
    """

    QueryStringPatterns = Union[KeyValuePatterns.Patterns, KeyValuePatterns]

    def __init__(
        self,
        *,
        url_pattern: str,
        query_string_patterns: QueryStringPatterns,
    ):
        self.url_pattern = url_pattern
        if not isinstance(query_string_patterns, KeyValuePatterns):
            query_string_patterns = KeyValuePatterns(query_string_patterns)
        self.query_string_patterns = query_string_patterns

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
        url_pattern: str,
        query_string_patterns: HttpRequestProcessor.QueryStringPatterns,
        response_decoder: Callable[
            [Union[str, bytes]],
            Dict[str, str],
        ] = json.loads,
        max_attempts: int = 1,
        attempt_delay: float = 0.5,
    ):
        self.request_processor = HttpRequestProcessor(
            url_pattern=url_pattern,
            query_string_patterns=query_string_patterns,
        )
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
