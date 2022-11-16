"""
GradeSens - External Source package - IO driver

Abstract class specifying the interface for IO operations, e.g. to retrieve
configuration data structures from a DB.
The actual implementation is to be provided by application-specific
integrations.
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, Gradesens AG"


import abc
from datetime import datetime, timedelta
from typing import Callable

from .configuration import (
    AuthenticationContext,
    CommonConfiguration,
    MachineConfiguration,
)
from .error import Error


class IODriver(abc.ABC):
    """
    Abstract class specifying the interface for IO operations, e.g. to retrieve
    configuration data structures from a DB.
    The actual implementation is to be provided by application-specific
    integrations.

    On top of the load methods to be specialized in derived classes,
    :class:`IODriver` provides a simple time-based-expiration caching
    layer so as to minimize IO operations.
    """

    DEFAULT_CACHE_EXPIRATION_DELAY = timedelta(minutes=30)

    class Cache:
        class Entry:
            def __init__(self, value, expiration_time):
                self.value = value
                self.expiration_time = expiration_time

        def __init__(
            self,
            entry_description: str,
            expiration_delay: timedelta,
            async_load_function: Callable,
        ):
            self.entry_description = entry_description
            self.expiration_delay = expiration_delay
            self.async_load_function = async_load_function
            self.entries = {}

        async def get(self, key):
            try:
                entry = self.entries[key]
            except KeyError:
                pass
            else:
                if entry.expiration_time >= datetime.now():
                    return entry.value

            try:
                value = await self.async_load_function(key)
                if value is None:
                    raise Error("")
            except Exception as err:
                err = str(err)
                if err != "":
                    err = f": {err}"
                raise Error(
                    f"Unable to load a {self.entry_description!r} for {key!r}"
                    f"{err}"
                ) from None
            entry = self.Entry(
                expiration_time=datetime.now() + self.expiration_delay,
                value=value,
            )
            self.entries[key] = entry
            return entry.value

        def clear(self):
            self.entries.clear()

    def __init__(
        self,
        *,
        cache_expiration_delay: timedelta = DEFAULT_CACHE_EXPIRATION_DELAY,
    ):

        self.authentication_contexts = self.Cache(
            entry_description="authentication context",
            expiration_delay=cache_expiration_delay,
            async_load_function=self.load_authentication_context,
        )
        self.common_configurations = self.Cache(
            entry_description="common_configuration",
            expiration_delay=cache_expiration_delay,
            async_load_function=self.load_common_configuration,
        )
        self.machine_configurations = self.Cache(
            entry_description="machine_configuration",
            expiration_delay=cache_expiration_delay,
            async_load_function=self.load_machine_configuration,
        )

    def clear_cache(self):
        """
        Clear all caches.
        """
        self.common_configurations.clear()
        self.machine_configurations.clear()
        self.authentication_contexts.clear()

    @abc.abstractmethod
    async def load_authentication_context(
        self, id: AuthenticationContext.Identifier
    ) -> AuthenticationContext:
        """
        The actual load method, to be implemented by derived classes.
        """

    @abc.abstractmethod
    async def load_common_configuration(
        self, id: CommonConfiguration.Identifier
    ) -> CommonConfiguration:
        """
        The actual load method, to be implemented by derived classes.
        """

    @abc.abstractmethod
    async def load_machine_configuration(
        self, id: MachineConfiguration.Identifier
    ) -> MachineConfiguration:
        """
        The actual load method, to be implemented by derived classes.
        """
