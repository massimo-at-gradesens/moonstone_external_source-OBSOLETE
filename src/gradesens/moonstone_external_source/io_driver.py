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
    AuthenticationConfiguration,
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

        async def get(self, id):
            try:
                entry = self.entries[id]
            except KeyError:
                pass
            else:
                if entry.expiration_time >= datetime.now():
                    return entry.value

            try:
                value = await self.async_load_function(id)
                if value is None:
                    raise Error("")
            except Exception as err:
                err = str(err)
                if err != "":
                    err = f": {err}"
                raise Error(
                    f"Unable to load a {self.entry_description!r} for {id!r}"
                    f"{err}"
                ) from None
            entry = self.Entry(
                expiration_time=datetime.now() + self.expiration_delay,
                value=value,
            )
            self.entries[id] = entry
            return entry.value

        def clear(self):
            self.entries.clear()

    class AuthenticationContextCache(Cache):
        def __init__(
            self,
            entry_description: str,
            expiration_delay: timedelta,
            async_configuration_load_function: Callable,
        ):
            super().__init__(
                entry_description=entry_description,
                expiration_delay=expiration_delay,
                async_load_function=self.load_context,
            )
            self.authentication_configurations = IODriver.Cache(
                entry_description="authentication configuration",
                expiration_delay=expiration_delay,
                async_load_function=async_configuration_load_function,
            )

        async def load_context(
            self, id: AuthenticationConfiguration.Id
        ) -> AuthenticationContext:
            auth_conf = await self.authentication_configurations.get(id)
            return await auth_conf.authenticate()

        def clear(self):
            super().clear()
            self.authentication_configurations.clear()

    def __init__(
        self,
        *,
        cache_expiration_delay: timedelta = DEFAULT_CACHE_EXPIRATION_DELAY,
    ):
        self.authentication_contexts = self.AuthenticationContextCache(
            entry_description="authentication context",
            expiration_delay=cache_expiration_delay,
            async_configuration_load_function=(
                self.load_authentication_configuration
            ),
        )
        self.common_configurations = self.Cache(
            entry_description="common configuration",
            expiration_delay=cache_expiration_delay,
            async_load_function=self.load_common_configuration,
        )
        self.machine_configurations = self.Cache(
            entry_description="machine configuration",
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
    async def load_authentication_configuration(
        self, id: AuthenticationConfiguration.Id
    ) -> AuthenticationConfiguration:
        """
        The actual load method, to be implemented by derived classes.
        """

    @abc.abstractmethod
    async def load_common_configuration(
        self, id: CommonConfiguration.Id
    ) -> CommonConfiguration:
        """
        The actual load method, to be implemented by derived classes.
        """

    @abc.abstractmethod
    async def load_machine_configuration(
        self, id: MachineConfiguration.Id
    ) -> MachineConfiguration:
        """
        The actual load method, to be implemented by derived classes.
        """
