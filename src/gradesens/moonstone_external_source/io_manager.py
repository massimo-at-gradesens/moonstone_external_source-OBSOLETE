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
from typing import Callable, Union

from .authentication_configuration import (
    AuthenticationConfiguration,
    AuthenticationContext,
)
from .backend_driver import HTTPBackendDriver
from .configuration import CommonConfiguration, MachineConfiguration
from .error import Error


class IODriver(abc.ABC):
    """
    Abstract class specifying the interface for IO operations, e.g. to retrieve
    configuration data structures from a DB.
    The actual implementation is to be provided by application-specific
    integrations.
    """

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


class Cache:
    DEFAULT_EXPIRATION_DELAY = timedelta(minutes=30)

    class Entry:
        def __init__(
            self,
            value: Union[str, bytes],
            creation_time: datetime,
            expiration_time: datetime,
        ):
            self.value = value
            self.creation_time = creation_time
            self.expiration_time = expiration_time

        @property
        def expired(self):
            return self.expiration_time < datetime.now()

    def __init__(
        self,
        entry_description: str,
        expiration_delay: timedelta,
        async_load_function: Callable,
        io_manager: "IOManager",
    ):
        self.entry_description = entry_description
        self.expiration_delay = expiration_delay
        self.async_load_function = async_load_function
        self.io_manager = io_manager
        self.entries = {}

    async def get(self, id):
        try:
            entry = self.entries[id]
        except KeyError:
            pass
        else:
            if not entry.expired:
                return entry.value

        try:
            value = await self.async_load_function(id)
            if value is None:
                raise Error("")
        except Error as err:
            err = str(err)
            if err != "":
                err = f": {err}"
            raise Error(
                f"Unable to load a {self.entry_description!r} for {id!r}"
                f"{err}"
            ) from None

        creation_time = datetime.now()
        entry = self.Entry(
            creation_time=creation_time,
            expiration_time=creation_time + self.expiration_delay,
            value=value,
        )
        self.entries[id] = entry
        return entry.value

    def clear(self):
        self.entries.clear()


class AuthenticationContextCache(Cache):
    class Entry(Cache.Entry):
        @property
        def expired(self):
            if super().expired:
                return True

            # check for authentication expiration
            now = datetime.now()
            for key, is_delta, unit in (
                ("expires_in", True, "seconds"),
                ("expiration_in", True, "seconds"),
                ("expires_at", False, None),
                ("expiration_at", False, None),
            ):
                try:
                    value = self.value[key]
                except KeyError:
                    continue

                if is_delta:
                    if isinstance(value, str):
                        try:
                            value = int(value)
                        except ValueError:
                            continue
                    if isinstance(value, int):
                        value = timedelta(**{unit: value})
                    elif not isinstance(value, timedelta):
                        continue
                    if self.creation_time + value < now:
                        return True
                else:
                    if isinstance(value, str):
                        try:
                            value = datetime.fromisoformat(value)
                        except ValueError:
                            continue
                    if not isinstance(value, datetime):
                        continue
                    if value < now:
                        return True

            return False

    def __init__(
        self,
        entry_description: str,
        expiration_delay: timedelta,
        async_configuration_load_function: Callable,
        io_manager: "IOManager",
    ):
        super().__init__(
            entry_description=entry_description,
            expiration_delay=expiration_delay,
            async_load_function=self.load_context,
            io_manager=io_manager,
        )
        self.authentication_configurations = Cache(
            entry_description="authentication configuration",
            expiration_delay=expiration_delay,
            async_load_function=async_configuration_load_function,
            io_manager=io_manager,
        )

    async def load_context(
        self, id: AuthenticationConfiguration.Id
    ) -> AuthenticationContext:
        auth_conf = await self.authentication_configurations.get(id)
        return await auth_conf.authenticate(io_manager=self.io_manager)

    def clear(self):
        super().clear()
        self.authentication_configurations.clear()


class IOManager:
    """
    An :class:`IOManager` adds a caching layer on top of the functionalities
    provided by :class:`IODriver`, eventually providing the main and unique
    interface for all the IO operations in this package.
    """

    def __init__(
        self,
        io_driver: IODriver,
        *,
        cache_expiration_delay: timedelta = Cache.DEFAULT_EXPIRATION_DELAY,
        authentication_context_cache_factory=AuthenticationContextCache,
        common_configuration_cache_factory=Cache,
        machine_configuration_cache_factory=Cache,
        backend_driver=None,
    ):
        self.io_driver = io_driver
        if backend_driver is None:
            backend_driver = HTTPBackendDriver()
        self.backend_driver = backend_driver

        self.authentication_contexts = authentication_context_cache_factory(
            entry_description="authentication context",
            expiration_delay=cache_expiration_delay,
            async_configuration_load_function=(
                io_driver.load_authentication_configuration
            ),
            io_manager=self,
        )
        self.common_configurations = common_configuration_cache_factory(
            entry_description="common configuration",
            expiration_delay=cache_expiration_delay,
            async_load_function=io_driver.load_common_configuration,
            io_manager=self,
        )
        self.machine_configurations = machine_configuration_cache_factory(
            entry_description="machine configuration",
            expiration_delay=cache_expiration_delay,
            async_load_function=io_driver.load_machine_configuration,
            io_manager=self,
        )

    def clear_cache(self):
        """
        Clear all caches.
        """
        self.common_configurations.clear()
        self.machine_configurations.clear()
        self.authentication_contexts.clear()
