"""
GradeSens - External Source package - IO driver

Abstract class specifying the interface for IO operations, e.g. to retrieve
configuration data structures from a DB.
The actual implementation is to be provided by application-specific
integrations.
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, GradeSens AG"


import abc
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Optional, Union

from .async_concurrent_pool import AsyncConcurrentPool
from .authorization_configuration import (
    AuthorizationConfiguration,
    AuthorizationContext,
)
from .backend_driver import AsyncHTTPBackendDriver, BackendDriver
from .configuration import MachineConfiguration
from .error import Error


class IODriver(abc.ABC):
    """
    Abstract class specifying the interface for IO operations, e.g. to retrieve
    configuration data structures from a DB.
    The actual implementation is to be provided by application-specific
    integrations.
    """

    @abc.abstractmethod
    async def load_authorization_configuration(
        self, id: AuthorizationConfiguration.Id
    ) -> AuthorizationConfiguration:
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
        async_load_entry: Callable,
    ):
        self.entry_description = entry_description
        self.expiration_delay = expiration_delay
        self.async_load_entry = async_load_entry
        self.entries = {}

    def clear(self):
        self.entries.clear()

    def client_session(
        self,
        io_manager_client_session: "IOManager.ClientSession",
    ) -> "Cache.ClientSession":
        """
        :class:`IOManager.ClientSession` uses :class:`Cache.ClientSession`s
        for the actual interaction with :class:`Cache`s.

        This method is called by :class:`IOManager.client_session` to create
        the desired :class:`Cache.ClientSession`s and link them to the
        :class:`IOManager.ClientSession` that :class:`IOManager.ClientSession`
        is creating.
        """
        return self.ClientSession(
            cache=self,
            io_manager=io_manager_client_session,
        )

    class ClientSession:
        """
        Base class for context-specific :class:`Cache` client sessions.

        .. seealso:: :meth:`Cache.client_session`.
        """

        def __init__(
            self,
            cache: "Cache",
            io_manager: "IOManager.ClientSession",
            async_load_entry: Optional[Callable] = None,
        ):
            self.cache = cache
            self.io_manager = io_manager
            self.async_load_entry = (
                cache.async_load_entry
                if async_load_entry is None
                else async_load_entry
            )

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            await self.close()

        async def close(self):
            pass

        async def get(self, id: Any) -> Any:
            try:
                entry = self.cache.entries[id]
            except KeyError:
                pass
            else:
                if not entry.expired:
                    return entry.value

            try:
                value = await self.async_load_entry(id)
                if value is None:
                    raise Error("")
            except Error as err:
                err = str(err)
                if err != "":
                    err = f": {err}"
                raise Error(
                    f"Unable to load a {self.cache.entry_description!r}"
                    f" for {id!r}{err}"
                ) from None

            creation_time = datetime.now()
            entry = self.cache.Entry(
                creation_time=creation_time,
                expiration_time=creation_time + self.cache.expiration_delay,
                value=value,
            )
            self.cache.entries[id] = entry
            return entry.value


class AuthorizationContextCache(Cache):
    class Entry(Cache.Entry):
        @property
        def expired(self):
            if super().expired:
                return True

            # check for authorization expiration
            now = datetime.now()
            for key, is_delta in (
                ("expires_in", True),
                ("expiration_in", True),
                ("expires_at", False),
                ("expiration_at", False),
            ):
                try:
                    value = self.value[key]
                except KeyError:
                    continue

                if is_delta:
                    if self.creation_time + value < now:
                        return True
                else:
                    if value < now:
                        return True

            return False

    def __init__(
        self,
        entry_description: str,
        expiration_delay: timedelta,
        async_load_configuration: Callable[[Any], AuthorizationConfiguration],
    ):
        super().__init__(
            entry_description=entry_description,
            expiration_delay=expiration_delay,
            async_load_entry=None,  # load func managed by self.ClientSession
        )
        self.authorization_configurations = Cache(
            async_load_entry=async_load_configuration,
            entry_description="authorization configuration",
            expiration_delay=expiration_delay,
        )

    def clear(self):
        super().clear()
        self.authorization_configurations.clear()

    class ClientSession(Cache.ClientSession):
        """
        Specialization of :meth:`Cache.ClientSession` for
        :class:`AuthorizationContext` entries, carrying out actual
        authorization requests to fetch the missing
        :class:`AuthorizationContext`s
        """

        def __init__(
            self,
            cache: "Cache",
            io_manager: "IOManager.ClientSession",
            async_load_entry: Optional[Callable] = None,
        ):
            assert async_load_entry is None
            super().__init__(
                cache=cache,
                io_manager=io_manager,
                async_load_entry=self.authenticate,
            )
            self.authorization_configurations = (
                cache.authorization_configurations.client_session(
                    self.io_manager
                )
            )

        async def close(self):
            await self.authorization_configurations.close()
            await super().close()

        async def authenticate(
            self, id: AuthorizationConfiguration.Id
        ) -> AuthorizationContext:
            auth_conf = await self.authorization_configurations.get(id)
            return await auth_conf.authenticate(client_session=self.io_manager)


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
        authorization_context_cache_factory=AuthorizationContextCache,
        common_configuration_cache_factory=Cache,
        machine_configuration_cache_factory=Cache,
        backend_driver: Optional[BackendDriver] = None,
    ):
        if backend_driver is None:
            backend_driver = AsyncHTTPBackendDriver()
        self.__backend_driver = backend_driver

        self.__caches = dict(
            authorization_contexts=authorization_context_cache_factory(
                entry_description="authorization context",
                expiration_delay=cache_expiration_delay,
                async_load_configuration=(
                    io_driver.load_authorization_configuration
                ),
            ),
            machine_configurations=machine_configuration_cache_factory(
                entry_description="machine configuration",
                expiration_delay=cache_expiration_delay,
                async_load_entry=io_driver.load_machine_configuration,
            ),
        )

    def clear_cache(self):
        """
        Clear all caches.
        """
        for cache in self.__caches.values():
            cache.clear

    def client_session(
        self,
        backend_kwargs: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> "IOManager.ClientSession":
        """
        In order to perform transactions with an :class:`IODriver`, a
        :class:`.ClientSession` has to be requested with this method.

        The typical use case is to create a client session at the beginning
        of a (possibly long) sequence of client transactions - e.g. to
        retrieve the measurements for many machines and for many time points.

        The client session **must** be close with :meth:`ClientSession.close`
        at the end of its use. This can be easily and robustly achieved by
        using a client session with an ``async with`` statement, given that a
        :class:`ClientSession` is an asynchronous context manager.

        A :class:`ClientSession` contains also a
        :class:`BackendDriver.ClientSession`, provided by :attr:`.backend`,
        that enables carrying out backed transactions.

        :param backend_kwargs:
            :attr:`.backend` contains a `BackendDriver.ClientSession` created
            by calling, with these keyword arguments,
            the method :meth:`BackendDriver.client_session` of
            the :class:`BackendDriver` registered at this :class:`IOManager`'s
            construction.
        """

        return self.ClientSession(
            caches=self.__caches,
            backend=self.__backend_driver.client_session(
                **({} if backend_kwargs is None else backend_kwargs)
            ),
            **kwargs,
        )

    class ClientSession:
        """
        See :meth:`IOManager.client_session`.
        """

        def __init__(
            self,
            caches: Dict[str, Cache],
            backend: BackendDriver.ClientSession,
            task_pool: Optional[Union[AsyncConcurrentPool, int]] = 10,
        ):
            self.__sub_client_sessions = []

            self.backend = backend
            self.__sub_client_sessions.append(self.backend)

            for cache_name, cache in caches.items():
                cache_client_session = cache.client_session(self)
                self.__sub_client_sessions.append(cache_client_session)
                setattr(self, cache_name, cache_client_session)

            if task_pool is None:
                task_pool = 1
            if isinstance(task_pool, int):
                task_pool = max(1, task_pool)
                task_pool = AsyncConcurrentPool(task_pool)
            self.task_pool = task_pool

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            await self.close()

        async def close(self):
            for sub_client_session in self.__sub_client_sessions:
                await sub_client_session.close()
