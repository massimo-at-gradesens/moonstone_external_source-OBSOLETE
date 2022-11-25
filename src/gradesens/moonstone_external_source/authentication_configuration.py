"""
GradeSens - External Source package - Configuration support

This file provides the configuration data classes to handle machine,
maeasurement and authorization configurations.
These configurations contain all the parameters requested to query the
external measurements on the target machines.
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, GradeSens AG"


from typing import TYPE_CHECKING, Iterable, Optional, Union

if TYPE_CHECKING:
    from .io_manager import IOManager

from .configuration_references import (
    ConfigurationReferences,
    ConfigurationReferenceTarget,
)
from .error import ConfigurationError, Error
from .http_settings import HTTPTransactionSettings
from .settings import Settings


class AuthenticationContext(Settings):
    """
    An :class:`AuthenticationContext` is nothing more than a ``[key, value]``
    dictionary of authentication data.

    The actual contents, including the list of keys, are strictly customer-
    and API-specific, and are not under the responsibility of this class.
    Rather, they are defined by configuration data structures used to
    initialize the objects of :class:`AuthenticationConfiguration`.
    See also :meth`IOManager.authentication_configurations.get` and
    :meth:`AuthenticationConfiguration.authenticate`
    """

    def __init__(
        self,
        other: Optional["AuthenticationContext"] = None,
        /,
        **kwargs: Settings.InputType,
    ):
        if other is not None:
            assert not kwargs
            super().__init__(other)
            return

        super().__init__(**kwargs)


class AuthenticationConfiguration(
    HTTPTransactionSettings,
    ConfigurationReferenceTarget,
    ConfigurationReferences,
):
    """
    An :class:`AuthenticationConfiguration` provides the required configuration
    to issue HTTP authentication requests and produced parsed output data from
    successful authentication responses. Successfully parsed authentication
    output data are instances of :class:`AuthenticationContext`s, which are
    used to grant access to specific resources to other API requests.

    See :class:`MachineConfiguration` about hierarchical resolution of
    :class:`Settings` from trees of :class`AuthenticationConfiguration`
    referencing each other via parameter ``authentication_configuration_ids``.
    """

    Id = str

    def __init__(
        self,
        other: Optional["AuthenticationConfiguration"] = None,
        /,
        *,
        id: Optional[Id] = None,
        authentication_configuration_ids: Optional[
            Union[
                Iterable["AuthenticationConfiguration.Id"],
                "AuthenticationConfiguration.Id",
            ]
        ] = None,
        _partial: Optional[bool] = None,
        **kwargs: Settings.InputType,
    ):
        if other is not None:
            assert id is None
            assert authentication_configuration_ids is None
            assert _partial is None
            assert not kwargs
            super().__init__(other)
            return

        if id is None and not _partial:
            raise ConfigurationError("Missing common configuration's 'id'")

        super().__init__(
            id=id,
            authentication_configuration_ids=authentication_configuration_ids,
            _configuration_ids_field="authentication_configuration_ids",
            _configuration_ids_get=(
                lambda client_session, configuration_id: (
                    (
                        client_session.authentication_contexts
                    ).authentication_configurations.get(configuration_id)
                )
            ),
            _partial=_partial,
            **kwargs,
        )

    async def authenticate(
        self,
        client_session: "IOManager.ClientSession",
    ) -> AuthenticationContext:
        try:
            return await self.fetch_result(client_session=client_session)
        except Error as err:
            err.index.insert(0, f"Authentication configuration {self['id']!r}")
            raise
