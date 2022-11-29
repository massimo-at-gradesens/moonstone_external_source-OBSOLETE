"""
GradeSens - Moonstone Snooper package - Configuration support

This file provides the configuration data classes to handle authorization
configurations.
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


class AuthorizationContext(Settings):
    """
    An :class:`AuthorizationContext` is nothing more than a ``[key, value]``
    dictionary of authorization data.

    The actual contents, including the list of keys, are strictly customer-
    and API-specific, and are not under the responsibility of this class.
    Rather, they are defined by configuration data structures used to
    initialize the objects of :class:`AuthorizationConfiguration`.
    See also :meth`IOManager.authorization_configurations.get` and
    :meth:`AuthorizationConfiguration.authenticate`
    """

    def __init__(
        self,
        other: Optional["AuthorizationContext"] = None,
        /,
        **kwargs: Settings.InputType,
    ):
        if other is not None:
            assert not kwargs
            super().__init__(other)
            return

        super().__init__(**kwargs)


class AuthorizationConfiguration(
    HTTPTransactionSettings,
    ConfigurationReferenceTarget,
    ConfigurationReferences,
):
    """
    An :class:`AuthorizationConfiguration` provides the required configuration
    to issue HTTP authorization requests and produced parsed output data from
    successful authorization responses. Successfully parsed authorization
    output data are instances of :class:`AuthorizationContext`s, which are
    used to grant access to specific resources to other API requests.

    See :class:`MachineConfiguration` about hierarchical resolution of
    :class:`Settings` from trees of :class`AuthorizationConfiguration`
    referencing each other via parameter ``authorization_configuration_ids``.
    """

    Id = str

    def __init__(
        self,
        other: Optional["AuthorizationConfiguration"] = None,
        /,
        *,
        id: Optional[Id] = None,
        authorization_configuration_ids: Optional[
            Union[
                Iterable["AuthorizationConfiguration.Id"],
                "AuthorizationConfiguration.Id",
            ]
        ] = None,
        _partial: Optional[bool] = None,
        **kwargs: Settings.InputType,
    ):
        if other is not None:
            assert id is None
            assert authorization_configuration_ids is None
            assert _partial is None
            assert not kwargs
            super().__init__(other)
            return

        if id is None and not _partial:
            raise ConfigurationError("Missing common configuration's 'id'")

        super().__init__(
            id=id,
            authorization_configuration_ids=authorization_configuration_ids,
            _configuration_ids_field="authorization_configuration_ids",
            _configuration_ids_get=(
                lambda client_session, configuration_id: (
                    (
                        client_session.authorization_contexts
                    ).authorization_configurations.get(configuration_id)
                )
            ),
            _partial=_partial,
            **kwargs,
        )

    async def authenticate(
        self,
        client_session: "IOManager.ClientSession",
    ) -> AuthorizationContext:
        try:
            return await self.fetch_result(client_session=client_session)
        except Error as err:
            err.index.insert(0, f"Authorization configuration {self['id']!r}")
            raise
