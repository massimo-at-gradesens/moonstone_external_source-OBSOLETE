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

from .configuration_ids_configuration import (
    ConfigurationIdsConfiguration,
    ConfigurationIdsSettings,
)
from .error import ConfigurationError
from .http_settings import HTTPTransactionSettings
from .settings import Settings


class _AuthenticationSettings(
    HTTPTransactionSettings,
    ConfigurationIdsSettings,
):
    """
    Configuration settings for a authentication requests and result
    processing.

    These configuration settings are used by :class:`CommonConfiguration`s,
    :class:`MeasurementConfiguration`s and :class:`MachineConfiguration`s.
    """

    def __init__(
        self,
        other: Optional["_AuthenticationSettings"] = None,
        /,
        *,
        authentication_configuration_ids: Optional[
            Union[
                Iterable["AuthenticationConfiguration.Id"],
                "AuthenticationConfiguration.Id",
            ]
        ] = None,
        **kwargs: Settings.InputType,
    ):
        if other is not None:
            assert authentication_configuration_ids is None
            assert not kwargs
            super().__init__(other)
            return

        super().__init__(
            authentication_configuration_ids=authentication_configuration_ids,
            _configuration_ids_field="authentication_configuration_ids",
            _configuration_ids_get=(
                lambda io_manager, configuration_id: (
                    (
                        io_manager.authentication_contexts
                    ).authentication_configurations.get(configuration_id)
                )
            ),
            **kwargs,
        )


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
    _AuthenticationSettings,
    ConfigurationIdsConfiguration,
):
    """
    An :class:`AuthenticationConfiguration` provides the required configuration
    to issue HTTP authentication requests and produced parsed output data from
    successful authentication responses. Successfully parsed authentication
    output data are instances of :class:`AuthenticationContext`s, which are
    used to grant access to specific resources to other API requests.

    See :class:`CommonConfiguration` about hierarchical resolution of
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
        **kwargs: Settings.InputType,
    ):
        if other is not None:
            assert id is None
            assert not kwargs
            super().__init__(other)
            return

        if id is None:
            raise ConfigurationError("Missing common configuration's 'id'")
        super().__init__(id=id, **kwargs)

    async def get_settings(
        self, io_manager: "IOManager"
    ) -> Settings.InterpolatedType:
        """
        Return the resolved (aka interpolated) settings for this
        :class:`AuthenticationConfiguration`
        """
        settings = await self.get_merged_settings(io_manager)
        settings = {
            key: value for key, value in settings.items() if not key[0] == "_"
        }
        parameters = settings

        interpolation_context = Settings.InterpolationContext(
            parameters=parameters,
        )

        return self.InterpolatedSettings(
            self.interpolated_items_from_dict(
                settings,
                context=interpolation_context,
            )
        )

    def interpolate(
        self, *args, **kwargs
    ) -> (HTTPTransactionSettings.InterpolatedSettings):
        return self.InterpolatedSettings(
            self.interpolated_items_from_dict(
                {key: value for key, value in self.items() if key[0] != "_"},
                *args,
                **kwargs,
            )
        )

    async def authenticate(
        self, io_manager: "IOManager"
    ) -> AuthenticationContext:
        settings = await self.get_settings(io_manager=io_manager)
        request = settings["request"]
        request_kwargs = {}

        for key in (
            "url",
            "data",
            "query_string",
            "headers",
        ):
            value = request.get(key, None)
            if value is None:
                continue
            request_kwargs[key] = value
        if "url" not in request_kwargs:
            raise ConfigurationError(
                f"AuthenticationConfiguration {self['id']!r}" " has no URL"
            )
        raw_result = await io_manager.backend_driver.get_raw_result(
            **request_kwargs
        )

        return settings.process_result(raw_result.data)
