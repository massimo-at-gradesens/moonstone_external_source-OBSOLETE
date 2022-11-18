"""
GradeSens - External Source package - Configuration support

This file provides the configuration data classes to handle machine,
maeasurement and authorization configurations.
These configurations contain all the parameters requested to query the
external measurements on the target machines.
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, Gradesens AG"


from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from .io_manager import IOManager

from .error import ConfigurationError
from .http_settings import HTTPRequestSettings, HTTPResultSettings
from .settings import Settings


class _AuthenticationSettings(Settings):
    """
    Configuration settings for a authentication requests and result
    processing.

    These configuration settings are used by :class:`CommonConfiguration`s,
    :class:`MeasurementConfiguration`s and :class:`MachineConfiguration`s.
    """

    def __init__(
        self,
        other: Union["_AuthenticationSettings", None] = None,
        *,
        request: Union[HTTPRequestSettings, None] = None,
        result: Union[HTTPResultSettings, None] = None,
        **kwargs: Settings.InputType,
    ):
        if other is not None:
            assert request is None
            assert result is None
            assert not kwargs
            super().__init__(other)
        else:
            # Force a non-copy creation, so as to instantiate the
            # field-specific settings classes, required for proper result
            # parsing.
            # Furthermore, always force the creation of request and result
            # settings object, even if empty, to make sure any settings-
            # specific features are properly set, such as the _interpolate
            # field of HTTPResultSettings
            if request is None:
                request = {}
            request = HTTPRequestSettings(**request)
            if result is None:
                result = {}
            result = HTTPResultSettings(**result)

            super().__init__(
                other,
                request=request,
                result=result,
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
        other: Union["AuthenticationContext", None] = None,
        **kwargs: Settings.InputType,
    ):
        if other is not None:
            assert not kwargs
            super().__init__(other)
        else:
            super().__init__(**kwargs)


class AuthenticationConfiguration(_AuthenticationSettings):
    """
    An :class:`AuthenticationConfiguration` provides the required configuration
    to issue HTTP authentication requests and produced parsed output data from
    successful authentication responses. Successfully parsed authentication
    output data are instances of :class:`AuthenticationContext`s, which are
    used to grant access to specific resources to other API requests.
    """

    Id = str

    def __init__(
        self,
        other: Union["AuthenticationConfiguration", None] = None,
        *,
        id: Union[Id, None] = None,
        **kwargs: Settings.InputType,
    ):
        if other is not None:
            assert id is None
            assert not kwargs
            super().__init__(other)
        else:
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
        parameters = {
            key: value for key, value in self.items() if not key[0] == "_"
        }

        settings = Settings(
            _raw_init=True,
            **{key: value for key, value in self.items() if not key[0] == "_"},
        )

        interpolation_context = Settings.InterpolationContext(
            parameters=parameters,
        )
        return settings.interpolate(context=interpolation_context)

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
        return raw_result.data
        pass
