"""
GradeSens - External Source package - Configuration support

This file provides the configuration data classes to handle machine,
maeasurement and authorization configurations.
These configurations contain all the parameters requested to query the
external measurements on the target machines.
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, GradeSens AG"


import asyncio
import collections
from datetime import datetime
from typing import TYPE_CHECKING, Dict, Iterable, Optional, Sequence, Union

if TYPE_CHECKING:
    from .io_manager import IOManager

from .authentication_configuration import AuthenticationConfiguration
from .configuration_ids_configuration import (
    ConfigurationIdsConfiguration,
    ConfigurationIdsSettings,
)
from .datetime import TimeDelta
from .error import ConfigurationError, Error, TimeError
from .http_settings import (
    HTTPRequestSettings,
    HTTPResultSettings,
    HTTPTransactionSettings,
)
from .settings import Settings


class _CommonConfigurationIdsSettings(ConfigurationIdsSettings):
    """
    Mixin to provide the optional setting
    :attr:`._common_configuration_ids` to reference to zero or more
    :class:`CommonConfiguration`s.

    These configuration settings are used by
    :class:`_MeasurementSettings`s, :class:`MachineConfiguration`s :class:`
    and :class:`AuthorizationConfiguration`s.
    """

    def __init__(
        self,
        other: Optional["_CommonConfigurationIdsSettings"] = None,
        /,
        *,
        common_configuration_ids: Optional[
            Union[
                Iterable["CommonConfiguration.Id"],
                "CommonConfiguration.Id",
            ]
        ] = None,
        **kwargs: Settings.InputType,
    ):
        if other is not None:
            assert common_configuration_ids is None
            assert not kwargs
            super().__init__(other)
            return

        super().__init__(
            common_configuration_ids=common_configuration_ids,
            _configuration_ids_field="common_configuration_ids",
            _configuration_ids_get=(
                lambda client_session, configuration_id: (
                    client_session.common_configurations.get(configuration_id)
                )
            ),
            **kwargs,
        )


class _MeasurementRequestSettings(HTTPRequestSettings):
    """
    Configuration settings for measurement request.

    These configuration settings are used by :class:`_MeasurementSettings`.
    """

    def __init__(
        self,
        other: Optional["_MeasurementRequestSettings"] = None,
        /,
        *,
        authentication_configuration_id: Optional[
            AuthenticationConfiguration.Id
        ] = None,
        start_time: Optional[TimeDelta.InputType] = None,
        end_time: Optional[TimeDelta.InputType] = None,
        time_margin: Optional[TimeDelta.InputType] = None,
        start_time_margin: Optional[TimeDelta.InputType] = None,
        end_time_margin: Optional[TimeDelta.InputType] = None,
        **kwargs: Settings.InputType,
    ):
        if other is not None:
            assert authentication_configuration_id is None
            assert start_time is None
            assert end_time is None
            assert time_margin is None
            assert start_time_margin is None
            assert end_time_margin is None
            super().__init__(other)
            return

        for key in (
            "start_time",
            "end_time",
        ):
            value = locals()[key]
            if value is not None:
                raise ConfigurationError(
                    f"Key {key!r} cannot be defined in configurations,"
                    " as it set dynamically at request time,"
                    " to represent the requested measurement time-stamp"
                )

        time_margins = {}
        for key in (
            "time_margin",
            "start_time_margin",
            "end_time_margin",
        ):
            value = locals()[key]
            if value is None:
                continue
            try:
                parsed_value = TimeDelta(value)
                if parsed_value.total_seconds() < 0:
                    raise ValueError(
                        f"Time margin cannot be negative: {value!r}"
                    )
            except ValueError as err:
                raise ConfigurationError(str(err), index=key) from None
            time_margins[key] = parsed_value
        for key in (
            "start_time_margin",
            "end_time_margin",
        ):
            value = time_margins.get(key)
            if value is None:
                value = time_margins.get("time_margin")
            if value is not None:
                kwargs[key] = value

        if "_authentication_configuration_id" in kwargs:
            assert authentication_configuration_id is None
        else:
            kwargs[
                "_authentication_configuration_id"
            ] = authentication_configuration_id
        super().__init__(
            **kwargs,
        )


class _MeasurementResultSettings(HTTPResultSettings):
    """
    Configuration settings for measurement results.

    These configuration settings are used by :class:`_MeasurementSettings`.
    """

    def __init__(
        self,
        other: Optional["_MeasurementResultSettings"] = None,
        /,
        *,
        value: Optional[Settings.InputType] = None,
        timestamp: Optional[Settings.InputType] = None,
        **kwargs,
    ):
        if other is not None:
            assert value is None
            assert timestamp is None
            super().__init__(other)
            return

        if value is not None:
            kwargs.update(value=value)
        if timestamp is not None:
            kwargs.update(timestamp=timestamp)
        super().__init__(**kwargs)


class _MeasurementSettings(
    HTTPTransactionSettings,
    _CommonConfigurationIdsSettings,
    request_type=_MeasurementRequestSettings,
    result_type=_MeasurementResultSettings,
):
    """
    Configuration settings for a single measurement.

    These configuration settings are used by :class:`CommonConfiguration`s,
    :class:`MeasurementConfiguration`s and :class:`MachineConfiguration`s.
    """

    def __init__(
        self,
        other: Optional["_MeasurementSettings"] = None,
        /,
        **kwargs: Settings.InputType,
    ):
        if other is not None:
            assert not kwargs
            super().__init__(other)
            return

        super().__init__(**kwargs)


class MeasurementConfiguration(
    _MeasurementSettings,
    ConfigurationIdsConfiguration,
):
    """
    Configuration for a single measurement (e.g. temperature, RPM, etc.).
    """

    Id = str
    InterpolatedSettings = HTTPTransactionSettings.InterpolatedSettings

    def __init__(
        self,
        other: Optional["MeasurementConfiguration"] = None,
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
            raise ConfigurationError(
                "Missing measurement configuration's 'id'"
            )
        super().__init__(
            id=id,
            **kwargs,
        )

    # Use a blank instance of _MeasurementSettings to infer the list of
    # interpolation result keys.
    __INTERPOLATION_KEYS = set(_MeasurementSettings().public_keys())

    def interpolation_keys(self) -> Iterable[str]:
        """
        Iterate over the keys to be retained for interpolation.

        By default this method returns all public keys, but sub-classes can
        further customize the set of interpolation output keys.
        """
        yield from self.__INTERPOLATION_KEYS

    async def get_aggregated_settings(
        self,
        client_session: "IOManager.ClientSession",
        machine_configuration: "MachineConfiguration",
        timestamp: Optional[datetime] = None,
    ) -> Settings.InterpolatedType:
        settings = type(self)(
            **{
                key: value
                for key, value in machine_configuration.items()
                if key != "measurements"
            }
        )
        settings.update(
            await super().get_aggregated_settings(
                client_session=client_session
            )
        )

        settings.update(self)

        request_settings = settings["request"]
        authentication_configuration_id = request_settings[
            "_authentication_configuration_id"
        ]
        if authentication_configuration_id is not None:
            authentication_context = (
                await client_session.authentication_contexts.get(
                    authentication_configuration_id
                )
            )
            try:
                authentication_settings = request_settings["authentication"]
            except KeyError:
                authentication_settings = authentication_context
            else:
                authentication_context.update(
                    Settings._RawInit(authentication_settings)
                )
            request_settings["authentication"] = authentication_context

        if timestamp is not None:
            self.__assert_aware_time("timestamp", timestamp)
            start_time_margin = settings["request"].get(
                "start_time_margin", TimeDelta(0)
            )
            end_time_margin = settings["request"].get(
                "end_time_margin", TimeDelta(0)
            )
            settings["request"]["start_time"] = timestamp - start_time_margin
            settings["request"]["end_time"] = timestamp + end_time_margin

        assert isinstance(settings, type(self))
        return settings

    def get_interpolation_parameters(
        self,
        settings: "Settings",
        machine_configuration: "MachineConfiguration",
        **kwargs,
    ) -> Dict[str, "Settings.ValueType"]:
        parameters = {
            "machine_id": machine_configuration["id"],
            "measurement_id": self["id"],
        }
        parameters.update(super().get_interpolation_parameters(settings))
        return parameters

    @staticmethod
    def __assert_aware_time(description, time):
        if time.tzinfo is None or time.tzinfo.utcoffset(time) is None:
            raise TimeError(f"{description} is not timezone-aware: {time}")


class _MachineConfigurationSettings(
    _MeasurementSettings,
    ConfigurationIdsConfiguration,
):
    """
    Configuration settings for one machine, containing a machine-specific
    number of :class:`MeasurementConfiguration`s.

    These configuration settings are used by :class:`CommonConfiguration`s and
    :class:`MachineConfiguration`s.
    """

    def __init__(
        self,
        other: Optional["_MachineConfigurationSettings"] = None,
        /,
        *,
        measurements: Optional[Sequence[Settings.InputType]] = None,
        **kwargs,
    ):
        if other is not None:
            assert measurements is None
            assert not kwargs
            super().__init__(other)
            return

        if measurements is None:
            measurements = []
        elif isinstance(measurements, dict):
            # this is coming from some "clone" copy operation
            measurements = list(measurements.values())
        measurement_dict = {}
        for index, measurement in enumerate(measurements):
            measurement_id = measurement["id"]
            if measurement_id in measurement_dict:
                raise ConfigurationError(
                    f"Duplicate measurement {measurement_id!r}",
                    index="measurements",
                )
            try:
                measurement_dict[measurement_id] = MeasurementConfiguration(
                    **measurement
                )
            except Error as err:
                err.index = ["measurements", measurement_id] + err.index
                raise
        super().__init__(measurements=measurement_dict, **kwargs)


class CommonConfiguration(
    _MachineConfigurationSettings,
):
    """
    An :class:`CommonConfiguration` is nothing more than a ``[key, value]``
    dictionary of configuration data, optionally referenced by
    :class:`MeasurementConfiguration` and :class:`MachineConfiguration`
    instances to load configuration data from a common shared configuration
    point.
    A :class:`CommonConfiguration` instance may refer to other
    :class:`CommonConfiguration` instances through the
    ``common_configuration_ids`` constructor parameter.
    See :meth:`.get_aggregated_settings` method for details about how the
    settings from the different :class:`CommonConfiguration` instances,
    including this one, are merged together.

    The actual contents, including the list of keys, are strictly customer-
    and API-specific, and are not under the responsibility of this class.
    """

    Id = str

    def __init__(
        self,
        other: Optional["CommonConfiguration"] = None,
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


class MachineConfiguration(_MachineConfigurationSettings):
    """
    Configuration for one machine, containing a machine-specific number of
    :class:`MeasurementConfiguration`s.
    """

    Id = str
    SettingsType = Dict[str, "MeasurementConfiguration.SettingsType"]
    MeasurementIdsType = Optional[Iterable[MeasurementConfiguration.Id]]

    def __init__(
        self,
        other: Optional["MeasurementConfiguration"] = None,
        /,
        *,
        id: Optional[Id] = None,
        **kwargs,
    ):
        if other is not None:
            assert id is None
            assert not kwargs
            super().__init__(other)
            return

        if id is None:
            raise ConfigurationError("Missing machine configuration'a 'id'")
        super().__init__(id=id, **kwargs)

    async def get_interpolated_settings(
        self,
        client_session: "IOManager.ClientSession",
        **kwargs,
    ) -> ("MachineConfiguration.SettingsType"):
        if self.get("_common_configuration_ids"):
            settings = await self.get_aggregated_settings(
                client_session=client_session
            )
            mach_conf = MachineConfiguration(settings)
            mach_conf.pop("_common_configuration_ids", None)
            result = await mach_conf.get_interpolated_settings(
                client_session=client_session, **kwargs
            )
            return result

        measurements = self["measurements"]
        if len(measurements) == 0:
            raise ConfigurationError(
                f"Machine {self.id!r}: No measurements specified"
            )

        tasks = collections.OrderedDict(
            **{
                measurement_id: self.__fetch_measurement_result(
                    client_session=client_session,
                    measurement_configuration=measurement_configuration,
                    **kwargs,
                )
                for measurement_id, measurement_configuration in (
                    measurements.items()
                )
            }
        )
        results = await asyncio.gather(*tasks.values())
        return {
            measurement_id: result
            for measurement_id, result in zip(tasks.keys(), results)
        }

    async def __fetch_measurement_result(
        self,
        measurement_configuration: MeasurementConfiguration,
        client_session: "IOManager.ClientSession",
        **kwargs,
    ):
        try:
            return await measurement_configuration.get_interpolated_settings(
                client_session=client_session,
                machine_configuration=self,
                **kwargs,
            )
        except Error as err:
            err.index = [
                self["id"],
                "measurements",
                measurement_configuration["id"],
            ] + err.index
            raise err from None
