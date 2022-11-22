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
import itertools
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Dict,
    Iterable,
    Optional,
    Sequence,
    Set,
    Union,
)

if TYPE_CHECKING:
    from .io_manager import IOManager

from .authentication_configuration import AuthenticationConfiguration
from .configuration_ids_configuration import (
    ConfigurationIdsConfiguration,
    ConfigurationIdsSettings,
)
from .error import ConfigurationError, Error, TimeError
from .http_settings import HTTPResultSettings, HTTPTransactionSettings
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
                lambda io_manager, configuration_id: (
                    io_manager.common_configurations.get(configuration_id)
                )
            ),
            **kwargs,
        )

    async def get_common_settings(
        self,
        io_manager: "IOManager",
        already_visited: Optional[Set["CommonConfiguration.Id"]] = None,
    ) -> Settings:
        """
        See details in :meth:`CommonConfiguration.get_common_settings`
        """
        return await self.get_merged_settings(
            io_manager=io_manager,
            already_visited=already_visited,
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
    ):
        if other is not None:
            assert value is None
            assert timestamp is None
            super().__init__(other)
            return

        kwargs = {}
        if value is not None:
            kwargs.update(value=value)
        if timestamp is not None:
            kwargs.update(timestamp=timestamp)
        super().__init__(**kwargs)


class _MeasurementSettings(
    HTTPTransactionSettings,
    _CommonConfigurationIdsSettings,
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
        *,
        authentication_configuration_id: Optional[
            AuthenticationConfiguration.Id
        ] = None,
        **kwargs: Settings.InputType,
    ):
        if other is not None:
            assert authentication_configuration_id is None
            assert not kwargs
            super().__init__(other)
            return

        super().__init__(
            _authentication_configuration_id=(authentication_configuration_id),
            **kwargs,
        )


class MeasurementConfiguration(_MeasurementSettings):
    """
    Configuration for a single measurement (e.g. temperature, RPM, etc.).
    """

    Id = str
    SettingsType = HTTPTransactionSettings.InterpolatedSettings

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

    class _SettingsResolver:
        # Build the list of actual settings to be retained after resolution.
        # Simple solution: create a bare _MeasurementSettings instance and get
        # its list of keys(), but filter out the few unwanted ones.
        __RESULT_KEYS = set(
            filter(
                lambda key: not key[0] == "_",
                _MeasurementSettings().keys(),
            )
        )

        def __init__(
            self,
            measurement_configuration: "MeasurementConfiguration",
            machine_configuration: "MachineConfiguration",
            io_manager: "IOManager",
        ):
            self.measurement_configuration = measurement_configuration
            self.machine_configuration = machine_configuration
            self.io_manager = io_manager

        async def get_settings(
            self,
            start_time: Optional[datetime] = None,
            end_time: Optional[datetime] = None,
        ) -> "MeasurementConfiguration.SettingsType":
            # Create a temp CommonConfiguration instance to resolve the common
            # configuration settings from both self.machine_configuration
            # and self.measurement_configuration
            temp_common_configuration = CommonConfiguration(
                # Use a non-string id, so that there is zero risk of conflict
                # with user-defined (string-only) ids
                id=(
                    self.machine_configuration["id"],
                    self.measurement_configuration["id"],
                ),
                common_configuration_ids=itertools.chain(
                    self.machine_configuration["_common_configuration_ids"],
                    self.measurement_configuration[
                        "_common_configuration_ids"
                    ],
                ),
            )
            settings = await temp_common_configuration.get_common_settings(
                io_manager=self.io_manager
            )

            settings.update(self.machine_configuration)

            if start_time is not None:
                self.__assert_aware_time("start_time", start_time)
                settings["start_time"] = start_time
            if end_time is not None:
                self.__assert_aware_time("end_time", end_time)
                settings["end_time"] = end_time

            settings.update(self.measurement_configuration)

            authentication_configuration_id = settings[
                "_authentication_configuration_id"
            ]
            if authentication_configuration_id is not None:
                authentication_context = (
                    await self.io_manager.authentication_contexts.get(
                        authentication_configuration_id
                    )
                )
                new_settings = Settings(authentication_context)
                new_settings.update(settings)
                settings = new_settings

            # Now settings contains the merging of AuthenticatioContext,
            # the optional CommonConfiguration, the MachineConfiguration, and
            # the MeasurementConfiguration.
            # Furthermore, it contains both the relevant fields (i.e., URL,
            # query string, headers), as well as values required to interpolate
            # possible patterns. Therefore, use settings as the base parameters
            # to interpolate the patterns in its own values.
            # Anyway, only keep the scalar settings as interpolation
            # parameters, and insert the machine and measurement ids
            parameters = {
                "machine_id": self.machine_configuration["id"],
                "measurement_id": self.measurement_configuration["id"],
            }
            parameters.update(
                {
                    key: value
                    for key, value in settings.items()
                    if not key[0] == "_"
                }
            )

            # As for settings, only keep the keys specified for
            # _MeasurementSettings, as they are the only relevant fields to be
            # presented to the backend driver.
            settings = Settings(
                _raw_init=True,
                **{
                    key: value
                    for key, value in settings.items()
                    if key in self.__RESULT_KEYS
                },
            )
            interpolation_context = Settings.InterpolationContext(
                parameters=parameters,
                machine_configuration=self.machine_configuration,
                measurement_configuration=self.measurement_configuration,
            )
            try:
                result = HTTPTransactionSettings.InterpolatedSettings(
                    Settings.interpolated_items_from_dict(
                        settings,
                        context=interpolation_context,
                    )
                )
            except Error as err:
                err.index = [
                    self.machine_configuration["id"],
                    "measurements",
                    self.measurement_configuration["id"],
                ] + err.index
                raise err from None
            return result

        @staticmethod
        def __assert_aware_time(description, time):
            if time.tzinfo is None or time.tzinfo.utcoffset(time) is None:
                raise TimeError(f"{description} is not timezone-aware: {time}")


class _MachineConfigurationSettings(_MeasurementSettings):
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
    ConfigurationIdsConfiguration,
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
    See :meth:`.get_common_settings` method for details about how the settings
    from the different :class:`CommonConfiguration` instances, including this
    one, are merged together.

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

    async def get_common_settings(
        self,
        io_manager: "IOManager",
        already_visited: Optional[Set["CommonConfiguration.Id"]] = None,
    ) -> Settings:
        """
        Return a :class:`Settings` instance containing all the
        (non-interpolated) settings specified by this
        :class:`CommonConfiguration`.

        The resulting :class:`Settings` are computed by merging, by means of
        :attr:`Settings.update` method, the contents of all the (optional)
        :class:`CommonConfiguration`s referenced by
        ``common_configuration_ids`` constructor parameter, in the same order
        in which they are listed in that same ``common_configuration_ids``
        parameter, and finally merging the contents of this
        :class:`CommonConfiguration`.
        Therefore, the settings from the aforementioned sequence of
        :class:`CommonConfiguration` contributors are applied in the increasing
        order of precedence. E.g. the settings from last contributor - i.e.
        from this :class:`CommonConfiguration` - have higher precedence over
        same-name settings from other :class:`CommonConfiguration`s
        """
        return await self.get_merged_settings(
            io_manager=io_manager,
            already_visited=already_visited,
        )


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
        if not self["measurements"]:
            raise ConfigurationError(
                "Missing machine configuration's 'measurements' field"
            )

    class _SettingsResolver:
        def __init__(
            self,
            machine_configuration: "MachineConfiguration",
            io_manager: "IOManager",
        ):
            self.machine_configuration = machine_configuration
            self.io_manager = io_manager

        class __MeasurementProxy:
            def __init__(self, parent):
                self.parent = parent

            def __getitem__(
                self, id: MeasurementConfiguration.Id
            ) -> MeasurementConfiguration._SettingsResolver:
                measurements = self.parent.machine_configuration[
                    "measurements"
                ]
                try:
                    measurement_configuration = measurements[id]
                except KeyError:
                    valid_ids = ", ".join(measurements.keys())
                    raise ConfigurationError(
                        f"Unrecognized measurement id {id}."
                        f" Valid ids: {valid_ids}"
                    ) from None
                return MeasurementConfiguration._SettingsResolver(
                    measurement_configuration=measurement_configuration,
                    machine_configuration=self.parent.machine_configuration,
                    io_manager=self.parent.io_manager,
                )

        def __getitem__(self, key):
            if key == "measurements":
                return self.__MeasurementProxy(self)
            raise KeyError(key)

        async def get_settings(
            self,
            measurement_ids: (
                "MachineConfiguration.MeasurementIdsType"
            ) = None,
            **kwargs,
        ) -> "MachineConfiguration.SettingsType":
            if measurement_ids is None:
                # If no ids are specified, return all measurements
                measurement_ids = self.machine_configuration[
                    "measurements"
                ].keys()
            measurement_ids = set(measurement_ids)

            # Retain only the ids that actually match this machine
            # measurements.
            # Silently drop the other ones.
            measurement_ids &= set(
                self.machine_configuration["measurements"].keys()
            )

            measurements_settings = self["measurements"]

            # Use an OrderedDict to guarantee order repeatability, so as to
            # guarantee that ids and results can be correctly zipped
            # together after asyncio.gather()
            tasks = collections.OrderedDict(
                **{
                    measurement_id: measurements_settings[
                        measurement_id
                    ].get_settings(**kwargs)
                    for measurement_id in measurement_ids
                }
            )
            results = await asyncio.gather(*tasks.values())
            return {
                measurement_id: result
                for measurement_id, result in zip(tasks.keys(), results)
            }

    def get_settings_resolver(
        self, io_manager: "IOManager"
    ) -> _SettingsResolver:
        return self._SettingsResolver(
            machine_configuration=self, io_manager=io_manager
        )
