"""
GradeSens - External Source package - Configuration support

This file provides the configuration data classes to handle machine,
maeasurement and authorization configurations.
These configurations contain all the parameters requested to query the
external measurements on the target machnes.
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, Gradesens AG"


import asyncio
from typing import TYPE_CHECKING, Any, Dict, Iterable, Sequence, Tuple, Union

if TYPE_CHECKING:
    from .io_driver import IODriver

from .error import Error, PatternError


class Settings(dict):
    """
    A :class:`dict` specialization containing a list of ``[key, value]``
    entries, where a ``value`` may either be a plain :class:`str` or a
    python `formatted string literal
    <https://docs.python.org/3/tutorial/inputoutput.html\
#formatted-string-literals>`_ to be interpolated by calling :meth:`.apply`.

    .. note::
        All the methods or operations that change the contents of a
        :class:`Settings` instance, including :meth:`.__init__`, automatically
        normalize all :class:`dict` values into :class:`Settings` instances,
        all through possible nested levels of :class:`dict`-in-:class:`dict`
        values.
        This guarantees a consistent :class:`Settings`-specific behavior all
        through a :class:`Settings` hierarchy, no matter how nested it may be.

    .. note::
        :meth:`.update` method's behavior differs from the behavior of its
        :meth:`dict.update` counterpart.
        See :meth:`.update` for more details.
    """

    KeyType = str
    ValueType = Union[str, "Settings", None]
    ItemType = Tuple[KeyType, ValueType]

    InputValueType = Union[ValueType, Dict[KeyType, "InputValueType"]]
    InputItemType = Tuple[KeyType, InputValueType]
    InputType = Union["Settings", Dict[KeyType, InputValueType]]

    InterpolationParametersType = Dict[str, Any]
    InterpolatedValueType = Union[str, None]
    InterpolatedType = Dict[
        str, Union[InterpolatedValueType, "InterpolatedType"]
    ]

    def __init__(
        self,
        other: Union[
            InputType,
            Iterable[InputItemType],
            None,
        ] = None,
        _raw_init=False,
        **kwargs: InputType,
    ):
        if other is None:
            init_dict = kwargs
        else:
            assert not kwargs
            init_dict = dict(other)

        if not _raw_init:
            init_dict = self.__normalize_values(init_dict, force_copy=True)
        super().__init__(**init_dict)

    def __setitem__(self, key: KeyType, value: InputValueType):
        super().__setitem__(key, self.__normalize_value(value))

    def setdefault(
        self, key: KeyType, default: InputValueType = None
    ) -> ValueType:
        try:
            return self[key]
        except KeyError:
            pass
        default = self.__normalize_value(default)
        self[key] = default
        return default

    def update(self, other: InputType):
        """
        :meth:`.update` method's behavior differs from the behavior of its
        :meth:`dict.update` counterpart on different points.
        1. :meth:`.update` is applied hierarchically to values which are
           themselves of type :class:`Settings`. I.e., if both ``self[key]``
           and ``other[key]`` exist for the same key, and their respective
           values are both of type :class:`Settings` (or of type :class:`dict`,
           which get automatically converted to :class:`Settings`), then the
           equivalent of ``self[key].update(other[key])`` is applied, instead
           of plainly replacing the target value as would be done by
           ``self[key] = other[key]``.
        2. For a give key, if ``other[key]`` is None, ``self[key]`` is not
           modified.
        """
        other = self.__normalize_value(other)
        for key, value in other.items():
            if value is None:
                self.setdefault(key, None)
                continue

            if isinstance(value, Settings):
                try:
                    self_value = self[key]
                except KeyError:
                    pass
                else:
                    if isinstance(self_value, Settings):
                        self_value.update(value)
                        continue

            self[key] = value

    def __normalize_values(self, other: InputType, force_copy: bool = False):
        return {
            key: self.__normalize_value(value, force_copy=force_copy)
            for key, value in other.items()
        }

    def __normalize_value(
        self, value: InputValueType, force_copy: bool = False
    ):
        if isinstance(value, Settings):
            if force_copy:
                return type(value)(value)
            return value

        if isinstance(value, dict):
            return Settings(
                self.__normalize_values(value, force_copy=force_copy)
            )

        return value

    def apply(
        self,
        parameters: InterpolationParametersType,
    ) -> InterpolatedType:
        return {
            key: self.__interpolate_value(
                key=key,
                value=value,
                parameters=parameters,
            )
            for key, value in self.items()
        }

    def __interpolate_value(self, key, value, parameters):
        if value is None:
            return None
        if isinstance(value, Settings):
            return value.apply(parameters)
        try:
            try:
                try:
                    return eval("f" + repr(value), {}, parameters)
                except KeyError:
                    raise
                except NameError as err:
                    raise KeyError(err.name) from None
                except Exception as err:
                    raise PatternError(str(err)) from None
            except KeyError as err:
                try:
                    missing_key = err.args[0]
                except IndexError:
                    raise PatternError(str(err)) from None
                raise PatternError(
                    f"key {missing_key!r} is not defined.\n"
                    "Valid keys:"
                    f" {', '.join(map(repr, parameters.keys()))}"
                ) from None
        except PatternError as err:
            raise PatternError(
                f"For key {key!r}:"
                f" unable to interpolate pattern {value!r}: {err}"
            ) from None

    def __str__(self):
        return super().__str__()


class AuthenticationContext(Settings):
    """
    An :class:`AuthenticationContext` is nothing more than a ``[key, value]``
    dictionary of authentication data.

    The actual contents, including the list of keys, are strictly customer-
    and API-specific, and are not under the responsibility of this class.
    """

    Identifier = str

    def __init__(
        self,
        other: Union["AuthenticationContext", None] = None,
        *,
        identifier: Union[Identifier, None] = None,
        **kwargs: Settings.InputType,
    ):
        if other is None:
            assert identifier is not None
            super().__init__(identifier=identifier, **kwargs)
        else:
            assert identifier is None
            assert not kwargs
            super().__init__(other)


class CommonConfiguration(Settings):
    """
    An :class:`CommonConfiguration` is nothing more than a ``[key, value]``
    dictionary of configuration data, optionally referenced by
    :class:`MeasurementConfiguration` and :class:`MachineConfiguration` objects
    to load configuration data from a common shared configuration point.

    The actual contents, including the list of keys, are strictly customer-
    and API-specific, and are not under the responsibility of this class.
    """

    Identifier = str

    def __init__(
        self,
        other: Union["CommonConfiguration", None] = None,
        *,
        identifier: Union[Identifier, None] = None,
        **kwargs: Settings.InputType,
    ):
        if other is None:
            assert identifier is not None
            super().__init__(identifier=identifier, **kwargs)
        else:
            assert identifier is None
            assert not kwargs
            super().__init__(other)


class _MeasurementConfigurationSettings(Settings):
    """
    Generic configuration shared by :class:`CommonConfiguration`s,
    :class:`MeasurementConfigurationBase`s.

    It provides all the settings for a given measurement.
    """

    def __init__(
        self,
        other: Union["_MeasurementConfigurationSettings", None] = None,
        *,
        url: str = None,
        query_string: Union[Settings.InputType, None] = None,
        headers: Union[Settings.InputType, None] = None,
        common_configuration_identifier: Union[
            CommonConfiguration.Identifier, None
        ] = None,
        authentication_context_identifier: Union[
            AuthenticationContext.Identifier, None
        ] = None,
        **kwargs: Settings.InputType,
    ):
        if other is not None:
            assert url is None
            assert query_string is None
            assert headers is None
            assert common_configuration_identifier is None
            assert authentication_context_identifier is None
            assert not kwargs
            super().__init__(other)
        else:
            super().__init__(
                other,
                url=url,
                query_string=Settings(query_string),
                headers=Settings(headers),
                common_configuration_identifier=(
                    common_configuration_identifier
                ),
                authentication_context_identifier=(
                    authentication_context_identifier
                ),
                **kwargs,
            )

    async def get_common_configuration(self) -> Dict[str, str]:
        if self.common_configuration_identifier is None:
            return Settings()
        common_configuration = await CommonConfiguration.load(
            self.common_configuration_identifier
        )
        return common_configuration


class MeasurementConfiguration(_MeasurementConfigurationSettings):
    """
    Configuration for a single measurement (e.g. temperature, RPM, etc.).
    """

    Identifier = str
    SettingsType = Settings.InterpolatedType

    def __init__(
        self,
        other: Union["MeasurementConfiguration", None] = None,
        *,
        identifier: Union[Identifier, None] = None,
        **kwargs: Settings.InputType,
    ):
        if other is None:
            assert identifier is not None
            super().__init__(
                identifier=identifier,
                **kwargs,
            )
        else:
            assert identifier is None
            assert not kwargs
            super().__init__(other)

    class _SettingResolver:
        # Build the list of actual settings to be retained after resolution.
        # Simple solution: create a bare _MeasurementConfigurationSettings
        # instance and get its list of keys(), but filter out the few unwanted
        # ones.
        __RESULT_KEYS = set(
            filter(
                lambda key: not (
                    key.endswith("_identifier") or key == "identifier"
                ),
                _MeasurementConfigurationSettings().keys(),
            )
        )

        def __init__(
            self,
            measurement_configuration: "MeasurementConfiguration",
            machine_configuration: "MachineConfiguration",
            io_driver: "IODriver",
        ):
            self.measurement_configuration = measurement_configuration
            self.machine_configuration = machine_configuration
            self.io_driver = io_driver

        async def get_settings(
            self,
        ) -> "MeasurementConfiguration.SettingsType":
            settings = Settings(self.machine_configuration)
            settings.update(self.measurement_configuration)

            common_configuration_identifier = self.measurement_configuration[
                "common_configuration_identifier"
            ]
            if common_configuration_identifier is None:
                common_configuration_identifier = self.machine_configuration[
                    "common_configuration_identifier"
                ]

            if common_configuration_identifier is not None:
                common_configuration = (
                    await self.io_driver.common_configurations.get(
                        common_configuration_identifier
                    )
                )
                new_settings = Settings(common_configuration)
                new_settings.update(settings)
                settings = new_settings

            authentication_context_identifier = settings[
                "authentication_context_identifier"
            ]
            if authentication_context_identifier is not None:
                authentication_context = (
                    await self.io_driver.authentication_contexts.get(
                        authentication_context_identifier
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
            # parameters, and insert the machine and measurement identifiers
            parameters = {
                "machine": self.machine_configuration["identifier"],
                "measurement": self.measurement_configuration["identifier"],
            }
            parameters.update(
                {
                    key: value
                    for key, value in settings.items()
                    if not (
                        isinstance(value, Settings)
                        or key[0] == "_"
                        or key.endswith("_identifier")
                        or key == "identifier"
                    )
                }
            )

            # As for settings, only keep the keys specified for
            # _MeasurementConfigurationSettings, as they are the only relevant
            # fields to be presented to the backend driver.

            settings = Settings(
                _raw_init=True,
                **{
                    key: value
                    for key, value in settings.items()
                    if key in self.__RESULT_KEYS
                },
            )
            result = settings.apply(parameters)
            return result


class MachineConfiguration(_MeasurementConfigurationSettings):
    """
    Configuration for one machine, containing a machine-specific number of
    :class:`MeasurementConfiguration`s.
    """

    SettingsType = Dict[str, "MeasurementConfiguration.SettingsType"]
    Identifier = str

    def __init__(
        self,
        other: Union["MeasurementConfiguration", None] = None,
        *,
        identifier: Union[Identifier, None] = None,
        measurements: Union[Sequence[Settings.InputType], None] = None,
        **kwargs,
    ):
        if other is None:
            assert identifier is not None
            assert measurements is not None
            measurements = {
                measurement["identifier"]: measurement
                for measurement in map(
                    lambda m: MeasurementConfiguration(**m), measurements
                )
            }
            super().__init__(
                identifier=identifier, measurements=measurements, **kwargs
            )
        else:
            assert identifier is None
            assert measurements is None
            assert not kwargs
            super().__init__(other)

    class _SettingResolver:
        def __init__(
            self,
            machine_configuration: "MachineConfiguration",
            io_driver: "IODriver",
        ):
            self.machine_configuration = machine_configuration
            self.io_driver = io_driver

        class __MeasurementProxy:
            def __init__(self, parent):
                self.parent = parent

            def __getitem__(
                self, identifier: MeasurementConfiguration.Identifier
            ) -> MeasurementConfiguration._SettingResolver:
                measurements = self.parent.machine_configuration[
                    "measurements"
                ]
                try:
                    measurement_configuration = measurements[identifier]
                except KeyError:
                    valid_identifiers = ", ".join(measurements.keys())
                    raise Error(
                        f"Unrecognized measurement identifier {identifier}."
                        f" Valid identifiers: {valid_identifiers}"
                    ) from None
                return MeasurementConfiguration._SettingResolver(
                    measurement_configuration=measurement_configuration,
                    machine_configuration=self.parent.machine_configuration,
                    io_driver=self.parent.io_driver,
                )

        def __getitem__(self, key):
            if key == "measurements":
                return self.__MeasurementProxy(self)
            raise KeyError(key)

        async def get_settings(
            self,
        ) -> "MachineConfiguration.SettingsType":
            measurements = list(
                self.machine_configuration["measurements"].values()
            )
            measurements_settings = self["measurements"]
            tasks = [
                measurements_settings[measurement["identifier"]].get_settings()
                for measurement in measurements
            ]
            results = await asyncio.gather(*tasks)
            return {
                measurement["identifier"]: result
                for measurement, result in zip(measurements, results)
            }

    def get_setting_resolver(self, io_driver: "IODriver") -> _SettingResolver:
        return self._SettingResolver(
            machine_configuration=self, io_driver=io_driver
        )
