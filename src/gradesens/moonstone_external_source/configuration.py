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
import builtins
import collections
import re
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, Iterable, Sequence, Tuple, Union

if TYPE_CHECKING:
    from .io_driver import IODriver

from .error import (
    ConfigurationError,
    DataTypeError,
    Error,
    PatternError,
    TimeError,
)


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
        _interpolatable=None,
        **kwargs: InputType,
    ):
        if other is None:
            init_dict = kwargs
            if _interpolatable is not None:
                kwargs["_interpolatable"] = _interpolatable
        else:
            assert _interpolatable is None
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
        return dict(self.__interpolate_values(parameters))

    def __interpolate_values(self, parameters):
        for key, value in self.items():
            if key[:1] == "_":
                continue

            if isinstance(value, Settings):
                if value.get("_interpolatable", True):
                    yield key, value.apply(parameters)
                continue

            if value is None:
                yield key, value
                continue

            try:
                yield key, self.__interpolate_value(value, parameters)
            except PatternError as err:
                raise PatternError(
                    f"For key {key!r}:"
                    f" unable to interpolate pattern {value!r}: {err}"
                ) from None

    @classmethod
    def __interpolate_value(cls, value, parameters):
        try:
            try:
                return eval(f"f{value!r}", {}, parameters)
            except KeyError:
                raise
            except NameError as err:
                try:
                    name = err.name
                except AttributeError:
                    # Given that is seems that err.name is available since
                    # py 3.10, try retrieving the key name by parsing the
                    # error string.
                    err = str(err)
                    re_match = re.match(r".*'([^']+)'.*", err)
                    name = err if re_match is None else re_match.group(1)
                raise KeyError(name) from None
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
            if identifier is None:
                raise ConfigurationError(
                    "Missing authentication context's 'identifier'"
                )
            super().__init__(identifier=identifier, **kwargs)
        else:
            assert identifier is None
            assert not kwargs
            super().__init__(other)


class _RegexSettings(Settings):
    """
    Configuration settings for regular expression-based text substitutions
    for measurement output data,
    used by :class:`_MeasurementResultFieldSettings` for output data
    processing.

    This regular expression support is based on Python's own
    `re module <https://docs.python.org/3/library/re.html>`, in particular on
    `re.sub() <https://docs.python.org/3/library/re.html#re.sub>` function.
    Refer to that function's manual for usage instructions.
    """

    def __init__(
        self,
        other: Union["_RegexSettings", None] = None,
        *,
        pattern: Union[str, None] = None,
        replacement: Union[str, None] = None,
        flags: Union[str, Iterable[str], int, None] = None,
    ):
        if other is not None:
            assert pattern is None
            assert replacement is None
            assert flags is None
            super().__init__(other)
        else:
            if pattern is None:
                raise ConfigurationError(
                    "Missing regular expression's 'pattern'"
                )
            if replacement is None:
                raise ConfigurationError(
                    "Missing regular expression's 'replacement' string"
                )

            if flags is None:
                flags = []
            elif isinstance(flags, str):
                flags = [flags]
            elif isinstance(flags, collections.abc.Iterable):
                flags = list(flags)
            else:
                raise ConfigurationError(
                    f"Invalid type {type(flags).__name__}"
                    " for regular expression's flag."
                    " Expected str or list of strings"
                )
            flags = list(map(lambda flag: str(flag).upper(), flags))
            flags_value = 0
            valid_flags = [re.IGNORECASE]
            for flag in flags:
                try:
                    real_flag = re.RegexFlag[flag]
                except KeyError:
                    raise ConfigurationError(
                        f"Invalid regular expression's flag {flag!r}"
                    ) from None
                if real_flag not in valid_flags:
                    supported_flags = ", ".join(
                        map(lambda flag: flag.name, valid_flags)
                    )
                    raise ConfigurationError(
                        f"Unsupported regular expression's flag {flag!r}."
                        f" Supported flags: {supported_flags}"
                    )
                flags_value |= int(real_flag)

            try:
                regular_expression = re.compile(pattern, flags=flags_value)
            except re.error as err:
                raise ConfigurationError(
                    f"Invalid regular expression's pattern {pattern!r}:"
                    f" {err}"
                ) from None

            super().__init__(
                other,
                regular_expression=regular_expression,
                replacement=replacement,
                flags=flags_value,
            )

    def process_result(self, str):
        regular_expression = self["regular_expression"]
        replacement = self["replacement"]
        try:
            return regular_expression.sub(replacement, str)
        except Exception as err:
            raise PatternError(
                f"Failed processing '{str!r}'"
                f" with regular expression {regular_expression.pattern!r}"
                f" and replacement string {replacement!r}:"
                f" {err}"
            ) from None


class _MeasurementResultFieldSettings(Settings):
    """
    Common configuration settings used to parse measurement raw result output
    data. All result field settings classes derive from this class.
    Specialized by :class:`_MeasurementResultValueFieldSettings` and
    :class:`_MeasurementResultTimestampFieldSettings`.
    """

    DEFAULT_TYPE = "str"
    VALID_TYPES = [
        "str",
        "float",
        "int",
        "bool",
        "datetime",
    ]

    def __init__(
        self,
        other: Union["_MeasurementResultFieldSettings", None] = None,
        *,
        type: Union[str, None] = None,
        raw_value: Union[str, None] = None,
        regular_expression: Union[
            Iterable[Settings.InputType], Settings.InputType, None
        ] = None,
    ):
        if other is not None:
            assert type is None
            assert raw_value is None
            assert regular_expression is None
            super().__init__(other)
        else:
            # "restore" the normal 'type()' operation, after having overridden
            # it with the 'type' parameter
            field_type = type
            type = builtins.type

            if field_type is None:
                field_type = self.DEFAULT_TYPE
            if field_type not in self.VALID_TYPES:
                valid_types = ", ".join(self.VALID_TYPES)
                raise ConfigurationError(
                    f"Invalid type {field_type}"
                    " for measurement result value field."
                    f" Valid types: {valid_types}"
                )
            field_type = eval(field_type)

            if regular_expression is None:
                regular_expressions = []
            elif isinstance(regular_expression, dict):
                regular_expressions = [regular_expression]
            elif isinstance(regular_expression, collections.abc.Iterable):
                regular_expressions = list(regular_expression)
            else:
                raise ConfigurationError(
                    "Dictionary or list expected for regular expression field"
                    f" instead of {type(regular_expression).__name__}:"
                    f" {regular_expression!r}"
                )
            for regular_expression in regular_expressions:
                if not isinstance(regular_expression, dict):
                    raise ConfigurationError(
                        "Dictionary expected for each regular expression"
                        f" instead of {type(regular_expression).__name__}:"
                        f" {regular_expression!r}"
                    )
            regular_expressions = list(
                map(
                    lambda regular_expression: _RegexSettings(
                        **regular_expression
                    ),
                    regular_expressions,
                )
            )

            kwargs = dict(
                type=field_type,
                regular_expressions=regular_expressions,
            )
            if raw_value is not None:
                kwargs.update(
                    raw_value=raw_value,
                )

            super().__init__(**kwargs)

    def process_result(
        self, raw_result: "_MeasurementResultSettings.RawResultType"
    ):
        try:
            raw_value = self["raw_value"]
        except KeyError:
            raise ConfigurationError(
                "Missing measurement result field's 'raw_value' field"
            ) from None
        try:
            field_type = self["type"]
        except KeyError:
            raise ConfigurationError(
                "Missing measurement result field's 'type' field"
            ) from None

        value = raw_value.apply(raw_result)

        for regular_expression in self["regular_expressions"]:
            value = regular_expression.process_result(value)

        try:
            return field_type(value)
        except Exception as err:
            raise DataTypeError(
                f"Failed converting {value!r}"
                f" to an object of type {field_type.__name__}:"
                f" {err}"
            ) from None

        return value


class _MeasurementResultTimestampFieldSettings(
    _MeasurementResultFieldSettings
):
    """
    Configuration settings used to parse measurement result raw output data
    into the eventual output timestamp field.
    These configuration settings are used by
    :class:`_MeasurementResultSettings`s
    """

    def __init__(
        self,
        other: Union["_MeasurementResultTimestampFieldSettings", None] = None,
        **kwargs: Settings.InputType,
    ):
        if other is not None:
            assert not kwargs
            super().__init__(other)
        else:
            super().__init__(type="datetime", **kwargs)


class _MeasurementResultSettings(Settings):
    """
    Configuration settings for a single measurement's results.

    These configuration settings are used by :class:`_MeasurementSettings`
    instances.
    """

    RawResultValueType = Any
    RawResultType = Dict[str, Union[RawResultValueType, "RawResultType"]]

    def __init__(
        self,
        other: Union["_MeasurementResultSettings", None] = None,
        *,
        value: Union[_MeasurementResultFieldSettings.InputType, None] = None,
        timestamp: Union[
            _MeasurementResultTimestampFieldSettings.InputType, None
        ] = None,
    ):
        if other is not None:
            assert value is None
            assert timestamp is None
            super().__init__(other)
        else:
            kwargs = dict(
                _interpolatable=False,
            )
            if value is not None:
                kwargs.update(value=_MeasurementResultFieldSettings(**value))
            if timestamp is not None:
                kwargs.update(
                    timestamp=_MeasurementResultTimestampFieldSettings(
                        **timestamp
                    )
                )
            super().__init__(**kwargs)

    def process_result(self, raw_result: RawResultType):
        result = {}
        for key, value in self.items():
            if value is None:
                raise ConfigurationError(f"Field {key!r} is not specified")
            result[key] = value.process_result(raw_result)
        return result


class _MeasurementSettings(Settings):
    """
    Configuration settings for a single measurement.

    These configuration settings are used by :class:`CommonConfiguration`s,
    :class:`MeasurementConfiguration`s and :class:`MachineConfiguration`s.
    """

    MeasurementResults = 0

    def __init__(
        self,
        other: Union["_MeasurementSettings", None] = None,
        *,
        url: str = None,
        query_string: Union[Settings.InputType, None] = None,
        headers: Union[Settings.InputType, None] = None,
        authentication_context_identifier: Union[
            AuthenticationContext.Identifier, None
        ] = None,
        result: Union[_MeasurementResultSettings, None] = None,
        **kwargs: Settings.InputType,
    ):
        if other is not None:
            assert url is None
            assert query_string is None
            assert headers is None
            assert authentication_context_identifier is None
            assert result is None
            assert not kwargs
            super().__init__(other)
        else:
            query_string = Settings(query_string)
            headers = Settings(headers)

            # Force a non-copy creation, so as to instantiate the
            # field-specific settings classes, required for proper result
            # parsing.
            # Furthermore, always force the creation a result settings object,
            # even if empty, to make sure the _interpolatable field is set.
            if result is None:
                result = {}
            result = _MeasurementResultSettings(**result)

            super().__init__(
                other,
                url=url,
                query_string=query_string,
                headers=headers,
                authentication_context_identifier=(
                    authentication_context_identifier
                ),
                result=result,
                **kwargs,
            )

    def process_result(
        self, raw_result: _MeasurementResultSettings.RawResultType
    ):
        result = self["result"]
        try:
            if result is None:
                raise ConfigurationError("Field 'result' is not specified")
            return result.process_result(raw_result)
        except Error as err:
            identifier = self.get("identifier", None)
            if identifier is None:
                raise err from None
            err_msg = f"For {identifier!r}: {err}"
            raise type(err)(err_msg) from None


class _CommonConfigurationIdentifierSettings(Settings):
    """
    Mixin to provide the optional setting
    :attr:`.common_configuration_identifier` to reference to a
    :class:`CommonConfiguration` instance.

    These configuration settings are used by :class:`MeasurementConfiguration`s
    and :class:`MachineConfiguration`s.
    """

    def __init__(
        self,
        other: Union["_CommonConfigurationIdentifierSettings", None] = None,
        *,
        common_configuration_identifier: Union[
            "CommonConfiguration.Identifier", None
        ] = None,
        **kwargs: Settings.InputType,
    ):
        if other is not None:
            assert common_configuration_identifier is None
            assert not kwargs
            super().__init__(other)
        else:
            super().__init__(
                other,
                common_configuration_identifier=(
                    common_configuration_identifier
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


class MeasurementConfiguration(
    _CommonConfigurationIdentifierSettings, _MeasurementSettings
):
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
            if identifier is None:
                raise ConfigurationError(
                    "Missing measurement configuration's 'identifier'"
                )
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
        # Simple solution: create a bare _MeasurementSettings instance and get
        # its list of keys(), but filter out the few unwanted ones.
        __RESULT_KEYS = set(
            filter(
                lambda key: not (
                    key.endswith("_identifier") or key == "identifier"
                ),
                _MeasurementSettings().keys(),
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
            start_time: Union[datetime, None] = None,
            end_time: Union[datetime, None] = None,
        ) -> "MeasurementConfiguration.SettingsType":
            settings = Settings(self.machine_configuration)

            if start_time is not None:
                self.__assert_aware_time("start_time", start_time)
                settings["start_time"] = start_time
            if end_time is not None:
                self.__assert_aware_time("end_time", end_time)
                settings["end_time"] = end_time

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
                        key[0] == "_"
                        or key.endswith("_identifier")
                        or key == "identifier"
                    )
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
            result = settings.apply(parameters)
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
        other: Union["_MachineConfigurationSettings", None] = None,
        *,
        measurements: Union[Sequence[Settings.InputType], None] = None,
        **kwargs,
    ):
        if other is None:
            if measurements is None:
                measurements = []
            measurements = {
                measurement["identifier"]: measurement
                for measurement in map(
                    lambda m: MeasurementConfiguration(**m), measurements
                )
            }
            super().__init__(measurements=measurements, **kwargs)
        else:
            assert measurements is None
            assert not kwargs
            super().__init__(other)


class CommonConfiguration(_MachineConfigurationSettings):
    """
    An :class:`CommonConfiguration` is nothing more than a ``[key, value]``
    dictionary of configuration data, optionally referenced by
    :class:`MeasurementConfiguration` and :class:`MachineConfiguration`
    instances to load configuration data from a common shared configuration
    point.

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
            if identifier is None:
                raise ConfigurationError(
                    "Missing common configuration's 'identifier'"
                )
            super().__init__(identifier=identifier, **kwargs)
        else:
            assert identifier is None
            assert not kwargs
            super().__init__(other)


class MachineConfiguration(
    _MachineConfigurationSettings, _CommonConfigurationIdentifierSettings
):
    """
    Configuration for one machine, containing a machine-specific number of
    :class:`MeasurementConfiguration`s.
    """

    Identifier = str
    SettingsType = Dict[str, "MeasurementConfiguration.SettingsType"]
    MeasurementIdentifiersType = Union[
        None, Iterable[MeasurementConfiguration.Identifier]
    ]

    def __init__(
        self,
        other: Union["MeasurementConfiguration", None] = None,
        *,
        identifier: Union[Identifier, None] = None,
        **kwargs,
    ):
        if other is None:
            if identifier is None:
                raise ConfigurationError(
                    "Missing machine configuration'a 'identifier'"
                )
            super().__init__(identifier=identifier, **kwargs)
            if not self["measurements"]:
                raise ConfigurationError(
                    "Missing machine configuration's 'measurements'"
                )
        else:
            assert identifier is None
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
                    raise ConfigurationError(
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
            measurement_identifiers: (
                "MachineConfiguration.MeasurementIdentifiersType"
            ) = None,
            **kwargs,
        ) -> "MachineConfiguration.SettingsType":
            if measurement_identifiers is None:
                # If no identifiers are specified, return all measurements
                measurement_identifiers = self.machine_configuration[
                    "measurements"
                ].keys()
            measurement_identifiers = set(measurement_identifiers)

            # Retain only the identifiers that actually match this machine
            # measurements.
            # Silently drop the other ones.
            measurement_identifiers &= set(
                self.machine_configuration["measurements"].keys()
            )

            measurements_settings = self["measurements"]

            # Use an OrderedDict to guarantee order repeatability, so as to
            # guarantee that identifiers and results can be correctly zipped
            # together after asyncio.gather()
            tasks = collections.OrderedDict(
                **{
                    measurement_identifier: measurements_settings[
                        measurement_identifier
                    ].get_settings(**kwargs)
                    for measurement_identifier in measurement_identifiers
                }
            )
            results = await asyncio.gather(*tasks.values())
            return {
                measurement_identifier: result
                for measurement_identifier, result in zip(
                    tasks.keys(), results
                )
            }

    def get_setting_resolver(self, io_driver: "IODriver") -> _SettingResolver:
        return self._SettingResolver(
            machine_configuration=self, io_driver=io_driver
        )
