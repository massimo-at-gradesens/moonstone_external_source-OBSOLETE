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
import itertools
import re
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Sequence,
    Set,
    Tuple,
    Union,
)

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
#formatted-string-literals>`_ to be interpolated by calling
    :meth:`.interpolate`.

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

    InterpolatedValueType = Union[str, None]
    InterpolatedItemsType = Union["InterpolatedValueType", "InterpolatedType"]
    InterpolatedType = Dict[str, InterpolatedItemsType]

    def __init__(
        self,
        other: Union[
            InputType,
            Iterable[InputItemType],
            None,
        ] = None,
        _raw_init=False,
        _interpolate=None,
        **kwargs: InputType,
    ):
        if other is None:
            init_dict = kwargs
            if _interpolate is not None:
                kwargs["_interpolate"] = _interpolate
        else:
            assert _interpolate is None
            assert not kwargs
            init_dict = dict(other)

        if not _raw_init:
            init_dict = self.__normalize_values(init_dict)
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
        for key, other_value in other.items():
            if other_value is None:
                self.setdefault(key, None)
                continue

            if isinstance(other_value, Settings):
                try:
                    self_value = self[key]
                except KeyError:
                    pass
                else:
                    if isinstance(self_value, Settings):
                        self_value.update(other_value)
                        continue

            self[key] = other_value

    def __normalize_values(self, other: InputType):
        return {
            key: self.__normalize_value(value) for key, value in other.items()
        }

    def __normalize_value(self, value: InputValueType):
        if isinstance(value, Settings):
            return type(value)(value)

        if isinstance(value, dict):
            return Settings(self.__normalize_values(value))

        return value

    class InterpolationContext(dict):
        ParametersType = Dict[str, Any]

        def __init__(self, parameters: ParametersType, **kwargs):
            super().__init__(kwargs)
            self.parameters = parameters

    def interpolate(self, *args, **kwargs) -> InterpolatedType:
        return dict(self.interpolated_items(*args, **kwargs))

    def interpolated_items(
        self,
        context: InterpolationContext,
        _interpolate: bool = True,
    ) -> Iterable[InterpolatedItemsType]:
        for key, value in self.items():
            if key[:1] == "_":
                continue

            try:
                yield key, self.__interpolate_value(
                    value=value,
                    context=context,
                    _interpolate=_interpolate,
                )
            except PatternError as err:
                err.index.insert(0, key)
                raise err from None

    @classmethod
    def __interpolate_value(cls, value, context, _interpolate):
        if value is None:
            return value

        if isinstance(value, Settings):
            return value.interpolate(
                context=context,
                _interpolate=(
                    _interpolate and value.get("_interpolate", True)
                ),
            )

        if isinstance(value, Iterable) and not isinstance(value, str):
            return type(value)(
                cls.__interpolate_value(
                    value=item,
                    context=context,
                    _interpolate=_interpolate,
                )
                for item in value
            )

        if not _interpolate:
            return value

        return cls.interpolate_value(
            value=value, parameters=context.parameters
        )

    @classmethod
    def interpolate_value(
        cls,
        value: str,
        parameters: InterpolationContext.ParametersType,
    ) -> str:
        if not isinstance(value, str):
            return value

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
                f"Pattern {value!r}: "
                f"key {missing_key!r} is not defined.\n"
                "Valid keys:"
                f" {list(parameters.keys())}"
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

    Id = str

    def __init__(
        self,
        other: Union["AuthenticationContext", None] = None,
        *,
        id: Union[Id, None] = None,
        **kwargs: Settings.InputType,
    ):
        if other is None:
            if id is None:
                raise ConfigurationError(
                    "Missing authentication context's 'id'"
                )
            super().__init__(id=id, **kwargs)
        else:
            assert id is None
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
            )

    class InterpolatedSettings(dict):
        def process_result(self, value: str) -> str:
            regular_expression = self["regular_expression"]
            replacement = self["replacement"]
            try:
                return regular_expression.sub(replacement, str(value))
            except Exception as err:
                raise PatternError(
                    f"Failed processing '{value!r}'"
                    f" with regular expression {regular_expression.pattern!r}"
                    f" and replacement string {replacement!r}:"
                    f" {err}"
                ) from None

    def interpolate(self, *args, **kwargs) -> InterpolatedSettings:
        return self.InterpolatedSettings(
            self.interpolated_items(*args, **kwargs)
        )


class _MeasurementResultFieldSettings(Settings):
    """
    Common configuration settings used to parse measurement raw result output
    data. All result field settings classes derive from this class.
    Specialized by :class:`_MeasurementResultValueFieldSettings` and
    :class:`_MeasurementResultTimestampFieldSettings`.
    """

    class Converter:
        def __init__(
            self,
            convert_func: Callable,
            name: Union[str, None] = None,
        ):
            self.convert_func = convert_func
            self.name = name if name is not None else convert_func.__name__

        def __call__(self, value: str) -> Any:
            return self.convert_func(value)

        def __str__(self):
            return self.name

        def __repr__(self):
            return f"{type(self).__name__}[{self.name}]"

    VALID_TYPES = collections.OrderedDict(
        tuple(
            (converter.name, converter)
            for converter in (
                Converter(str),
                Converter(lambda value: int(value, 0), "int"),
                Converter(float),
                Converter(bool),
                Converter(datetime.fromisoformat, "datetime"),
            )
        )
    )

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

            if field_type is not None:
                try:
                    field_type = self.VALID_TYPES[field_type]
                except KeyError:
                    valid_types = ", ".join(map(repr, self.VALID_TYPES))
                    raise ConfigurationError(
                        f"Invalid type {field_type!r}"
                        " for measurement result value field."
                        f" Valid types: {valid_types}"
                    ) from None

            kwargs = dict()

            if regular_expression is not None:
                if isinstance(regular_expression, dict):
                    regular_expressions = (regular_expression,)
                elif isinstance(regular_expression, collections.abc.Iterable):
                    regular_expressions = tuple(regular_expression)
                else:
                    raise ConfigurationError(
                        "Dictionary or list expected"
                        " for regular expression field"
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
                regular_expressions = tuple(
                    map(
                        lambda regular_expression: _RegexSettings(
                            **regular_expression
                        ),
                        regular_expressions,
                    )
                )

                kwargs.update(
                    dict(
                        regular_expressions=regular_expressions,
                    )
                )

            if field_type is not None:
                kwargs.update(
                    type=field_type,
                )
            if raw_value is not None:
                kwargs.update(
                    raw_value=raw_value,
                )

            super().__init__(**kwargs)

    class InterpolatedSettings(dict):
        def process_result(
            self, raw_result: "_MeasurementResultSettings.RawResultType"
        ) -> "_MeasurementResultSettings.ResultValueType":
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

            value = Settings.interpolate_value(
                value=raw_value,
                parameters=raw_result,
            )

            for regular_expression in self.get("regular_expressions", []):
                value = regular_expression.process_result(value)

            try:
                return field_type(value)
            except Exception as err:
                raise DataTypeError(
                    f"Failed converting {value!r}"
                    f" to an object of type {str(field_type)!r}:"
                    f" {err}"
                ) from None

            return value

    def interpolate(self, *args, **kwargs) -> InterpolatedSettings:
        return self.InterpolatedSettings(
            self.interpolated_items(*args, **kwargs)
        )


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


class _CommonConfigurationIdSettings(Settings):
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
        other: Union["_CommonConfigurationIdSettings", None] = None,
        *,
        common_configuration_ids: Union[
            Iterable["CommonConfiguration.Id"],
            "CommonConfiguration.Id",
            None,
        ] = None,
        **kwargs: Settings.InputType,
    ):
        if other is not None:
            assert common_configuration_ids is None
            assert not kwargs
            super().__init__(other)
        else:
            if common_configuration_ids is None:
                common_configuration_ids = ()
            elif isinstance(common_configuration_ids, CommonConfiguration.Id):
                common_configuration_ids = (common_configuration_ids,)
            elif isinstance(common_configuration_ids, Iterable):
                common_configuration_ids = tuple(common_configuration_ids)
            else:
                common_configuration_ids = (common_configuration_ids,)
            super().__init__(
                other,
                _common_configuration_ids=(common_configuration_ids),
                **kwargs,
            )

    async def get_common_settings(
        self,
        io_driver: "IODriver",
        already_visited: Union[Set["CommonConfiguration.Id"], None] = None,
    ) -> Settings:
        """
        See details in :meth:`CommonConfiguration.get_common_settings`
        """
        common_configuration_ids = self["_common_configuration_ids"]
        if len(common_configuration_ids) == 0:
            return Settings()

        tasks = [
            io_driver.common_configurations.get(common_configuration_id)
            for common_configuration_id in common_configuration_ids
        ]
        common_configurations = await asyncio.gather(*tasks)

        if already_visited is None:
            already_visited = set()
        tasks = [
            common_configuration.get_common_settings(
                io_driver=io_driver, already_visited=already_visited
            )
            for common_configuration in common_configurations
        ]
        all_settings = await asyncio.gather(*tasks)

        result = all_settings[0]
        for settings in all_settings[1:]:
            result.update(settings)

        return result


class _HTTPRequestSettings(Settings):
    """
    Configuration settings for a generic HTTP request.

    These configuration settings are used by
    :class:`_MeasurementSettings` and :class:`_AuthenticationSettings`
    """

    def __init__(
        self,
        other: Union["_HTTPRequestSettings", None] = None,
        *,
        url: str = None,
        query_string: Union[Settings.InputType, None] = None,
        headers: Union[Settings.InputType, None] = None,
        **kwargs: Settings.InputType,
    ):
        if other is not None:
            assert url is None
            assert query_string is None
            assert headers is None
            assert not kwargs
            super().__init__(other)
        else:
            query_string = Settings(query_string)
            headers = Settings(headers)

            super().__init__(
                other,
                url=url,
                query_string=query_string,
                headers=headers,
                **kwargs,
            )


class _MeasurementResultSettings(Settings):
    """
    Configuration settings for measurement results.

    These configuration settings are used by :class:`_MeasurementSettings`.
    """

    RawResultValueType = Any
    RawResultType = Dict[str, Union[RawResultValueType, "RawResultType"]]
    ResultValueType = Any
    ResultType = Dict[str, ResultValueType]

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
                _interpolate=False,
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

    class InterpolatedSettings(dict):
        def __init__(
            self, *, context: Settings.InterpolationContext, **kwargs
        ):
            self.__context = context
            super().__init__(kwargs)

        def process_result(
            self, raw_result: "_MeasurementResultSettings.RawResultType"
        ) -> "_MeasurementResultSettings.ResultType":
            result = {}
            for key, value in self.items():
                if value is None:
                    raise ConfigurationError(f"Field {key!r} is not specified")
                try:
                    result[key] = value.process_result(raw_result)
                except Error as err:
                    machine_id = self.__context["machine_configuration"]["id"]
                    measurement_id = self.__context[
                        "measurement_configuration"
                    ]["id"]
                    raise type(err)(
                        f"Machine {machine_id!r}: "
                        f"Measurement {measurement_id!r}: "
                        f"Field {key!r}: "
                        f"{err}"
                    ) from None
            return result

    def interpolate(
        self, *args, context: Settings.InterpolationContext, **kwargs
    ) -> InterpolatedSettings:
        return self.InterpolatedSettings(
            context=context,
            **dict(self.interpolated_items(*args, context=context, **kwargs)),
        )


class _MeasurementSettings(_CommonConfigurationIdSettings):
    """
    Configuration settings for a single measurement.

    These configuration settings are used by :class:`CommonConfiguration`s,
    :class:`MeasurementConfiguration`s and :class:`MachineConfiguration`s.
    """

    def __init__(
        self,
        other: Union["_MeasurementSettings", None] = None,
        *,
        authentication_configuration_id: Union[
            AuthenticationContext.Id, None
        ] = None,
        request: Union[_HTTPRequestSettings, None] = None,
        result: Union[_MeasurementResultSettings, None] = None,
        **kwargs: Settings.InputType,
    ):
        if other is not None:
            assert authentication_configuration_id is None
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
            # field of _MeasurementResultSettings
            if request is None:
                request = {}
            request = _HTTPRequestSettings(**request)
            if result is None:
                result = {}
            result = _MeasurementResultSettings(**result)

            super().__init__(
                other,
                _authentication_configuration_id=(
                    authentication_configuration_id
                ),
                request=request,
                result=result,
                **kwargs,
            )


class MeasurementConfiguration(_MeasurementSettings):
    """
    Configuration for a single measurement (e.g. temperature, RPM, etc.).
    """

    Id = str
    SettingsType = "MeasurementConfiguration.InterpolatedSettings"

    def __init__(
        self,
        other: Union["MeasurementConfiguration", None] = None,
        *,
        id: Union[Id, None] = None,
        **kwargs: Settings.InputType,
    ):
        if other is None:
            if id is None:
                raise ConfigurationError(
                    "Missing measurement configuration's 'id'"
                )
            super().__init__(
                id=id,
                **kwargs,
            )
        else:
            assert id is None
            assert not kwargs
            super().__init__(other)

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
                io_driver=self.io_driver
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
                    await self.io_driver.authentication_contexts.get(
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
                result = settings.interpolate(context=interpolation_context)
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
        other: Union["_MachineConfigurationSettings", None] = None,
        *,
        measurements: Union[Sequence[Settings.InputType], None] = None,
        **kwargs,
    ):
        if other is None:
            if measurements is None:
                measurements = []
            measurements = {
                measurement["id"]: measurement
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
        other: Union["CommonConfiguration", None] = None,
        *,
        id: Union[Id, None] = None,
        **kwargs: Settings.InputType,
    ):
        if other is None:
            if id is None:
                raise ConfigurationError("Missing common configuration's 'id'")
            super().__init__(id=id, **kwargs)
        else:
            assert id is None
            assert not kwargs
            super().__init__(other)

    async def get_common_settings(
        self,
        io_driver: "IODriver",
        already_visited: Union[Set["CommonConfiguration.Id"], None] = None,
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

        common_configuration_id = self["id"]
        if already_visited is None:
            already_visited = {common_configuration_id}
        else:
            if common_configuration_id in already_visited:
                raise ConfigurationError(
                    "Common configuration loop"
                    f" for {common_configuration_id!r}."
                    " All visited common configurations"
                    f" in this loop: {already_visited}"
                )
            already_visited.add(common_configuration_id)

        result = await super().get_common_settings(
            io_driver=io_driver,
            already_visited=already_visited,
        )
        result.update(self)
        return result


class MachineConfiguration(_MachineConfigurationSettings):
    """
    Configuration for one machine, containing a machine-specific number of
    :class:`MeasurementConfiguration`s.
    """

    Id = str
    SettingsType = Dict[str, "MeasurementConfiguration.SettingsType"]
    MeasurementIdsType = Union[None, Iterable[MeasurementConfiguration.Id]]

    def __init__(
        self,
        other: Union["MeasurementConfiguration", None] = None,
        *,
        id: Union[Id, None] = None,
        **kwargs,
    ):
        if other is None:
            if id is None:
                raise ConfigurationError(
                    "Missing machine configuration'a 'id'"
                )
            super().__init__(id=id, **kwargs)
            if not self["measurements"]:
                raise ConfigurationError(
                    "Missing machine configuration's 'measurements'"
                )
        else:
            assert id is None
            assert not kwargs
            super().__init__(other)

    class _SettingsResolver:
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
                    io_driver=self.parent.io_driver,
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

    def get_setting_resolver(self, io_driver: "IODriver") -> _SettingsResolver:
        return self._SettingsResolver(
            machine_configuration=self, io_driver=io_driver
        )
