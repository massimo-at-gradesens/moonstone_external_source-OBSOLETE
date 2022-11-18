"""
GradeSens - External Source package - Configuration support

This module provides the configuration data classes to handle generic HTTP
transactions and parse their responses into higher abstraction results,
according to user specified rules.
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, Gradesens AG"


import builtins
import collections
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, Union

from .error import ConfigurationError, DataTypeError, Error
from .settings import RegexSettings, Settings


class HTTPRequestSettings(Settings):
    """
    Configuration settings for a generic HTTP request.

    These configuration settings are used by
    :class:`_MeasurementSettings` and :class:`_AuthenticationSettings`
    """

    def __init__(
        self,
        other: Union["HTTPRequestSettings", None] = None,
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


class HTTPResultFieldSettings(Settings):
    """
    Common configuration settings used to parse raw HTTP response output
    data into target data. All result field settings classes derive from this
    class.
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
        other: Union["HTTPResultFieldSettings", None] = None,
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
                        f"Invalid type {field_type!r} for result value field."
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
                        lambda regular_expression: RegexSettings(
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
            self, raw_result: "HTTPResultSettings.RawResultType"
        ) -> "HTTPResultSettings.ResultValueType":
            try:
                raw_value = self["raw_value"]
            except KeyError:
                raise ConfigurationError(
                    "Missing result field's 'raw_value' field"
                ) from None
            try:
                field_type = self["type"]
            except KeyError:
                raise ConfigurationError(
                    "Missing result field's 'type' field"
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


class HTTPResultTimestampFieldSettings(HTTPResultFieldSettings):
    """
    Configuration settings used to parse measurement result raw output data
    into the eventual output timestamp field.
    These configuration settings are used by
    :class:`_MeasurementResultSettings`s
    """

    def __init__(
        self,
        other: Union["HTTPResultTimestampFieldSettings", None] = None,
        **kwargs: Settings.InputType,
    ):
        if other is not None:
            assert not kwargs
            super().__init__(other)
        else:
            if "type" in kwargs:
                raise ConfigurationError(
                    "Parameter 'type'"
                    " cannot be specified with timestamp fields"
                )
            super().__init__(type="datetime", **kwargs)


class HTTPResultSettings(Settings):
    """
    Configuration settings for generic HTTP response processing to produce
    target results.

    These configuration settings are used by
    :class:`HTTPTransactionSettings`.
    """

    RawResultValueType = Any
    RawResultType = Dict[str, Union[RawResultValueType, "RawResultType"]]
    ResultValueType = Any
    ResultType = Dict[str, ResultValueType]

    def __init__(
        self, other: Union["HTTPResultSettings", None] = None, **kwargs
    ):
        if other is not None:
            assert not kwargs
            super().__init__(other)
        else:
            kwargs2 = dict(
                _interpolate=False,
            )
            for key, value in kwargs.items():
                if isinstance(value, HTTPResultFieldSettings):
                    pass
                elif isinstance(value, dict):
                    value = HTTPResultFieldSettings(value)
                elif key[0] != "_":
                    raise ConfigurationError(
                        f"Field {key}: invalid type {type(value).__name__!r}."
                        " Expected HTTPResultFieldSettings or dict"
                    )
                kwargs2[key] = value
            super().__init__(**kwargs2)

    class InterpolatedSettings(dict):
        def process_result(
            self, raw_result: "HTTPResultSettings.RawResultType"
        ) -> "HTTPResultSettings.ResultType":
            result = {}
            for key, value in self.items():
                if value is None:
                    raise ConfigurationError(f"Field {key!r} is not specified")
                try:
                    result[key] = value.process_result(raw_result)
                except Error as err:
                    raise type(err)(f"Field {key!r}: {err}") from None
            return result

    def interpolate(self, *args, **kwargs) -> InterpolatedSettings:
        return self.InterpolatedSettings(
            self.interpolated_items(*args, **kwargs)
        )
