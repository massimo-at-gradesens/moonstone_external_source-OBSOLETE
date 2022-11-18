"""
GradeSens - External Source package - Configuration support

This module provides the generic configuration settings use throughout this
whole package. All feature-specific settings are implemented as classes
derived or using the classes in this module.
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, Gradesens AG"


import collections
import re
from typing import Any, Dict, Iterable, Tuple, Union

from .error import ConfigurationError, PatternError


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


class RegexSettings(Settings):
    """
    Configuration settings for regular expression-based text substitutions
    for measurement output data,
    used by :class:`HTTPResultFieldSettings` for output data processing.

    This regular expression support is based on Python's own
    `re module <https://docs.python.org/3/library/re.html>`, in particular on
    `re.sub() <https://docs.python.org/3/library/re.html#re.sub>` function.
    Refer to that function's manual for usage instructions.
    """

    def __init__(
        self,
        other: Union["RegexSettings", None] = None,
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
