"""
GradeSens - External Source package - Configuration support

This module provides the generic configuration settings use throughout this
whole package. All feature-specific settings are implemented as classes
derived or using the classes in this module.
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, GradeSens AG"


import abc
import collections
import re
from datetime import date, datetime, time, timedelta
from typing import Any, Callable, Dict, Iterable, Optional, Tuple, Union

from .datetime import Date, DateTime, Time, TimeDelta
from .error import (
    ConfigurationError,
    DataTypeError,
    DataValueError,
    Error,
    EvalError,
    PatternError,
)


class Settings(dict):
    """
    A :class:`dict` specialization containing a list of ``[key, value]``
    entries, where a ``value`` may either be a plain :class:`str` or a
    python `formatted string literal
    <https://docs.python.org/3/tutorial/inputoutput.html\
#formatted-string-literals>`_ to be interpolated by calling
    :meth:`.interpolate`.

    Also, to ease the readability in configuration fields, the dictionary
    elements can be retrieved also as attributes - of course, provided that
    there are no context-specific attributes with the same name defined in the
    same object. For instance ``an_instance["a_key"]`` and
    ``an_instance.a_key`` would produce the same result.

    .. note::
        All the methods or operations that change the contents of a
        :class:`Settings` instance, including :meth:`.__init__`, automatically
        normalize all :class:`dict` values into :class:`Settings` instances,
        or into lists of :class:`Settings` instances, or into any multi-level
        nested combination thereof.
        This guarantees a consistent :class:`Settings`-specific behavior all
        through a :class:`Settings` hierarchy, no matter how nested it may be.

    .. note::
        :meth:`.update` method's behavior differs from the behavior of its
        :meth:`dict.update` counterpart.
        See :meth:`.update` for more details.
    """

    KeyType = str
    ValueType = Optional[Union[str, "Settings"]]
    ItemType = Tuple[KeyType, ValueType]

    InputValueType = Union[ValueType, Dict[KeyType, "InputValueType"]]
    InputItemType = Tuple[KeyType, InputValueType]
    InputType = Union["Settings", InputValueType]

    InterpolatedValueType = Optional[str]
    InterpolatedItemsType = Union["InterpolatedValueType", "InterpolatedType"]
    InterpolatedType = Dict[str, InterpolatedItemsType]

    class _RawInit:
        """
        Wrapper for initialization or assignment values, to indicate raw
        initialization is required instead of the default copy-and-normalize
        one.
        """

        def __init__(self, value):
            self.value = value

    def __init__(
        self,
        other: Optional[
            Union[
                InputType,
                Iterable[InputItemType],
            ]
        ] = None,
        /,
        *,
        _interpolation_settings: Optional[
            "Settings.InterpolationSettings"
        ] = None,
        **kwargs: InputType,
    ):
        if other is not None:
            assert _interpolation_settings is None
            assert not kwargs
            initializer = other
            has_processors = Processors.KEY in kwargs
        else:
            try:
                processors = kwargs.pop(Processors.KEY)
            except KeyError:
                has_processors = False
            else:
                has_processors = True
            if has_processors:
                if len(kwargs) != 0:
                    raise ConfigurationError(
                        f"Key {Processors.KEY!r} must be the only only key,"
                        f" but other keys were specified:"
                        f" {', '.join(map(repr, kwargs))}"
                    )
                try:
                    processors = Processors(configuration=processors)
                except Error as err:
                    err.index.insert(0, Processors.KEY)
                    raise
                kwargs = {Processors.KEY: processors}

            if _interpolation_settings is not None:
                kwargs["_interpolation_settings"] = _interpolation_settings
            initializer = kwargs

        if isinstance(initializer, self._RawInit):
            initializer = initializer.value
            if not isinstance(initializer, dict):
                initializer = dict(initializer)
        elif not has_processors:
            initializer = self.__normalize_values(initializer)

        super().__init__(**initializer)

    def __getattr__(self, name):
        return self[name]

    def __setitem__(self, key: KeyType, value: InputValueType):
        super().__setitem__(key, self.__normalize_value(value, key=key))

    def setdefault(
        self, key: KeyType, default: InputValueType = None
    ) -> ValueType:
        try:
            return self[key]
        except KeyError:
            pass
        default = self.__normalize_value(default, key=key)
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

    @classmethod
    def __normalize_values(cls, other: InputType):
        iterator = other.items() if isinstance(other, dict) else iter(other)
        return {
            key: cls.__normalize_value(value, key=key)
            for key, value in iterator
        }

    __NORMALIZE_VALUE_TYPES = (
        (datetime, DateTime),
        (date, Date),
        (time, Time),
        (timedelta, TimeDelta),
    )

    @classmethod
    def __normalize_value(cls, value: InputValueType, key: Any = None):
        if isinstance(value, cls._RawInit):
            return value.value

        try:
            if isinstance(value, Settings):
                return type(value)(value)

            if isinstance(value, dict):
                return Settings(**value)

            if isinstance(value, (list, tuple)):
                return type(value)(
                    cls.__normalize_value(item, index)
                    for index, item in enumerate(value)
                )

            for src_type, dst_type in cls.__NORMALIZE_VALUE_TYPES:
                if isinstance(value, src_type):
                    return dst_type(value)

            return value
        except Error as err:
            if key is not None:
                err.index.insert(0, key)
            raise

    class InterpolationContext(dict):
        ParametersType = Dict[str, Any]

        def __init__(self, parameters: ParametersType, **kwargs):
            super().__init__(kwargs)
            self.parameters = parameters

    class InterpolationSettings:
        def __init__(
            self,
            other: "Settings.InterpolationSettings" = None,
            /,
            *,
            interpolate=True,
        ):
            if other is not None:
                interpolate = other.interpolate
            self.interpolate = interpolate

        def __and__(self, other):
            return type(self)(
                interpolate=self.interpolate and other.interpolate,
            )

        def __eq__(self, other):
            return self.interpolate == other.interpolate

        def __repr__(self):
            params = ", ".join(
                f"{key}={repr(getattr(self, key))}" for key in ("interpolate",)
            )
            return f"{type(self).__name__}({params})"

    def interpolate(self, *args, **kwargs) -> InterpolatedType:
        return dict(self.interpolated_items(*args, **kwargs))

    @classmethod
    def interpolate_dict(
        cls,
        source: Dict[str, Any],
        *args,
        **kwargs,
    ) -> InterpolatedType:
        return dict(cls.interpolated_items_from_dict(source, *args, **kwargs))

    def interpolated_items(
        self,
        context: InterpolationContext,
        settings: Optional[InterpolationSettings] = None,
    ) -> Iterable[InterpolatedItemsType]:
        yield from self.interpolated_items_from_dict(
            source=self,
            context=context,
            settings=settings,
        )

    @classmethod
    def interpolated_items_from_dict(
        cls,
        source: Dict[str, Any],
        context: InterpolationContext,
        settings: Optional[InterpolationSettings] = None,
    ) -> Iterable[InterpolatedItemsType]:
        if settings is None:
            settings = cls.InterpolationSettings()

        for key, value in source.items():
            if key[:1] == "_":
                continue

            try:
                yield key, cls.__interpolate_value(
                    value=value,
                    context=context,
                    settings=settings,
                )
            except PatternError as err:
                err.index.insert(0, key)
                raise err from None

    @classmethod
    def __interpolate_value(cls, value, context, settings):
        if value is None:
            return value

        if isinstance(value, dict):
            try:
                value_settings = settings and value["_interpolation_settings"]
            except KeyError:
                value_settings = settings

            try:
                interpolate = value.interpolate
                extra_kwargs = dict()
            except AttributeError:
                interpolate = cls.interpolate_dict
                extra_kwargs = dict(source=value)
            result = interpolate(
                **extra_kwargs, context=context, settings=value_settings
            )
            if value_settings.interpolate:
                return result.get(Processors.KEY, result)
            return result

        if isinstance(value, Iterable) and not isinstance(value, str):
            result = type(value)(
                cls.__interpolate_value(
                    value=item,
                    context=context,
                    settings=settings,
                )
                for item in value
            )
            if settings.interpolate and isinstance(value, Processors):
                return result.process(context.parameters)
            return result

        if not settings.interpolate:
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
                    f" key {missing_key!r} is not defined."
                    f" Valid keys: {list(parameters.keys())}"
                ) from None
        except Error as err:
            raise type(err)(f"Pattern {value!r}:" f" {err}") from None


class ProcessorMeta(type(abc.ABC)):
    """
    Simple metaclass for automatic recording of processors in
    :attr:`Processor.MAP`
    """

    __BASE_CLASS = None

    def __new__(cls, name, bases, kwargs):
        if cls.__BASE_CLASS is None:
            cls.__BASE_CLASS = super().__new__(cls, name, bases, kwargs)
            return cls.__BASE_CLASS

        key = kwargs["KEY"]

        result_fields = kwargs.setdefault("FIELDS", set())
        result_optional_fields = kwargs.setdefault("OPTIONAL_FIELDS", set())
        result_fields |= cls.__BASE_CLASS._COMMON_FIELDS
        result_optional_fields |= cls.__BASE_CLASS._COMMON_OPTIONAL_FIELDS

        if kwargs.get("INPUT_VALUE_REQUIRED", Processor.INPUT_VALUE_REQUIRED):
            result_optional_fields |= {Processor._INPUT_KEY_FIELD}

        fields = result_fields | result_optional_fields
        for field in fields:
            try:
                processor_key_set = cls.__BASE_CLASS._FIELD_TO_PROCESSOR_KEYS[
                    field
                ]
            except KeyError:
                processor_key_set = set()
                cls.__BASE_CLASS._FIELD_TO_PROCESSOR_KEYS[
                    field
                ] = processor_key_set
            processor_key_set.add(key)

        result = super().__new__(cls, name, bases, kwargs)

        assert key not in cls.__BASE_CLASS._KEY_TO_PROCESSOR
        cls.__BASE_CLASS._KEY_TO_PROCESSOR[key] = result

        return result


class Processor(Settings, metaclass=ProcessorMeta):
    """
    Special type of settings for processing :class:`Settings` values when
    settings are interpolated via :meth:`Settings.interpolate`.

    .. note:
        All string fiels within :class:`Processor` instances undergo a
        preliminary f-string-like interpolation when
        :meth:`Settings.interpolate` is called, **before**
        :meth:`Processor.process` is called.

        This enables injecting configuration values within :class:`Processor`
        settings.

    .. warning:
        Because of the preliminary string interpolation described in previous
        note, care must be take with curly braces '{' and '}'. If they are
        meant as an actual character for the processor's settings, they must be
        escaped into double curly braces '{{' amd '}}', as described at
        `formatted string literal
        <https://docs.python.org/3/reference/lexical_analysis.html\
#formatted-string-literals>`_ .
    """

    _KEY_TO_PROCESSOR = collections.OrderedDict()
    _FIELD_TO_PROCESSOR_KEYS = collections.OrderedDict()

    _INPUT_KEY_FIELD = "input_key"
    _OUTPUT_KEY_FIELD = "output_key"

    _COMMON_FIELDS = set()
    _COMMON_OPTIONAL_FIELDS = {
        _OUTPUT_KEY_FIELD,
    }

    KEY = None

    FIELDS = set()
    OPTIONAL_FIELDS = set()

    INPUT_VALUE_REQUIRED = False
    DEFAULT_OUTPUT_KEY = "_"

    def __init__(
        self,
        other: Optional["Processor"] = None,
        /,
        *,
        input_key: Optional[str] = None,
        output_key: Optional[str] = None,
        **kwargs,
    ):
        if other is not None:
            assert input_key is None
            assert output_key is None
            assert not kwargs
            super().__init__(other)
            return

        for param_name in (self._INPUT_KEY_FIELD, self._OUTPUT_KEY_FIELD):
            value = locals()[param_name]
            if value is None:
                continue
            if not isinstance(value, str):
                raise ConfigurationError(
                    f"Processor {self.KEY}:"
                    f" 'str' expected for {param_name!r}"
                    f" field instead of {type(value).__name__!r}:"
                    f" {value!r}"
                )
            if not value.isidentifier():
                raise ConfigurationError(
                    f"Processor {self.KEY}:"
                    f" invalid {param_name!r} value"
                    f" {value!r}."
                    " A python identifier-compliant name is required."
                )
            kwargs[param_name] = value
        super().__init__(**kwargs)

    @abc.abstractmethod
    def process(
        self,
        parameters_or_value: Union[
            Settings.InterpolationContext.ParametersType, Any
        ],
    ) -> Any:
        pass

    @classmethod
    def create_processor(
        cls,
        configuration: Dict[str, Any],
    ):
        """
        Processor factory that instantiates a new instance of a
        :class:`Processor`'s sub-class, by attempting inferring such target
        :class:`Processor` sub-class from the list of fields specified for the
        processor.
        """

        if not isinstance(configuration, dict):
            raise ConfigurationError(
                "Dictionary expected for a processor"
                f" instead of {type(configuration).__name__!r}"
                f": {configuration!r}"
            )
        if len(configuration) != 1:
            raise ConfigurationError(
                "A dictionary with a single entry expected"
                f" for a processor, where the entry's key identifies"
                " the target processor,"
                f" instead of {len(configuration)} entries"
                f": {configuration!r}"
            )
        processor_key, processor_value = configuration.popitem()

        if not isinstance(processor_key, str):
            raise ConfigurationError(
                "Invalid processor value of type"
                " 'str' instead of"
                f" {type(processor_key).__name__} as expected:"
                f" {processor_key!r}"
            )
        try:
            processor = cls._KEY_TO_PROCESSOR[processor_key]
        except KeyError:
            raise ConfigurationError(
                f"Invalid processor value {processor_key!r}."
                " Valid values:"
                f" {', '.join(map(repr, cls._KEY_TO_PROCESSOR))}"
            ) from None

        if not isinstance(processor_value, dict):
            if len(processor.FIELDS) > 1:
                raise ConfigurationError(
                    f"Processor {processor_key!r}:"
                    f" {len(processor.FIELDS)} mandatory fields expected:"
                    f" {', '.join(map(repr, sorted(processor.FIELDS)))}."
                    f" A dictionary must be used for this processor,"
                    f" instead of {processor_value!r}"
                )
            processor_value = {list(processor.FIELDS)[0]: processor_value}

        valid_fields = processor.FIELDS | processor.OPTIONAL_FIELDS
        wrong_fields = set(processor_value) - valid_fields
        if len(wrong_fields) > 0:
            raise ConfigurationError(
                f"Processor {processor_key!r}:"
                " Invalid field(s)"
                f" {', '.join(map(repr, sorted(wrong_fields)))}."
                " Valid field(s):"
                f" {', '.join(map(repr, sorted(valid_fields)))}."
            )

        missing_fields = processor.FIELDS - set(processor_value)
        if len(missing_fields) > 0:
            raise ConfigurationError(
                f"Processor {processor_key!r}: Missing mandatory fields: "
                f" {', '.join(map(repr, sorted(missing_fields)))}."
            )

        return processor(**processor_value)

    def interpolate(self, *args, **kwargs) -> "Processor":
        return type(self)(self.interpolated_items(*args, **kwargs))


class InterpolateProcessor(Processor):
    """
    Processor for f-string interpolation. This is the default processor, used
    when no ``process`` field is specified.

    The evaluation is based on a :class:`dict` of user-defined parameters.

    The :class:`InterpolateProcessor` can be seen as a simplified sub-set of
    :class:`EnumProcessor`, specifically tailored around string interpolation.
    Nevertheless, it is worth noting that all functionalities of
    :class:`InterpolateProcessor` could also be achieved with
    :class:`EnumProcessor`, albeit with slightly more verbose and/or more
    complex field values.

    .. warning:
        Because of the preliminary string interpolation described for
        :class:`Processor`, this processor actually results in performing two
        interpolations.

        Although this is normally redundant, it is relevant for HTTP response
        processing, where the first interpolation is used to interpolate
        configuration settings in the result, while the second interpolation is
        used to interpolate HTTP response's output data in the result.
    """

    KEY = "interpolate"
    FIELDS = {"string"}

    def __init__(
        self,
        other: Optional["InterpolateProcessor"] = None,
        /,
        *,
        string: Optional[str] = None,
        **kwargs,
    ):
        if other is not None:
            assert string is None
            assert not kwargs
            super().__init__(other)
            return

        if not isinstance(string, str):
            raise ConfigurationError(
                f"Processor {self.KEY}: 'str' expected for 'string'"
                f" field instead of {type(string).__name__!r}:"
                f" {string!r}"
            )
        super().__init__(string=string, **kwargs)

    def process(
        self,
        parameters: Settings.InterpolationContext.ParametersType,
    ) -> Any:
        return self.process_value(self["string"], parameters)

    @classmethod
    def process_value(
        cls,
        value: str,
        parameters: Settings.InterpolationContext.ParametersType,
    ) -> str:
        try:
            return EvalProcessor.process_value(f"f{value!r}", parameters)
        except EvalError as err:
            raise PatternError(err.message, value)

    def interpolate(self, *args, **kwargs) -> "Processor":
        # Prevent double-interpolation, but simply copying the object as-is and
        # deferring the interpolation at processor's execution.
        return type(self)(self)


class EvalProcessor(Processor):
    """
    Processor for evaluating Python expression, basically a
    :class:`Settings`-based wrapper around Python's :func:`eval`.

    The evaluation is based on a :class:`dict` of user-defined parameters, plus
    a few extra features provided by :attr:`EvalProcessor.EXTRA_PARAMETERS`.
    """

    KEY = "eval"
    FIELDS = {"expression"}

    EXTRA_PARAMETERS = {
        "datetime": DateTime,
        "date": Date,
        "time": Time,
        "timedelta": TimeDelta,
    }

    def __init__(
        self,
        other: Optional["EvalProcessor"] = None,
        /,
        *,
        expression: Optional[str] = None,
        **kwargs,
    ):
        if other is not None:
            assert expression is None
            assert not kwargs
            super().__init__(other)
            return

        if not isinstance(expression, str):
            raise ConfigurationError(
                f"Processor {self.KEY}: 'str' expected for 'expression'"
                f" field instead of {type(expression).__name__!r}:"
                f" {expression!r}"
            )
        super().__init__(expression=expression, **kwargs)

    def process(
        self,
        parameters: Settings.InterpolationContext.ParametersType,
    ) -> Any:

        all_parameters = dict(self.EXTRA_PARAMETERS)
        all_parameters.update(parameters)

        return self.process_value(self["expression"], all_parameters)

    @classmethod
    def process_value(
        cls,
        value: str,
        parameters: Settings.InterpolationContext.ParametersType,
    ) -> str:
        if not isinstance(value, str):
            raise DataTypeError(
                f"Processor {cls.KEY!r} expects 'str' input values"
                f" instead of {type(value).__name__!r}: {value!r}"
            )

        try:
            try:
                return eval(value, {}, parameters)
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
                raise EvalError(str(err), expression=value) from None
        except KeyError as err:
            try:
                missing_key = err.args[0]
            except IndexError:
                raise EvalError(str(err), expression=value) from None
            raise EvalError(
                f"Key {missing_key!r} is not defined."
                f" Valid keys: {list(parameters.keys())}",
                expression=value,
            ) from None


class TypeProcessor(Processor):
    """
    Processor to convert an input value into a desired type.

    The evaluation is based on a :class:`dict` of user-defined parameters, plus
    a few extra features provided by :attr:`EvalProcessor.EXTRA_PARAMETERS`.
    """

    KEY = "type"
    FIELDS = {"target"}
    OPTIONAL_FIELDS = {"radix"}
    INPUT_VALUE_REQUIRED = True

    class Converter:
        def __init__(
            self,
            convert_func: Callable,
            name: Optional[str] = None,
            with_radix: bool = False,
        ):
            self.convert_func = convert_func
            self.name = name if name is not None else convert_func.__name__
            self.with_radix = with_radix

        def __call__(self, value: Any) -> Any:
            return self.convert_func(value)

        def __str__(self):
            return self.name

        def __repr__(self):
            return f"{type(self).__name__}[{self.name}]"

    CONVERTERS = collections.OrderedDict(
        tuple(
            (converter.name, converter)
            for converter in (
                Converter(str),
                Converter(int, with_radix=True),
                Converter(float),
                Converter(bool),
                Converter(DateTime, "datetime"),
                Converter(Date, "date"),
                Converter(Time, "time"),
                Converter(TimeDelta, "timedelta"),
            )
        )
    )

    def __init__(
        self,
        other: Optional["TypeProcessor"] = None,
        /,
        *,
        target: Optional[str] = None,
        radix: Optional[int] = None,
        **kwargs,
    ):
        if other is not None:
            assert target is None
            assert radix is None
            assert not kwargs
            super().__init__(other)
            return

        if not isinstance(target, str):
            raise ConfigurationError(
                f"Processor {self.KEY}: 'str' expected for 'target'"
                f" field instead of {type(target).__name__!r}:"
                f" {target!r}"
            )

        target_comps = target.split(":", maxsplit=1)
        if len(target_comps) == 1:
            radix_from_target = False
        else:
            if radix is not None:
                raise ConfigurationError(
                    "Radix cannot be specified both"
                    f" in field 'radix' ({radix!r})"
                    f" and within target after a colon ':' ({target!r})"
                )
            radix_from_target = True
            target, radix = target_comps

        try:
            converter = self.CONVERTERS[target]
        except KeyError:
            valid_types = ", ".join(map(repr, self.CONVERTERS))
            raise ConfigurationError(
                f"Processor {self.KEY}:"
                f" invalid target type {target!r}."
                f" Valid types: {valid_types}"
            ) from None

        if radix is not None:
            if not converter.with_radix:
                raise ConfigurationError(
                    f"Processor {self.KEY}: target type {target!r}"
                    f" does not support the 'radix' field"
                )
            if radix_from_target:
                try:
                    radix = int(radix)
                except ValueError:
                    pass
            if not isinstance(radix, int):
                raise ConfigurationError(
                    f"Processor {self.KEY}: 'int' expected for 'radix'"
                    f" field instead of {type(radix).__name__!r}:"
                    f" {radix!r}"
                )
            orig_convert_func = converter.convert_func
            converter = self.Converter(
                convert_func=lambda value: orig_convert_func(value, radix),
                name=converter.name + f":{radix}",
                with_radix=False,
            )

        super().__init__(converter=converter, **kwargs)

    def process(
        self,
        value: Any,
    ) -> Any:
        converter = self["converter"]
        try:
            return self["converter"](value)
        except Exception as err:
            raise DataTypeError(
                f"Processor {self.KEY}:"
                f" Unable to convert {value!r}"
                f" of type {type(value).__name__!r}"
                f" into a {converter.name!r}: {err}"
            ) from None


class RegexProcessor(Processor):
    """
    Processor to transform a string via regular expression-based text
    substitutions.

    This regular expression support is based on Python's own
    `re module <https://docs.python.org/3/library/re.html>`, in particular on
    `re.sub() <https://docs.python.org/3/library/re.html#re.sub>` function.
    Refer to that function's manual for usage instructions.
    """

    KEY = "regex"
    FIELDS = {"pattern", "replacement"}
    OPTIONAL_FIELDS = {"flags"}
    INPUT_VALUE_REQUIRED = True

    def __init__(
        self,
        other: Optional["RegexProcessor"] = None,
        /,
        *,
        pattern: Optional[str] = None,
        replacement: Optional[str] = None,
        flags: Optional[Union[str, Iterable[str], int]] = None,
        **kwargs,
    ):
        if other is not None:
            assert pattern is None
            assert replacement is None
            assert flags is None
            assert not kwargs
            super().__init__(other)
            return

        for param_name in self.FIELDS:
            value = locals()[param_name]
            if not isinstance(value, str):
                raise ConfigurationError(
                    f"Processor {self.KEY}: 'str' expected for {param_name!r}"
                    f" field instead of {type(value).__name__!r}:"
                    f" {value!r}"
                )

        if flags is None:
            flags = []
        elif isinstance(flags, str):
            flags = [flags]
        elif isinstance(flags, collections.abc.Iterable):
            flags = list(flags)
        else:
            raise ConfigurationError(
                f"Processor {self.KEY}:"
                f" invalid type {type(flags).__name__}"
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
                    f"Processor {self.KEY}:"
                    f" invalid regular expression's flag {flag!r}"
                ) from None
            if real_flag not in valid_flags:
                supported_flags = ", ".join(
                    map(lambda flag: flag.name, valid_flags)
                )
                raise ConfigurationError(
                    f"Processor {self.KEY}:"
                    f" unsupported regular expression's flag {flag!r}."
                    f" Supported flags: {supported_flags}"
                )
            flags_value |= int(real_flag)

        super().__init__(
            pattern=pattern,
            replacement=replacement,
            flags=flags_value,
            **kwargs,
        )

    def process(
        self,
        value: Any,
    ) -> Any:
        pattern = self["pattern"]
        replacement = self["replacement"]
        flags = self["flags"]

        try:
            regex = re.compile(pattern, flags=flags)
        except re.error as err:
            raise ConfigurationError(
                f"Processor {self.KEY}:"
                f" invalid regular expression's pattern {pattern!r}:"
                f" {err}"
            ) from None

        try:
            return regex.sub(replacement, str(value))
        except Exception as err:
            raise PatternError(
                f"Processor {self.KEY}:"
                f" failed processing {value!r}"
                f" with regular expression {pattern!r}"
                f" and replacement string {replacement!r}:"
                f" {err}"
            ) from None


class Processors(list):
    """
    THE list of :class:`Processor`s optionally attached to a leaf value.

    This class is responsible of configuration parsing and corresponding
    instantiation of specific classes derived from :class:`Processor`, and of
    the processor execution.
    """

    # A reasonably unique key, yet a valid yaml identifier
    KEY = "<process"

    def __init__(
        self,
        other: Optional["Processors"] = None,
        /,
        *,
        configuration: Optional[
            Union[Settings.InputType, Iterable[Settings.InputType]]
        ] = None,
    ):

        if other is not None:
            assert configuration is None
            super().__init__(other)
            return

        if isinstance(configuration, dict):
            configuration = (configuration,)
        elif not isinstance(configuration, Iterable):
            raise ConfigurationError(
                "Dictionary or list of dictionaries expected"
                f" for {self.KEY} field,"
                f" instead of {type(configuration).__name__!r}"
                f": {configuration!r}"
            )
        if len(configuration) == 0:
            raise ConfigurationError(
                "Non-empty list of dictionaries expected"
                f" for {self.KEY} field"
            )
        super().__init__(map(Processor.create_processor, configuration))

        processor = self[0]
        if (
            processor.INPUT_VALUE_REQUIRED
            and Processor._INPUT_KEY_FIELD not in processor
        ):
            raise ConfigurationError(
                f"When a processor of type {processor.KEY!r} is used as"
                f" first processor, it requires an explicit input value"
                f" key, but field {Processor._INPUT_KEY_FIELD!r} was not"
                " specified"
            )
        processor = self[-1]
        if Processor._OUTPUT_KEY_FIELD in processor:
            raise ConfigurationError(
                "The last processor cannot specify an output value key"
                ", i.e. it cannot contain"
                f" field {Processor._OUTPUT_KEY_FIELD!r}"
            )

    def process(
        self, parameters: Settings.InterpolationContext.ParametersType
    ) -> Any:
        value_key = None
        parameters = dict(parameters)
        for processor in self:
            if processor.INPUT_VALUE_REQUIRED:
                value_key = processor.get(
                    Processor._INPUT_KEY_FIELD, value_key
                )
                try:
                    input = parameters[value_key]
                except KeyError:
                    raise DataValueError(
                        f"Processor {processor.KEY!r}:"
                        f" input value key {value_key!r} is not defined."
                        f" Valid keys: {list(parameters.keys())}",
                    ) from None
            else:
                input = parameters
            output = processor.process(input)
            value_key = processor.get(
                Processor._OUTPUT_KEY_FIELD, Processor.DEFAULT_OUTPUT_KEY
            )
            parameters[value_key] = output
        return output
