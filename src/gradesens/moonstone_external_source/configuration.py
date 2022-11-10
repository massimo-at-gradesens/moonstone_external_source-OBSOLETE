"""
GradeSens - External Source package - machine configuration

A machine configuration contains all the parameters requested to query the
external measurements on that machine.
This file provides both the configuration data classes and the facilities to
retrieve the configuration for a given machine.
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, Gradesens AG"


import abc
import asyncio
from typing import Any, Dict, Iterable, Sequence, Tuple, Union

from .error import Error, PatternError


class Settings(dict):
    """
    A :class:`dict` specialization containing a list of ``[key, value]``
    entries, where a ``value`` may either be a plain :class:`str` or a
    :meth:`str.format`-ready pattern to be interpolated with :meth:`.apply`.

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
                    return value.format(**parameters)
                except NameError as err:
                    raise KeyError(err.name) from None
                except ValueError as err:
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

    class LoadDriver(abc.ABC):
        """
        The load driver for :class:`AuthenticationContext`, which provides the
        actual load functionality for :meth:`AuthenticationContext.load` to
        work.

        In order to call :meth:`AuthenticationContext.load`, an instance of a
        class derived by this abstract class must first be registered with
        :meth:`AuthenticationContext.register_load_driver`.
        """

        @abc.abstractmethod
        async def load(
            self, identifier: "AuthenticationContext.Identifier"
        ) -> "AuthenticationContext":
            """
            The actual load method, to be implemented by derived classes.
            """

    __load_driver: Union[LoadDriver, None] = None

    @classmethod
    def register_load_driver(cls, load_driver: LoadDriver):
        cls.__load_driver = load_driver

    @classmethod
    async def load(cls, identifier: Identifier) -> "AuthenticationContext":
        """
        Load the :class:`AuthenticationContext` for a given ID.
        To be implemented by derived classes.
        """
        if cls.__load_driver is None:
            raise Error(f"{cls.__name__}: no LoadDriver registered")
        return await cls.__load_driver.load(identifier)


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

    class LoadDriver(abc.ABC):
        """
        The load driver for :class:`CommonConfiguration`, which provides the
        actual load functionality for :meth:`CommonConfiguration.load` to
        work.

        In order to call :meth:`CommonConfiguration.load`, an instance of a
        class derived by this abstract class must first be registered with
        :meth:`CommonConfiguration.register_load_driver`.
        """

        @abc.abstractmethod
        async def load(
            self, identifier: "CommonConfiguration.Identifier"
        ) -> "CommonConfiguration":
            """
            The actual load method, to be implemented by derived classes.
            """

    __load_driver: Union[LoadDriver, None] = None

    @classmethod
    def register_load_driver(cls, load_driver: LoadDriver):
        cls.__load_driver = load_driver

    @classmethod
    async def load(cls, identifier: Identifier) -> "CommonConfiguration":
        """
        Load the :class:`CommonConfiguration` for a given ID.
        To be implemented by derived classes.
        """
        if cls.__load_driver is None:
            raise Error(f"{cls.__name__}: no LoadDriver registered")
        return await cls.__load_driver.load(identifier)


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
        _valid_kwarg_keys: Union[Iterable[str], None] = None,
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
            if _valid_kwarg_keys is not None:
                _valid_kwarg_keys = set(_valid_kwarg_keys)
                for key in kwargs.keys():
                    if key not in _valid_kwarg_keys:
                        raise TypeError(
                            "__init__() got an unexpected keyword argument"
                            f" {key!r}"
                        )
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
                _valid_kwarg_keys=("identifier",),
                **kwargs,
            )
        else:
            assert identifier is None
            assert not kwargs
            super().__init__(other)


class MachineConfiguration(_MeasurementConfigurationSettings):
    """
    Configuration for one machine, containing a machine-specific number of
    :class:`MeasurementConfiguration`s.
    """

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

    class LoadDriver(abc.ABC):
        """
        The load driver for :class:`MachineConfiguration`, which provides the
        actual load functionality for :meth:`MachineConfiguration.load` to
        work.

        In order to call :meth:`MachineConfiguration.load`, an instance of a
        class derived by this abstract class must first be registered with
        :meth:`MachineConfiguration.register_load_driver`.
        """

        @abc.abstractmethod
        async def load(
            self, identifier: "MachineConfiguration.Identifier"
        ) -> "MachineConfiguration":
            """
            The actual load method, to be implemented by derived classes.
            """

    __load_driver: Union[LoadDriver, None] = None

    @classmethod
    def register_load_driver(cls, load_driver: LoadDriver):
        cls.__load_driver = load_driver

    @classmethod
    async def load(cls, identifier: Identifier) -> "MachineConfiguration":
        """
        Load the :class:`MachineConfiguration` for a given ID.
        To be implemented by derived classes.
        """
        if cls.__load_driver is None:
            raise Error(f"{cls.__name__}: no LoadDriver registered")
        return await cls.__load_driver.load(identifier)

    class __ResolutionContext:
        # Build the list of actual settings to be retains after resolution
        __RESULT_KEYS = set(
            filter(
                lambda key: not key.endswith("_identifier"),
                _MeasurementConfigurationSettings().keys(),
            )
        )

        def __init__(self, parent: "MachineConfiguration"):
            self.parent = parent

            # simple caches to avoid attempting retrieving multiple times the
            # same CommonConfiguration's and/or AuthenticationContext's
            self.common_configurations = {}
            self.authentication_contexts = {}

        async def get_measurement_settings(
            self, identifier: MeasurementConfiguration.Identifier
        ) -> Settings.InterpolatedValueType:
            settings = Settings(self.parent)

            measurement = self.parent["measurements"][identifier]
            settings.update(measurement)

            common_configuration_identifier = measurement[
                "common_configuration_identifier"
            ]
            if common_configuration_identifier is None:
                common_configuration_identifier = self.parent[
                    "common_configuration_identifier"
                ]

            if common_configuration_identifier is not None:
                try:
                    common_configuration = self.common_configurations[
                        common_configuration_identifier
                    ]
                except KeyError:
                    common_configuration = None
                if common_configuration is None:
                    common_configuration = await CommonConfiguration.load(
                        common_configuration_identifier
                    )
                    self.common_configurations[
                        common_configuration_identifier
                    ] = common_configuration
                new_settings = Settings(common_configuration)
                new_settings.update(settings)
                settings = new_settings

            authentication_context_identifier = settings[
                "authentication_context_identifier"
            ]
            if authentication_context_identifier is not None:
                try:
                    authentication_context = self.authentication_contexts[
                        authentication_context_identifier
                    ]
                except KeyError:
                    authentication_context = None
                if authentication_context is None:
                    authentication_context = await AuthenticationContext.load(
                        authentication_context_identifier
                    )
                    self.authentication_contexts[
                        authentication_context_identifier
                    ] = authentication_context

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
                "machine": self.parent["identifier"],
                "measurement": measurement["identifier"],
            }
            parameters.update(
                {
                    key: value
                    for key, value in settings.items()
                    if not isinstance(value, Settings)
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

    async def get_measurement_settings(
        self, identifier: MeasurementConfiguration.Identifier
    ) -> Settings.InterpolatedValueType:
        resolution_context = self.__ResolutionContext(self)
        return await resolution_context.get_measurement_settings(
            identifier=identifier
        )

    async def get_all_measurement_settings(
        self,
    ) -> Dict[str, Settings.InterpolatedValueType]:
        resolution_context = self.__ResolutionContext(self)
        measurements = list(self["measurements"].values())
        tasks = [
            resolution_context.get_measurement_settings(
                identifier=measurement["identifier"]
            )
            for measurement in measurements
        ]
        results = await asyncio.gather(tasks)
        return {
            measurement.identifier: result
            for measurement, result in zip(measurements, results)
        }
