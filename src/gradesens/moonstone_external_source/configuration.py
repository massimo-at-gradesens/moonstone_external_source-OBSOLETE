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

    def __init__(
        self,
        other: Union[
            InputType,
            Iterable[InputItemType],
            None,
        ] = None,
        **kwargs: InputType,
    ):
        if other is None:
            init_dict = kwargs
        else:
            assert not kwargs
            init_dict = dict(other)

        init_dict = self.__normalize_values(init_dict, force_copy=True)
        super().__init__(**init_dict)

    def __setitem__(self, key: KeyType, value: InputValueType):
        super().__setitem__(self, key, self.__normalize_value(value))

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
        :meth:`dict.update` counterpart, because it is applied hierarchically
        to values which are themselves of type :class:`Settings`.
        I.e., if both ``self[key]`` and ``other[key]`` exist for the same key,
        and their respective values are both of type :class:`Settings` (or of
        type :class:`dict`, which get automatically converted to
        :class:`Settings`), then the equivalent of
        ``self[key].update(other[key])`` is applied, instead of plainly
        replacing the target value as would be done by
        ``self[key] = other[key]``.
        """
        other = self.__normalize_value(other)
        for key, value in other.items():
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

    class InterpolatedSettings:
        """
        A proxy to provide simplified :class:`dict`-like access to interpolated
        settings, i.e. settings where patterns have been interpolated through
        :meth:`str.format`.
        """

        InterpolationParametersType = Dict[str, Any]

        def __init__(
            self,
            settings: "Settings",
            parameters: InterpolationParametersType,
        ):
            self.settings = settings
            self.parameters = parameters

        def __len__(self):
            return len(self.settings)

        def __iter__(self):
            return iter(self.settings)

        def __getitem__(self, key: "Settings.KeyType") -> "Settings.ValueType":
            return self.__interpolate_setting(self.settings[key])

        def get(
            self,
            key: "Settings.KeyType",
            default: "Settings.ValueType" = None,
        ) -> "Settings.ValueType":
            return self.__interpolate_setting(self.settings.get(key, default))

        def items(self) -> Iterable["Settings.ItemType"]:
            for setting, value in self.settings.items():
                yield setting, self.__interpolate_setting_value(value)

        def __interpolate_setting(self, value):
            if value is None:
                return None
            if isinstance(value, Settings):
                return value.apply(self.parameters)
            return value.format(**self.parameters)

    def apply(
        self,
        parameters: InterpolatedSettings.InterpolationParametersType,
    ) -> InterpolatedSettings:
        return self.InterpolatedSettings(self, parameters)


class AuthenticationContext(Settings, abc.ABC):
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

    @classmethod
    @abc.abstractmethod
    async def retrieve(cls, identifier: Identifier) -> "AuthenticationContext":
        """
        Retrieve the :class:`AuthenticationContext` for a given ID.
        To be implemented by derived classes.
        """
        pass


class CommonConfiguration(Settings, abc.ABC):
    """
    An :class:`CommonConfiguration` is nothing more than a ``[key, value]``
    dictionary of configuration data, optionally referenced by
    :class:`MeasurementConfiguration` and :class:`MachineConfiguration` objects
    to retrieve configuration data from a common shared configuration point.

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

    @classmethod
    @abc.abstractmethod
    async def retrieve(cls, identifier: Identifier) -> "CommonConfiguration":
        """
        Retrieve the :class:`CommonConfiguration` for a given ID.
        To be implemented by derived classes.
        """
        pass


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
        common_configuration = await CommonConfiguration.retrieve(
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


class MachineConfiguration(_MeasurementConfigurationSettings, abc.ABC):
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

    @classmethod
    @abc.abstractmethod
    async def retrieve(cls, identifier: Identifier) -> "MachineConfiguration":
        """
        Retrieve the :class:`MachineConfiguration` for a given ID.
        To be implemented by derived classes.
        """
        pass

    class __ResolutionContext:
        def __init__(self, parent: "MachineConfiguration"):
            self.parent = parent

            # simple caches to avoid attempting retrieving multiple times the
            # same CommonConfiguration's and/or AuthenticationContext's
            self.common_configurations = {}
            self.authentication_contexts = {}

        async def get_measurement_settings(
            self, identifier: MeasurementConfiguration.Identifier
        ) -> Settings.InterpolatedSettings:
            settings = Settings(self.parent)

            measurement_configuration = self.parent[
                "measurement_configurations"
            ][identifier]
            settings.update(measurement_configuration)

            try:
                common_configuration_identifier = self.common_configurations[
                    "common_configuration_identifier"
                ]
            except KeyError:
                pass
            else:
                try:
                    common_configuration = self.common_configurations[
                        common_configuration_identifier
                    ]
                except KeyError:
                    common_configuration = await CommonConfiguration.retrieve(
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
            try:
                authentication_context = self.authentication_contexts[
                    authentication_context_identifier
                ]
            except KeyError:
                authentication_context = await AuthenticationContext.retrieve(
                    authentication_context_identifier
                )
                self.authentication_contexts[
                    authentication_context_identifier
                ] = authentication_context

            if authentication_context is not None:
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
            result = settings.apply(settings)
            return result

    async def get_measurement_settings(
        self, identifier: MeasurementConfiguration.Identifier
    ) -> Settings.InterpolatedSettings:
        resolution_context = self.__ResolutionContext()
        return await resolution_context.get_measurement_configuration(
            identifier=identifier
        )

    async def get_all_measurement_settings(
        self,
    ) -> Settings.InterpolatedSettings:
        resolution_context = self.__ResolutionContext()
        tasks = [
            resolution_context.get_measurement_settings(
                identifier=measurement_configuration["identifier"]
            )
            for measurement_configuration in self["measurement_configurations"]
        ]
        results = await asyncio.gather(tasks)
        return {
            measurement_configuration.identifier: result
            for measurement_configuration, result in zip(
                self["measurement_configurations"], results
            )
        }
