"""
GradeSens - External Source package - Configuration support

This file provides the configuration data classes to handle machine and
maeasurement configurations.
These configurations contain all the parameters requested to query the
external measurements on the target machines.
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, GradeSens AG"


import asyncio
import itertools
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Union

if TYPE_CHECKING:
    from .io_manager import IOManager

from .authorization_configuration import AuthorizationConfiguration
from .configuration_references import (
    ConfigurationReferences,
    ConfigurationReferenceTarget,
)
from .datetime import TimeDelta
from .error import ConfigurationError, Error, TimeError
from .http_settings import (
    HTTPRequestSettings,
    HTTPResultSettings,
    HTTPTransactionSettings,
)
from .settings import Settings
from .utils import find_nearest, iter_sub_ranges


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
        authorization_configuration_id: Optional[
            AuthorizationConfiguration.Id
        ] = None,
        start_time: Optional[TimeDelta.InputType] = None,
        end_time: Optional[TimeDelta.InputType] = None,
        time_margin: Optional[TimeDelta.InputType] = None,
        start_time_margin: Optional[TimeDelta.InputType] = None,
        end_time_margin: Optional[TimeDelta.InputType] = None,
        merged_request_window: Optional[TimeDelta.InputType] = None,
        **kwargs: Settings.InputType,
    ):
        if other is not None:
            assert authorization_configuration_id is None
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

        time_delta_fields = {}
        for key in (
            "time_margin",
            "start_time_margin",
            "end_time_margin",
            "merged_request_window",
        ):
            value = locals()[key]
            if value is None:
                continue
            try:
                parsed_value = TimeDelta(value)
                if parsed_value.total_seconds() < 0:
                    raise ValueError(
                        f"Field {key!r} cannot be negative: {value!r}"
                    )
            except ValueError as err:
                raise ConfigurationError(
                    f"{err}."
                    " A literal value is expected, and string interpolation"
                    " is not supported for this field",
                    index=key,
                ) from None
            time_delta_fields[key] = parsed_value

        try:
            time_margin_value = time_delta_fields["time_margin"]
        except KeyError:
            pass
        else:
            for key in (
                "start_time_margin",
                "end_time_margin",
            ):
                value = time_delta_fields.get(key)
                if value is None:
                    time_delta_fields[key] = time_margin_value

        for key in (
            "start_time_margin",
            "end_time_margin",
            "merged_request_window",
        ):
            value = time_delta_fields.get(key)
            if value is not None:
                kwargs[key] = value

        if "_authorization_configuration_id" in kwargs:
            assert authorization_configuration_id is None
        else:
            kwargs[
                "_authorization_configuration_id"
            ] = authorization_configuration_id
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
    ConfigurationReferences,
    request_type=_MeasurementRequestSettings,
    result_type=_MeasurementResultSettings,
):
    """
    Configuration settings for a single measurement.

    These configuration settings are used by
    :class:`MeasurementConfiguration`s and :class:`MachineConfiguration`s.
    """

    def __init__(
        self,
        other: Optional["_MeasurementSettings"] = None,
        /,
        machine_configuration_ids: Optional[
            Union[
                Iterable["_MeasurementSettings.Id"],
                "_MeasurementSettings.Id",
            ]
        ] = None,
        **kwargs: Settings.InputType,
    ):
        if other is not None:
            assert machine_configuration_ids is None
            assert not kwargs
            super().__init__(other)
            return

        super().__init__(
            machine_configuration_ids=machine_configuration_ids,
            _configuration_ids_field="machine_configuration_ids",
            _configuration_ids_get=(
                lambda client_session, configuration_id: (
                    client_session.machine_configurations.get(configuration_id)
                )
            ),
            **kwargs,
        )


class MeasurementConfiguration(_MeasurementSettings):
    """
    Configuration for a single measurement (e.g. temperature, RPM, etc.).
    """

    Id = str
    InterpolatedSettings = HTTPTransactionSettings.InterpolatedSettings
    SettingsType = InterpolatedSettings

    def __init__(
        self,
        other: Optional["MeasurementConfiguration"] = None,
        /,
        *,
        id: Optional[Id] = None,
        _partial: Optional[bool] = None,
        **kwargs: Settings.InputType,
    ):
        if other is not None:
            assert id is None
            assert not kwargs
            super().__init__(other)
            return

        if id is None and not _partial:
            raise ConfigurationError(
                "Missing measurement configuration's 'id'"
            )
        super().__init__(
            id=id,
            _partial=_partial,
            **kwargs,
        )

    # Use a blank instance of _MeasurementSettings to infer the list of
    # interpolation result keys.
    __INTERPOLATION_KEYS = set(_MeasurementSettings(id="dummy").public_keys())

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
        **kwargs,
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
        authorization_configuration_id = request_settings[
            "_authorization_configuration_id"
        ]
        if authorization_configuration_id is not None:
            authorization_context = (
                await client_session.authorization_contexts.get(
                    authorization_configuration_id
                )
            )
            try:
                authorization_settings = request_settings["authorization"]
            except KeyError:
                authorization_settings = authorization_context
            else:
                authorization_context.update(
                    Settings._RawInit(authorization_settings)
                )
            request_settings["authorization"] = authorization_context

        assert isinstance(settings, type(self))
        return settings

    def get_interpolation_parameters(
        self,
        settings: "Settings",
        machine_configuration: "MachineConfiguration",
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        **kwargs,
    ) -> Dict[str, "Settings.ValueType"]:
        parameters = {
            "machine_id": machine_configuration["id"],
            "measurement_id": self["id"],
        }

        if start_time is not None:
            self.__assert_aware_time("start_time", start_time)
            start_time_margin = settings["request"].get(
                "start_time_margin", TimeDelta(0)
            )
            settings["request"]["start_time"] = start_time - start_time_margin
        if end_time is not None:
            self.__assert_aware_time("end_time", end_time)
            end_time_margin = settings["request"].get(
                "end_time_margin", TimeDelta(0)
            )
            settings["request"]["end_time"] = end_time + end_time_margin

        parameters.update(super().get_interpolation_parameters(settings))
        return parameters

    @staticmethod
    def __assert_aware_time(field_name, time):
        if time.tzinfo is None or time.tzinfo.utcoffset(time) is None:
            raise TimeError(f"{field_name!r} is not timezone-aware: {time!r}")


class MachineConfiguration(
    _MeasurementSettings,
    ConfigurationReferenceTarget,
):
    """
    Configuration for one machine, containing a machine-specific number of
    :class:`MeasurementConfiguration`s.

    A :class:`MachineConfiguration` instance may refer to other
    :class:`MachineConfiguration` instances through the
    ``machine_configuration_ids`` constructor parameter.
    See :meth:`.get_aggregated_settings` method for details about how the
    settings from the different :class:`MachineConfiguration` instances,
    including this one, are merged together.

    The actual contents, including the list of keys, are strictly customer-
    and API-specific, and are not under the responsibility of this class.
    """

    Id = str

    SettingsType = Dict[str, MeasurementConfiguration.SettingsType]

    def __init__(
        self,
        other: Optional[MeasurementConfiguration] = None,
        /,
        *,
        id: Optional[Id] = None,
        measurements: Optional[Settings.InputValueType] = None,
        _partial: Optional[bool] = None,
        **kwargs,
    ):
        if other is not None:
            assert id is None
            assert measurements is None
            assert _partial is None
            assert not kwargs
            super().__init__(other)
            return

        if id is None and not _partial:
            raise ConfigurationError("Missing machine configuration's 'id'")

        if measurements is None:
            measurements = {}
        elif not isinstance(measurements, dict):
            raise ConfigurationError(
                "'dict' expected for 'measurements' field,"
                f" instead of {type(measurements).__name__!r}:"
                f" {measurements!r}"
            )
            # this is coming from some "clone" copy operation
        measurement_dict = {}
        for measurement_id, measurement in measurements.items():
            if measurement is None:
                measurement = {}
            try:
                try:
                    measurement_id2 = measurement["id"]
                    if measurement_id2 is None:
                        raise KeyError
                except KeyError:
                    measurement["id"] = measurement_id
                else:
                    if measurement_id != measurement_id2:
                        raise ConfigurationError(
                            "Mismatching measurement IDs:"
                            f" {measurement_id!r} from key vs"
                            f" {measurement_id2!r} from value"
                        )
                measurement_dict[measurement_id] = MeasurementConfiguration(
                    **measurement
                )
            except Error as err:
                err.index = ["measurements", measurement_id] + err.index
                raise

        super().__init__(
            id=id,
            measurements=measurement_dict,
            _partial=_partial,
            **kwargs,
        )

    async def get_interpolated_settings(
        self,
        client_session: "IOManager.ClientSession",
        **kwargs,
    ) -> "MachineConfiguration.SettingsType":
        if self.get("_machine_configuration_ids"):
            settings = await self.get_aggregated_settings(
                client_session=client_session
            )
            machine_configuration = MachineConfiguration(settings)
            machine_configuration.pop("_machine_configuration_ids", None)
            result = await machine_configuration.get_interpolated_settings(
                client_session=client_session, **kwargs
            )
            return result

        measurements = list(self["measurements"].values())
        if len(measurements) == 0:
            raise ConfigurationError(
                f"Machine {self.id!r}: No measurements specified"
            )

        tasks = [
            self._get_measurement_interpolated_settings(
                client_session=client_session,
                measurement_configuration=measurement,
                **kwargs,
            )
            for measurement in measurements
        ]

        results = await asyncio.gather(*tasks)
        return self.InterpolatedSettings(
            **{
                measurement.id: result
                for measurement, result in zip(measurements, results)
            }
        )

    async def _get_measurement_interpolated_settings(
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

    class __MeasurementFetchResult:
        class RequestWindow:
            def __init__(self):
                self.count = None
                self.start_time = None
                self.end_time = None
                self.measurement_settings = None
                self.task = None

            @classmethod
            async def create(
                cls,
                measurement_id: MeasurementConfiguration.Id,
                timestamps: Iterable[datetime],
                machine_configuration: "MachineConfiguration",
                measurement_aggregated_settings: Settings.InterpolatedType,
                client_session: "IOManager.ClientSession",
            ):
                result = cls()
                result.count = len(timestamps)
                result.start_time = timestamps[0]
                result.end_time = timestamps[-1]

                measurement_configuration = machine_configuration[
                    "measurements"
                ][measurement_id]
                result.measurement_settings = await (
                    machine_configuration
                )._get_measurement_interpolated_settings(
                    client_session=client_session,
                    measurement_configuration=measurement_configuration,
                    aggregated_settings=measurement_aggregated_settings,
                    start_time=result.start_time,
                    end_time=result.end_time,
                )
                return result

            def create_task(self, client_session: "IOManager.ClientSession"):
                assert self.task is None
                self.task = client_session.task_scheduler(
                    self.measurement_settings.fetch_result(
                        client_session=client_session,
                    )
                )

            def __lt__(self, other):
                return self.start_time < other.start_time

            def __str__(self):
                return (
                    f"count={self.count},"
                    f" [{self.start_time}..{self.end_time}]"
                )

        def __init__(self):
            self.measurement_id = None
            self.merged_request_window = None
            self.request_windows = None
            self.request_window_iterator = None
            self.current_window_results = None
            self.current_window_count = 0
            self.last_result_pos = 0

            # for debugging purposes
            self.processed_count = 0

        @classmethod
        async def create(
            cls,
            measurement_id: MeasurementConfiguration.Id,
            machine_configuration: "MachineConfiguration",
            timestamps: List[datetime],
            client_session: "IOManager.ClientSession",
        ):
            result = cls()
            result.measurement_id = measurement_id
            measurement = machine_configuration["measurements"][measurement_id]
            measurement_aggregated_settings = await (
                measurement.get_aggregated_settings(
                    client_session=client_session,
                    machine_configuration=machine_configuration,
                )
            )
            result.merged_request_window = measurement_aggregated_settings[
                "request"
            ].get("merged_request_window", TimeDelta(0))
            request_windows_tasks = list(
                map(
                    lambda timestamps_sub_range: result.RequestWindow.create(
                        timestamps=timestamps_sub_range,
                        measurement_id=measurement_id,
                        machine_configuration=machine_configuration,
                        measurement_aggregated_settings=(
                            measurement_aggregated_settings
                        ),
                        client_session=client_session,
                    ),
                    iter_sub_ranges(timestamps, result.merged_request_window),
                )
            )
            result.request_windows = await asyncio.gather(
                *request_windows_tasks
            )
            result.request_window_iterator = iter(result.request_windows)

            return result

    class ResultEntry:
        """
        A single entry in a :class:`MachineConfiguration.ResultRow`
        """

        def __init__(
            self,
            timestamp: datetime,
            value: Any,
        ):
            self.timestamp = timestamp
            self.value = value

        def str(self, reference_timestamp=None):
            time_info = (
                self.timestamp
                if reference_timestamp is None
                else self.timestamp - reference_timestamp
            )
            return f"{self.value}@{time_info}"

        def __str__(self):
            return self.str()

    class ResultsRow(list):
        """
        A single row in a :class:`MachineConfiguration.Results` frame.
        """

        def __init__(
            self,
            timestamp: datetime,
            values: List[Dict[str, Any]],
        ):
            self.timestamp = timestamp
            super().__init__(
                None
                if value is None
                else MachineConfiguration.ResultEntry(**value)
                for value in values
            )

        def str(self, with_time_errors=False):
            item_str_kwargs = {}
            if with_time_errors:
                item_str_kwargs["reference_timestamp"] = self.timestamp
            return ", ".join(
                map(
                    str,
                    [self.timestamp]
                    + list(
                        map(
                            lambda item: (
                                str(None)
                                if item is None
                                else item.str(**item_str_kwargs)
                            ),
                            self,
                        )
                    ),
                )
            )

        def __str__(self):
            return self.str()

    class Results(list):
        """
        A full result frame, comprising headers and a list of
        :class:`MachineMeasurement.ResultsRow`s
        """

        def __init__(
            self, measurement_ids: Iterable[MeasurementConfiguration.Id]
        ):
            super().__init__()
            self.measurement_ids = list(measurement_ids)

        def append(
            self,
            timestamp: datetime,
            values=Dict[MeasurementConfiguration.Id, Dict[str, Any]],
        ):
            super().append(
                MachineConfiguration.ResultsRow(
                    timestamp=timestamp,
                    values=values,
                )
            )

        def str(self, with_time_errors=False):
            result = repr(self.measurement_ids) + "\n"
            result += "\n".join(
                map(
                    lambda item: item.str(with_time_errors=with_time_errors),
                    self,
                )
            )
            return result

        def __str__(self):
            return self.str()

    async def fetch_result(
        self,
        client_session: "IOManager.ClientSession",
        timestamps: Iterable[datetime],
        **kwargs,
    ) -> "HTTPResultSettings.ResultType":
        aggregated_settings = await self.get_aggregated_settings(
            client_session=client_session
        )
        machine_configuration = MachineConfiguration(aggregated_settings)
        machine_configuration.pop("_machine_configuration_ids", None)
        # result = await machine_configuration.get_interpolated_settings(
        #    client_session=client_session, **kwargs
        # )

        timestamps = sorted(timestamps)
        measurement_fetch_results = [
            await self.__MeasurementFetchResult.create(
                measurement_id=measurement_id,
                timestamps=timestamps,
                client_session=client_session,
                machine_configuration=machine_configuration,
            )
            for measurement_id in machine_configuration["measurements"].keys()
        ]

        # schedule tasks in start_time timestamp order, to ensure no deadlock
        # occurs if urgent tasks are blocked in an AsyncConcurrentPool by
        # not-yet-awaited later tasks
        all_request_windows = sorted(
            itertools.chain(
                *map(
                    lambda item: item.request_windows,
                    measurement_fetch_results,
                )
            )
        )

        results = self.Results(
            measurement_ids=[
                mfr.measurement_id for mfr in measurement_fetch_results
            ]
        )
        try:
            for request_window in all_request_windows:
                request_window.create_task(client_session=client_session)

            awaited_task_count = 0
            for timestamp in timestamps:
                exausted_windows = list(
                    filter(
                        lambda mfr: mfr.current_window_count <= 0,
                        measurement_fetch_results,
                    )
                )
                if len(exausted_windows) > 0:
                    request_tasks = []
                    for exausted_window in exausted_windows:
                        request_window = next(
                            exausted_window.request_window_iterator
                        )
                        exausted_window.current_window_count = (
                            request_window.count
                        )
                        exausted_window.last_result_pos = 0
                        exausted_window.processed_count += request_window.count
                        request_tasks.append(request_window.task)
                    request_task_gather = asyncio.gather(*request_tasks)
                    try:
                        request_results = await request_task_gather
                    except BaseException as excp:
                        request_task_gather.cancel()
                        try:
                            await request_task_gather
                        except BaseException:
                            pass
                        raise excp
                    awaited_task_count += len(request_tasks)
                    for exausted_window, request_result in zip(
                        exausted_windows, request_results
                    ):
                        if request_result is None:
                            request_result = []
                        elif not isinstance(request_result, list):
                            request_result = [request_result]
                        exausted_window.current_window_results = request_result

                timestamp_results = []
                for mfr in measurement_fetch_results:
                    mfr.last_result_pos = find_nearest(
                        mfr.current_window_results,
                        timestamp,
                        lo=mfr.last_result_pos,
                        key=lambda result: result["timestamp"],
                    )
                    try:
                        result = mfr.current_window_results[
                            mfr.last_result_pos
                        ]
                    except IndexError:
                        assert len(mfr.current_window_results) == 0
                        assert mfr.last_result_pos == 0
                        result = None
                    timestamp_results.append(result)
                    mfr.current_window_count -= 1
                results.append(timestamp=timestamp, values=timestamp_results)
        except BaseException as excp:
            unawaited_tasks = []
            for mfr in measurement_fetch_results:
                for rw in mfr.request_window_iterator:
                    task = rw.task
                    if task is not None:
                        unawaited_tasks.append(task)
            if len(unawaited_tasks) > 0:
                unawaited_tasks_gather = asyncio.gather(*unawaited_tasks)
                unawaited_tasks_gather.cancel()
                try:
                    await unawaited_tasks_gather
                except BaseException:
                    pass

            # Got errors from tests: "Task was destroyed but it is pending".
            # To avoid them, make sure all tasks have enough time to go into
            # 'done' state
            while True:
                for rw in all_request_windows:
                    if rw.task is not None and not rw.task.done():
                        break
                else:
                    break
                await asyncio.sleep(0.001)

            raise excp
        else:
            # for debugging purposes
            for mfr in measurement_fetch_results:
                try:
                    next(mfr.request_window_iterator)
                except StopIteration:
                    pass
                else:
                    raise Exception(
                        f"Window iterator for {mfr.measurement_id!r}"
                        " did not stop"
                    )
                assert mfr.processed_count == len(timestamps)
            assert awaited_task_count == len(all_request_windows)

        return results
