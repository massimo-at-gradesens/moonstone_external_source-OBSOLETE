from datetime import datetime, timezone

import pytest

from gradesens.moonstone_external_source import (
    CommonConfiguration,
    MachineConfiguration,
    MeasurementConfiguration,
    PatternError,
    Settings,
    TimeError,
)

from .utils import assert_eq_dicts


@pytest.mark.usefixtures("common_configuration_1")
def test_common_configuration(common_configuration_1):
    assert isinstance(common_configuration_1, CommonConfiguration)
    expected = {
        "identifier": "cc1",
        "authentication_context_identifier": "ac1",
        "url": (
            "https://gradesens.com/{zone}/{machine}" "/{device}/{measurement}"
        ),
        "zone": "area42",
        "query_string": {
            "HELLO": "{region}@world",
        },
        "headers": {
            "head": "oval",
            "fingers": "count_{finger_count}",
            "bearer": "{token}",
        },
        "device": "best device ever",
    }
    assert_eq_dicts(common_configuration_1, expected)


@pytest.mark.usefixtures("machine_configuration_1")
def test_machine_configuration(machine_configuration_1):
    assert isinstance(machine_configuration_1, MachineConfiguration)
    for identifier, conf in machine_configuration_1["measurements"].items():
        assert isinstance(conf, MeasurementConfiguration)
        assert identifier == conf["identifier"]

    expected = {
        "identifier": "mach1",
        "common_configuration_identifier": "cc1",
        "authentication_context_identifier": None,
        "url": (
            "https://gradesens.com/{zone}/MACHINE/{machine}/{device}"
            "/{measurement}"
        ),
        "query_string": {},
        "headers": {},
        "finger_count": 5,
        "measurements": {
            "temperature": {
                "identifier": "temperature",
                "common_configuration_identifier": "cc2",
                "authentication_context_identifier": None,
                "url": None,
                "query_string": {
                    "depth": "12",
                    "width": "P_{param}_xx",
                },
                "headers": {},
            },
            "rpm": {
                "identifier": "rpm",
                "common_configuration_identifier": None,
                "authentication_context_identifier": None,
                "url": (
                    "https://gradesens.com/{zone}/{machine}"
                    "/{device}/RPM/{measurement}"
                ),
                "query_string": {
                    "dune": "worms",
                },
                "headers": {},
                "region": "Wallis",
            },
            "power": {
                "identifier": "power",
                "common_configuration_identifier": "cc2",
                "authentication_context_identifier": None,
                "url": None,
                "query_string": {},
                "headers": {
                    "animal": "cow",
                },
            },
        },
    }

    assert_eq_dicts(machine_configuration_1, expected)


@pytest.mark.usefixtures("machine_configuration_1")
@pytest.mark.usefixtures("io_driver_1")
@pytest.mark.asyncio
async def test_interpolated_measurement_settings(
    machine_configuration_1,
    io_driver_1,
):
    assert isinstance(machine_configuration_1, MachineConfiguration)
    resolver = machine_configuration_1.get_setting_resolver(io_driver_1)
    settings = await resolver["measurements"]["temperature"].get_settings()
    assert isinstance(settings, dict)
    assert not isinstance(settings, Settings)

    expected = {
        "url": (
            "https://gradesens.com/Connecticut/MACHINE"
            "/mach1/better than cc1 device/temperature"
        ),
        "query_string": {
            "depth": "12",
            "width": "P_I am a parameter_xx",
        },
        "headers": {
            "hello": "world",
        },
    }
    assert_eq_dicts(settings, expected)


@pytest.mark.usefixtures("machine_configuration_1")
@pytest.mark.usefixtures("io_driver_1")
@pytest.mark.asyncio
async def test_interpolated_measurement_all_settings(
    machine_configuration_1,
    io_driver_1,
):
    assert isinstance(machine_configuration_1, MachineConfiguration)
    resolver = machine_configuration_1.get_setting_resolver(io_driver_1)
    settings = await resolver.get_settings()
    assert isinstance(settings, dict)
    assert not isinstance(settings, Settings)

    expected = {
        "temperature": {
            "url": (
                "https://gradesens.com/Connecticut/MACHINE"
                "/mach1/better than cc1 device/temperature"
            ),
            "query_string": {
                "depth": "12",
                "width": "P_I am a parameter_xx",
            },
            "headers": {
                "hello": "world",
            },
        },
        "rpm": {
            "url": (
                "https://gradesens.com/area42/mach1"
                "/best device ever/RPM/rpm"
            ),
            "query_string": {
                "HELLO": "Wallis@world",
                "dune": "worms",
            },
            "headers": {
                "head": "oval",
                "fingers": "count_5",
                "bearer": "I am a secret",
            },
        },
        "power": {
            "url": (
                "https://gradesens.com/Connecticut/MACHINE"
                "/mach1/better than cc1 device/power"
            ),
            "query_string": {},
            "headers": {
                "hello": "world",
                "animal": "cow",
            },
        },
    }
    assert_eq_dicts(settings, expected)


@pytest.mark.usefixtures("io_driver_1")
@pytest.mark.asyncio
async def test_complex_interpolated_measurement_all_settings(
    io_driver_1,
):
    mach_conf_2 = await io_driver_1.machine_configurations.get("mach2")
    assert isinstance(mach_conf_2, MachineConfiguration)
    resolver = mach_conf_2.get_setting_resolver(io_driver_1)
    settings = await resolver.get_settings()
    assert isinstance(settings, dict)
    assert not isinstance(settings, Settings)

    expected = {
        "temperature": {
            "url": "this is a 4: FouR",
            "query_string": {
                "depth": ":: world :: tw0 ::",
                "plain": "I am a plain string",
            },
            "headers": {},
        }
    }
    assert_eq_dicts(settings, expected)


@pytest.mark.usefixtures("io_driver_1")
@pytest.mark.asyncio
async def test_start_end_times(
    io_driver_1,
):
    mach_conf_1 = await io_driver_1.machine_configurations.get("mach_w_time")
    resolver = mach_conf_1.get_setting_resolver(io_driver_1)
    with pytest.raises(TimeError) as exc_info:
        await resolver.get_settings(start_time=datetime.now())
    assert "start_time" in str(exc_info.value)

    with pytest.raises(TimeError) as exc_info:
        await resolver.get_settings(end_time=datetime.now())
    assert "end_time" in str(exc_info.value)

    with pytest.raises(PatternError) as exc_info:
        await resolver.get_settings()
    with pytest.raises(PatternError) as exc_info:
        await resolver["measurements"]["temperature"].get_settings()
    with pytest.raises(PatternError) as exc_info:
        await resolver.get_settings(start_time=datetime.now(timezone.utc))
    with pytest.raises(PatternError) as exc_info:
        await resolver.get_settings(end_time=datetime.now(timezone.utc))

    settings = await resolver["measurements"]["temperature"].get_settings(
        start_time=datetime(
            year=2022,
            month=11,
            day=14,
            hour=17,
            minute=34,
            second=17,
            tzinfo=timezone.utc,
        ),
        end_time=datetime(
            year=3022,
            month=9,
            day=2,
            hour=5,
            minute=27,
            second=3,
            tzinfo=timezone.utc,
        ),
    )
    query_string = settings["query_string"]

    assert isinstance(query_string["start"], str)
    assert query_string["start"] == "2022-11-14T17:34:17+00:00"

    assert isinstance(query_string["end"], str)
    assert query_string["end"] == "3022-09-02T05:27:03+00:00"
