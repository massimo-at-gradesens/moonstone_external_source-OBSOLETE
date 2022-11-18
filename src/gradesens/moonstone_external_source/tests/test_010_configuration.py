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
        "id": "cc1",
        "_authentication_configuration_id": "ac1",
        "_common_configuration_ids": (),
        "request": {
            "url": (
                "https://gradesens.com/{zone}/{machine_id}"
                "/{device}/{measurement_id}"
            ),
            "headers": {
                "head": "oval",
                "fingers": "count_{finger_count}",
                "bearer": "{token}",
            },
            "query_string": {
                "HELLO": "{region}@world",
            },
            "data": None,
        },
        "zone": "area42",
        "device": "best device ever",
        "measurements": {},
        "result": {},
    }
    assert_eq_dicts(common_configuration_1, expected)


@pytest.mark.usefixtures("machine_configuration_1")
def test_machine_configuration(machine_configuration_1):
    assert isinstance(machine_configuration_1, MachineConfiguration)
    for id, conf in machine_configuration_1["measurements"].items():
        assert isinstance(conf, MeasurementConfiguration)
        assert id == conf["id"]

    expected = {
        "id": "mach1",
        "_common_configuration_ids": ("cc1",),
        "_authentication_configuration_id": None,
        "request": {
            "url": (
                "https://gradesens.com/{zone}/MACHINE/{machine_id}/{device}"
                "/{measurement_id}"
            ),
            "headers": {},
            "query_string": {},
            "data": None,
        },
        "finger_count": 5,
        "result": {},
        "region": "basel",
        "measurements": {
            "temperature": {
                "id": "temperature",
                "_common_configuration_ids": ("cc2",),
                "_authentication_configuration_id": None,
                "region": "zurich",
                "request": {
                    "url": None,
                    "headers": {},
                    "query_string": {
                        "depth": "12",
                        "width": "P_{param}_xx",
                    },
                    "data": None,
                },
                "result": {},
            },
            "rpm": {
                "id": "rpm",
                "_common_configuration_ids": (),
                "_authentication_configuration_id": None,
                "request": {
                    "url": (
                        "https://gradesens.com/{zone}/{machine_id}"
                        "/{device}/RPM/{measurement_id}"
                    ),
                    "headers": {},
                    "query_string": {
                        "dune": "worms",
                    },
                    "data": None,
                },
                "region": "Wallis",
                "result": {},
            },
            "power": {
                "id": "power",
                "_common_configuration_ids": ("cc2",),
                "_authentication_configuration_id": None,
                "request": {
                    "url": None,
                    "headers": {
                        "animal": "cow",
                    },
                    "query_string": {},
                    "data": None,
                },
                "result": {},
            },
        },
    }

    assert_eq_dicts(machine_configuration_1, expected)


@pytest.mark.usefixtures("machine_configuration_1")
@pytest.mark.usefixtures("io_manager_1")
@pytest.mark.asyncio
async def test_interpolated_measurement_settings(
    machine_configuration_1,
    io_manager_1,
):
    assert isinstance(machine_configuration_1, MachineConfiguration)
    resolver = machine_configuration_1.get_settings_resolver(io_manager_1)
    settings = await resolver["measurements"]["temperature"].get_settings()
    assert isinstance(settings, dict)
    assert not isinstance(settings, Settings)

    expected = {
        "request": {
            "url": (
                "https://gradesens.com/Connecticut/MACHINE"
                "/mach1/better than cc1 device/temperature"
            ),
            "headers": {
                "hello": "world",
                "head": "oval",
                "fingers": "count_5",
                "bearer": "I am a secret",
            },
            "query_string": {
                "depth": "12",
                "width": "P_I am a parameter_xx",
                "HELLO": "zurich@world",
            },
            "data": None,
        },
        "result": {},
    }
    assert_eq_dicts(settings, expected)


@pytest.mark.usefixtures("machine_configuration_1")
@pytest.mark.usefixtures("io_manager_1")
@pytest.mark.asyncio
async def test_interpolated_measurement_all_settings(
    machine_configuration_1,
    io_manager_1,
):
    assert isinstance(machine_configuration_1, MachineConfiguration)
    resolver = machine_configuration_1.get_settings_resolver(io_manager_1)
    settings = await resolver.get_settings()
    assert isinstance(settings, dict)
    assert not isinstance(settings, Settings)

    expected = {
        "temperature": {
            "request": {
                "url": (
                    "https://gradesens.com/Connecticut/MACHINE"
                    "/mach1/better than cc1 device/temperature"
                ),
                "headers": {
                    "hello": "world",
                    "head": "oval",
                    "fingers": "count_5",
                    "bearer": "I am a secret",
                },
                "query_string": {
                    "depth": "12",
                    "width": "P_I am a parameter_xx",
                    "HELLO": "zurich@world",
                },
                "data": None,
            },
            "result": {},
        },
        "rpm": {
            "request": {
                "url": (
                    "https://gradesens.com/area42/mach1"
                    "/best device ever/RPM/rpm"
                ),
                "headers": {
                    "head": "oval",
                    "fingers": "count_5",
                    "bearer": "I am a secret",
                },
                "query_string": {
                    "HELLO": "Wallis@world",
                    "dune": "worms",
                },
                "data": None,
            },
            "result": {},
        },
        "power": {
            "request": {
                "url": (
                    "https://gradesens.com/Connecticut/MACHINE"
                    "/mach1/better than cc1 device/power"
                ),
                "headers": {
                    "hello": "world",
                    "animal": "cow",
                    "head": "oval",
                    "fingers": "count_5",
                    "bearer": "I am a secret",
                },
                "query_string": {
                    "HELLO": "basel@world",
                },
                "data": None,
            },
            "result": {},
        },
    }
    assert_eq_dicts(settings, expected)


@pytest.mark.usefixtures("io_manager_1")
@pytest.mark.asyncio
async def test_complex_interpolated_measurement_all_settings(
    io_manager_1,
):
    mach_conf_2 = await io_manager_1.machine_configurations.get("mach2")
    assert isinstance(mach_conf_2, MachineConfiguration)
    resolver = mach_conf_2.get_settings_resolver(io_manager_1)
    settings = await resolver.get_settings()
    assert isinstance(settings, dict)
    assert not isinstance(settings, Settings)

    expected = {
        "temperature": {
            "request": {
                "url": "this is a 4: FouR",
                "headers": {},
                "query_string": {
                    "depth": ":: world :: tw0 ::",
                    "plain": "I am a plain string",
                },
                "data": None,
            },
            "result": {},
        }
    }
    assert_eq_dicts(settings, expected)


@pytest.mark.usefixtures("io_manager_1")
@pytest.mark.asyncio
async def test_start_end_times(
    io_manager_1,
):
    mach_conf_1 = await io_manager_1.machine_configurations.get("mach_w_time")
    resolver = mach_conf_1.get_settings_resolver(io_manager_1)
    with pytest.raises(TimeError) as exc:
        await resolver.get_settings(start_time=datetime.now())
    assert "start_time" in str(exc.value)

    with pytest.raises(TimeError) as exc:
        await resolver.get_settings(end_time=datetime.now())
    assert "end_time" in str(exc.value)

    with pytest.raises(PatternError) as exc:
        await resolver.get_settings()
    with pytest.raises(PatternError) as exc:
        await resolver["measurements"]["temperature"].get_settings()
    with pytest.raises(PatternError) as exc:
        await resolver.get_settings(start_time=datetime.now(timezone.utc))
    with pytest.raises(PatternError) as exc:
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
    query_string = settings["request"]["query_string"]

    assert isinstance(query_string["start"], str)
    assert query_string["start"] == "2022-11-14T17:34:17+00:00"

    assert isinstance(query_string["end"], str)
    assert query_string["end"] == "3022-09-02T05:27:03+00:00"
