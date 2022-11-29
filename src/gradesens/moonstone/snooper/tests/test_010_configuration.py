from datetime import timedelta, timezone

import pytest

from gradesens.moonstone.snooper import (
    DateTime,
    MachineConfiguration,
    MeasurementConfiguration,
    PatternError,
    Settings,
    TimeError,
)

from .utils import assert_eq, expand_processors


@pytest.mark.usefixtures("common_configuration_1")
def test_common_configuration(common_configuration_1):
    assert isinstance(common_configuration_1, MachineConfiguration)
    expected = {
        "id": "cc1",
        "_machine_configuration_ids": (),
        "request": {
            "_authorization_configuration_id": "ac1",
            "url": (
                "https://gradesens.com/{zone}/{machine_id}"
                "/{device}/{measurement_id}"
            ),
            "headers": {
                "head": "oval",
                "fingers": "count_{finger_count}",
                "bearer": "{request.authorization.token}",
                "test_processors_sun": {
                    "<process": [
                        {
                            "__processor": "eval",
                            "expression": "'hello' + zone",
                            "output_key": "my_private_key",
                        },
                        {
                            "__processor": "eval",
                            "expression": "my_private_key + '350'",
                        },
                        {
                            "__processor": "regex",
                            "pattern": "([ne]+)",
                            "replacement": r"<\1\1>",
                            "flags": 0,
                        },
                    ],
                },
                "test_processors_moon": {
                    "<process": [
                        {
                            "__processor": "interpolate",
                            "string": "1{hex_42}",
                        },
                        {
                            "__processor": "type",
                            "converter": "int:16",
                        },
                    ],
                },
                "test_processors_jupiter": {
                    "<process": [
                        {
                            "__processor": "type",
                            "input_key": "a_timestamp",
                            "converter": "datetime",
                        },
                    ],
                },
            },
            "query_string": {
                "HELLO": "{region}@world",
            },
            "data": None,
        },
        "result": {
            "_interpolation_settings": Settings.InterpolationSettings(
                interpolate=False,
            ),
        },
        "zone": "area42",
        "device": "best device ever",
        "a_timestamp": "2022-11-12",
        "hex_42": "2a",
        "measurements": {},
    }
    common_configuration_1 = expand_processors(common_configuration_1)
    assert_eq(common_configuration_1, expected)


@pytest.mark.usefixtures("machine_configuration_1")
def test_machine_configuration(machine_configuration_1):
    assert isinstance(machine_configuration_1, MachineConfiguration)
    for id, conf in machine_configuration_1["measurements"].items():
        assert isinstance(conf, MeasurementConfiguration)
        assert id == conf["id"]

    expected = {
        "id": "mach1",
        "_machine_configuration_ids": ("cc1",),
        "request": {
            "_authorization_configuration_id": None,
            "url": (
                "https://gradesens.com/{zone}/MACHINE/{machine_id}/{device}"
                "/{measurement_id}"
            ),
            "headers": {},
            "query_string": {},
            "data": None,
        },
        "finger_count": 5,
        "result": {
            "_interpolation_settings": Settings.InterpolationSettings(
                interpolate=False,
            ),
        },
        "region": "basel",
        "measurements": {
            "temperature": {
                "id": "temperature",
                "_machine_configuration_ids": ("cc2",),
                "region": "zurich",
                "request": {
                    "_authorization_configuration_id": None,
                    "url": None,
                    "headers": {},
                    "query_string": {
                        "depth": "12",
                        "width": "P_{param}_xx",
                    },
                    "data": None,
                },
                "result": {
                    "_interpolation_settings": Settings.InterpolationSettings(
                        interpolate=False,
                    ),
                },
            },
            "rpm": {
                "id": "rpm",
                "_machine_configuration_ids": (),
                "request": {
                    "_authorization_configuration_id": None,
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
                "result": {
                    "_interpolation_settings": Settings.InterpolationSettings(
                        interpolate=False,
                    ),
                },
            },
            "power": {
                "id": "power",
                "_machine_configuration_ids": ("cc2",),
                "request": {
                    "_authorization_configuration_id": None,
                    "url": None,
                    "headers": {
                        "animal": "cow",
                    },
                    "query_string": {},
                    "data": None,
                },
                "result": {
                    "_interpolation_settings": Settings.InterpolationSettings(
                        interpolate=False,
                    ),
                },
            },
        },
    }

    assert_eq(machine_configuration_1, expected)


@pytest.mark.usefixtures("machine_configuration_1")
@pytest.mark.usefixtures("io_manager_1")
@pytest.mark.asyncio
async def test_interpolated_measurement_settings(
    machine_configuration_1,
    io_manager_1,
):
    assert isinstance(machine_configuration_1, MachineConfiguration)
    async with io_manager_1.client_session() as client_session:
        settings = await machine_configuration_1.get_interpolated_settings(
            client_session
        )
    assert isinstance(settings, dict)
    assert not isinstance(settings, Settings)

    settings = settings["temperature"]
    assert isinstance(settings, dict)
    assert not isinstance(settings, Settings)

    expected = {
        "id": "temperature",
        "request": {
            "authorization": {
                "token": "I am a secret",
                "expires_in": timedelta(seconds=100),
            },
            "url": (
                "https://gradesens.com/Connecticut/MACHINE"
                "/mach1/better than cc1 device/temperature"
            ),
            "headers": {
                "hello": "world",
                "head": "oval",
                "fingers": "count_5",
                "bearer": "I am a secret",
                "test_processors_sun": "h<ee>lloCo<nnenne>cticut350",
                "test_processors_moon": 0x12A,
                "test_processors_jupiter": DateTime(2022, 11, 12),
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
    assert_eq(settings, expected)


@pytest.mark.usefixtures("machine_configuration_1")
@pytest.mark.usefixtures("io_manager_1")
@pytest.mark.asyncio
async def test_interpolated_measurement_all_settings(
    machine_configuration_1,
    io_manager_1,
):
    assert isinstance(machine_configuration_1, MachineConfiguration)
    async with io_manager_1.client_session() as client_session:
        settings = await machine_configuration_1.get_interpolated_settings(
            client_session
        )
    assert isinstance(settings, dict)
    assert not isinstance(settings, Settings)

    expected = {
        "temperature": {
            "id": "temperature",
            "request": {
                "authorization": {
                    "token": "I am a secret",
                    "expires_in": timedelta(seconds=100),
                },
                "url": (
                    "https://gradesens.com/Connecticut/MACHINE"
                    "/mach1/better than cc1 device/temperature"
                ),
                "headers": {
                    "hello": "world",
                    "head": "oval",
                    "fingers": "count_5",
                    "bearer": "I am a secret",
                    "test_processors_sun": "h<ee>lloCo<nnenne>cticut350",
                    "test_processors_moon": 0x12A,
                    "test_processors_jupiter": DateTime(2022, 11, 12),
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
            "id": "rpm",
            "request": {
                "authorization": {
                    "token": "I am a secret",
                    "expires_in": timedelta(seconds=100),
                },
                "url": (
                    "https://gradesens.com/area42/mach1"
                    "/best device ever/RPM/rpm"
                ),
                "headers": {
                    "head": "oval",
                    "fingers": "count_5",
                    "bearer": "I am a secret",
                    "test_processors_sun": "h<ee>lloar<ee>a42350",
                    "test_processors_moon": 0x12A,
                    "test_processors_jupiter": DateTime(2022, 11, 12),
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
            "id": "power",
            "request": {
                "authorization": {
                    "token": "I am a secret",
                    "expires_in": timedelta(seconds=100),
                },
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
                    "test_processors_sun": "h<ee>lloCo<nnenne>cticut350",
                    "test_processors_moon": 0x12A,
                    "test_processors_jupiter": DateTime(2022, 11, 12),
                },
                "query_string": {
                    "HELLO": "basel@world",
                },
                "data": None,
            },
            "result": {},
        },
    }
    assert_eq(settings, expected)


@pytest.mark.usefixtures("io_manager_1")
@pytest.mark.asyncio
async def test_complex_interpolated_measurement_all_settings(
    io_manager_1,
):
    async with io_manager_1.client_session() as client_session:
        mach_conf_2 = await client_session.machine_configurations.get("mach2")
        assert isinstance(mach_conf_2, MachineConfiguration)
        settings = await mach_conf_2.get_interpolated_settings(client_session)
    assert isinstance(settings, dict)
    assert not isinstance(settings, Settings)

    expected = {
        "temperature": {
            "id": "temperature",
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
    assert_eq(settings, expected)


@pytest.mark.usefixtures("io_manager_1")
@pytest.mark.asyncio
async def test_start_end_times(
    io_manager_1,
):
    async with io_manager_1.client_session() as client_session:
        mach_conf_1 = await client_session.machine_configurations.get(
            "mach_w_time"
        )
        with pytest.raises(TimeError) as exc_info:
            await mach_conf_1.get_interpolated_settings(
                client_session,
                start_time=DateTime.now(),
            )
        assert "'start_time'" in str(exc_info.value)
        assert "aware" in str(exc_info.value)

        with pytest.raises(PatternError) as exc_info:
            await mach_conf_1.get_interpolated_settings(
                client_session,
            )

        settings = (
            await mach_conf_1.get_interpolated_settings(
                client_session,
                start_time=DateTime(
                    year=2022,
                    month=11,
                    day=14,
                    hour=17,
                    minute=34,
                    second=17,
                    tzinfo=timezone.utc,
                ),
                end_time=DateTime(
                    year=2022,
                    month=11,
                    day=14,
                    hour=17,
                    minute=34,
                    second=27,
                    tzinfo=timezone.utc,
                ),
            )
        )["temperature"]
        query_string = settings["request"]["query_string"]

        assert isinstance(query_string["start"], str)
        assert query_string["start"] == "2022-11-14T17:34:15+00:00"

        assert isinstance(query_string["end"], str)
        assert query_string["end"] == "2022-11-14T17:34:29+00:00"
