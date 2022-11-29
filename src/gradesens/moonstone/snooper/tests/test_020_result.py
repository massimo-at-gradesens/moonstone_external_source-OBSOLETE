import re
from datetime import date

import pytest

from gradesens.moonstone.snooper import Settings

from .utils import assert_eq, expand_processors


@pytest.mark.usefixtures("io_manager_1")
@pytest.mark.asyncio
async def test_machine_configuration_with_result(io_manager_1):
    async with io_manager_1.client_session() as client_session:
        mach_conf = await client_session.machine_configurations.get(
            "mach_w_result"
        )
    expected = {
        "id": "mach_w_result",
        "_machine_configuration_ids": ("cc_w_result",),
        "request": {
            "_authorization_configuration_id": None,
            "url": None,
            "headers": {},
            "query_string": {},
            "data": None,
        },
        "out_field2": date.fromisoformat("2022-11-19"),
        "result": {
            "_interpolation_settings": Settings.InterpolationSettings(
                interpolate=False,
            ),
            "timestamp": {
                "<process": [
                    {
                        "__processor": "regex",
                        "input_key": "raw_timestamp",
                        "pattern": "^(|.*[^0-9])(?P<year>[0-9]{{2,4}}).*",
                        "replacement": r"20\g<year>-11-15",
                        "flags": re.IGNORECASE,
                    },
                ]
            },
            "value": "{get}{the}{raw}",
        },
        "measurements": {
            "temperature": {
                "id": "temperature",
                "_machine_configuration_ids": (),
                "request": {
                    "_authorization_configuration_id": None,
                    "url": None,
                    "headers": {},
                    "query_string": {},
                    "data": None,
                },
                "result": {
                    "_interpolation_settings": Settings.InterpolationSettings(
                        interpolate=False,
                    ),
                    "value": {
                        "<process": [
                            {
                                "__processor": "type",
                                "input_key": "temp_value",
                                "converter": "float",
                            }
                        ]
                    },
                    "timestamp": {
                        "<process": [
                            {
                                "__processor": "interpolate",
                                "string": "{temp_ts_raw}",
                            }
                        ]
                    },
                },
            },
            "rpm": {
                "id": "rpm",
                "_machine_configuration_ids": (),
                "request": {
                    "_authorization_configuration_id": None,
                    "url": None,
                    "headers": {},
                    "query_string": {},
                    "data": None,
                },
                "result": {
                    "_interpolation_settings": Settings.InterpolationSettings(
                        interpolate=False,
                    ),
                    "value": {
                        "<process": [
                            {
                                "__processor": "regex",
                                "input_key": "rpm_value",
                                "pattern": "^(.*)$",
                                "replacement": r"0x\1",
                                "flags": 0,
                            },
                            {
                                "__processor": "regex",
                                "pattern": "[83]",
                                "replacement": r"7",
                                "flags": 0,
                            },
                            {
                                "__processor": "type",
                                "converter": "int:0",
                            },
                        ],
                    },
                    "timestamp": {
                        "<process": [
                            {
                                "__processor": "eval",
                                "expression": "23",
                            },
                            {
                                "__processor": "regex",
                                "pattern": "^(.*)$",
                                "replacement": r"17-\1\1-08",
                                "flags": 0,
                            },
                            {
                                "__processor": "regex",
                                "pattern": "^(?P<d>.*)-(?P<y>.*)-(?P<m>.*)$",
                                "replacement": r"\g<y>-\g<m>-\g<d>",
                                "flags": 0,
                            },
                            {
                                "__processor": "type",
                                "converter": "date",
                            },
                        ],
                    },
                },
            },
            "humidity": {
                "id": "humidity",
                "_machine_configuration_ids": (),
                "request": {
                    "_authorization_configuration_id": None,
                    "url": None,
                    "headers": {},
                    "query_string": {},
                    "data": None,
                },
                "result": {
                    "_interpolation_settings": Settings.InterpolationSettings(
                        interpolate=False,
                    ),
                    "timestamp": {
                        "<process": [
                            {
                                "__processor": "regex",
                                "input_key": "out_field2",
                                "pattern": "^(.*)$",
                                "replacement": r"<\1>",
                                "flags": 0,
                            },
                        ],
                    },
                },
            },
        },
    }

    mach_conf = expand_processors(mach_conf)
    assert_eq(mach_conf, expected)


@pytest.mark.usefixtures("io_manager_1")
@pytest.mark.asyncio
async def test_machine_settings_with_result(io_manager_1):
    async with io_manager_1.client_session() as client_session:
        mach_conf = await client_session.machine_configurations.get(
            "mach_w_result"
        )
        settings = await mach_conf.get_interpolated_settings(client_session)
    expected = {
        "temperature": {
            "id": "temperature",
            "request": {
                "url": None,
                "headers": {},
                "query_string": {},
                "data": None,
            },
            "result": {
                "value": {
                    "<process": [
                        {
                            "__processor": "type",
                            "input_key": "temp_value",
                            "converter": "float",
                        }
                    ]
                },
                "timestamp": {
                    "<process": [
                        {
                            "__processor": "interpolate",
                            "string": "{temp_ts_raw}",
                        }
                    ]
                },
            },
        },
        "rpm": {
            "id": "rpm",
            "request": {
                "url": None,
                "headers": {},
                "query_string": {},
                "data": None,
            },
            "result": {
                "value": {
                    "<process": [
                        {
                            "__processor": "regex",
                            "input_key": "rpm_value",
                            "pattern": "^(.*)$",
                            "replacement": r"0x\1",
                            "flags": 0,
                        },
                        {
                            "__processor": "regex",
                            "pattern": "[83]",
                            "replacement": r"7",
                            "flags": 0,
                        },
                        {
                            "__processor": "type",
                            "converter": "int:0",
                        },
                    ],
                },
                "timestamp": {
                    "<process": [
                        {
                            "__processor": "eval",
                            "expression": "23",
                        },
                        {
                            "__processor": "regex",
                            "pattern": "^(.*)$",
                            "replacement": r"17-\1\1-08",
                            "flags": 0,
                        },
                        {
                            "__processor": "regex",
                            "pattern": "^(?P<d>.*)-(?P<y>.*)-(?P<m>.*)$",
                            "replacement": r"\g<y>-\g<m>-\g<d>",
                            "flags": 0,
                        },
                        {
                            "__processor": "type",
                            "converter": "date",
                        },
                    ],
                },
            },
        },
        "humidity": {
            "id": "humidity",
            "request": {
                "url": None,
                "headers": {},
                "query_string": {},
                "data": None,
            },
            "result": {
                "timestamp": {
                    "<process": [
                        {
                            "__processor": "regex",
                            "input_key": "out_field2",
                            "pattern": "^(.*)$",
                            "replacement": r"<\1>",
                            "flags": 0,
                        },
                    ],
                },
                "value": "{get}{the}{raw}",
            },
        },
    }

    settings = expand_processors(settings)
    assert_eq(settings, expected)


@pytest.mark.usefixtures("io_manager_1")
@pytest.mark.asyncio
async def test_machine_result(io_manager_1):
    async with io_manager_1.client_session() as client_session:
        mach_conf = await client_session.machine_configurations.get(
            "mach_w_result"
        )
        settings = await mach_conf.get_interpolated_settings(client_session)

    result = settings["temperature"].process_result(
        dict(
            temp_value="3.14",
            temp_ts_raw="hello 31 jk",
        )
    )
    expected = {
        "value": 3.14,
        "timestamp": "hello 31 jk",
    }
    assert_eq(result, expected)

    result = settings["rpm"].process_result(dict(rpm_value="1283849"))
    expected = {
        "value": 0x1277749,
        "timestamp": date(2323, 8, 17),
    }
    assert_eq(result, expected)

    result = settings["humidity"].process_result(
        dict(
            out_field2="jump",
            get="GET",
            the="<THE>",
            raw="`RAW'",
        )
    )
    expected = {
        "value": "GET<THE>`RAW'",
        "timestamp": "<jump>",
    }
    assert_eq(result, expected)
