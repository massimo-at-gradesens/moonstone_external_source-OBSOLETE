import re
from datetime import date, datetime

import pytest

from gradesens.moonstone_external_source import configuration

from .utils import assert_eq_dicts

converters = configuration.HTTPResultFieldSettings.VALID_TYPES


@pytest.mark.usefixtures("io_manager_1")
@pytest.mark.asyncio
async def test_machine_configuration_with_result(io_manager_1):
    mach_conf = await io_manager_1.machine_configurations.get("mach_w_result")
    expected = {
        "id": "mach_w_result",
        "_common_configuration_ids": ("cc_w_result",),
        "_authentication_configuration_id": None,
        "request": {
            "url": None,
            "headers": {},
            "query_string": {},
            "data": None,
        },
        "out_field2": date.fromisoformat("2022-11-19"),
        "result": {
            "timestamp": {
                "type": converters["datetime"],
                "regular_expressions": (
                    {
                        "pattern": "^(|.*[^0-9])(?P<year>[0-9]{{2,4}}).*",
                        "replacement": r"20\g<year>-11-15",
                        "flags": re.IGNORECASE,
                    },
                ),
            },
            "value": {
                "input": "{{get}}{{the}}{{raw}}",
            },
        },
        "measurements": {
            "temperature": {
                "id": "temperature",
                "_common_configuration_ids": (),
                "_authentication_configuration_id": None,
                "request": {
                    "url": None,
                    "headers": {},
                    "query_string": {},
                    "data": None,
                },
                "result": {
                    "value": {
                        "type": converters["float"],
                    },
                    "timestamp": {
                        "type": converters["datetime"],
                        "input": "{{temp_ts_raw}}",
                    },
                },
            },
            "rpm": {
                "id": "rpm",
                "_common_configuration_ids": (),
                "_authentication_configuration_id": None,
                "request": {
                    "url": None,
                    "headers": {},
                    "query_string": {},
                    "data": None,
                },
                "result": {
                    "timestamp": {
                        "type": converters["datetime"],
                        "input": 23,
                        "regular_expressions": (
                            {
                                "pattern": "^(.*)$",
                                "replacement": r"17-\1\1-08",
                                "flags": 0,
                            },
                            {
                                "pattern": "^(?P<d>.*)-(?P<y>.*)-(?P<m>.*)$",
                                "replacement": r"\g<y>-\g<m>-\g<d>",
                                "flags": 0,
                            },
                        ),
                    },
                    "value": {
                        "type": converters["int"],
                        "regular_expressions": (
                            {
                                "pattern": "^(.*)$",
                                "replacement": r"0x\1",
                                "flags": 0,
                            },
                            {
                                "pattern": "[83]",
                                "replacement": r"7",
                                "flags": 0,
                            },
                        ),
                    },
                },
            },
            "humidity": {
                "id": "humidity",
                "_common_configuration_ids": (),
                "_authentication_configuration_id": None,
                "request": {
                    "url": None,
                    "headers": {},
                    "query_string": {},
                    "data": None,
                },
                "result": {
                    "timestamp": {
                        "type": converters["datetime"],
                        "input": "{out_field2}",
                        "regular_expressions": (),
                    },
                },
            },
        },
    }

    assert_eq_dicts(mach_conf, expected)


@pytest.mark.usefixtures("io_manager_1")
@pytest.mark.asyncio
async def test_machine_settings_with_result(io_manager_1):
    mach_conf = await io_manager_1.machine_configurations.get("mach_w_result")
    resolver = mach_conf.get_settings_resolver(io_manager_1)
    settings = await resolver.get_settings()
    expected = {
        "temperature": {
            "request": {
                "url": None,
                "headers": {},
                "query_string": {},
                "data": None,
            },
            "result": {
                "timestamp": {
                    "type": converters["datetime"],
                    "input": "{temp_ts_raw}",
                    "regular_expressions": (
                        {
                            "pattern": "^(|.*[^0-9])(?P<year>[0-9]{2,4}).*",
                            "replacement": r"20\g<year>-11-15",
                            "flags": re.IGNORECASE,
                        },
                    ),
                },
                "value": {
                    "type": converters["float"],
                    "input": "{get}{the}{raw}",
                },
            },
        },
        "rpm": {
            "request": {
                "url": None,
                "headers": {},
                "query_string": {},
                "data": None,
            },
            "result": {
                "timestamp": {
                    "type": converters["datetime"],
                    "input": 23,
                    "regular_expressions": (
                        {
                            "pattern": "^(.*)$",
                            "replacement": r"17-\1\1-08",
                            "flags": 0,
                        },
                        {
                            "pattern": "^(?P<d>.*)-(?P<y>.*)-(?P<m>.*)$",
                            "replacement": r"\g<y>-\g<m>-\g<d>",
                            "flags": 0,
                        },
                    ),
                },
                "value": {
                    "input": "{get}{the}{raw}",
                    "type": converters["int"],
                    "regular_expressions": (
                        {
                            "pattern": "^(.*)$",
                            "replacement": r"0x\1",
                            "flags": 0,
                        },
                        {
                            "pattern": "[83]",
                            "replacement": r"7",
                            "flags": 0,
                        },
                    ),
                },
            },
        },
        "humidity": {
            "request": {
                "url": None,
                "headers": {},
                "query_string": {},
                "data": None,
            },
            "result": {
                "timestamp": {
                    "type": converters["datetime"],
                    "input": "2022-11-19",
                    "regular_expressions": (),
                },
                "value": {
                    "input": "{get}{the}{raw}",
                },
            },
        },
    }

    assert_eq_dicts(settings, expected)


@pytest.mark.usefixtures("io_manager_1")
@pytest.mark.asyncio
async def test_machine_result(io_manager_1):
    mach_conf = await io_manager_1.machine_configurations.get("mach_w_result")
    resolver = mach_conf.get_settings_resolver(io_manager_1)
    settings = await resolver.get_settings()

    result = settings["temperature"]["result"].process_result(
        dict(
            get="3",
            the=".",
            raw="14",
            temp_ts_raw="hello 31 jk",
        )
    )
    expected = {
        "value": 3.14,
        "timestamp": datetime(2031, 11, 15),
    }
    assert_eq_dicts(result, expected)

    result = settings["rpm"]["result"].process_result(
        dict(
            get="18",
            the="33",
            raw="25",
            temp_ts_raw="hello 31 jk",
        )
    )
    expected = {
        "value": 0x177725,
        "timestamp": datetime(2323, 8, 17),
    }

    result = settings["humidity"]["result"].process_result(
        dict(
            get="GET",
            the="<THE>",
            raw="`RAW'",
        )
    )
    expected = {
        "value": "GET<THE>`RAW'",
        "timestamp": datetime(2022, 11, 19),
    }
    assert_eq_dicts(result, expected)
