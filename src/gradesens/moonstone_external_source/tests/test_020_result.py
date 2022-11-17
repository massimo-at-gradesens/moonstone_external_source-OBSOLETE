import re
from datetime import datetime

import pytest

from gradesens.moonstone_external_source import configuration

from .utils import assert_eq_dicts

converters = configuration._HTTPResultFieldSettings.VALID_TYPES


@pytest.mark.usefixtures("io_driver_1")
@pytest.mark.asyncio
async def test_machine_configuration_with_result(io_driver_1):
    mach_conf = await io_driver_1.machine_configurations.get("mach_w_result")
    expected = {
        "id": "mach_w_result",
        "_common_configuration_ids": ("cc_w_result",),
        "_authentication_configuration_id": None,
        "request": {
            "url": None,
            "query_string": {},
            "headers": {},
        },
        "result": {
            "_interpolate": False,
            "timestamp": {
                "type": converters["datetime"],
                "regular_expressions": (
                    {
                        "regular_expression": re.compile(
                            "^(|.*[^0-9])(?P<year>[0-9]+).*", re.IGNORECASE
                        ),
                        "replacement": r"20\g<year>-11-15",
                    },
                ),
            },
            "value": {
                "raw_value": "{get}{the}{raw}",
            },
        },
        "measurements": {
            "temperature": {
                "id": "temperature",
                "_common_configuration_ids": (),
                "_authentication_configuration_id": None,
                "request": {
                    "url": None,
                    "query_string": {},
                    "headers": {},
                },
                "result": {
                    "_interpolate": False,
                    "value": {
                        "type": converters["float"],
                    },
                    "timestamp": {
                        "type": converters["datetime"],
                        "raw_value": "{temp_ts_raw}",
                    },
                },
            },
            "rpm": {
                "id": "rpm",
                "_common_configuration_ids": (),
                "_authentication_configuration_id": None,
                "request": {
                    "url": None,
                    "query_string": {},
                    "headers": {},
                },
                "result": {
                    "_interpolate": False,
                    "timestamp": {
                        "type": converters["datetime"],
                        "raw_value": 23,
                        "regular_expressions": (
                            {
                                "regular_expression": re.compile("^(.*)$"),
                                "replacement": r"17-\1\1-08",
                            },
                            {
                                "regular_expression": re.compile(
                                    "^(?P<d>.*)-(?P<y>.*)-(?P<m>.*)$"
                                ),
                                "replacement": r"\g<y>-\g<m>-\g<d>",
                            },
                        ),
                    },
                    "value": {
                        "type": converters["int"],
                        "regular_expressions": (
                            {
                                "regular_expression": re.compile("^(.*)$"),
                                "replacement": r"0x\1",
                            },
                            {
                                "regular_expression": re.compile("[83]"),
                                "replacement": r"7",
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
                    "query_string": {},
                    "headers": {},
                },
                "result": {
                    "_interpolate": False,
                    "timestamp": {
                        "type": converters["datetime"],
                        "raw_value": "{out_field2}",
                    },
                },
            },
        },
    }

    assert_eq_dicts(mach_conf, expected)


@pytest.mark.usefixtures("io_driver_1")
@pytest.mark.asyncio
async def test_machine_settings_with_result(io_driver_1):
    mach_conf = await io_driver_1.machine_configurations.get("mach_w_result")
    resolver = mach_conf.get_setting_resolver(io_driver_1)
    settings = await resolver.get_settings()
    expected = {
        "temperature": {
            "request": {
                "url": None,
                "query_string": {},
                "headers": {},
            },
            "result": {
                "timestamp": {
                    "type": converters["datetime"],
                    "raw_value": "{temp_ts_raw}",
                    "regular_expressions": (
                        {
                            "regular_expression": re.compile(
                                "^(|.*[^0-9])(?P<year>[0-9]+).*", re.IGNORECASE
                            ),
                            "replacement": r"20\g<year>-11-15",
                        },
                    ),
                },
                "value": {
                    "type": converters["float"],
                    "raw_value": "{get}{the}{raw}",
                },
            },
        },
        "rpm": {
            "request": {
                "url": None,
                "query_string": {},
                "headers": {},
            },
            "result": {
                "timestamp": {
                    "type": converters["datetime"],
                    "raw_value": 23,
                    "regular_expressions": (
                        {
                            "regular_expression": re.compile("^(.*)$"),
                            "replacement": r"17-\1\1-08",
                        },
                        {
                            "regular_expression": re.compile(
                                "^(?P<d>.*)-(?P<y>.*)-(?P<m>.*)$"
                            ),
                            "replacement": r"\g<y>-\g<m>-\g<d>",
                        },
                    ),
                },
                "value": {
                    "raw_value": "{get}{the}{raw}",
                    "type": converters["int"],
                    "regular_expressions": (
                        {
                            "regular_expression": re.compile("^(.*)$"),
                            "replacement": r"0x\1",
                        },
                        {
                            "regular_expression": re.compile("[83]"),
                            "replacement": r"7",
                        },
                    ),
                },
            },
        },
        "humidity": {
            "request": {
                "url": None,
                "query_string": {},
                "headers": {},
            },
            "result": {
                "timestamp": {
                    "type": converters["datetime"],
                    "raw_value": "{out_field2}",
                    "regular_expressions": (
                        {
                            "regular_expression": re.compile(
                                "^(|.*[^0-9])(?P<year>[0-9]+).*", re.IGNORECASE
                            ),
                            "replacement": r"20\g<year>-11-15",
                        },
                    ),
                },
                "value": {
                    "raw_value": "{get}{the}{raw}",
                },
            },
        },
    }

    assert_eq_dicts(settings, expected)


@pytest.mark.usefixtures("io_driver_1")
@pytest.mark.asyncio
async def test_machine_result(io_driver_1):
    mach_conf = await io_driver_1.machine_configurations.get("mach_w_result")
    resolver = mach_conf.get_setting_resolver(io_driver_1)
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
    assert_eq_dicts(result, expected)
