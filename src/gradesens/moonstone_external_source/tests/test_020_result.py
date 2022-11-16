import re
from datetime import datetime

import pytest

from .utils import assert_eq_dicts


@pytest.mark.usefixtures("io_driver_1")
@pytest.mark.asyncio
async def test_machine_configuration_with_result(io_driver_1):
    mach_conf = await io_driver_1.machine_configurations.get("mach_w_result")
    expected = {
        "id": "mach_w_result",
        "common_configuration_id": "cc_w_result",
        "authentication_context_id": None,
        "url": None,
        "query_string": {},
        "headers": {},
        "result": {
            "_interpolate": False,
            "timestamp": {
                "type": datetime,
                "regular_expressions": [
                    {
                        "regular_expression": re.compile(
                            "(?P<hello>[a-g]+)", re.IGNORECASE
                        ),
                        "replacement": r"YX \g<hello> XY",
                    },
                ],
            },
            "value": {
                "raw_value": "{get}{the}{raw}",
            },
        },
        "measurements": {
            "temperature": {
                "id": "temperature",
                "common_configuration_id": None,
                "authentication_context_id": None,
                "url": None,
                "query_string": {},
                "headers": {},
                "result": {
                    "_interpolate": False,
                    "value": {
                        "type": float,
                    },
                },
            },
            "rpm": {
                "id": "rpm",
                "common_configuration_id": None,
                "authentication_context_id": None,
                "url": None,
                "query_string": {},
                "headers": {},
                "result": {
                    "_interpolate": False,
                    "timestamp": {
                        "type": datetime,
                        "regular_expressions": [
                            {
                                "regular_expression": re.compile("(.*)"),
                                "replacement": r"\1\1",
                            },
                            {
                                "regular_expression": re.compile(
                                    "(.*)([0-9a-f]+)(.*)"
                                ),
                                "replacement": r"\3\1",
                            },
                        ],
                    },
                },
            },
            "humidity": {
                "id": "humidity",
                "common_configuration_id": None,
                "authentication_context_id": None,
                "url": None,
                "query_string": {},
                "headers": {},
                "result": {
                    "_interpolate": False,
                    "timestamp": {
                        "type": datetime,
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
            "url": None,
            "query_string": {},
            "headers": {},
            "result": {
                "timestamp": {
                    "type": datetime,
                    "regular_expressions": [
                        {
                            "regular_expression": re.compile(
                                "(?P<hello>[a-g]+)",
                                re.IGNORECASE,
                            ),
                            "replacement": r"YX \g<hello> XY",
                        },
                    ],
                },
                "value": {
                    "type": float,
                    "raw_value": "{get}{the}{raw}",
                },
            },
        },
        "rpm": {
            "url": None,
            "query_string": {},
            "headers": {},
            "result": {
                "timestamp": {
                    "type": datetime,
                    "regular_expressions": [
                        {
                            "regular_expression": re.compile("(.*)"),
                            "replacement": r"\1\1",
                        },
                        {
                            "regular_expression": re.compile(
                                "(.*)([0-9a-f]+)(.*)"
                            ),
                            "replacement": r"\3\1",
                        },
                    ],
                },
                "value": {
                    "raw_value": "{get}{the}{raw}",
                },
            },
        },
        "humidity": {
            "url": None,
            "query_string": {},
            "headers": {},
            "result": {
                "timestamp": {
                    "type": datetime,
                    "raw_value": "{out_field2}",
                    "regular_expressions": [
                        {
                            "regular_expression": re.compile(
                                "(?P<hello>[a-g]+)",
                                re.IGNORECASE,
                            ),
                            "replacement": r"YX \g<hello> XY",
                        },
                    ],
                },
                "value": {
                    "raw_value": "{get}{the}{raw}",
                },
            },
        },
    }

    assert_eq_dicts(settings, expected)
