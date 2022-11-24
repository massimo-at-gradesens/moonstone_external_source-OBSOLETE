from datetime import timedelta

import pytest

from gradesens.moonstone_external_source import (
    Error,
    MachineConfiguration,
    Settings,
)

from .utils import assert_eq, load_yaml


def test_time_parsing():
    mach_conf = MachineConfiguration(
        **load_yaml(
            """
    id: time-margin-mach
    request:
        time_margin: 2m
    measurements:
        - id: temperature
    """
        )
    )
    expected = {
        "id": "time-margin-mach",
        "request": {
            "_authentication_configuration_id": None,
            "start_time_margin": timedelta(minutes=2),
            "end_time_margin": timedelta(minutes=2),
            "url": None,
            "headers": {},
            "query_string": {},
            "data": None,
        },
        "result": {
            "_interpolation_settings": Settings.InterpolationSettings(
                interpolate=False,
            ),
        },
        "_machine_configuration_ids": (),
        "measurements": {
            "temperature": {
                "_machine_configuration_ids": (),
                "id": "temperature",
                "request": {
                    "_authentication_configuration_id": None,
                    "url": None,
                    "headers": {},
                    "query_string": {},
                    "data": None,
                },
                "result": {
                    "_interpolation_settings": Settings.InterpolationSettings(
                        interpolate=False,
                    ),
                },
            }
        },
    }
    assert_eq(mach_conf, expected)

    mach_conf = MachineConfiguration(
        **load_yaml(
            """
    id: time-margin-mach
    request:
        time_margin: 2m

        # Quote it, otherwise yaml interprets it on its on will... which does
        # not match TimeDelta's will
        start_time_margin: "17:21"

    measurements:
        - id: temperature
    """
        )
    )
    expected["request"]["start_time_margin"] = timedelta(
        minutes=17, seconds=21
    )
    assert_eq(mach_conf, expected)

    mach_conf = MachineConfiguration(
        **load_yaml(
            """
    id: time-margin-mach
    request:
        time_margin: 2m

        # Quote it, otherwise yaml interprets it on its on will... which does
        # not match TimeDelta's will
        start_time_margin: "17:21"

        end_time_margin: 13 hours

    measurements:
        - id: temperature
    """
        )
    )
    expected["request"]["start_time_margin"] = timedelta(
        minutes=17, seconds=21
    )
    expected["request"]["end_time_margin"] = timedelta(hours=13)
    assert_eq(mach_conf, expected)

    mach_conf = MachineConfiguration(
        **load_yaml(
            """
    id: time-margin-mach
    request:
        time_margin: 2m
        end_time_margin: 13 hours

    measurements:
        - id: temperature
    """
        )
    )
    expected["request"]["start_time_margin"] = timedelta(minutes=2)
    expected["request"]["end_time_margin"] = timedelta(hours=13)
    assert_eq(mach_conf, expected)

    mach_conf = MachineConfiguration(
        **load_yaml(
            """
    id: time-margin-mach

    request:
        end_time_margin: 13 hours

    measurements:
        - id: temperature
    """
        )
    )
    del expected["request"]["start_time_margin"]
    expected["request"]["end_time_margin"] = timedelta(hours=13)
    assert_eq(mach_conf, expected)

    mach_conf = MachineConfiguration(
        **load_yaml(
            """
    id: time-margin-mach

    request:
        start_time_margin: 33 days

    measurements:
        - id: temperature
    """
        )
    )
    expected["request"]["start_time_margin"] = timedelta(days=33)
    del expected["request"]["end_time_margin"]
    assert_eq(mach_conf, expected)


def test_time_marging_error():
    good_mach_conf = load_yaml(
        """
        id: time-margin-mach
        request:
            time_margin: 1
            start_time_margin: 1
            end_time_margin: 1
        measurements:
            - id: temperature
        """
    )

    mach_conf = dict(good_mach_conf)
    for key in (
        "time_margin",
        "start_time_margin",
        "end_time_margin",
    ):
        mach_conf["request"] = dict(good_mach_conf["request"])
        # create a MachineConfiguration to validate the good conf does not
        # raise
        MachineConfiguration(**mach_conf)

        mach_conf["request"][key] = timedelta(-1)
        with pytest.raises(Error) as err:
            MachineConfiguration(**mach_conf)
        assert f"{key!r}" in str(err.value)
