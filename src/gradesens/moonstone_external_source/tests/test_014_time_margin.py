from datetime import timedelta

import pytest

from gradesens.moonstone_external_source import (
    Error,
    MachineConfiguration,
    Settings,
)

from .utils import assert_eq, load_yaml


def test_time_marging():
    mach_conf = MachineConfiguration(
        **load_yaml(
            """
    id: time-margin-mach
    time_margin: 2m
    measurements:
        - id: temperature
    """
        )
    )
    expected = {
        "start_time_margin": timedelta(minutes=2),
        "end_time_margin": timedelta(minutes=2),
        "id": "time-margin-mach",
        "request": {
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
        "_common_configuration_ids": (),
        "_authentication_configuration_id": None,
        "measurements": {
            "temperature": {
                "id": "temperature",
                "request": {
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
                "_common_configuration_ids": (),
                "_authentication_configuration_id": None,
            }
        },
    }
    assert_eq(mach_conf, expected)

    mach_conf = MachineConfiguration(
        **load_yaml(
            """
    id: time-margin-mach
    time_margin: 2m

    # Quote it, otherwise yaml interprets it on its on will... which does not
    # match TimeDelta's will
    start_time_margin: "17:21"

    measurements:
        - id: temperature
    """
        )
    )
    expected["start_time_margin"] = timedelta(minutes=17, seconds=21)
    assert_eq(mach_conf, expected)

    mach_conf = MachineConfiguration(
        **load_yaml(
            """
    id: time-margin-mach
    time_margin: 2m

    # Quote it, otherwise yaml interprets it on its on will... which does not
    # match TimeDelta's will
    start_time_margin: "17:21"

    end_time_margin: 13 hours

    measurements:
        - id: temperature
    """
        )
    )
    expected["start_time_margin"] = timedelta(minutes=17, seconds=21)
    expected["end_time_margin"] = timedelta(hours=13)
    assert_eq(mach_conf, expected)

    mach_conf = MachineConfiguration(
        **load_yaml(
            """
    id: time-margin-mach
    time_margin: 2m

    end_time_margin: 13 hours

    measurements:
        - id: temperature
    """
        )
    )
    expected["start_time_margin"] = timedelta(minutes=2)
    expected["end_time_margin"] = timedelta(hours=13)
    assert_eq(mach_conf, expected)

    mach_conf = MachineConfiguration(
        **load_yaml(
            """
    id: time-margin-mach

    end_time_margin: 13 hours

    measurements:
        - id: temperature
    """
        )
    )
    del expected["start_time_margin"]
    expected["end_time_margin"] = timedelta(hours=13)
    assert_eq(mach_conf, expected)

    mach_conf = MachineConfiguration(
        **load_yaml(
            """
    id: time-margin-mach

    start_time_margin: 33 days

    measurements:
        - id: temperature
    """
        )
    )
    expected["start_time_margin"] = timedelta(days=33)
    del expected["end_time_margin"]
    assert_eq(mach_conf, expected)


def test_time_marging_error():
    good_mach_conf = load_yaml(
        """
        id: time-margin-mach
        time_margin: 1
        start_time_margin: 1
        end_time_margin: 1
        measurements:
            - id: temperature
        """
    )

    for key in (
        "time_margin",
        "start_time_margin",
        "end_time_margin",
    ):
        mach_conf = dict(good_mach_conf)
        # create a MachineConfiguration to validate the good conf does not
        # raise
        MachineConfiguration(**mach_conf)

        mach_conf[key] = timedelta(-1)
        with pytest.raises(Error) as err:
            MachineConfiguration(**mach_conf)
        assert f"{key!r}" in str(err.value)
