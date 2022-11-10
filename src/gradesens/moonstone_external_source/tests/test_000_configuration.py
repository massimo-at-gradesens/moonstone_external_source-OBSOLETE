import textwrap

import pytest
import yaml

from gradesens.moonstone_external_source import (
    CommonConfiguration,
    MachineConfiguration,
    MeasurementConfiguration,
    Settings,
)


def test_settings_init():
    settings = Settings(
        hello="world",
        sub=dict(
            lausanne="VD",
            inner=dict(
                bern="BE",
            ),
        ),
    )

    assert isinstance(settings, Settings)
    assert isinstance(settings["sub"], Settings)
    assert isinstance(settings["sub"]["inner"], Settings)

    settings = Settings(
        None,
        hello="world",
        sub=dict(
            lausanne="VD",
            inner=dict(
                bern="BE",
            ),
        ),
    )
    assert settings == {
        "hello": "world",
        "sub": {
            "lausanne": "VD",
            "inner": {
                "bern": "BE",
            },
        },
    }

    settings = Settings(
        (
            ("yellow", "red"),
            ("green", "blue"),
        ),
    )
    assert settings == {
        "yellow": "red",
        "green": "blue",
    }


def test_settings_copy():
    settings = Settings(
        hello="world",
        sub=dict(
            lausanne="VD",
            inner=dict(
                bern="BE",
            ),
        ),
    )

    settings2 = Settings(settings)
    assert settings is not settings2
    assert settings == settings2
    assert settings["sub"] is not settings2["sub"]
    assert settings["sub"] == settings2["sub"]
    assert settings["sub"]["inner"] is not settings2["sub"]["inner"]
    assert settings["sub"]["inner"] == settings2["sub"]["inner"]


def test_settings_patterns():
    settings = Settings(
        hello="world{shape}",
        sub=dict(
            lausanne="{vd}{canton}",
            inner=dict(
                bern="{be}{canton}",
            ),
        ),
    )

    orig_params = dict(
        shape=" is round",
        canton=" is a canton",
        vd="Vaud",
        be="Bern",
        dummy="I am not used in the test",
    )

    for param_builder in (
        lambda params: dict(params),
        lambda params: Settings(params),
        lambda params: Settings(**params),
    ):
        params = param_builder(orig_params)
        assert params is not orig_params
        assert len(params) == len(orig_params)

        values = settings.apply(params)

        assert isinstance(values, Settings.InterpolatedSettings)
        assert isinstance(values["sub"], Settings.InterpolatedSettings)
        assert isinstance(
            values["sub"]["inner"], Settings.InterpolatedSettings
        )

        assert len(values) == len(settings)
        assert len(values["sub"]) == len(settings["sub"])
        assert len(values["sub"]["inner"]) == len(settings["sub"]["inner"])

        assert values["hello"] == "world is round"
        assert values["sub"]["lausanne"] == "Vaud is a canton"
        assert values["sub"]["inner"]["bern"] == "Bern is a canton"


def load_yaml(text):
    if not isinstance(text, str):
        with open(text, "rt") as f:
            text = f.read()
    return yaml.load(text, yaml.Loader)


@pytest.fixture
def common_configuration_1_text():
    return textwrap.dedent(
        """
    identifier: cc1
    url:
        "https://gradesens.com/{zone}/{machine.identifier}\\
        /{device}/{measurement.identifier}"
    zone: area42
    query_string:
        hello: "{region}@world"
    headers:
        head: oval
        fingers: count_{finger_count}
    """
    )


@pytest.fixture
def common_configuration_2_text():
    return textwrap.dedent(
        """
    identifier: cc2
    zone: Connecticut
    """
    )


@pytest.fixture
def machine_configuration_1_text():
    return textwrap.dedent(
        """
    identifier: mach1
    common_configuration: cc1
    measurements:
        -   identifier: temperature
            common_configuration_identifier: cc2
            query_string:
                depth: "12"
                param: P_{param}_xx

        -   identifier: rpm
            url:
                "https://gradesens.com/{zone}/{machine.identifier}\\
                /{device}/RPM/{measurement.identifier}"

        -   identifier: power
            common_configuration_identifier: cc2
            headers:
                animal: cow
    """
    )


@pytest.fixture
def common_configuration_1(common_configuration_1_text):
    params = load_yaml(common_configuration_1_text)
    return CommonConfiguration(**params)


@pytest.fixture
def common_configuration_2(common_configuration_2_text):
    params = load_yaml(common_configuration_2_text)
    return CommonConfiguration(**params)


@pytest.fixture
def machine_configuration_1(machine_configuration_1_text):
    params = load_yaml(machine_configuration_1_text)
    return MachineConfiguration(**params)


def test_common_configuration(common_configuration_1):
    assert isinstance(common_configuration_1, CommonConfiguration)
    assert common_configuration_1 == {
        "identifier": "cc1",
        "url": (
            "https://gradesens.com/{zone}/{machine.identifier}"
            "/{device}/{measurement.identifier}"
        ),
        "zone": "area42",
        "query_string": {
            "hello": "{region}@world",
        },
        "headers": {
            "head": "oval",
            "fingers": "count_{finger_count}",
        },
    }


def test_machine_configuration(machine_configuration_1):
    assert isinstance(machine_configuration_1, MachineConfiguration)
    for identifier, conf in machine_configuration_1["measurements"].items():
        assert isinstance(conf, MeasurementConfiguration)
        assert identifier == conf["identifier"]

    assert machine_configuration_1 == {
        "identifier": "mach1",
        "common_configuration": "cc1",
        "authentication_context_identifier": None,
        "url": None,
        "query_string": {},
        "headers": {},
        "common_configuration_identifier": None,
        "measurements": {
            "temperature": {
                "identifier": "temperature",
                "common_configuration_identifier": "cc2",
                "authentication_context_identifier": None,
                "url": None,
                "query_string": {
                    "depth": "12",
                    "param": "P_{param}_xx",
                },
                "headers": {},
            },
            "rpm": {
                "identifier": "rpm",
                "common_configuration_identifier": None,
                "authentication_context_identifier": None,
                "url": (
                    "https://gradesens.com/{zone}/{machine.identifier}"
                    "/{device}/RPM/{measurement.identifier}"
                ),
                "query_string": {},
                "headers": {},
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
