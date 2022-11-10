import textwrap

import pytest
import yaml

from gradesens.moonstone_external_source import (
    AuthenticationContext,
    CommonConfiguration,
    MachineConfiguration,
    MeasurementConfiguration,
    Settings,
)

from .utils import assert_eq_dicts


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

        for value in (
            values,
            values["sub"],
            values["sub"]["inner"],
        ):
            assert isinstance(value, dict)
            assert not isinstance(value, Settings)

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
def authentication_context_1_text():
    return textwrap.dedent(
        """
    identifier: ac1
    token: I am a secret
    """
    )


@pytest.fixture
def common_configuration_1_text():
    return textwrap.dedent(
        """
    identifier: cc1
    authentication_context_identifier: ac1
    url:
        "https://gradesens.com/{zone}/{machine}/{device}/{measurement}"
    zone: area42
    query_string:
        HELLO: "{region}@world"
    headers:
        head: oval
        fingers: "count_{finger_count}"
        bearer: "{token}"
    device: "best device ever"
    """
    )


@pytest.fixture
def common_configuration_2_text():
    return textwrap.dedent(
        """
    identifier: cc2
    zone: Connecticut
    param: I am a parameter
    device: "better than cc1 device"
    headers:
        hello: world
    """
    )


@pytest.fixture
def machine_configuration_1_text():
    return textwrap.dedent(
        """
    identifier: mach1
    common_configuration_identifier: cc1
    finger_count: 5
    url:
        "https://gradesens.com/{zone}/MACHINE/{machine}/{device}/{measurement}"
    measurements:
        -   identifier: temperature
            common_configuration_identifier: cc2
            query_string:
                depth: "12"
                width: P_{param}_xx

        -   identifier: rpm
            region: Wallis
            url:
                "https://gradesens.com/{zone}/{machine}\\
                /{device}/RPM/{measurement}"
            query_string:
                dune: worms

        -   identifier: power
            common_configuration_identifier: cc2
            headers:
                animal: cow
    """
    )


@pytest.fixture
def authentication_context_1(authentication_context_1_text):
    params = load_yaml(authentication_context_1_text)
    return AuthenticationContext(**params)


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


@pytest.fixture
def common_configuration_ld(
    common_configuration_1,
    common_configuration_2,
):
    items = {
        item["identifier"]: item
        for item in [
            common_configuration_1,
            common_configuration_2,
        ]
    }

    class CommonConfigurationLD(CommonConfiguration.LoadDriver):
        __items = dict(items)

        async def load(
            self, identifier: CommonConfiguration.Identifier
        ) -> CommonConfiguration:
            return self.__items[identifier]

    CommonConfiguration.register_load_driver(CommonConfigurationLD())


@pytest.fixture
def authentication_context_ld(
    authentication_context_1,
):
    items = {
        item["identifier"]: item
        for item in [
            authentication_context_1,
        ]
    }

    class AuthenticationContextLD(AuthenticationContext.LoadDriver):
        __items = dict(items)

        async def load(
            self, identifier: AuthenticationContext.Identifier
        ) -> AuthenticationContext:
            return self.__items[identifier]

    AuthenticationContext.register_load_driver(AuthenticationContextLD())


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


@pytest.mark.asyncio
async def test_interpolated_measurement_settings(
    machine_configuration_1,
    common_configuration_ld,
    authentication_context_ld,
):
    assert isinstance(machine_configuration_1, MachineConfiguration)
    settings = await machine_configuration_1.get_measurement_settings(
        "temperature"
    )
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


@pytest.mark.asyncio
async def test_interpolated_measurement_all_settings(
    machine_configuration_1,
    common_configuration_ld,
    authentication_context_ld,
):
    assert isinstance(machine_configuration_1, MachineConfiguration)
    settings = await machine_configuration_1.get_all_measurement_settings()
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
