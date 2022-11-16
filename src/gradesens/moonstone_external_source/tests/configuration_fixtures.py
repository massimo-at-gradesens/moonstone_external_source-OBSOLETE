import textwrap

import pytest
import yaml

from gradesens.moonstone_external_source import (
    AuthenticationContext,
    CommonConfiguration,
    IODriver,
    MachineConfiguration,
)


def load_yaml(text):
    if isinstance(text, str):
        text = textwrap.dedent(text)
    else:
        with open(text, "rt") as f:
            text = f.read()
    return yaml.load(text, yaml.Loader)


@pytest.fixture
def authentication_context_1():
    params = load_yaml(
        """
    identifier: ac1
    token: I am a secret
    """
    )
    return AuthenticationContext(**params)


@pytest.fixture
def common_configuration_1():
    params = load_yaml(
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
    return CommonConfiguration(**params)


@pytest.fixture
def common_configuration_2():
    params = load_yaml(
        """
    identifier: cc2
    zone: Connecticut
    param: I am a parameter
    device: "better than cc1 device"
    headers:
        hello: world
    """
    )
    return CommonConfiguration(**params)


@pytest.fixture
def machine_configuration_1():
    params = load_yaml(
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
    return MachineConfiguration(**params)


@pytest.fixture
def machine_configuration_2():
    params = load_yaml(
        """
    identifier: mach2
    key: "hello"
    another_key: "THREE"
    mapping:
        hello: world
        one: tw0
        three: FouR
    measurements:
        -   identifier: temperature
            url: "this is a 4: {mapping[another_key.lower()]}"
            query_string:
                depth: ":: {mapping[key]} :: {mapping['one']} ::"
                plain: I am a plain string
    """
    )
    return MachineConfiguration(**params)


@pytest.fixture
def machine_configuration_with_time():
    params = load_yaml(
        """
    identifier: mach_w_time
    measurements:
        -   identifier: temperature
            query_string:
                start: "{start_time.isoformat()}"
                end: "{end_time.isoformat()}"
    """
    )
    return MachineConfiguration(**params)


@pytest.fixture
def common_configuration_with_result():
    params = load_yaml(
        r"""
    identifier: cc_w_result
    measurements:
        -   identifier: temperature
            result:
                value:
                    type: float
                    regular_expression:
                        pattern: "^(.*)$"
                        replacement: ">>\\1<<"
        -   identifier: rpm
            result:
                timestamp:
                    raw_value: "{out_field1}"
    """
    )
    return CommonConfiguration(**params)


@pytest.fixture
def machine_configuration_with_result():
    params = load_yaml(
        r"""
    identifier: mach_w_result
    common_configuration_identifier: cc_w_result
    result:
        timestamp:
            regular_expression:
                pattern: "(?P<hello>[a-g]+)"
                flags: i
                replacement: "YX \\g<hello> XY"
        value:
            raw_value: "{get}{the}{raw}"
    measurements:
        -   identifier: temperature
            result:
                value:
                    type: float
        -   identifier: rpm
            result:
                timestamp:
                    regular_expression:
                        -   pattern: "(.*)"
                            replacement: "\\1\\1"
                        -   pattern: "(.*)([0-9a-f]+)(.*)"
                            replacement: "\\3\\1"
        -   identifier: humidity
            result:
                timestamp:
                    raw_value: "{out_field2}"
    """
    )
    return MachineConfiguration(**params)


@pytest.fixture
def io_driver_1(
    authentication_context_1,
    common_configuration_1,
    common_configuration_2,
    common_configuration_with_result,
    machine_configuration_1,
    machine_configuration_2,
    machine_configuration_with_time,
    machine_configuration_with_result,
):
    class TestIODriver(IODriver):
        def __init__(
            self,
            *args,
            authentication_contexts,
            common_configurations,
            machine_configurations,
            **kwargs
        ):
            super().__init__(*args, **kwargs)
            self.__authentication_contexts = {
                item["identifier"]: item for item in authentication_contexts
            }

            self.__common_configurations = {
                item["identifier"]: item for item in common_configurations
            }

            self.__machine_configurations = {
                item["identifier"]: item for item in machine_configurations
            }

            self.authentication_context_load_count = 0
            self.common_configuration_load_count = 0
            self.machine_configuration_load_count = 0

        async def load_authentication_context(
            self, identifier: AuthenticationContext.Identifier
        ) -> AuthenticationContext:
            self.authentication_context_load_count += 1
            return self.__authentication_contexts[identifier]

        async def load_common_configuration(
            self, identifier: CommonConfiguration.Identifier
        ) -> CommonConfiguration:
            self.common_configuration_load_count += 1
            return self.__common_configurations[identifier]

        async def load_machine_configuration(
            self, identifier: MachineConfiguration.Identifier
        ) -> MachineConfiguration:
            self.machine_configuration_load_count += 1
            return self.__machine_configurations[identifier]

    return TestIODriver(
        authentication_contexts=[
            authentication_context_1,
        ],
        common_configurations=[
            common_configuration_1,
            common_configuration_2,
            common_configuration_with_result,
        ],
        machine_configurations=[
            machine_configuration_1,
            machine_configuration_2,
            machine_configuration_with_time,
            machine_configuration_with_result,
        ],
    )
