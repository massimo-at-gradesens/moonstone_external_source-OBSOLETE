import pytest

from gradesens.moonstone_external_source import (
    AuthenticationConfiguration,
    AuthenticationContext,
    CommonConfiguration,
    IODriver,
    IOManager,
    MachineConfiguration,
)
from gradesens.moonstone_external_source.io_manager import (
    AuthenticationContextCache,
)

from .utils import load_yaml


@pytest.fixture
def authentication_context_1():
    params = load_yaml(
        """
    token: I am a secret
    expires_in: 100
    """
    )
    return AuthenticationContext(**params)


@pytest.fixture
def authentication_configuration_1(authentication_context_1):
    params = load_yaml(
        """
    id: ac1
    """
    )

    class CustomAuthenticationConfiguration(AuthenticationConfiguration):
        async def authenticate(
            self,
            io_manager: IOManager,
        ) -> AuthenticationContext:
            return authentication_context_1

    return CustomAuthenticationConfiguration(**params)


@pytest.fixture
def authentication_context_expired():
    params = load_yaml(
        """
    token: I am another secret
    expires_in: -1
    """
    )
    return AuthenticationContext(**params)


@pytest.fixture
def authentication_configuration_expired(authentication_context_expired):
    params = load_yaml(
        """
    id: ac-expired
    """
    )

    class CustomAuthenticationConfiguration(AuthenticationConfiguration):
        async def authenticate(
            self,
            io_manager: IOManager,
        ) -> AuthenticationContext:
            return authentication_context_expired

    return CustomAuthenticationConfiguration(**params)


@pytest.fixture
def common_configuration_1():
    params = load_yaml(
        """
    id: cc1
    authentication_configuration_id: ac1
    zone: area42
    request:
        url:
            "https://gradesens.com/{zone}/{machine_id}/\\
            {device}/{measurement_id}"
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
    id: cc2
    zone: Connecticut
    param: I am a parameter
    device: "better than cc1 device"
    request:
        headers:
            hello: world
    """
    )
    return CommonConfiguration(**params)


@pytest.fixture
def machine_configuration_1():
    params = load_yaml(
        """
    id: mach1
    common_configuration_ids: cc1
    finger_count: 5
    region: basel
    request:
        url:
            "https://gradesens.com/{zone}/MACHINE/{machine_id}\\
            /{device}/{measurement_id}"
    measurements:
        -   id: temperature
            region: zurich
            common_configuration_ids: cc2
            request:
                query_string:
                    depth: "12"
                    width: P_{param}_xx

        -   id: rpm
            region: Wallis
            request:
                url:
                    "https://gradesens.com/{zone}/{machine_id}\\
                    /{device}/RPM/{measurement_id}"
                query_string:
                    dune: worms

        -   id: power
            common_configuration_ids: cc2
            request:
                headers:
                    animal: cow
    """
    )
    return MachineConfiguration(**params)


@pytest.fixture
def machine_configuration_2():
    params = load_yaml(
        """
    id: mach2
    key: "hello"
    another_key: "THREE"
    mapping:
        hello: world
        one: tw0
        three: FouR
    measurements:
        -   id: temperature
            request:
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
    id: mach_w_time
    measurements:
        -   id: temperature
            request:
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
    id: cc_w_result
    measurements:
        -   id: temperature
            result:
                value:
                    type: float
                    regular_expression:
                        pattern: "^(.*)$"
                        replacement: ">>\\1<<"
        -   id: rpm
            result:
                timestamp:
                    input: "{out_field1}"
    """
    )
    return CommonConfiguration(**params)


@pytest.fixture
def machine_configuration_with_result():
    params = load_yaml(
        r"""
    id: mach_w_result
    common_configuration_ids: cc_w_result
    result:
        timestamp:
            regular_expression:
                pattern: "^(|.*[^0-9])(?P<year>[0-9]+).*"
                flags: i
                replacement: "20\\g<year>-11-15"
        value:
            input: "{{get}}{{the}}{{raw}}"
    out_field2: 2022-11-19
    measurements:
        -   id: temperature
            result:
                value:
                    type: float
                timestamp:
                    input: "{{temp_ts_raw}}"
        -   id: rpm
            result:
                value:
                    type: int
                    regular_expression:
                        -   pattern: "^(.*)$"
                            replacement: "0x\\1"
                        -   pattern: "[83]"
                            replacement: "7"
                timestamp:
                    input: 23
                    regular_expression:
                        -   pattern: "^(.*)$"
                            replacement: "17-\\1\\1-08"
                        -   pattern: "^(?P<d>.*)-(?P<y>.*)-(?P<m>.*)$"
                            replacement: "\\g<y>-\\g<m>-\\g<d>"
        -   id: humidity
            result:
                timestamp:
                    regular_expression:
                    input: "{out_field2}"
    """
    )
    return MachineConfiguration(**params)


@pytest.fixture
def common_configuration_nested_1():
    params = load_yaml(
        """
    id: cc1-n
    zone: area42
    request:
        url:
            "123"
        query_string:
            hello: world
            east: west
        headers:
            head: oval
    """
    )
    return CommonConfiguration(**params)


@pytest.fixture
def common_configuration_nested_2():
    params = load_yaml(
        """
    id: cc2-n
    request:
        query_string:
            hello: moon
            south: north
        headers:
            square: four sides
            circle: round
    measurements:
        -   id: temperature
            request:
                headers:
                    bicycle: two wheels
                    token: super secret
    """
    )
    return CommonConfiguration(**params)


@pytest.fixture
def common_configuration_nested_3():
    params = load_yaml(
        """
    id: cc3-n
    common_configuration_ids:
        - cc1-n
        - cc2-n

    request:
        headers:
            circle: really ROUND
    measurements:
        -   id: temperature
            request:
                headers:
                    token: password
        -   id: rpm
    """
    )
    return CommonConfiguration(**params)


@pytest.fixture
def common_configuration_nested_4_loop():
    params = load_yaml(
        """
    id: cc4-n
    common_configuration_ids:
        - cc1-n
        - cc5-n
        - cc2-n
    """
    )
    return CommonConfiguration(**params)


@pytest.fixture
def common_configuration_nested_5_loop():
    params = load_yaml(
        """
    id: cc5-n
    common_configuration_ids:
        - cc4-n
    """
    )
    return CommonConfiguration(**params)


@pytest.fixture
def io_driver_1(
    authentication_configuration_1,
    authentication_configuration_expired,
    common_configuration_1,
    common_configuration_2,
    common_configuration_with_result,
    common_configuration_nested_1,
    common_configuration_nested_2,
    common_configuration_nested_3,
    common_configuration_nested_4_loop,
    common_configuration_nested_5_loop,
    machine_configuration_1,
    machine_configuration_2,
    machine_configuration_with_time,
    machine_configuration_with_result,
):
    class TestIODriver(IODriver):
        def __init__(
            self,
            *args,
            authentication_configurations,
            common_configurations,
            machine_configurations,
            **kwargs
        ):
            super().__init__(*args, **kwargs)
            self.__authentication_configurations = {
                item["id"]: item for item in authentication_configurations
            }

            self.__common_configurations = {
                item["id"]: item for item in common_configurations
            }

            self.__machine_configurations = {
                item["id"]: item for item in machine_configurations
            }

            self.authentication_configuration_load_count = 0
            self.common_configuration_load_count = 0
            self.machine_configuration_load_count = 0

        async def load_authentication_configuration(
            self, id: AuthenticationConfiguration.Id
        ) -> AuthenticationConfiguration:
            self.authentication_configuration_load_count += 1
            return self.__authentication_configurations[id]

        async def load_common_configuration(
            self, id: CommonConfiguration.Id
        ) -> CommonConfiguration:
            self.common_configuration_load_count += 1
            return self.__common_configurations[id]

        async def load_machine_configuration(
            self, id: MachineConfiguration.Id
        ) -> MachineConfiguration:
            self.machine_configuration_load_count += 1
            return self.__machine_configurations[id]

    return TestIODriver(
        authentication_configurations=[
            authentication_configuration_1,
            authentication_configuration_expired,
        ],
        common_configurations=[
            common_configuration_1,
            common_configuration_2,
            common_configuration_with_result,
            common_configuration_nested_1,
            common_configuration_nested_2,
            common_configuration_nested_3,
            common_configuration_nested_4_loop,
            common_configuration_nested_5_loop,
        ],
        machine_configurations=[
            machine_configuration_1,
            machine_configuration_2,
            machine_configuration_with_time,
            machine_configuration_with_result,
        ],
    )


@pytest.fixture
def io_manager_1(io_driver_1):
    class TestAuthenticationContextCache(AuthenticationContextCache):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.load_count = 0

        async def load_context(
            self, id: AuthenticationConfiguration.Id
        ) -> AuthenticationContext:
            self.load_count += 1
            return await super().load_context(id)

    return IOManager(
        io_driver_1,
        authentication_context_cache_factory=TestAuthenticationContextCache,
    )
