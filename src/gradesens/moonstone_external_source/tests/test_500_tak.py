from datetime import datetime

import pytest

from gradesens.moonstone_external_source import (
    AuthenticationConfiguration,
    IODriver,
    IOManager,
    MachineConfiguration,
)

from .configuration_fixtures import load_yaml

# from .utils import to_basic_types


@pytest.fixture
def authentication_configuration_oauth2_password():
    params = load_yaml(
        """
    id: oauth2:password

    request:
        headers:
            client_id: "{client_id}"
            client_secret: "{client_secret}"
        data:
            username: "{username}"
            password: "{password}"
            grant_type: password
    result:
        token: "{access_token}"
        expiration_at:
            <process:
                - eval: "datetime.now() + timedelta(seconds=expires_in)"
    """
    )
    return AuthenticationConfiguration(**params)


@pytest.fixture
def authentication_configuration_tak_client():
    params = load_yaml(
        """
    id: "tak-dev:client"

    client_id: e907a1bb2b364845b1a9dd0c87554c88
    client_secret: c50Be97912A5447eA2678295316a1eF4
    """
    )
    return AuthenticationConfiguration(**params)


@pytest.fixture
def authentication_configuration_tak_credentials():
    params = load_yaml(
        """
    id: "tak-dev:creds"

    username: SVC-1119901
    password: nDOTIa1faQmQHNBsu9KZ
    """
    )
    return AuthenticationConfiguration(**params)


@pytest.fixture
def authentication_configuration_tak_dev():
    params = load_yaml(
        """
    id: tak-dev
    authentication_configuration_ids:
        - "oauth2:password"
        - "tak-dev:client"
        - "tak-dev:creds"

    env: dev

    request:
        url:
            "https://api-us-aws2.takeda.com/sail-proxy-sys/api/security/\\
            oauth2/token"
        headers:
            env: "{env}"
        data:
            authority: ad
    result:
        env: "{env}"
    """
    )
    return AuthenticationConfiguration(**params)


@pytest.fixture
def common_configuration_tak_dev_1():
    params = load_yaml(
        """
    id: tak-common

    request:
        authentication_configuration_id: tak-dev
        time_margin: 2m

        url:
            "https://api-us-aws2.takeda.com/sail-proxy-sys/api/v2/\
            execfunction/tak-clarityenergy/api/aggResponse"

        query_string:
            start: {request.start_time}
            end: {request.end_time}
            item:
                "/System/Core/CHQNC-Relay/CHQNC/\
                Redundant items for vibration monitoring/\
                ROLoop(ZU4)/RC11/ZU4_RC11_PUC260_temperature"
            secp: iwa

        headers:
            client_id: "{request.authentication.client_id}"
            client_secret: "{request.authentication.client_secret}"
            env: "{request.authentication.env}"

    result:
        timestamp: "{ts}"

    measurements:
        -   id: result
            value:
                <process:
                    -   type:
                            target: bool
                            input_key: v
    """
    )
    return MachineConfiguration(**params)


@pytest.fixture
def machine_configuration_tak_dev_1():
    params = load_yaml(
        """
    id: tak-mach-1
    common_configuration_ids: tak-common
    """
    )
    return MachineConfiguration(**params)


@pytest.fixture
def io_driver_tak_dev(
    authentication_configuration_oauth2_password,
    authentication_configuration_tak_client,
    authentication_configuration_tak_credentials,
    authentication_configuration_tak_dev,
    #
    common_configuration_tak_dev_1,
    #
    machine_configuration_tak_dev_1,
):
    class TestIODriver(IODriver):
        def __init__(
            self,
            *args,
            authentication_configurations,
            machine_configurations,
            **kwargs
        ):
            super().__init__(*args, **kwargs)
            self.__authentication_configurations = {
                item["id"]: item for item in authentication_configurations
            }

            self.__machine_configurations = {
                item["id"]: item for item in machine_configurations
            }

        async def load_authentication_configuration(
            self, id: AuthenticationConfiguration.Id
        ) -> AuthenticationConfiguration:
            return self.__authentication_configurations[id]

        async def load_machine_configuration(
            self, id: MachineConfiguration.Id
        ) -> MachineConfiguration:
            return self.__machine_configurations[id]

    return TestIODriver(
        authentication_configurations=[
            authentication_configuration_oauth2_password,
            authentication_configuration_tak_client,
            authentication_configuration_tak_credentials,
            authentication_configuration_tak_dev,
        ],
        machine_configurations=[
            common_configuration_tak_dev_1,
            machine_configuration_tak_dev_1,
        ],
    )


@pytest.fixture
def io_manager_tak_dev(io_driver_tak_dev):
    return IOManager(
        io_driver_tak_dev,
    )


@pytest.mark.asyncio
async def test_authentication(io_manager_tak_dev):
    async with io_manager_tak_dev.client_session() as client_session:
        auth_context = await client_session.authentication_contexts.get(
            "tak-dev"
        )
    assert set(auth_context.keys()) == {
        "token",
        "expiration_at",
        "env",
    }

    assert isinstance(auth_context["expiration_at"], datetime)
    assert auth_context["expiration_at"] > datetime.now()

    assert auth_context["env"] == "dev"

    async with io_manager_tak_dev.client_session() as client_session:
        _ = await client_session.machine_configurations.get("tak-mach-1")
        # mach = await mach.get_interpolated_settings(
        #    client_session=client_session
        # )

        # import json


#        print(json.dumps(to_basic_types(mach), indent=2))
