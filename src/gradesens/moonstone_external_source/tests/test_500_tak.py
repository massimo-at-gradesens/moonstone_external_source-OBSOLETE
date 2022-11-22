from datetime import datetime

import pytest

from gradesens.moonstone_external_source import (
    AuthenticationConfiguration,
    CommonConfiguration,
    IODriver,
    IOManager,
    MachineConfiguration,
)

from .configuration_fixtures import load_yaml


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
        - "tak-dev:client"
        - "tak-dev:creds"

    env: dev

    request:
        url:
            "https://api-us-aws2.takeda.com/sail-proxy-sys/api/security/\\
            oauth2/token"
        headers:
            client_id: "{client_id}"
            client_secret: "{client_secret}"
            env: "{env}"
        data:
            username: "{username}"
            password: "{password}"
            grant_type: password
            authority: ad
    result:
        token: "{access_token}"
        expiration_at:
            <process:
                eval: "datetime.now() + timedelta(seconds=expires_in)"
        env: "{env}"
    """
    )
    return AuthenticationConfiguration(**params)


@pytest.fixture
def io_driver_tak_dev(
    authentication_configuration_tak_client,
    authentication_configuration_tak_credentials,
    authentication_configuration_tak_dev,
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

        async def load_authentication_configuration(
            self, id: AuthenticationConfiguration.Id
        ) -> AuthenticationConfiguration:
            return self.__authentication_configurations[id]

        async def load_common_configuration(
            self, id: CommonConfiguration.Id
        ) -> CommonConfiguration:
            return self.__common_configurations[id]

        async def load_machine_configuration(
            self, id: MachineConfiguration.Id
        ) -> MachineConfiguration:
            return self.__machine_configurations[id]

    return TestIODriver(
        authentication_configurations=[
            authentication_configuration_tak_client,
            authentication_configuration_tak_credentials,
            authentication_configuration_tak_dev,
        ],
        common_configurations=[],
        machine_configurations=[],
    )


@pytest.fixture
def io_manager_tak_dev(io_driver_tak_dev):
    return IOManager(
        io_driver_tak_dev,
    )


@pytest.mark.asyncio
async def test_authentication(io_manager_tak_dev):
    auth_context = await io_manager_tak_dev.authentication_contexts.get(
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
