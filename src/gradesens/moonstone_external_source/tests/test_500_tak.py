from datetime import datetime, timezone

import pytest

from gradesens.moonstone_external_source import (
    AuthorizationConfiguration,
    DateTime,
    ExternalSourceSession,
    IODriver,
    IOManager,
    MachineConfiguration,
    Settings,
)

from .configuration_fixtures import load_yaml


@pytest.fixture
def authorization_configuration_oauth2_password():
    return load_yaml(
        """
    id: common:oauth2:password

    request:
        headers:
            client_id: "{client_id}"
            client_secret: "{client_secret}"
        data:
            username: "{username}"
            password: "{password}"
            grant_type: password
    result:
        access_token: "{access_token}"
        token_type: "{token_type}"
        expiration_at:
            <process:
                - eval: "datetime.now() + timedelta(seconds=expires_in)"
    """
    )


@pytest.fixture
def common_configuration_oauth2_bearer():
    return load_yaml(
        """
    id: common:oauth2:bearer

    request:
        headers:
            Authorization: "\\
                {request.authorization.token_type} \\
                {request.authorization.access_token}"
    """
    )


@pytest.fixture
def authorization_configuration_tak_client():
    return load_yaml(
        """
    id: "tak:dev:client"

    client_id: e907a1bb2b364845b1a9dd0c87554c88
    client_secret: c50Be97912A5447eA2678295316a1eF4
    """
    )


@pytest.fixture
def authorization_configuration_tak_credentials():
    return load_yaml(
        """
    id: "tak:dev:creds"

    username: SVC-1119901
    password: nDOTIa1faQmQHNBsu9KZ
    """
    )


@pytest.fixture
def authorization_configuration_tak_dev():
    return load_yaml(
        """
    id: tak:dev
    authorization_configuration_ids:
        - "common:oauth2:password"
        - "tak:dev:client"
        - "tak:dev:creds"

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
        client_id: "{client_id}"
        client_secret: "{client_secret}"
    """
    )


@pytest.fixture
def common_configuration_tak():
    return load_yaml(
        """
    id: tak:common
    machine_configuration_ids:
        - common:oauth2:bearer

    customer:
        machine_type_to_description:
            WH: WFIHotLoop
            WC: WFIHotLoop
            RC: ROLoop
            WW: WFIDistillator

    request:
        query_string:
            start: "{request.start_time.z_utc_format(timespec='microseconds')}"
            end: "{request.end_time.z_utc_format(timespec='microseconds')}"
            item:
                "/System/Core/CHQNC-Relay/CHQNC\\
                /Redundant items for vibration monitoring\\
                /{customer.machine_type_to_description[customer.machine_type]}\\
                ({customer.utility_unit})\\
                /{customer.machine_type}{customer.machine_id}\\
                /{customer.utility_unit}\\
                _{customer.machine_type}{customer.machine_id}\\
                _{customer.device_type}{customer.device_id}\\
                _{measurement_id}"
            secp: iwa

        headers:
            client_id: "{request.authorization.client_id}"
            client_secret: "{request.authorization.client_secret}"
            env: "{request.authorization.env}"

    result:
        timestamp:
            <process:
                type: datetime
                input_key: ts

    measurements:
        # run:
        #     result:
        #         value:
        #             <process:
        #                 type:
        #                     input_key: v
        #                     target: bool
        # power:
        #     result:
        #         value:
        #             <process:
        #                 type:
        #                     input_key: v
        #                     target: float
        temperature:
            result:
                value:
                    <process:
                        type:
                            input_key: v
                            target: float
                            allow_none: yes
        # rpm:
        #     result:
        #         value:
        #             <process:
        #                 type:
        #                     input_key: v
        #                     target: float
    """
    )


@pytest.fixture
def common_configuration_tak_raw():
    return load_yaml(
        """
    id: tak:common:raw-request

    request:
        time_margin: 10s

        url: "{customer.base_url}/readrawhistory"
    """
    )


@pytest.fixture
def common_configuration_tak_aggregate():
    return load_yaml(
        """
    id: tak:common:aggregate-request

    request:
        time_margin: 1m

        url: "{customer.base_url}/aggResponse"
    """
    )


@pytest.fixture
def common_configuration_tak_dev():
    return load_yaml(
        """
    id: tak:dev:common

    machine_configuration_ids:
        - tak:common

    customer:
        base_url:
            "https://api-us-aws2.takeda.com/sail-proxy-sys/api/v2\\
            /execfunction/tak-clarityenergy/api"

    request:
        authorization_configuration_id: tak:dev
    """
    )


@pytest.fixture
def common_configuration_tak_raw_dev():
    return load_yaml(
        """
    id: tak:dev:common:raw-request

    machine_configuration_ids:
        - tak:dev:common
        - tak:common:raw-request
    """
    )


@pytest.fixture
def common_configuration_tak_aggregate_dev():
    return load_yaml(
        """
    id: tak:dev:common:aggregate-request

    machine_configuration_ids:
        - tak:dev:common
        - tak:common:aggregate-request
    """
    )


@pytest.fixture
def machine_configuration_tak_mach_1_dev():
    return load_yaml(
        """
    id: tak:dev:mach-1
    machine_configuration_ids: tak:dev:common:aggregate-request

    customer:
        utility_unit: ZU4
        machine_type: "RC"
        machine_id: "11"
        device_type: "PUC"
        device_id: "260"
    """
    )


@pytest.fixture
def io_driver_tak_dev(
    authorization_configuration_oauth2_password,
    authorization_configuration_tak_client,
    authorization_configuration_tak_credentials,
    authorization_configuration_tak_dev,
    #
    common_configuration_oauth2_bearer,
    #
    common_configuration_tak,
    common_configuration_tak_raw,
    common_configuration_tak_aggregate,
    common_configuration_tak_dev,
    common_configuration_tak_raw_dev,
    common_configuration_tak_aggregate_dev,
    #
    machine_configuration_tak_mach_1_dev,
):
    class TestIODriver(IODriver):
        def __init__(
            self,
            *args,
            authorization_configurations,
            machine_configurations,
            **kwargs
        ):
            super().__init__(*args, **kwargs)
            self.__authorization_configurations = {
                item["id"]: AuthorizationConfiguration(**item)
                for item in authorization_configurations
            }

            self.__machine_configurations = {
                item["id"]: MachineConfiguration(**item)
                for item in machine_configurations
            }

        async def load_authorization_configuration(
            self, id: AuthorizationConfiguration.Id
        ) -> AuthorizationConfiguration:
            return self.__authorization_configurations[id]

        async def load_machine_configuration(
            self, id: MachineConfiguration.Id
        ) -> MachineConfiguration:
            return self.__machine_configurations[id]

    return TestIODriver(
        authorization_configurations=[
            authorization_configuration_oauth2_password,
            authorization_configuration_tak_client,
            authorization_configuration_tak_credentials,
            authorization_configuration_tak_dev,
        ],
        machine_configurations=[
            common_configuration_oauth2_bearer,
            #
            common_configuration_tak,
            common_configuration_tak_raw,
            common_configuration_tak_aggregate,
            common_configuration_tak_dev,
            common_configuration_tak_raw_dev,
            common_configuration_tak_aggregate_dev,
            #
            machine_configuration_tak_mach_1_dev,
        ],
    )


@pytest.fixture
def io_manager_tak_dev(io_driver_tak_dev):
    return IOManager(
        io_driver_tak_dev,
    )


@pytest.mark.asyncio
async def test_authorization(
    io_manager_tak_dev,
    authorization_configuration_tak_dev,
    authorization_configuration_tak_client,
):
    async with io_manager_tak_dev.client_session() as client_session:
        auth_context = await client_session.authorization_contexts.get(
            "tak:dev"
        )
    assert set(auth_context.keys()) == {
        "access_token",
        "token_type",
        "expiration_at",
        "env",
        "client_id",
        "client_secret",
    }

    assert isinstance(auth_context["expiration_at"], datetime)
    assert auth_context["expiration_at"] > datetime.now()

    assert auth_context["env"] == authorization_configuration_tak_dev["env"]
    assert (
        auth_context["client_id"]
        == authorization_configuration_tak_client["client_id"]
    )
    assert (
        auth_context["client_secret"]
        == authorization_configuration_tak_client["client_secret"]
    )

    async with io_manager_tak_dev.client_session() as client_session:
        mach = await client_session.machine_configurations.get(
            "tak:dev:mach-1"
        )
        mach_settings = await mach.get_interpolated_settings(
            client_session=client_session,
            timestamp=datetime.now(timezone.utc),
        )
        assert isinstance(mach_settings, Settings.InterpolatedSettings)
        # import json
        # from .utils import to_basic_types
        # print(json.dumps(to_basic_types(mach_settings), indent=2))


@pytest.mark.asyncio
async def test_extenal_source(io_manager_tak_dev):
    async with io_manager_tak_dev.client_session() as client_session:
        async with ExternalSourceSession(client_session) as es_session:
            result = await es_session.get_data(
                machine_id="tak:dev:mach-1",
                timestamps=[
                    DateTime("2022-07-25T00:01:00+00:00"),
                ],
            )
            print(result)
