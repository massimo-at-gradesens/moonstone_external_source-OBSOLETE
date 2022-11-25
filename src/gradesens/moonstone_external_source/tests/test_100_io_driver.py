import pytest

from gradesens.moonstone_external_source import MachineConfiguration


@pytest.mark.usefixtures("io_manager_1")
@pytest.mark.asyncio
async def test_io_manager_cache(
    io_manager_1,
):
    async with io_manager_1.client_session() as client_session:
        assert client_session.io_driver.machine_configuration_load_count == 0

        mach_conf_1 = await client_session.machine_configurations.get("mach1")
        assert client_session.io_driver.machine_configuration_load_count == 1
        assert isinstance(mach_conf_1, MachineConfiguration)

        mach_conf_1b = await client_session.machine_configurations.get("mach1")
        assert client_session.io_driver.machine_configuration_load_count == 1
        assert mach_conf_1b is mach_conf_1

        mach_conf_2 = await client_session.machine_configurations.get("mach2")
        assert client_session.io_driver.machine_configuration_load_count == 2
        assert isinstance(mach_conf_2, MachineConfiguration)

        mach_conf_1b = await client_session.machine_configurations.get("mach1")
        assert client_session.io_driver.machine_configuration_load_count == 2
        assert mach_conf_1b is mach_conf_1

        mach_conf_2b = await client_session.machine_configurations.get("mach2")
        assert client_session.io_driver.machine_configuration_load_count == 2
        assert mach_conf_2b is mach_conf_2


@pytest.mark.usefixtures("io_manager_1")
@pytest.mark.asyncio
async def test_io_manager_token_expiration(
    io_manager_1,
):
    async with io_manager_1.client_session() as client_session:
        assert (
            client_session.io_driver.authorization_configuration_load_count
            == 0
        )
        assert client_session.authorization_contexts.cache.load_count == 0

        await client_session.authorization_contexts.get("ac1")
        assert (
            client_session.io_driver.authorization_configuration_load_count
            == 1
        )
        assert client_session.authorization_contexts.cache.load_count == 1

        await client_session.authorization_contexts.get("ac1")
        assert (
            client_session.io_driver.authorization_configuration_load_count
            == 1
        )
        assert client_session.authorization_contexts.cache.load_count == 1

        await client_session.authorization_contexts.get("ac-expired")
        assert (
            client_session.io_driver.authorization_configuration_load_count
            == 2
        )
        assert client_session.authorization_contexts.cache.load_count == 2

        await client_session.authorization_contexts.get("ac-expired")
        assert (
            client_session.io_driver.authorization_configuration_load_count
            == 2
        )
        assert client_session.authorization_contexts.cache.load_count == 3
