import pytest

from gradesens.moonstone_external_source import MachineConfiguration


@pytest.mark.usefixtures("io_manager_1")
@pytest.mark.asyncio
async def test_io_manager_cache(
    io_manager_1,
):
    assert io_manager_1.io_driver.machine_configuration_load_count == 0

    mach_conf_1 = await io_manager_1.machine_configurations.get("mach1")
    assert io_manager_1.io_driver.machine_configuration_load_count == 1
    assert isinstance(mach_conf_1, MachineConfiguration)

    mach_conf_1b = await io_manager_1.machine_configurations.get("mach1")
    assert io_manager_1.io_driver.machine_configuration_load_count == 1
    assert mach_conf_1b is mach_conf_1

    mach_conf_2 = await io_manager_1.machine_configurations.get("mach2")
    assert io_manager_1.io_driver.machine_configuration_load_count == 2
    assert isinstance(mach_conf_2, MachineConfiguration)

    mach_conf_1b = await io_manager_1.machine_configurations.get("mach1")
    assert io_manager_1.io_driver.machine_configuration_load_count == 2
    assert mach_conf_1b is mach_conf_1

    mach_conf_2b = await io_manager_1.machine_configurations.get("mach2")
    assert io_manager_1.io_driver.machine_configuration_load_count == 2
    assert mach_conf_2b is mach_conf_2
