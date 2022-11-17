import pytest

from gradesens.moonstone_external_source import ConfigurationError

from .utils import assert_eq_dicts


@pytest.mark.usefixtures("io_driver_1")
@pytest.mark.asyncio
async def test_common_configuration_nesting(
    io_driver_1,
):
    comm_conf = await io_driver_1.common_configurations.get("cc1-n")
    settings = await comm_conf.get_common_settings(io_driver_1)
    expected = {
        "id": "cc1-n",
        "_authentication_configuration_id": None,
        "_common_configuration_ids": (),
        "zone": "area42",
        "url": "123",
        "query_string": {
            "hello": "world",
            "east": "west",
        },
        "headers": {
            "head": "oval",
        },
        "measurements": {},
        "result": {
            "_interpolate": False,
        },
    }
    assert_eq_dicts(settings, expected)

    comm_conf = await io_driver_1.common_configurations.get("cc2-n")
    settings = await comm_conf.get_common_settings(io_driver_1)
    expected = {
        "id": "cc2-n",
        "_authentication_configuration_id": None,
        "_common_configuration_ids": (),
        "url": None,
        "query_string": {
            "hello": "moon",
            "south": "north",
        },
        "headers": {
            "square": "four sides",
            "circle": "round",
        },
        "measurements": {
            "temperature": {
                "id": "temperature",
                "_authentication_configuration_id": None,
                "_common_configuration_ids": (),
                "url": None,
                "query_string": {},
                "headers": {
                    "bicycle": "two wheels",
                    "token": "super secret",
                },
                "result": {
                    "_interpolate": False,
                },
            },
        },
        "result": {
            "_interpolate": False,
        },
    }
    assert_eq_dicts(settings, expected)

    comm_conf = await io_driver_1.common_configurations.get("cc3-n")
    settings = await comm_conf.get_common_settings(io_driver_1)
    expected = {
        "id": "cc3-n",
        "_authentication_configuration_id": None,
        "_common_configuration_ids": (
            "cc1-n",
            "cc2-n",
        ),
        "zone": "area42",
        "url": "123",
        "query_string": {
            "hello": "moon",
            "south": "north",
            "east": "west",
        },
        "headers": {
            "head": "oval",
            "square": "four sides",
            "circle": "really ROUND",
        },
        "measurements": {
            "temperature": {
                "id": "temperature",
                "_authentication_configuration_id": None,
                "_common_configuration_ids": (),
                "url": None,
                "query_string": {},
                "headers": {
                    "bicycle": "two wheels",
                    "token": "password",
                },
                "result": {
                    "_interpolate": False,
                },
            },
            "rpm": {
                "id": "rpm",
                "_authentication_configuration_id": None,
                "_common_configuration_ids": (),
                "url": None,
                "query_string": {},
                "headers": {},
                "result": {
                    "_interpolate": False,
                },
            },
        },
        "result": {
            "_interpolate": False,
        },
    }
    assert_eq_dicts(settings, expected)


@pytest.mark.usefixtures("io_driver_1")
@pytest.mark.asyncio
async def test_common_configuration_nesting_loop_failure(
    io_driver_1,
):
    comm_conf = await io_driver_1.common_configurations.get("cc4-n")
    with pytest.raises(ConfigurationError) as exc:
        await comm_conf.get_common_settings(io_driver_1)
    assert "loop" in str(exc).lower()

    comm_conf = await io_driver_1.common_configurations.get("cc5-n")
    with pytest.raises(ConfigurationError) as exc:
        await comm_conf.get_common_settings(io_driver_1)
    assert "loop" in str(exc).lower()
