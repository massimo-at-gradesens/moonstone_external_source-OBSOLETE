import pytest

from gradesens.moonstone_external_source import ConfigurationError, Settings

from .utils import assert_eq


@pytest.mark.usefixtures("io_manager_1")
@pytest.mark.asyncio
async def test_common_configuration_nesting(
    io_manager_1,
):
    async with io_manager_1.client_session() as client_session:
        comm_conf = await client_session.common_configurations.get("cc1-n")
        settings = await comm_conf.get_common_settings(client_session)
    expected = {
        "id": "cc1-n",
        "_common_configuration_ids": (),
        "zone": "area42",
        "request": {
            "_authentication_configuration_id": None,
            "url": "123",
            "headers": {
                "head": "oval",
            },
            "query_string": {
                "hello": "world",
                "east": "west",
            },
            "data": None,
        },
        "measurements": {},
        "result": {
            "_interpolation_settings": Settings.InterpolationSettings(
                interpolate=False,
            ),
        },
    }
    assert_eq(settings, expected)

    async with io_manager_1.client_session() as client_session:
        comm_conf = await client_session.common_configurations.get("cc2-n")
        settings = await comm_conf.get_common_settings(client_session)
    expected = {
        "id": "cc2-n",
        "_common_configuration_ids": (),
        "request": {
            "_authentication_configuration_id": None,
            "url": None,
            "headers": {
                "square": "four sides",
                "circle": "round",
            },
            "query_string": {
                "hello": "moon",
                "south": "north",
            },
            "data": None,
        },
        "result": {
            "_interpolation_settings": Settings.InterpolationSettings(
                interpolate=False,
            ),
        },
        "measurements": {
            "temperature": {
                "id": "temperature",
                "_common_configuration_ids": (),
                "request": {
                    "_authentication_configuration_id": None,
                    "url": None,
                    "headers": {
                        "bicycle": "two wheels",
                        "token": "super secret",
                    },
                    "query_string": {},
                    "data": None,
                },
                "result": {
                    "_interpolation_settings": Settings.InterpolationSettings(
                        interpolate=False,
                    ),
                },
            },
        },
    }
    assert_eq(settings, expected)

    async with io_manager_1.client_session() as client_session:
        comm_conf = await client_session.common_configurations.get("cc3-n")
        settings = await comm_conf.get_common_settings(client_session)
    expected = {
        "id": "cc3-n",
        "_common_configuration_ids": (
            "cc1-n",
            "cc2-n",
        ),
        "zone": "area42",
        "request": {
            "_authentication_configuration_id": None,
            "url": "123",
            "headers": {
                "head": "oval",
                "square": "four sides",
                "circle": "really ROUND",
            },
            "query_string": {
                "hello": "moon",
                "south": "north",
                "east": "west",
            },
            "data": None,
        },
        "result": {
            "_interpolation_settings": Settings.InterpolationSettings(
                interpolate=False,
            ),
        },
        "measurements": {
            "temperature": {
                "id": "temperature",
                "_common_configuration_ids": (),
                "request": {
                    "_authentication_configuration_id": None,
                    "url": None,
                    "headers": {
                        "bicycle": "two wheels",
                        "token": "password",
                    },
                    "query_string": {},
                    "data": None,
                },
                "result": {
                    "_interpolation_settings": Settings.InterpolationSettings(
                        interpolate=False,
                    ),
                },
            },
            "rpm": {
                "id": "rpm",
                "_common_configuration_ids": (),
                "request": {
                    "_authentication_configuration_id": None,
                    "url": None,
                    "headers": {},
                    "query_string": {},
                    "data": None,
                },
                "result": {
                    "_interpolation_settings": Settings.InterpolationSettings(
                        interpolate=False,
                    ),
                },
            },
        },
    }
    assert_eq(settings, expected)


@pytest.mark.usefixtures("io_manager_1")
@pytest.mark.asyncio
async def test_common_configuration_nesting_loop_failure(
    io_manager_1,
):
    async with io_manager_1.client_session() as client_session:
        comm_conf = await client_session.common_configurations.get("cc4-n")
        with pytest.raises(ConfigurationError) as exc:
            await comm_conf.get_common_settings(client_session)
        assert "loop" in str(exc).lower()

        comm_conf = await client_session.common_configurations.get("cc5-n")
        with pytest.raises(ConfigurationError) as exc:
            await comm_conf.get_common_settings(client_session)
        assert "loop" in str(exc).lower()
