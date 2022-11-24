from gradesens.moonstone_external_source import Settings

from .utils import assert_eq


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
    assert_eq(
        settings,
        {
            "hello": "world",
            "sub": {
                "lausanne": "VD",
                "inner": {
                    "bern": "BE",
                },
            },
        },
    )

    settings = Settings(
        (
            ("yellow", "red"),
            ("green", "blue"),
        ),
    )
    assert_eq(
        settings,
        {
            "yellow": "red",
            "green": "blue",
        },
    )


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
        complex=">>{array[index]}<<",
    )

    orig_params = dict(
        shape=" is round",
        canton=" is a canton",
        vd="Vaud",
        be="Bern",
        dummy="I am not used in the test",
        array=dict(tree="branches", branch="leaves"),
        index="tree",
    )

    for param_builder in (
        lambda params: dict(params),
        lambda params: Settings(params),
        lambda params: Settings(**params),
    ):
        params = param_builder(orig_params)
        assert params is not orig_params
        assert len(params) == len(orig_params)

        values = settings.interpolate(Settings.InterpolationContext(params))

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
        assert values["complex"] == ">>branches<<"


def test_settings_attribute_access():
    settings = Settings(
        hello="world{shape}",
        sub=dict(
            lausanne="{vd}{canton}",
            inner=dict(
                bern="{be}{canton}",
            ),
        ),
        complex=">>{array[index]}<<",
    )

    assert settings["sub"] is settings.sub
    assert settings["sub"]["inner"] is settings.sub.inner
    assert settings["sub"]["inner"] is settings["sub"].inner
    assert settings["sub"]["inner"] is settings.sub["inner"]
