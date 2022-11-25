import pytest

from gradesens.moonstone_external_source import DataTypeError, Settings

from .configuration_fixtures import load_yaml


def test_boolean_conversion_non_str():
    settings = Settings(
        **load_yaml(
            """
            expression:
                <process:
                    type:
                        target: bool
                        input_key: value
        """
        )
    )

    for value in (
        0,
        1,
        100,
        True,
        False,
        0.0,
        1.1,
    ):
        result = settings.interpolate(
            Settings.InterpolationContext(
                dict(
                    value=value,
                )
            )
        )
        computed_value = result["expression"]
        expected_value = bool(value)
        assert (
            isinstance(computed_value, bool)
            and isinstance(expected_value, bool)
            and computed_value == expected_value
        ), (
            f"Mismatching boolean conversion for {value!r}"
            f" of type {type(value).__name__!r}:"
            f" expected {expected_value!r}"
            f" of type {type(expected_value).__name__!r},"
            f" got {computed_value}"
            f" of type {type(computed_value).__name__!r}"
        )


def test_boolean_conversion_str():
    settings = Settings(
        **load_yaml(
            """
            expression:
                <process:
                    type:
                        target: bool
                        input_key: value
        """
        )
    )

    for value, expected_value in (
        ("Y", True),
        ("  y", True),
        ("YEs", True),
        ("yeS", True),
        ("N", False),
        ("n ", False),
        ("no", False),
        ("NO", False),
        ("On", True),
        (" oFF ", False),
        ("t", True),
        ("trUe", True),
        ("f", False),
        ("FaLsE", False),
        ("+", True),
        ("-", False),
        (" +  ", True),
        (" - ", False),
        ("1", True),
        ("0", False),
    ):
        result = settings.interpolate(
            Settings.InterpolationContext(
                dict(
                    value=value,
                )
            )
        )
        computed_value = result["expression"]
        assert (
            isinstance(computed_value, bool)
            and isinstance(expected_value, bool)
            and computed_value == expected_value
        ), (
            f"Mismatching boolean conversion for {value!r}"
            f" of type {type(value).__name__!r}:"
            f" expected {expected_value!r}"
            f" of type {type(expected_value).__name__!r},"
            f" got {computed_value}"
            f" of type {type(computed_value).__name__!r}"
        )

    wrong_literal = "wrong literal"
    with pytest.raises(DataTypeError) as exc_info:
        result = settings.interpolate(
            Settings.InterpolationContext(
                dict(
                    value=wrong_literal,
                )
            )
        )
    assert f"{wrong_literal!r}" in str(exc_info.value)
