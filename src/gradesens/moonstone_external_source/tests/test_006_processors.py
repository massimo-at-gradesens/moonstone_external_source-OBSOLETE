import pytest

from gradesens.moonstone_external_source import ConfigurationError, Settings
from gradesens.moonstone_external_source.settings import (
    Processors,
    RegexProcessor,
    TypeProcessor,
)

from .configuration_fixtures import load_yaml


def test_processor_configuration_1():
    settings = Settings(
        **load_yaml(
            """
            a_value:
                <process:
                    type:
                        target: bool
                        input_key: value
        """
        )
    )
    processors = settings.a_value["<process"]
    assert isinstance(processors, Processors)
    assert len(processors) == 1
    assert isinstance(processors[0], TypeProcessor)
    assert processors[0].converter is TypeProcessor.CONVERTERS["bool"]
    assert processors[0].input_key == "value"
    assert "output_key" not in processors[0]

    settings = Settings(
        **load_yaml(
            """
            a_value:
                <process:
                    -   type: bool
                        input_key: value
        """
        )
    )
    processors = settings.a_value["<process"]
    assert isinstance(processors, Processors)
    assert len(processors) == 1
    assert isinstance(processors[0], TypeProcessor)
    assert processors[0].converter is TypeProcessor.CONVERTERS["bool"]
    assert processors[0].input_key == "value"
    assert "output_key" not in processors[0]

    settings = Settings(
        **load_yaml(
            """
            a_value:
                <process:
                    -   type:
                        target: bool
                        input_key: value
        """
        )
    )
    processors = settings.a_value["<process"]
    assert isinstance(processors, Processors)
    assert len(processors) == 1
    assert isinstance(processors[0], TypeProcessor)
    assert processors[0].converter is TypeProcessor.CONVERTERS["bool"]
    assert processors[0].input_key == "value"
    assert "output_key" not in processors[0]

    settings = Settings(
        **load_yaml(
            """
            a_value:
                <process:
                    -   target: bool
                        input_key: value
        """
        )
    )
    processors = settings.a_value["<process"]
    assert isinstance(processors, Processors)
    assert len(processors) == 1
    assert isinstance(processors[0], TypeProcessor)
    assert processors[0].converter is TypeProcessor.CONVERTERS["bool"]
    assert processors[0].input_key == "value"
    assert "output_key" not in processors[0]

    settings = Settings(
        **load_yaml(
            """
            a_value:
                <process:
                    -   type: bool
                        input_key: value
                        output_key: hello
                    -   type: int
        """
        )
    )
    processors = settings.a_value["<process"]
    assert isinstance(processors, Processors)
    assert len(processors) == 2
    assert isinstance(processors[0], TypeProcessor)
    assert processors[0].converter is TypeProcessor.CONVERTERS["bool"]
    assert processors[0].input_key == "value"
    assert processors[0].output_key == "hello"
    assert isinstance(processors[1], TypeProcessor)
    assert processors[1].converter is TypeProcessor.CONVERTERS["int"]
    assert "input_key" not in processors[1]
    assert "output_key" not in processors[1]


def test_processor_configuration_2():
    settings = Settings(
        **load_yaml(
            """
            a_value:
                <process:
                    regex:
                        input_key: value
                        pattern: ".*"
                        replacement: "123"
        """
        )
    )
    processors = settings.a_value["<process"]
    assert isinstance(processors, Processors)
    assert len(processors) == 1
    assert isinstance(processors[0], RegexProcessor)
    assert processors[0].input_key == "value"
    assert processors[0].pattern == ".*"
    assert processors[0].replacement == "123"
    assert "output_key" not in processors[0]

    settings = Settings(
        **load_yaml(
            """
            a_value:
                <process:
                    -   regex: ".*"
                        input_key: value
                        replacement: "123"
        """
        )
    )
    processors = settings.a_value["<process"]
    assert isinstance(processors, Processors)
    assert len(processors) == 1
    assert isinstance(processors[0], RegexProcessor)
    assert processors[0].input_key == "value"
    assert processors[0].pattern == ".*"
    assert processors[0].replacement == "123"
    assert "output_key" not in processors[0]

    settings = Settings(
        **load_yaml(
            """
            a_value:
                <process:
                    -   regex:
                        pattern: ".*"
                        input_key: value
                        replacement: "123"
        """
        )
    )
    processors = settings.a_value["<process"]
    assert isinstance(processors, Processors)
    assert len(processors) == 1
    assert isinstance(processors[0], RegexProcessor)
    assert processors[0].input_key == "value"
    assert processors[0].pattern == ".*"
    assert processors[0].replacement == "123"
    assert "output_key" not in processors[0]

    settings = Settings(
        **load_yaml(
            """
            a_value:
                <process:
                    -   pattern: ".*"
                        input_key: value
                        replacement: "123"
        """
        )
    )
    processors = settings.a_value["<process"]
    assert isinstance(processors, Processors)
    assert len(processors) == 1
    assert isinstance(processors[0], RegexProcessor)
    assert processors[0].input_key == "value"
    assert processors[0].pattern == ".*"
    assert processors[0].replacement == "123"
    assert "output_key" not in processors[0]


def test_processor_errors():
    with pytest.raises(ConfigurationError) as exc_info:
        Settings(
            **load_yaml(
                """
                a_value:
                    <process:
                        -   type: int
            """
            )
        )
    assert "'input_key'" in str(exc_info.value)
    assert " explicit input value " in str(exc_info.value)

    with pytest.raises(ConfigurationError) as exc_info:
        Settings(
            **load_yaml(
                """
                a_value:
                    <process:
                        type:
                            target: int
            """
            )
        )
    assert "'input_key'" in str(exc_info.value)
    assert " explicit input value " in str(exc_info.value)

    Settings(
        **load_yaml(
            """
            a_value:
                <process:
                    -   type: int
                        input_key: hello
        """
        )
    )

    with pytest.raises(ConfigurationError) as exc_info:
        Settings(
            **load_yaml(
                """
                a_value:
                    <process:
                        -   type: int
                            input_key: hello
                            output_key: world
            """
            )
        )
    assert "The last processor" in str(exc_info.value)
    assert " output value " in str(exc_info.value)

    with pytest.raises(ConfigurationError) as exc_info:
        Settings(
            **load_yaml(
                """
                a_value:
                    <process:
                        -   type:
                                target: int
                                input_key: hello
                                output_key: world
            """
            )
        )
    assert "The last processor" in str(exc_info.value)
    assert " output value " in str(exc_info.value)

    with pytest.raises(ConfigurationError) as exc_info:
        Settings(
            **load_yaml(
                """
                a_value:
                    <process:
                        -   type:
                                input_key: hello
            """
            )
        )
    assert "Missing mandatory fields: 'target'" in str(exc_info.value)

    with pytest.raises(ConfigurationError) as exc_info:
        Settings(
            **load_yaml(
                """
                a_value:
                    <process:
                        -   type: hello
                            target: world
                            input_key: value
            """
            )
        )
    assert "'type'" in str(exc_info.value)
    assert "'hello'" in str(exc_info.value)
    assert "'target'" in str(exc_info.value)
    assert "'world'" in str(exc_info.value)

    with pytest.raises(ConfigurationError) as exc_info:
        Settings(
            **load_yaml(
                """
                a_value:
                    <process:
                        regex:
                            input_key: value
                            pattern: ".*"
            """
            )
        )
    assert "Missing mandatory fields" in str(exc_info.value)
    assert "'replacement'" in str(exc_info.value)

    with pytest.raises(ConfigurationError) as exc_info:
        Settings(
            **load_yaml(
                """
                a_value:
                    <process:
                        regex:
                            input_key: value
                            replacement: ".*"
            """
            )
        )
    assert "Missing mandatory fields" in str(exc_info.value)
    assert "'pattern'" in str(exc_info.value)

    with pytest.raises(ConfigurationError) as exc_info:
        Settings(
            **load_yaml(
                """
                a_value:
                    <process:
                        regex: ".*"
                        input_key: value
            """
            )
        )
    assert "Missing mandatory fields" in str(exc_info.value)
    assert "'replacement'" in str(exc_info.value)
