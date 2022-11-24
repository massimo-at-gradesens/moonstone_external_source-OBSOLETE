import textwrap

import yaml

from gradesens.moonstone_external_source.settings import (
    Processors,
    TypeProcessor,
)


def path_prefix(key_path, suffix=" "):
    if not key_path:
        return ""
    key_path = "".join(map(lambda key: f"[{key!r}]", key_path))
    return f"@{key_path}:{suffix}"


def assert_eq(obj1, obj2, path=()):
    if isinstance(obj1, dict) and isinstance(obj2, dict):
        keys1 = set(obj1.keys())
        keys2 = set(obj2.keys())
        if keys1 != keys2:
            raise AssertionError(
                f"{path_prefix(path)}Different keys:\n"
                f"obj1 extra keys: {list(keys1 - keys2)}\n"
                f"obj2 extra keys: {list(keys2 - keys1)}"
            )

        for key, value1 in obj1.items():
            value2 = obj2[key]
            assert_eq(value1, value2, path + (key,))
        return

    if (isinstance(obj1, list) and isinstance(obj2, list)) or (
        isinstance(obj1, tuple) and isinstance(obj2, tuple)
    ):
        for index, (item1, item2) in enumerate(zip(obj1, obj2)):
            assert_eq(item1, item2, path=path + (index,))
        if len(obj1) != len(obj2):
            raise AssertionError(
                f"{path_prefix(path)}Different number of elements\n"
                f"obj1 #elements: {len(obj1)}\n"
                f"obj2 #elements: {len(obj2)}"
            )
        return

    if obj1 != obj2:
        raise AssertionError(
            f"{path_prefix(path)}\n"
            f"obj1 value: {type(obj1).__name__}, {obj1}\n"
            f"obj2 value: {type(obj2).__name__}, {obj2}"
        )


def expand_processors(value):
    if isinstance(value, Processors):
        return [
            dict(__processor=item.KEY, **expand_processors(item))
            for item in value
        ]

    if isinstance(value, TypeProcessor):
        result = dict(value)
        result["converter"] = result["converter"].name
        return result

    if isinstance(value, dict):
        return {
            key: expand_processors(value2) for key, value2 in value.items()
        }

    if isinstance(value, (list, tuple)):
        return type(value)(map(expand_processors, value))

    return value


def load_yaml(text):
    if isinstance(text, str):
        text = textwrap.dedent(text)
    else:
        with open(text, "rt") as f:
            text = f.read()
    return yaml.load(text, yaml.Loader)


def to_basic_types(value):
    if isinstance(value, dict):
        return {key: to_basic_types(value) for key, value in value.items()}

    if isinstance(value, (list, tuple)):
        return list(map(to_basic_types, value))

    if isinstance(value, (int, float, bool)):
        return value

    return str(value)
