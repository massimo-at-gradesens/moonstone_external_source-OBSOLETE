def key_path_prefix(key_path, suffix=" "):
    if not key_path:
        return ""
    key_path = "".join(map(lambda key: f"[{key!r}]", key_path))
    return f"@{key_path}:{suffix}"


def assert_eq_dicts(dict1, dict2, key_path=()):
    if not (isinstance(dict1, dict) and isinstance(dict2, dict)):
        assert dict1 == dict2
        return

    keys1 = set(dict1.keys())
    keys2 = set(dict2.keys())
    try:
        assert keys1 == keys2
    except AssertionError:
        raise AssertionError(
            f"{key_path_prefix(key_path)}Different keys:\n"
            f"dict1 extra keys: {list(keys1 - keys2)}\n"
            f"dict2 extra keys: {list(keys2 - keys1)}"
        ) from None

    for key, value1 in dict1.items():
        this_key_path = key_path + (key,)
        value2 = dict2[key]
        if isinstance(value1, dict) and isinstance(value2, dict):
            assert_eq_dicts(value1, value2, this_key_path)
            continue
        try:
            assert value1 == value2
        except AssertionError:
            raise AssertionError(
                f"{key_path_prefix(this_key_path)}\n"
                f"dict1 value: {type(value1).__name__}, {value1}\n"
                f"dict2 value: {type(value2).__name__}, {value2}"
            ) from None
