def key_path_prefix(key_path, suffix=" "):
    if key_path:
        return f"At {'.'.join(map(repr, key_path))}:{suffix}"
    return ""


def assert_eq_dicts(dict1, dict2, key_path=()):
    if not (isinstance(dict1, dict) and isinstance(dict2, dict)):
        assert dict1 == dict2
        return

    try:
        assert set(dict1.keys()) == set(dict2.keys())
    except AssertionError:
        raise AssertionError(
            f"{key_path_prefix(key_path)}Different keys:\n"
            f"Left : {set(dict1.keys())}\n"
            f"Right: {set(dict2.keys())}"
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
                f"Left : {type(value1).__name__}, {value1}\n"
                f"Right: {type(value2).__name__}, {value2}"
            ) from None
