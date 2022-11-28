"""
GradeSens - External Source package - Various support tools
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, GradeSens AG"

import bisect


def iter_sub_ranges(values, range_value_span):
    values = sorted(values)
    n_values = len(values)
    start_index = 0
    while start_index < n_values:
        range_end_value = values[start_index] + range_value_span
        end_index = bisect.bisect_right(
            values, range_end_value, lo=start_index
        )
        yield values[start_index:end_index]
        start_index = end_index


class classproperty:
    """
    Multi Python version-friendly RO class property
    """

    def __init__(self, method):
        self.__method = method

    def __get__(self, instance, owner):
        return self.__method(owner)
