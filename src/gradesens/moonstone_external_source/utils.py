"""
GradeSens - External Source package - Various support tools
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, GradeSens AG"

import bisect


def iter_windows(values, window_size):
    values = sorted(values)
    n_values = len(values)
    start_index = 0
    while start_index < n_values:
        window_end = values[start_index] + window_size
        end_index = bisect.bisect_right(values, window_end, lo=start_index)
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
