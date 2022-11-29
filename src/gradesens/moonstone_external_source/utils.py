"""
GradeSens - External Source package - Various support tools
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, GradeSens AG"

import bisect


def iter_sub_ranges(values, value_range_span):
    """
    Given a list of ``values`` and a ``value_range_span``, return an iterator
    over ordered sub-ranges (i.e. ``values`` subsets) of ordered values, such
    that the values in each sub-range are all contained within
    `[first_value .. first_value + value_range_span]`
    """
    values = sorted(values)
    n_values = len(values)
    start_index = 0
    while start_index < n_values:
        range_end_value = values[start_index] + value_range_span
        end_index = bisect.bisect_right(
            values, range_end_value, lo=start_index
        )
        yield values[start_index:end_index]
        start_index = end_index


def find_nearest(a, x, lo=0, hi=None, **kwargs):
    """
    This function is similar to python's `bisect_right
    <https://docs.python.org/3/library/bisect.html#bisect.bisect_right>`_ and
    expects exactly the same arguments.

    It returns the index of the element in ``a`` with the closest value to
    ``x``.
    """
    if hi is None:
        hi = len(a)
    pos = bisect.bisect_right(a, x, lo=lo, hi=hi, **kwargs)
    if pos >= hi:
        return hi - 1
    if pos <= lo:
        return lo
    before = a[pos - 1]
    after = a[pos]
    return pos if x - before >= after - x else pos - 1


class classproperty:
    """
    Multi Python version-friendly RO class property
    """

    def __init__(self, method):
        self.__method = method

    def __get__(self, instance, owner):
        return self.__method(owner)
