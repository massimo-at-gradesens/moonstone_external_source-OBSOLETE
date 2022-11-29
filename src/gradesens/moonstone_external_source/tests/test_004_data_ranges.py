import itertools
import random

import pytest

from gradesens.moonstone_external_source.utils import (
    find_nearest,
    iter_sub_ranges,
)


def test_iter_sub_ranges():
    values = [random.random()]
    for _ in range(100):
        values.append(values[-1] + random.random())
    value_span = values[-1] - values[0]
    test_span = value_span / 10
    reconstructed_values = []
    for sub_range in iter_sub_ranges(values, test_span):
        reconstructed_values += sub_range
    assert reconstructed_values == values

    values = [random.random()]
    for _ in range(100):
        values.append(values[-1] + random.random() + 1.0)
    value_span = values[-1] - values[0]
    test_span = value_span / 10
    reconstructed_values = []
    count = 0
    for sub_range in iter_sub_ranges(values, test_span):
        reconstructed_values += sub_range
        count += 1
    assert reconstructed_values == values
    assert count > 1

    values = [random.random()]
    for _ in range(100):
        values.append(values[-1] + random.random())
    value_span = values[-1] - values[0]
    test_span = value_span
    reconstructed_values = []
    count = 0
    for sub_range in iter_sub_ranges(values, test_span):
        reconstructed_values += sub_range
        count += 1
    assert reconstructed_values == values
    assert count == 1

    values = [random.random()]
    for _ in range(100):
        values.append(values[-1] + random.random() + 0.01)
    test_span = 0
    reconstructed_values = []
    count = 0
    for sub_range in iter_sub_ranges(values, test_span):
        reconstructed_values += sub_range
        count += 1
    assert reconstructed_values == values
    assert count == len(values)


def test_iter_multiple_sub_ranges():
    values = list(map(lambda x: x * 2 + 10, range(20)))

    class SubRange:
        def __init__(self, index, values):
            self.index = index
            self.count = len(values)
            self.min = values[0]
            self.max = values[-1]

        def __lt__(self, other):
            return self.min < other.min

        def __str__(self):
            return (
                f"@{self.index}, count={self.count}, [{self.min}..{self.max}]"
            )

    for range_spans in (
        (5, 3),
        (5, 3, 8),
        (20, 22, 20),
        (60, 50, 202),
        (0, 0, 6, 0, 5, 4, 1),
    ):
        sub_ranges = [
            list(
                map(
                    lambda sub_range: SubRange(index, sub_range),
                    iter_sub_ranges(values, range_span),
                )
            )
            for index, range_span in enumerate(range_spans)
        ]

        flat_sub_ranges = sorted(itertools.chain(*sub_ranges))
        current_pos = values[0]
        end_pos = current_pos
        end_pos_count = 1
        for index, sub_range in enumerate(flat_sub_ranges):
            # print(">>", sub_range)
            if index < len(sub_ranges):
                assert sub_range.min == current_pos
            else:
                assert sub_range.min >= current_pos
                current_pos = sub_range.min
            if end_pos == sub_range.max:
                end_pos_count += 1
            else:
                end_pos = sub_range.max
                end_pos_count = 1
        assert end_pos == values[-1]

        sub_range_iters = list(map(iter, sub_ranges))
        sub_range_count = [0] * len(sub_ranges)

        reconstructed_counts = [0] * len(range_spans)
        for value_index in range(len(values)):
            for index in range(len(range_spans)):
                if sub_range_count[index] <= 0:
                    sub_range = next(sub_range_iters[index])
                    # print(
                    #     f"@{value_index} = {values[value_index]!r}:"
                    #     f" new range for {index}: {sub_range}"
                    # )
                    sub_range_count[index] = sub_range.count
                    assert sub_range.count > 0
                    reconstructed_counts[index] += sub_range.count
                sub_range_count[index] -= 1
        for index in range(len(range_spans)):
            with pytest.raises(StopIteration):
                next(sub_range_iters[index])
            assert reconstructed_counts[index] == len(values)


def test_find_nearest():
    value0 = 2.0
    factor = 3.0
    n_values = 10
    values = [value0 + factor * index for index in range(n_values)]

    for ref_pos, extra_value in (
        (0, -factor * 100),
        (n_values - 1, factor * 100),
    ):
        ref_value = values[ref_pos]
        pos = find_nearest(values, ref_value + extra_value)
        assert pos == ref_pos
        pos = find_nearest(values, ref_value - factor * 0.1)
        assert pos == ref_pos
        pos = find_nearest(values, ref_value)
        assert pos == ref_pos
        pos = find_nearest(values, ref_value + factor * 0.1)
        assert pos == ref_pos

    ref_pos = 4
    ref_value = values[ref_pos]
    n_tests = 5
    for offset in range(-n_tests, n_tests):
        pos = find_nearest(values, ref_value + factor * 0.5 * offset / n_tests)
        assert pos == ref_pos, f"{offset} / {n_tests}"
    pos = find_nearest(values, ref_value + factor * 0.5)
    assert pos == ref_pos + 1
    pos = find_nearest(values, ref_value - factor * 0.51)
    assert pos == ref_pos - 1

    pos = find_nearest(values, ref_value, lo=ref_pos + 2)
    assert pos == ref_pos + 2

    pos = find_nearest(values, values[-1] * 2.0, hi=n_values - 3)
    assert pos == n_values - 4
