import random
from datetime import date, datetime, time, timedelta, timezone

import pytest

from gradesens.moonstone_snooper import (
    Date,
    DateTime,
    Settings,
    Time,
    TimeDelta,
    TimeZone,
)


def test_datetime():
    tested_type = DateTime
    ref_type = datetime

    with pytest.raises(TypeError):
        ref_type()
    with pytest.raises(TypeError):
        tested_type()

    reference = ref_type(
        2022,
        11,
        21,
        9,
        58,
        13,
        87654,
        tzinfo=timezone.utc,
        fold=1,
    )

    value = tested_type(
        reference.year,
        reference.month,
        reference.day,
        reference.hour,
        reference.minute,
        reference.second,
        reference.microsecond,
        reference.tzinfo,
        fold=reference.fold,
    )
    assert isinstance(value, ref_type)
    assert value == reference

    value = tested_type(
        year=reference.year,
        month=reference.month,
        day=reference.day,
        hour=reference.hour,
        minute=reference.minute,
        second=reference.second,
        microsecond=reference.microsecond,
        tzinfo=reference.tzinfo,
        fold=reference.fold,
    )
    assert isinstance(value, ref_type)
    assert value == reference

    value = tested_type(reference)
    assert isinstance(value, ref_type)
    assert value == reference

    value = reference.isoformat()
    assert isinstance(value, str)
    value = tested_type(value)
    assert isinstance(value, ref_type)
    assert value == reference

    value = reference.timestamp()
    assert isinstance(value, float)
    value = tested_type(value)
    assert isinstance(value, ref_type)
    assert value.timestamp() == reference.timestamp()

    value = reference.timetz()
    assert isinstance(value, time)
    assert not isinstance(value, ref_type)
    value = tested_type(value)
    assert isinstance(value, ref_type)
    assert value == reference.replace(
        year=date.min.year,
        month=date.min.month,
        day=date.min.day,
    )

    value = reference.date()
    assert isinstance(value, date)
    assert not isinstance(value, ref_type)
    value = tested_type(value)
    assert isinstance(value, ref_type)
    assert value == reference.replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
        tzinfo=None,
        fold=0,
    )

    value = tested_type("2022-07-25T12:40:31Z")
    assert value == ref_type(
        year=2022,
        month=7,
        day=25,
        hour=12,
        minute=40,
        second=31,
        tzinfo=timezone.utc,
    )
    assert value == tested_type("2022-07-25T12:40:31+00:00")


def test_time():
    tested_type = Time
    ref_type = time

    value = tested_type()
    reference = ref_type()
    assert isinstance(value, ref_type)
    assert value == reference

    reference = ref_type(
        9,
        58,
        13,
        87654,
        tzinfo=timezone.utc,
        fold=1,
    )

    value = tested_type(
        reference.hour,
        reference.minute,
        reference.second,
        reference.microsecond,
        reference.tzinfo,
        fold=reference.fold,
    )
    assert isinstance(value, ref_type)
    assert value == reference

    value = tested_type(
        hour=reference.hour,
        minute=reference.minute,
        second=reference.second,
        microsecond=reference.microsecond,
        tzinfo=reference.tzinfo,
        fold=reference.fold,
    )
    assert isinstance(value, ref_type)
    assert value == reference

    value = tested_type(reference)
    assert isinstance(value, ref_type)
    assert value == reference

    value = reference.isoformat()
    assert isinstance(value, str)
    value = tested_type(value)
    assert isinstance(value, ref_type)
    assert value == reference

    value = DateTime(reference).timestamp()
    assert isinstance(value, float)
    value = tested_type(value)
    assert isinstance(value, ref_type)
    assert (
        DateTime(value)
        .replace(tzinfo=reference.tzinfo, fold=reference.fold)
        .timestamp()
        == DateTime(reference).timestamp()
    )

    value = datetime.combine(date.min, reference)
    assert isinstance(value, datetime)
    assert not isinstance(value, ref_type)
    value = tested_type(value)
    assert isinstance(value, ref_type)
    assert value == reference

    value = tested_type("12:40:31Z")
    assert value == ref_type(
        hour=12,
        minute=40,
        second=31,
        tzinfo=timezone.utc,
    )
    assert value == tested_type("12:40:31+00:00")


def test_date():
    tested_type = Date
    ref_type = date

    reference = ref_type(2022, 11, 21)

    with pytest.raises(TypeError):
        ref_type()
    with pytest.raises(TypeError):
        tested_type()

    value = tested_type(
        reference.year,
        reference.month,
        reference.day,
    )
    assert isinstance(value, ref_type)
    assert value == reference

    value = tested_type(
        year=reference.year,
        month=reference.month,
        day=reference.day,
    )
    assert isinstance(value, ref_type)
    assert value == reference

    value = tested_type(reference)
    assert isinstance(value, ref_type)
    assert value == reference

    value = reference.isoformat()
    assert isinstance(value, str)
    value = tested_type(value)
    assert isinstance(value, ref_type)
    assert value == reference

    value = DateTime(reference).timestamp()
    assert isinstance(value, float)
    value = tested_type(value)
    assert isinstance(value, ref_type)
    assert DateTime(value).timestamp() == DateTime(reference).timestamp()

    value = datetime.combine(reference, time.min)
    assert isinstance(value, datetime)
    value = tested_type(value)
    assert isinstance(value, ref_type)
    assert value == reference


def test_timedelta():
    tested_type = TimeDelta
    ref_type = timedelta

    value = tested_type()
    reference = ref_type()
    assert isinstance(value, ref_type)
    assert value == reference

    reference_kwargs = dict(
        weeks=3, days=2, hours=13, minutes=37, seconds=19, microseconds=876345
    )
    reference = ref_type(**reference_kwargs)

    value = tested_type(
        reference.days,
        reference.seconds,
        reference.microseconds,
    )
    assert isinstance(value, ref_type)
    assert value == reference

    value = tested_type(**reference_kwargs)
    assert isinstance(value, ref_type)
    assert value == reference

    value = tested_type(reference)
    assert isinstance(value, ref_type)
    assert value is not reference
    assert value == reference

    value = tested_type(7.5)
    assert isinstance(value, ref_type)
    assert value == timedelta(days=7, hours=12)

    value = tested_type(6)
    assert isinstance(value, ref_type)
    assert value == timedelta(days=6)

    value = tested_type("12:56")
    assert isinstance(value, ref_type)
    assert value == timedelta(minutes=12, seconds=56)

    value = tested_type("12:56.13")
    assert isinstance(value, ref_type)
    assert value == timedelta(minutes=12, seconds=56, milliseconds=130)

    value = tested_type("12 wks 56.13 secs")
    assert isinstance(value, ref_type)
    assert value == timedelta(weeks=12, seconds=56, milliseconds=130)

    value = tested_type("13 d, 17 m")
    assert isinstance(value, ref_type)
    assert value == timedelta(days=13, minutes=17)


def test_conversions():
    tested_type = DateTime
    ref_type = datetime

    reference = ref_type(
        2022,
        11,
        21,
        9,
        58,
        13,
        87654,
        tzinfo=timezone.utc,
        fold=1,
    )

    value = tested_type(
        reference.year,
        reference.month,
        reference.day,
        reference.hour,
        reference.minute,
        reference.second,
        reference.microsecond,
        reference.tzinfo,
        fold=reference.fold,
    )

    ref_type = reference.time()
    value_time = value.time()
    assert isinstance(ref_type, time)
    assert isinstance(value_time, Time)
    assert ref_type == value_time

    ref_type = reference.timetz()
    value_time = value.timetz()
    assert isinstance(ref_type, time)
    assert isinstance(value_time, Time)
    assert ref_type == value_time

    reference_date = reference.date()
    value_date = value.date()
    assert isinstance(reference_date, date)
    assert isinstance(value_date, Date)
    assert reference_date == value_date

    reference2 = datetime.combine(reference_date, ref_type)
    value2 = DateTime.combine(reference_date, ref_type)
    value3 = DateTime.combine(value_date, value_time)
    assert isinstance(reference2, datetime)
    assert isinstance(value2, DateTime)
    assert isinstance(value3, DateTime)
    assert reference2 == value2
    assert reference2 == value3


def test_settings_date_time_conversions():
    init = dict(
        time=time(hour=10, minute=20),
        date=date(year=2022, month=11, day=24),
        datetime=datetime(year=2022, month=11, day=24),
        timedelta=timedelta(12.34),
    )
    type_map = {
        time: Time,
        date: Date,
        datetime: DateTime,
        timedelta: TimeDelta,
    }

    settings = Settings(**init)

    assert isinstance(settings, Settings)
    assert set(settings.keys()) == set(init.keys())

    for key, init_value in init.items():
        init_type = type(init_value)
        settings_type = type_map[init_type]
        assert not isinstance(init_value, settings_type)
        settings_value = settings[key]
        assert isinstance(settings_value, settings_type)


@pytest.fixture
def random_timedelta_kwargs_factory():
    def factory():
        return dict(
            days=random.randint(0, 365 * 100),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
            microseconds=random.randint(0, 999999),
        )

    return factory


@pytest.fixture
def random_datetime_kwargs_factory(random_timedelta_kwargs_factory):
    def factory():
        timedelta_value = timedelta(**random_timedelta_kwargs_factory())
        if random.random() >= 0.5:
            timedelta_value = -timedelta_value
        result = datetime.now() + timedelta_value
        return dict(
            year=result.year,
            month=result.month,
            day=result.day,
            hour=result.hour,
            minute=result.minute,
            second=result.second,
            microsecond=result.microsecond,
            tzinfo=timezone(timedelta(hours=random.randint(-23, 23))),
            fold=random.randint(0, 1),
        )

    return factory


@pytest.fixture
def random_date_kwargs_factory(random_datetime_kwargs_factory):
    def factory():
        datetime_kwargs = random_datetime_kwargs_factory()
        return dict(
            year=datetime_kwargs["year"],
            month=datetime_kwargs["month"],
            day=datetime_kwargs["day"],
        )

    return factory


@pytest.fixture
def random_time_kwargs_factory(random_datetime_kwargs_factory):
    def factory():
        datetime_kwargs = random_datetime_kwargs_factory()
        return dict(
            hour=datetime_kwargs["hour"],
            minute=datetime_kwargs["minute"],
            second=datetime_kwargs["second"],
            microsecond=datetime_kwargs["microsecond"],
            tzinfo=datetime_kwargs["tzinfo"],
            fold=datetime_kwargs["fold"],
        )

    return factory


def test_timedelta_operations(random_timedelta_kwargs_factory):
    tested_type = TimeDelta
    ref_type = timedelta

    assert isinstance(tested_type.min, tested_type)
    assert tested_type.min == ref_type.min

    assert isinstance(tested_type.max, tested_type)
    assert tested_type.max == ref_type.max

    value1_kwargs = random_timedelta_kwargs_factory()
    value2_kwargs = random_timedelta_kwargs_factory()
    assert value1_kwargs != value2_kwargs
    factor = random.random() * 5.0 + 2.0

    for type1, type2 in (
        (tested_type, tested_type),
        (tested_type, ref_type),
        (ref_type, tested_type),
    ):
        t = type1(**value1_kwargs) + type2(**value2_kwargs)
        assert isinstance(t, tested_type)
        assert t == ref_type(**value1_kwargs) + ref_type(**value2_kwargs)

        t = type1(**value1_kwargs)
        t += type2(**value2_kwargs)
        assert isinstance(t, tested_type)
        assert t == ref_type(**value1_kwargs) + ref_type(**value2_kwargs)

        t = type1(**value1_kwargs) - type2(**value2_kwargs)
        assert isinstance(t, tested_type)
        assert t == ref_type(**value1_kwargs) - ref_type(**value2_kwargs)

        t = type1(**value1_kwargs)
        t -= type2(**value2_kwargs)
        assert isinstance(t, tested_type)
        assert t == ref_type(**value1_kwargs) - ref_type(**value2_kwargs)

        t = type1(**value1_kwargs) / type2(**value2_kwargs)
        assert isinstance(t, float)
        assert t == ref_type(**value1_kwargs) / ref_type(**value2_kwargs)

        t = type1(**value1_kwargs) // type2(**value2_kwargs)
        assert isinstance(t, int)
        assert t == ref_type(**value1_kwargs) // ref_type(**value2_kwargs)

        q, r = divmod(type1(**value1_kwargs), type2(**value2_kwargs))
        assert isinstance(q, int)
        assert isinstance(r, tested_type)
        q2, r2 = divmod(ref_type(**value1_kwargs), ref_type(**value2_kwargs))
        assert q == q2
        assert r == r2

    t = tested_type(**value1_kwargs) * factor
    assert isinstance(t, tested_type)
    assert t == ref_type(**value1_kwargs) * factor

    t = factor * tested_type(**value1_kwargs)
    assert isinstance(t, tested_type)
    assert t == factor * ref_type(**value1_kwargs)

    t = tested_type(**value1_kwargs)
    t *= factor
    assert isinstance(t, tested_type)
    assert t == ref_type(**value1_kwargs) * factor

    t = tested_type(**value1_kwargs) / factor
    assert isinstance(t, tested_type)
    assert t == ref_type(**value1_kwargs) / factor

    t = tested_type(**value1_kwargs)
    t /= int(factor)
    assert isinstance(t, tested_type)
    assert t == ref_type(**value1_kwargs) / int(factor)

    t = tested_type(**value1_kwargs) // int(factor)
    assert isinstance(t, tested_type)
    assert t == ref_type(**value1_kwargs) // int(factor)

    t = tested_type(**value1_kwargs)
    t //= int(factor)
    assert isinstance(t, tested_type)
    assert t == ref_type(**value1_kwargs) // int(factor)

    t = tested_type(**value1_kwargs)
    t2 = +t
    assert isinstance(t2, tested_type)
    assert t is not t2
    assert t2 == ref_type(**value1_kwargs)

    t = tested_type(**value1_kwargs)
    t2 = -t
    assert isinstance(t2, tested_type)
    assert t is not t2
    assert t2 == -ref_type(**value1_kwargs)

    t = tested_type(-ref_type(**value1_kwargs))
    assert t == -ref_type(**value1_kwargs)
    t2 = abs(t)
    assert isinstance(t2, tested_type)
    assert t is not t2
    assert t2 == ref_type(**value1_kwargs)

    for test_func in (
        lambda a, b: a == b,
        lambda a, b: a != b,
        lambda a, b: a > b,
        lambda a, b: a >= b,
        lambda a, b: a < b,
        lambda a, b: a <= b,
    ):
        for type1, type2 in (
            (tested_type, tested_type),
            (tested_type, ref_type),
            (ref_type, tested_type),
        ):
            assert test_func(
                type1(**value1_kwargs), type2(**value2_kwargs)
            ) == test_func(
                ref_type(**value1_kwargs), ref_type(**value2_kwargs)
            )
            assert test_func(
                type1(**value2_kwargs), type2(**value1_kwargs)
            ) == test_func(
                ref_type(**value2_kwargs), ref_type(**value1_kwargs)
            )


def test_datetime_operations(
    random_timedelta_kwargs_factory,
    random_datetime_kwargs_factory,
):
    tested_type = DateTime
    ref_type = datetime

    assert isinstance(tested_type.min, tested_type)
    assert tested_type.min == ref_type.min

    assert isinstance(tested_type.max, tested_type)
    assert tested_type.max == ref_type.max

    assert isinstance(tested_type.resolution, TimeDelta)
    assert tested_type.resolution == ref_type.resolution

    value1_kwargs = random_datetime_kwargs_factory()
    value2_kwargs = random_datetime_kwargs_factory()
    td_kwargs = random_timedelta_kwargs_factory()

    for type1, type2 in (
        (tested_type, tested_type),
        (tested_type, ref_type),
        (ref_type, tested_type),
    ):
        dt = type1(**value1_kwargs) - type2(**value2_kwargs)
        assert isinstance(dt, TimeDelta)
        assert dt == ref_type(**value1_kwargs) - ref_type(**value2_kwargs)

    for type1, td_type in (
        (tested_type, timedelta),
        (tested_type, TimeDelta),
        # (ref_type, TimeDelta),
    ):
        t = type1(**value1_kwargs) + td_type(**td_kwargs)
        assert isinstance(t, tested_type)
        assert t == ref_type(**value1_kwargs) + timedelta(**td_kwargs)

        t = td_type(**td_kwargs) + type1(**value1_kwargs)
        assert isinstance(t, tested_type)
        assert t == timedelta(**td_kwargs) + ref_type(**value1_kwargs)

        t = type1(**value1_kwargs) - td_type(**td_kwargs)
        assert isinstance(t, tested_type)
        assert t == ref_type(**value1_kwargs) - timedelta(**td_kwargs)

    for test_func in (
        lambda a, b: a == b,
        lambda a, b: a != b,
        lambda a, b: a > b,
        lambda a, b: a >= b,
        lambda a, b: a < b,
        lambda a, b: a <= b,
    ):
        for type1, type2 in (
            (tested_type, tested_type),
            (tested_type, ref_type),
            (ref_type, tested_type),
        ):
            assert test_func(
                type1(**value1_kwargs), type2(**value2_kwargs)
            ) == test_func(
                ref_type(**value1_kwargs), ref_type(**value2_kwargs)
            )
            assert test_func(
                type1(**value2_kwargs), type2(**value1_kwargs)
            ) == test_func(
                ref_type(**value2_kwargs), ref_type(**value1_kwargs)
            )


def test_date_operations(
    random_timedelta_kwargs_factory,
    random_date_kwargs_factory,
):
    tested_type = Date
    ref_type = date

    assert isinstance(tested_type.min, tested_type)
    assert tested_type.min == ref_type.min

    assert isinstance(tested_type.max, tested_type)
    assert tested_type.max == ref_type.max

    assert isinstance(tested_type.resolution, TimeDelta)
    assert tested_type.resolution == ref_type.resolution

    value1_kwargs = random_date_kwargs_factory()
    value2_kwargs = random_date_kwargs_factory()
    td_kwargs = random_timedelta_kwargs_factory()

    for type1, type2 in (
        (tested_type, tested_type),
        (tested_type, ref_type),
        (ref_type, tested_type),
    ):
        dt = type1(**value1_kwargs) - type2(**value2_kwargs)
        assert isinstance(dt, TimeDelta)
        assert dt == ref_type(**value1_kwargs) - ref_type(**value2_kwargs)

    for type1, td_type in (
        (tested_type, timedelta),
        (tested_type, TimeDelta),
        # (ref_type, TimeDelta),
    ):
        t = type1(**value1_kwargs) + td_type(**td_kwargs)
        assert isinstance(t, tested_type)
        assert t == ref_type(**value1_kwargs) + timedelta(**td_kwargs)

        t = td_type(**td_kwargs) + type1(**value1_kwargs)
        assert isinstance(t, tested_type)
        assert t == timedelta(**td_kwargs) + ref_type(**value1_kwargs)

        t = type1(**value1_kwargs) - td_type(**td_kwargs)
        assert isinstance(t, tested_type)
        assert t == ref_type(**value1_kwargs) - timedelta(**td_kwargs)

    for test_func in (
        lambda a, b: a == b,
        lambda a, b: a != b,
        lambda a, b: a > b,
        lambda a, b: a >= b,
        lambda a, b: a < b,
        lambda a, b: a <= b,
    ):
        for type1, type2 in (
            (tested_type, tested_type),
            (tested_type, ref_type),
            (ref_type, tested_type),
        ):
            assert test_func(
                type1(**value1_kwargs), type2(**value2_kwargs)
            ) == test_func(
                ref_type(**value1_kwargs), ref_type(**value2_kwargs)
            )
            assert test_func(
                type1(**value2_kwargs), type2(**value1_kwargs)
            ) == test_func(
                ref_type(**value2_kwargs), ref_type(**value1_kwargs)
            )


def test_time_operations(
    random_time_kwargs_factory,
):
    tested_type = Time
    ref_type = time

    assert isinstance(tested_type.min, tested_type)
    assert tested_type.min == ref_type.min

    assert isinstance(tested_type.max, tested_type)
    assert tested_type.max == ref_type.max

    assert isinstance(tested_type.resolution, TimeDelta)
    assert tested_type.resolution == ref_type.resolution

    value1_kwargs = random_time_kwargs_factory()
    value2_kwargs = random_time_kwargs_factory()

    for test_func in (
        lambda a, b: a == b,
        lambda a, b: a != b,
        lambda a, b: a > b,
        lambda a, b: a >= b,
        lambda a, b: a < b,
        lambda a, b: a <= b,
    ):
        for type1, type2 in (
            (tested_type, tested_type),
            (tested_type, ref_type),
            (ref_type, tested_type),
        ):
            assert test_func(
                type1(**value1_kwargs), type2(**value2_kwargs)
            ) == test_func(
                ref_type(**value1_kwargs), ref_type(**value2_kwargs)
            )
            assert test_func(
                type1(**value2_kwargs), type2(**value1_kwargs)
            ) == test_func(
                ref_type(**value2_kwargs), ref_type(**value1_kwargs)
            )


def test_timezone_operations():
    assert isinstance(TimeZone.utc, TimeZone)
    assert TimeZone.utc == timezone.utc
