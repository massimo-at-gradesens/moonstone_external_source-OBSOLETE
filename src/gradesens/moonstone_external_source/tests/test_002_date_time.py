from datetime import date, datetime, time, timedelta, timezone

import pytest

from gradesens.moonstone_external_source import (
    Date,
    DateTime,
    Settings,
    Time,
    TimeDelta,
)


def test_datetime():
    tested_type = DateTime
    reference_type = datetime

    with pytest.raises(TypeError):
        reference_type()
    with pytest.raises(TypeError):
        tested_type()

    reference = reference_type(
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
    assert isinstance(value, reference_type)
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
    assert isinstance(value, reference_type)
    assert value == reference

    value = tested_type(reference)
    assert isinstance(value, reference_type)
    assert value == reference

    value = reference.isoformat()
    assert isinstance(value, str)
    value = tested_type(value)
    assert isinstance(value, reference_type)
    assert value == reference

    value = reference.timestamp()
    assert isinstance(value, float)
    value = tested_type(value)
    assert isinstance(value, reference_type)
    assert value.timestamp() == reference.timestamp()

    value = reference.timetz()
    assert isinstance(value, time)
    assert not isinstance(value, reference_type)
    value = tested_type(value)
    assert isinstance(value, reference_type)
    assert value == reference.replace(
        year=date.min.year,
        month=date.min.month,
        day=date.min.day,
    )

    value = reference.date()
    assert isinstance(value, date)
    assert not isinstance(value, reference_type)
    value = tested_type(value)
    assert isinstance(value, reference_type)
    assert value == reference.replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
        tzinfo=None,
        fold=0,
    )

    value = tested_type("2022-07-25T12:40:31Z")
    assert value == reference_type(
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
    reference_type = time

    value = tested_type()
    reference = reference_type()
    assert isinstance(value, reference_type)
    assert value == reference

    reference = reference_type(
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
    assert isinstance(value, reference_type)
    assert value == reference

    value = tested_type(
        hour=reference.hour,
        minute=reference.minute,
        second=reference.second,
        microsecond=reference.microsecond,
        tzinfo=reference.tzinfo,
        fold=reference.fold,
    )
    assert isinstance(value, reference_type)
    assert value == reference

    value = tested_type(reference)
    assert isinstance(value, reference_type)
    assert value == reference

    value = reference.isoformat()
    assert isinstance(value, str)
    value = tested_type(value)
    assert isinstance(value, reference_type)
    assert value == reference

    value = DateTime(reference).timestamp()
    assert isinstance(value, float)
    value = tested_type(value)
    assert isinstance(value, reference_type)
    assert (
        DateTime(value)
        .replace(tzinfo=reference.tzinfo, fold=reference.fold)
        .timestamp()
        == DateTime(reference).timestamp()
    )

    value = datetime.combine(date.min, reference)
    assert isinstance(value, datetime)
    assert not isinstance(value, reference_type)
    value = tested_type(value)
    assert isinstance(value, reference_type)
    assert value == reference

    value = tested_type("12:40:31Z")
    assert value == reference_type(
        hour=12,
        minute=40,
        second=31,
        tzinfo=timezone.utc,
    )
    assert value == tested_type("12:40:31+00:00")


def test_date():
    tested_type = Date
    reference_type = date

    reference = reference_type(2022, 11, 21)

    with pytest.raises(TypeError):
        reference_type()
    with pytest.raises(TypeError):
        tested_type()

    value = tested_type(
        reference.year,
        reference.month,
        reference.day,
    )
    assert isinstance(value, reference_type)
    assert value == reference

    value = tested_type(
        year=reference.year,
        month=reference.month,
        day=reference.day,
    )
    assert isinstance(value, reference_type)
    assert value == reference

    value = tested_type(reference)
    assert isinstance(value, reference_type)
    assert value == reference

    value = reference.isoformat()
    assert isinstance(value, str)
    value = tested_type(value)
    assert isinstance(value, reference_type)
    assert value == reference

    value = DateTime(reference).timestamp()
    assert isinstance(value, float)
    value = tested_type(value)
    assert isinstance(value, reference_type)
    assert DateTime(value).timestamp() == DateTime(reference).timestamp()

    value = datetime.combine(reference, time.min)
    assert isinstance(value, datetime)
    value = tested_type(value)
    assert isinstance(value, reference_type)
    assert value == reference


def test_timedelta():
    tested_type = TimeDelta
    reference_type = timedelta

    value = tested_type()
    reference = reference_type()
    assert isinstance(value, reference_type)
    assert value == reference

    reference_kwargs = dict(
        weeks=3, days=2, hours=13, minutes=37, seconds=19, microseconds=876345
    )
    reference = reference_type(**reference_kwargs)

    value = tested_type(
        reference.days,
        reference.seconds,
        reference.microseconds,
    )
    assert isinstance(value, reference_type)
    assert value == reference

    value = tested_type(**reference_kwargs)
    assert isinstance(value, reference_type)
    assert value == reference

    value = tested_type(reference)
    assert isinstance(value, reference_type)
    assert value is not reference
    assert value == reference

    value = tested_type(7.5)
    assert isinstance(value, reference_type)
    assert value == timedelta(days=7, hours=12)

    value = tested_type(6)
    assert isinstance(value, reference_type)
    assert value == timedelta(days=6)

    value = tested_type("12:56")
    assert isinstance(value, reference_type)
    assert value == timedelta(minutes=12, seconds=56)

    value = tested_type("12:56.13")
    assert isinstance(value, reference_type)
    assert value == timedelta(minutes=12, seconds=56, milliseconds=130)

    value = tested_type("12 wks 56.13 secs")
    assert isinstance(value, reference_type)
    assert value == timedelta(weeks=12, seconds=56, milliseconds=130)

    value = tested_type("13 d, 17 m")
    assert isinstance(value, reference_type)
    assert value == timedelta(days=13, minutes=17)


def test_conversions():
    tested_type = DateTime
    reference_type = datetime

    reference = reference_type(
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

    reference_time = reference.time()
    value_time = value.time()
    assert isinstance(reference_time, time)
    assert isinstance(value_time, Time)
    assert reference_time == value_time

    reference_time = reference.timetz()
    value_time = value.timetz()
    assert isinstance(reference_time, time)
    assert isinstance(value_time, Time)
    assert reference_time == value_time

    reference_date = reference.date()
    value_date = value.date()
    assert isinstance(reference_date, date)
    assert isinstance(value_date, Date)
    assert reference_date == value_date

    reference2 = datetime.combine(reference_date, reference_time)
    value2 = DateTime.combine(reference_date, reference_time)
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
