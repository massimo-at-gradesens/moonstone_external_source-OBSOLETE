from datetime import date, datetime, time, timezone

from gradesens.moonstone_external_source import Date, DateTime, Time


def test_datetime():
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


def test_time():
    tested_type = Time

    reference_type = time
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


def test_date():
    tested_type = Date

    reference_type = date
    reference = reference_type(2022, 11, 21)

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
