"""
GradeSens - External Source package - Datetime support tools

This module provides extensions to stock module :module:`datetime`.
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, GradeSens AG"


from datetime import date, datetime, time


class DateTime(datetime):
    """
    A clone of stock library :class:`datetime.datetime`, extending the
    constructor capabilities to enable creating a new object directly from
    any of the following:
    * another :class:`datetime.datetime` object,
    * an ISO 8601 string,
    * a POSIX timestamp,
    * a :class:`datetime.date` object,
    * a :class:`datetime.time` object.
    """

    def __new__(cls, year, *args, **kwargs):
        if isinstance(year, int):
            return super().__new__(cls, year, *args, **kwargs)

        assert not args
        assert not kwargs
        other = cls.__convert(year)
        return super().__new__(
            cls,
            year=other.year,
            month=other.month,
            day=other.day,
            hour=other.hour,
            minute=other.minute,
            second=other.second,
            microsecond=other.microsecond,
            tzinfo=other.tzinfo,
            fold=other.fold,
        )

    @classmethod
    def __convert(cls, value):
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            return datetime.fromisoformat(value)
        if isinstance(value, float):
            return datetime.fromtimestamp(value)
        if isinstance(value, time):
            return datetime.combine(date.min, value)
        if isinstance(value, date):
            return datetime.combine(value, time.min)
        raise ValueError(
            f"Don't know how to create a {cls.__name__!r} object from"
            f" a {type(value).__name__!r} object: {value!r}"
        )


class Date(date):
    """
    A clone of stock library :class:`datetime.date`, extending the constructor
    capabilities to enable creating a new object directly from any of the
    following:
    * another :class:`datetime.date` object,
    * an ISO 8601 string,
    * a POSIX timestamp,
    * a :class:`datetime.datetime` object,
    """

    def __new__(cls, year, *args, **kwargs):
        if isinstance(year, int):
            return super().__new__(cls, year, *args, **kwargs)

        assert not args
        assert not kwargs
        other = cls.__convert(year)
        return super().__new__(
            cls,
            year=other.year,
            month=other.month,
            day=other.day,
        )

    @classmethod
    def __convert(cls, value):
        # this covers datetime as well, given that datetime is a subclass of
        # date
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            return date.fromisoformat(value)
        if isinstance(value, float):
            return datetime.fromtimestamp(value).date()
        raise ValueError(
            f"Don't know how to create a {cls.__name__!r} object from"
            f" a {type(value).__name__!r} object: {value!r}"
        )


class Time(time):
    """
    A clone of stock library :class:`datetime.time`, extending the constructor
    capabilities to enable creating a new object directly from any of the
    following:
    * another :class:`datetime.time` object,
    * an ISO 8601 string,
    * a POSIX timestamp,
    * a :class:`datetime.datetime` object,
    """

    def __new__(cls, hour, *args, **kwargs):
        if isinstance(hour, int):
            return super().__new__(cls, hour, *args, **kwargs)

        assert not args
        assert not kwargs
        other = cls.__convert(hour)
        return super().__new__(
            cls,
            hour=other.hour,
            minute=other.minute,
            second=other.second,
            microsecond=other.microsecond,
            tzinfo=other.tzinfo,
            fold=other.fold,
        )

    @classmethod
    def __convert(cls, value):
        if isinstance(value, time):
            return value
        if isinstance(value, str):
            return time.fromisoformat(value)
        if isinstance(value, float):
            second, decimal = divmod(value, 1)
            microsecond = int(decimal * 1_000_000)
            second = int(second)
            minute, second = divmod(second, 60)
            hour, minute = divmod(minute, 60)
            hour %= 24
            return time(
                hour=hour,
                minute=minute,
                second=second,
                microsecond=microsecond,
            )
        if isinstance(value, datetime):
            return value.timetz()
        raise ValueError(
            f"Don't know how to create a {cls.__name__!r} object from"
            f" a {type(value).__name__!r} object: {value!r}"
        )
