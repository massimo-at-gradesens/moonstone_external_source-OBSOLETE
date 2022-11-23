"""
GradeSens - External Source package - Datetime support tools

This module provides extensions to stock module :module:`datetime`.
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, GradeSens AG"


from datetime import date, datetime, time, timedelta
from typing import Union

from pytimeparse import parse as parse_seconds


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

    Furthermore, :meth:`.__str__` relies on :meth:`.isoformat()` instead of
    the default human-readable format of stock :class:`datetime.datetime`
    class.
    """

    InputType = Union[int, datetime, str, float, time, date]

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

    def date(self, *args, **kwargs) -> "Date":
        return Date(super().date(*args, **kwargs))

    def time(self, *args, **kwargs) -> "Time":
        return Time(super().time(*args, **kwargs))

    def timetz(self, *args, **kwargs) -> "Time":
        return Time(super().timetz(*args, **kwargs))

    @classmethod
    def combine(cls, *args, **kwargs) -> "DateTime":
        return DateTime(datetime.combine(*args, **kwargs))

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

    def __str__(self):
        return self.isoformat()


class Date(date):
    """
    A clone of stock library :class:`datetime.date`, extending the constructor
    capabilities to enable creating a new object directly from any of the
    following:
    * another :class:`datetime.date` object,
    * an ISO 8601 string,
    * a POSIX timestamp,
    * a :class:`datetime.datetime` object,

    Furthermore, :meth:`.__str__` relies on :meth:`.isoformat()` instead of
    the default human-readable format of stock :class:`datetime.date` class.
    """

    InputType = Union[int, date, str, float]

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

    def __str__(self):
        return self.isoformat()


class Time(time):
    """
    A clone of stock library :class:`datetime.time`, extending the constructor
    capabilities to enable creating a new object directly from any of the
    following:
    * another :class:`datetime.time` object,
    * an ISO 8601 string,
    * a POSIX timestamp,
    * a :class:`datetime.datetime` object,

    Furthermore, :meth:`.__str__` relies on :meth:`.isoformat()` instead of
    the default human-readable format of stock :class:`datetime.time` class.
    """

    InputType = Union[int, time, str, float, datetime]

    def __new__(cls, hour=0, *args, **kwargs):
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

    def __str__(self):
        return self.isoformat()


class TimeDelta(timedelta):
    """
    A clone of stock library :class:`datetime.timedelta`, extending the
    constructor capabilities to enable creating a new object directly from any
    of the following:
    * another :class:`datetime.timedelta` object,
    * a floating-point number of days (i.e. single value construction from
      stock :meth:`datetime.timedelta.__init__` constructor)
    * a human readable string representation of time:
      * ``[[[DD:]HH:]MM:]SS[.fractional part]``
      * ``value unit [,/] [value unit [,/] [...]]`` with unit equal to any of:
        * weeks, w, week, wks, wk,
        * days, d, day, dys, dy,
        * hours, h, hour, hrs, hr,
        * minutes, m, minute, mins, min,
        * seconds, s, second, secs, sec.
        For this format to be valid, the `value unit` components must follow
        the same order as the list of units here above.
    """

    InputType = Union[int, float, timedelta, str]

    def __new__(cls, days=0, *args, **kwargs):
        if isinstance(days, (int, float)):
            return super().__new__(cls, days, *args, **kwargs)

        assert not args
        assert not kwargs
        other = cls.__convert(days)
        return super().__new__(cls, seconds=other)

    @classmethod
    def __convert(cls, value):
        if isinstance(value, timedelta):
            return value.total_seconds()
        if isinstance(value, str):
            result = parse_seconds(value)
            if result is not None:
                return result
            raise ValueError(
                f"Not a valid {cls.__name__!r} representation {value!r}"
            )
        raise ValueError(
            f"Don't know how to create a {cls.__name__!r} object from"
            f" a {type(value).__name__!r} object: {value!r}"
        )
