"""
GradeSens - External Source package - Datetime support tools

This module provides extensions to stock module :module:`datetime`.
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, GradeSens AG"


from datetime import date, datetime, time, timedelta, timezone, tzinfo
from typing import Dict, Tuple, Union

from pytimeparse import parse as parse_seconds

from .utils import classproperty


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

    def __new__(cls, year: InputType, *args, **kwargs):
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
    def __convert(cls, value) -> datetime:
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            if value[-1:].lower() == "z":
                value = value[:-1] + "+00:00"
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

    @classproperty
    def min(cls) -> "DateTime":
        return DateTime(super().min)

    @classproperty
    def max(cls) -> "DateTime":
        return DateTime(super().max)

    @classproperty
    def resolution(cls) -> "TimeDelta":
        return TimeDelta(super().resolution)

    @property
    def tzinfo(cls) -> Union["TimeZone", None]:
        result = super().tzinfo
        if result is None:
            return None
        return TimeZone(result)

    @classmethod
    def today(cls, *args, **kwargs) -> "DateTime":
        return DateTime(super().today(*args, **kwargs))

    @classmethod
    def now(cls, *args, **kwargs) -> "DateTime":
        return DateTime(super().now(*args, **kwargs))

    @classmethod
    def utcnow(cls, *args, **kwargs) -> "DateTime":
        return DateTime(super().utcnow(*args, **kwargs))

    @classmethod
    def fromtimestamp(cls, *args, **kwargs) -> "DateTime":
        return DateTime(super().fromtimestamp(*args, **kwargs))

    @classmethod
    def utcfromtimestamp(cls, *args, **kwargs) -> "DateTime":
        return DateTime(super().utcfromtimestamp(*args, **kwargs))

    @classmethod
    def fromordinal(cls, *args, **kwargs) -> "DateTime":
        return DateTime(super().fromordinal(*args, **kwargs))

    @classmethod
    def combine(cls, *args, **kwargs) -> "DateTime":
        return DateTime(datetime.combine(*args, **kwargs))

    @classmethod
    def fromisoformat(cls, *args, **kwargs) -> "DateTime":
        return DateTime(super().fromisoformat(*args, **kwargs))

    @classmethod
    def fromisocaledar(cls, *args, **kwargs) -> "DateTime":
        return DateTime(super().fromisocalendar(*args, **kwargs))

    @classmethod
    def strptime(cls, *args, **kwargs) -> "DateTime":
        return DateTime(super().strptime(*args, **kwargs))

    def date(self, *args, **kwargs) -> "Date":
        return Date(super().date(*args, **kwargs))

    def time(self, *args, **kwargs) -> "Time":
        return Time(super().time(*args, **kwargs))

    def timetz(self, *args, **kwargs) -> "Time":
        return Time(super().timetz(*args, **kwargs))

    def replace(self, *args, **kwargs) -> "DateTime":
        return DateTime(super().replace(*args, **kwargs))

    def astimezone(self, *args, **kwargs) -> "DateTime":
        return DateTime(super().astimezone(*args, **kwargs))

    def utcoffset(self, *args, **kwargs) -> Union["TimeDelta", None]:
        result = super().utcoffset(*args, **kwargs)
        if result is None:
            return None
        return TimeDelta(result)

    def dst(self, *args, **kwargs) -> Union["TimeDelta", None]:
        result = super().dst(*args, **kwargs)
        if result is None:
            return None
        return TimeDelta(result)

    def z_utc_format(self, *args, **kwargs) -> "str":
        tmp = self.astimezone(timezone.utc)
        return tmp.replace(tzinfo=None).isoformat(*args, **kwargs) + "Z"

    def __add__(self, other) -> "DateTime":
        return DateTime(super().__add__(other))

    def __radd__(self, other) -> "DateTime":
        return DateTime(super().__radd__(other))

    def __sub__(self, other) -> Union["DateTime", "TimeDelta"]:
        result = super().__sub__(other)
        if isinstance(result, timedelta):
            return TimeDelta(result)
        return DateTime(result)

    def __rsub__(self, other) -> Union["DateTime", "TimeDelta"]:
        result = super().__rsub__(other)
        if isinstance(result, timedelta):
            return TimeDelta(result)
        return DateTime(result)

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

    def __new__(cls, hour: InputType = 0, *args, **kwargs):
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
    def __convert(cls, value) -> time:
        if isinstance(value, time):
            return value
        if isinstance(value, str):
            if value[-1:].lower() == "z":
                value = value[:-1] + "+00:00"
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

    @classproperty
    def min(cls) -> "Time":
        return Time(super().min)

    @classproperty
    def max(cls) -> "Time":
        return Time(super().max)

    @classproperty
    def resolution(cls) -> "TimeDelta":
        return TimeDelta(super().resolution)

    @property
    def tzinfo(cls) -> Union["TimeZone", None]:
        result = super().tzinfo
        if result is None:
            return None
        return TimeZone(result)

    @classmethod
    def fromisoformat(cls, *args, **kwargs) -> "Time":
        return Time(super().fromisoformat(*args, **kwargs))

    def replace(self, *args, **kwargs) -> "Time":
        return Time(super().replace(*args, **kwargs))

    def utcoffset(self, *args, **kwargs) -> Union["TimeDelta", None]:
        result = super().utcoffset(*args, **kwargs)
        if result is None:
            return None
        return TimeDelta(result)

    def dst(self, *args, **kwargs) -> Union["TimeDelta", None]:
        result = super().dst(*args, **kwargs)
        if result is None:
            return None
        return TimeDelta(result)

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

    def __new__(cls, year: InputType, *args, **kwargs):
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
    def __convert(cls, value) -> date:
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

    @classproperty
    def min(cls) -> "Date":
        return Date(super().min)

    @classproperty
    def max(cls) -> "Date":
        return Date(super().max)

    @classproperty
    def resolution(cls) -> "TimeDelta":
        return TimeDelta(super().resolution)

    @classmethod
    def today(cls, *args, **kwargs) -> "Date":
        return Date(super().today(*args, **kwargs))

    @classmethod
    def fromtimestamp(cls, *args, **kwargs) -> "Date":
        return Date(super().fromtimestamp(*args, **kwargs))

    @classmethod
    def fromordinal(cls, *args, **kwargs) -> "Date":
        return Date(super().fromordinal(*args, **kwargs))

    @classmethod
    def fromisoformat(cls, *args, **kwargs) -> "Date":
        return Date(super().fromisoformat(*args, **kwargs))

    @classmethod
    def fromisocalendar(cls, *args, **kwargs) -> "Date":
        return Date(super().fromisocalendar(*args, **kwargs))

    def __add__(self, other) -> "Date":
        return Date(super().__add__(other))

    def __radd__(self, other) -> "Date":
        return Date(super().__radd__(other))

    def __sub__(self, other) -> Union["Date", "TimeDelta"]:
        result = super().__sub__(other)
        if isinstance(result, timedelta):
            return TimeDelta(result)
        return Date(result)

    def __rsub__(self, other) -> Union["Date", "TimeDelta"]:
        result = super().__rsub__(other)
        if isinstance(result, timedelta):
            return TimeDelta(result)
        return Date(result)

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

    def __new__(cls, days: InputType = 0, *args, **kwargs):
        if isinstance(days, (int, float)):
            return super().__new__(cls, days, *args, **kwargs)

        assert not args
        assert not kwargs
        init_dict = cls.__convert(days)
        return super().__new__(cls, **init_dict)

    @classmethod
    def __convert(cls, value) -> Dict[str, Union[int, float]]:
        if isinstance(value, timedelta):
            return dict(
                days=value.days,
                seconds=value.seconds,
                microseconds=value.microseconds,
            )
        if isinstance(value, str):
            result = parse_seconds(value)
            if result is not None:
                return dict(seconds=result)
            raise ValueError(
                f"Not a valid {cls.__name__!r} representation {value!r}"
            )
        raise ValueError(
            f"Don't know how to create a {cls.__name__!r} object from"
            f" a {type(value).__name__!r} object: {value!r}"
        )

    @classproperty
    def min(cls) -> "TimeDelta":
        return TimeDelta(super().min)

    @classproperty
    def max(cls) -> "TimeDelta":
        return TimeDelta(super().max)

    def __add__(self, other) -> Union["TimeDelta", "DateTime", "Date"]:
        if isinstance(other, datetime):
            return DateTime(other + self)
        if isinstance(other, date):
            return Date(other + self)
        return TimeDelta(super().__add__(other))

    def __radd__(self, other) -> Union["TimeDelta", "DateTime", "Date"]:
        return TimeDelta(super().__radd__(other))

    def __sub__(self, other) -> Union["TimeDelta", "DateTime", "Date"]:
        return TimeDelta(super().__sub__(other))

    def __rsub__(self, other) -> "TimeDelta":
        return TimeDelta(super().__rsub__(other))

    def __mul__(self, other) -> "TimeDelta":
        return TimeDelta(super().__mul__(other))

    def __rmul__(self, other) -> "TimeDelta":
        return TimeDelta(super().__rmul__(other))

    def __truediv__(self, other) -> Union["TimeDelta", float]:
        result = super().__truediv__(other)
        if isinstance(result, timedelta):
            return TimeDelta(result)
        return result

    def __floordiv__(self, other) -> Union["TimeDelta", int]:
        result = super().__floordiv__(other)
        if isinstance(result, timedelta):
            return TimeDelta(result)
        return result

    def __mod__(self, other) -> "TimeDelta":
        return TimeDelta(super().__mod__(other))

    def __divmod__(self, other) -> Tuple[int, "TimeDelta"]:
        q, r = super().__divmod__(other)
        return q, TimeDelta(r)

    def __rdivmod__(self, other) -> Tuple[int, "TimeDelta"]:
        q, r = super().__rdivmod__(other)
        return q, TimeDelta(r)

    def __pos__(self) -> "TimeDelta":
        return TimeDelta(super().__pos__())

    def __neg__(self) -> "TimeDelta":
        return TimeDelta(super().__neg__())

    def __abs__(self) -> "TimeDelta":
        return TimeDelta(super().__abs__())

    def __str__(self):
        if self >= timedelta(0):
            return super().__str__()
        return f"-{-self}"


class TimeZone(tzinfo):
    """
    A clone of stock library :class:`datetime.timezone`, for seamless
    integration with the other :package:`datetime` clones in this module.
    The constructor capabilities to enable creating a new object directly from
    any of the following:
    * another :class:`datetime.timezone` object,
    """

    InputType = Union[timedelta, tzinfo]

    def __init__(self, offset: InputType, *args, **kwargs):
        if isinstance(offset, timedelta):
            value = timezone(offset, *args, **kwargs)
        else:
            assert not args
            assert not kwargs
            if not isinstance(offset, tzinfo):
                raise ValueError(
                    "Don't know how to create"
                    f" a {type(self).__name__!r} object from"
                    f" a {type(offset).__name__!r} object: {offset!r}"
                )
            value = offset
        self._value = value

    @classproperty
    def utc(cls) -> "TimeZone":
        return TimeZone(timezone.utc)

    def utcoffset(self, *args, **kwargs) -> "TimeDelta":
        return TimeDelta(self._value.utcoffset(*args, **kwargs))

    def dst(self, *args, **kwargs) -> "TimeDelta":
        result = self._value.dst(*args, **kwargs)
        if result is None:
            return None
        return TimeDelta(result)

    def tzname(self, *args, **kwargs) -> str:
        return self._value.tzname(*args, **kwargs)

    def fromutc(self, *args, **kwargs) -> "DateTime":
        return DateTime(self._value.fromutc(*args, **kwargs))

    def __eq__(self, other):
        if isinstance(other, TimeZone):
            other = other._value
        return self._value == other

    def __lt__(self, other):
        if isinstance(other, TimeZone):
            other = other._value
        return self._value < other
