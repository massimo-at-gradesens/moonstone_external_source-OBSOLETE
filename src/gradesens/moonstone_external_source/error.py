"""
GradeSens - External Source package - Package-specific exceptions
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, Gradesens AG"


from collections.abc import Iterable


class Error(Exception):
    def __init__(self, *args, index=None, **kwargs):
        super().__init__(*args, **kwargs)
        if index is None:
            index = []
        elif isinstance(index, str) or not isinstance(index, Iterable):
            index = [index]
        else:
            index = list(index)
        self.index = index

    def __str__(self):
        msg = super().__str__()
        if len(self.index) == 0:
            return msg
        index = "".join(f"[{elem!r}]" for elem in self.index)
        return f"@{index}: {msg}"


class HTTPError(Error):
    def __init__(self, msg, status=None):
        super().__init__(msg)
        self.status = status

    def __str__(self):
        status_suffix = (
            "" if self.status is None else f"(status={self.status})"
        )
        return f"HTTP ERROR{status_suffix}: {super().__str__()}"


class ConfigurationError(Error):
    pass


class HTTPResponseError(HTTPError):
    pass


class EvalError(Error):
    def __init__(self, message=None, expression=None):
        super().__init__(message)
        self.expression = expression
        self.message = message

    def __str__(self):
        message = "" if self.message is None else str(self.message)
        if self.expression is None:
            return message
        if message:
            message = ": " + message
        return f"Expression {self.expression!r}{message}"


class PatternError(Error):
    def __init__(self, message=None, pattern=None):
        super().__init__(message)
        self.pattern = pattern
        self.message = message

    def __str__(self):
        message = "" if self.message is None else str(self.message)
        if self.pattern is None:
            return message
        if message:
            message = ": " + message
        return f"Pattern {self.pattern!r}{message}"


class TimeError(Error):
    pass


class DataTypeError(Error):
    pass


class DataValueError(Error):
    pass
