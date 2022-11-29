"""
GradeSens - Moonstone Snooper package - Package-specific exceptions
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, GradeSens AG"


from collections.abc import Iterable


class Error(Exception):
    def __init__(self, message, index=None):
        self.message = message

        if index is None:
            index = []
        elif isinstance(index, str) or not isinstance(index, Iterable):
            index = [index]
        else:
            index = list(index)
        self.index = index

    def _raw_str(self):
        if self.message is None:
            return ""
        return str(self.message)

    def __str__(self):
        message = self._raw_str()
        if len(self.index) == 0:
            return message
        index = "".join(f"[{elem!r}]" for elem in self.index)
        return f"@{index}: {message}"


class BackendError(Error):
    pass


class HTTPError(BackendError):
    def __init__(self, message, status=None):
        super().__init__(message)
        self.status = status

    def _raw_str(self):
        status_suffix = (
            "" if self.status is None else f"(status={self.status})"
        )
        return f"HTTP ERROR{status_suffix}: {super()._raw_str()}"


class HTTPResponseError(HTTPError):
    pass


class ConfigurationError(Error):
    pass


class EvalError(Error):
    def __init__(self, message=None, expression=None):
        super().__init__(message)
        self.expression = expression

    def _raw_str(self):
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

    def _raw_str(self):
        message = "" if self.message is None else str(self.message)
        if self.pattern is None:
            return message
        if len(message) != 0:
            message = f": {message}"
        return f"Pattern {self.pattern!r}{message}"


class TimeError(Error):
    pass


class DataTypeError(Error):
    pass


class DataValueError(Error):
    pass
