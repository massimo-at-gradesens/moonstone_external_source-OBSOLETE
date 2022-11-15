"""
GradeSens - External Source package - Package-specific exceptions
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, Gradesens AG"


class Error(Exception):
    pass


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


class PatternError(Error):
    pass


class TimeError(Error):
    pass


class DataTypeError(Error):
    pass
