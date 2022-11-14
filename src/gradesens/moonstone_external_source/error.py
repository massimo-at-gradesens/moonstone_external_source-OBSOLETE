"""
GradeSens - External Source package - Package-specific exceptions
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, Gradesens AG"


class Error(Exception):
    pass


class HTTPError(Error):
    def __init__(self, msg, status):
        super().__init__(msg)
        self.status = status

    def __str__(self):
        return f"HTTP ERROR (status={self.status}): {super().__str__()}"


class PatternError(Error):
    pass


class TimeError(Error):
    pass
