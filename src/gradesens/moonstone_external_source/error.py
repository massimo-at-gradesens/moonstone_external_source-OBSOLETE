"""
GradeSens - External Source package - package-specific exceptions
"""
__author__ = "Massimo Ravasi"
__copyright__ = "Copyright 2022, Gradesens AG"


class Error(Exception):
    pass


class HttpError(Error):
    def __init__(self, msg, status):
        super().__init__(msg)
        self.status = status

    def __str__(self):
        return f"HTTP ERROR (status={self.status}): {super().__str__()}"
