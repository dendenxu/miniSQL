"""
Considering changing constant return value to exception
try and catch stuff
"""


class MiniSQLException(Exception):
    """
    raised when we cannot find a particular key
    or duplication occurs
    """

    def __init__(self, message, errors=None):
        # Call the base class constructor with the parameters it needs
        super().__init__(message)

        # Now for your custom code...
        self.errors = errors


class KeyException(MiniSQLException):
    def __init__(self, message, errors=None):
        super().__init__(message, errors)


class RangeException(MiniSQLException):
    def __init__(self, message, errors=None):
        super().__init__(message, errors)


class TreeException(MiniSQLException):
    def __init__(self, message, errors=None):
        super().__init__(message, errors)


def dummy_cmp(x, y):
    return x < y
