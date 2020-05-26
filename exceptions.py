"""
Considering changing constant return value to exception
try and catch stuff
"""


class KeyException(Exception):
    """
    raised when we cannot find a particular key
    or duplication occurs
    """
    def __init__(self, message, errors=None):
        # Call the base class constructor with the parameters it needs
        super().__init__(message)

        # Now for your custom code...
        self.errors = errors


class RangeException(Exception):
    """
    raised when range is invalid
    for example when:
    front > end
    front > max
    min > end
    """
    def __init__(self, message, errors=None):
        # Call the base class constructor with the parameters it needs
        super().__init__(message)

        # Now for your custom code...
        self.errors = errors


class TreeException(Exception):
    """
    Tree is empty
    or SortedListIsEmpty
    """
    def __init__(self, message, errors=None):
        # Call the base class constructor with the parameters it needs
        super().__init__(message)

        # Now for your custom code...
        self.errors = errors
