class Error(Exception):
    """Base class for other exceptions"""
    pass


class ValueTooSmallError(Error):
    """Raised when the input value is too small"""
    pass


class ValueTooLargeError(Error):
    """Raised when the input value is too large"""
    pass


class WrongExtError(Error):
    """Raised when file extension is """
    pass


class MissingS3FileError(Error):
    pass


class MissingDataError(Error):
    pass


class ZeroSizeError(Error):
    pass


class FileDiffSize(Error):
    pass
