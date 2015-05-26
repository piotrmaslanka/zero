class ZeroException(Exception):
    pass

class IOException(ZeroException):
    pass

class SeriesNotFoundException(ZeroException):
    pass

class DefinitionMismatchException(ZeroException):
    pass

class IllegalArgumentException(ZeroException):
    pass