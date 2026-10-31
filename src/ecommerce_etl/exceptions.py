"""Pipeline-specific exceptions."""


class ETLException(Exception):
    """Base class for expected, user-facing pipeline errors."""


class SchemaError(ETLException):
    """The source file does not have the expected shape."""


class DataQualityError(ETLException):
    """A post-load quality gate failed."""
