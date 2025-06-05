__all__ = [
    "BlockParPathError",
    "BlockParFormatError",
    "BlockParContentHashError",
]


class BlockParError(Exception):
    """Base exception for BlockPar-related errors."""


class BlockParPathError(BlockParError, KeyError):
    """Raised when a path to a parameter or block is invalid or does not exist."""


class BlockParFormatError(BlockParError, ValueError):
    """Raised when the textual representation has an invalid structure or format."""


class BlockParContentHashError(BlockParError):
    """Raised when a .dat file's content hash is incorrect."""
