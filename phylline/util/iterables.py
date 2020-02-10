"""Utilities for working with iterables."""

# Builtins

# Packages


def make_collection(iterable_or_singleton, type=list):
    """Make a collection from either an iterable of elements or a singleton element."""
    try:
        iterator = iter(iterable_or_singleton)
        return type(iterator)
    except TypeError:
        return type((iterable_or_singleton,))


def remove_none(iterable):
    """Return all Nones from an iterable."""
    return (item for item in iterable if item is not None)
