"""Test the util.iterables module."""

# Builtins

# Packages

from phylline.util.iterables import make_collection, remove_none


def test_make_collection_singleton():
    """Test whether the make_collection function makes collections from singletons."""
    assert make_collection(42) != 42
    assert make_collection(42) == [42]
    assert make_collection(42) != (42,)

    assert make_collection(42, type=tuple) != 42
    assert make_collection(42, type=tuple) != [42]
    assert make_collection(42, type=tuple) == (42,)


def test_make_collection_iterable():
    """Test whether the make_collection function makes collections from iterables."""
    assert make_collection(range(5)) != range(5)
    assert make_collection(range(5)) == list(range(5))
    assert make_collection(range(5)) != tuple(range(5))

    assert make_collection(range(5), type=tuple) != range(5)
    assert make_collection(range(5), type=tuple) != list(range(5))
    assert make_collection(range(5), type=tuple) == tuple(range(5))


def test_remove_none():
    """Test whether remove_none removes Nones correctly."""
    assert len(tuple(range(5))) == len(tuple(remove_none(range(5))))
    for (initial, filtered) in zip(range(5), remove_none(range(5))):
        assert initial == filtered
    assert len(tuple(remove_none([1, 2, None, 3]))) == 3
    assert tuple(remove_none([1, 2, None, 3])) == (1, 2, 3)
