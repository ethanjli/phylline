"""Test the processors module."""

# Builtins

# Packages

from phylline.processors import event_processor, receive, send
from phylline.processors import proceed, wait
from phylline.processors import read, read_until, stream_processor, write

import pytest


# Basic event processor


@event_processor
def incrementer(last_received=[None]):
    """Increment every received number."""
    while True:
        number = yield from receive()
        yield from send(number + 1)
        last_received[0] = number


@event_processor
def incrementer_proceed(last_received=[None]):
    """Increment every received number."""
    while True:
        number = yield from receive()
        yield from send(number + 1)
        yield from proceed()  # this basically does nothing
        last_received[0] = number


def assert_incrementing(processor, last_received, number):
    """Test whether the processor increments the input."""
    processor.send(number)
    assert last_received == [number]
    assert processor.has_read()
    assert processor.read() == number + 1
    assert not processor.has_read()


@pytest.mark.parametrize('incrementer_processor', [
    incrementer,
    incrementer_proceed
])
def test_event_processor(incrementer_processor):
    """Test whether the event_processor works correctly with sends and receives."""
    last_received = [None]
    processor = incrementer_processor(last_received=last_received)

    # Individual processing
    assert_incrementing(processor, last_received, 1)
    assert_incrementing(processor, last_received, -2)

    # Batch processing
    for i in range(5):
        processor.send(i)
        assert last_received == [i]
    for i in range(5):
        assert processor.has_read()
        assert processor.read() == i + 1
    assert not processor.has_read()

    processor.directly_to_read(5)  # bypass the incrementer and add to read queue
    assert processor.has_read()
    assert processor.read() == 5


# Event processor with waiting


@event_processor
def incrementer_wait(last_received=[None]):
    """Increment every received number."""
    while True:
        number = yield from receive()
        yield from send(number + 1)
        yield from wait()  # wait until data is added to the receive queue before proceeding
        last_received[0] = number


def test_event_processor_wait():
    """Test whether the event_processor works correctly with sends and receives."""
    last_received = [None]
    processor = incrementer_wait(last_received=last_received)

    # Individual processing
    processor.send(1)
    assert last_received == [None]
    assert processor.read() == 2
    processor.send(2)
    assert last_received == [1]
    assert processor.read() == 3


# Event processor with return value


@event_processor
def incrementer_return():
    """Increment every received number."""
    number = yield from receive()
    yield from send(number + 1)
    return 10 * number


def test_event_processor_return():
    """Test whether the event_processor works correctly with returns."""
    processor = incrementer_return()

    processor.send(1)
    assert processor.read() == 2
    assert processor.get_result() == 10


# Invalid event processors


def test_event_processor_uncallable():
    """Test whether the event_processor decorator handles invalid inputs correctly."""
    with pytest.raises(ValueError):
        event_processor(5)


def test_event_processor_nongenerator():
    """Test whether the event_processor decorator handles invalid functions correctly."""
    with pytest.raises(ValueError):
        @event_processor
        def invalid_nongenerator():
            return 2


def test_event_processor_invalid_yield():
    """Test whether the event_processor decorator handles invalid yields correctly."""
    @event_processor
    def invalid_early():
        yield 42
    with pytest.raises(RuntimeError):
        processor = invalid_early()

    @event_processor
    def invalid_late():
        while True:
            number = yield from receive()
            yield number + 1
    processor = invalid_late()
    with pytest.raises(RuntimeError):
        processor.send(1)


# Basic stream processor


@stream_processor
def chunker():
    """Split the stream into chunks."""
    while True:
        buffer = yield from read_until(b'\0')
        yield from write(buffer)


@stream_processor
def passthrough():
    """Split the stream into chunks."""
    while True:
        buffer = yield from read()
        yield from write(buffer)


def test_stream_processor():
    """Test whether the event_processor works correctly with sends and receives."""
    processor = chunker()

    processor.send(b'\1\2\3\4')
    assert processor.read() == b''
    processor.send(b'\0')
    assert processor.read() == b'\1\2\3\4'
    processor.send(b'\1\2\0\3\4')
    assert processor.read() == b'\1\2'
    processor.send(b'\0\1\2\0\3\4\0')
    assert processor.read() == b'\3\4\1\2\3\4'

    processor = passthrough()
    processor.send(b'\1\2\3\4')
    assert processor.read() == b'\1\2\3\4'
