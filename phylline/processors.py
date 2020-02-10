"""Utilities for writing Phyllo protocol processors.

Processors can be formally described as nodes/processes of a Kahn process network
(see https://en.wikipedia.org/wiki/Kahn_process_networks for details), but
always with one input channel and one output channel.

Provides interfaces to write byte buffer-based stream protocol processors or
queue-based discrete event protocol processors.

The terminology here is a bit overloaded: protocol processors are called "protocols"
 n ohneio, but they are not network protocols. So instead we call them processors.
"""

# Builtins

import functools
import inspect
from collections import deque

# Packages

import ohneio


# Flow control


wait = ohneio.wait
_proceed = ohneio._Action('proceed')  # continue without waiting


def proceed():
    """Proceed without waiting for any action to be triggered on the consumer."""
    yield _proceed


# Event-based processors

class EventConsumer(ohneio.Consumer):
    """Generalized consumer which uses handles inputs and outputs on a deque.

    Allows the developer to send, read, and get the result of a processor function.

    This never needs to be instantiated, since this is internally done by the
    processor decorator.

    Adapted from ohneio.
    """

    def __init__(self, gen, input_factory=deque, output_factory=deque):
        """Initialize members."""
        self.gen = gen
        self.input = input_factory()
        self.output = output_factory()
        self.state = next(gen)
        if not isinstance(self.state, ohneio._Action):
            raise RuntimeError(
                "Can't yield anyting else than an action. Using `yield` instead "
                'of `yield from`?'
            )
        self.res = ohneio._no_result

    def _process(self):
        if self.has_result:
            return

        while self.state is ohneio._wait:
            self._next_state()
        while True:
            if self.state is ohneio._get_output:
                self._next_state(self.output)
            elif self.state is ohneio._get_input:
                self._next_state(self.input)
            elif self.state is _proceed:
                self._next_state()
            else:
                break

    def read(self):
        """Read and consume event from the output of the processor."""
        return self.output.popleft()

    def has_read(self):
        """Return events at the output of the processor, without consuming them."""
        return len(self.output) > 0

    def send(self, event):
        """Send event to the input of the processor."""
        self.input.append(event)
        self._process()

    def directly_to_read(self, event):
        """Add event to the output queue of the processor."""
        self.output.append(event)
        self._process()


def event_processor(func, input=deque, output=deque):
    """Wrap a Phyllo processor generator function as a decorator.

    Under the hood this wraps the generator inside an EventConsumer.
    """
    if not callable(func):
        raise ValueError('A processor needs to be a callable')
    if not inspect.isgeneratorfunction(func):
        raise ValueError('A processor needs to be a generator function')

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return EventConsumer(func(*args, **kwargs), input_factory=input, output_factory=output)

    return wrapper


def receive():
    """Receive and consume events.

    Receive and consume events from the processor input queue.
    """
    while True:
        input_queue = yield ohneio._get_input
        try:
            return input_queue.popleft()
        except IndexError:
            pass
        yield from wait()


def send(event):
    """Send event so that it can be consumed.

    Send event to the processor output queue.
    """
    output_queue = yield ohneio._get_output
    output_queue.append(event)


# ohneio-style stream-based processors

stream_processor = ohneio.protocol


def can_read(num_bytes=1):
    """Wait until we can receive and consume at least the specified number of bytes."""
    while True:
        input_buffer = yield ohneio._get_input
        if len(input_buffer) >= num_bytes:
            return
        else:
            pass
        yield from wait()


# We don't reimport ohneio.read because we provide an implementation whose
# interface is a superset of what is offered by ohneio.read.
def read(min_bytes=1, max_bytes=None):
    """Receive and consume the specified number of bytes.

    If min_bytes is nonzero, only returns when new bytes are on the input buffer.
    If max_bytes is None, then it reads everything available.
    """
    yield from can_read(min_bytes)  # never return an empty buffer
    if max_bytes is None:
        max_bytes = 0
    buffer = yield from ohneio.read(max_bytes)
    return buffer


def read_until(separator):
    """Generate a buffer of data until the next separator in the stream buffer.

    Waits for a complete frame to become available.
    """
    chunk_buffer = bytearray()
    while True:
        byte = yield from read(max_bytes=1)
        chunk_buffer.extend(byte)
        if chunk_buffer.endswith(separator):
            return bytes(chunk_buffer[:-len(separator)])


# We don't reimport ohneio.write because we provide an implementation which
# doesn't block the processor while waiting for the output to be entirely consumed.
def write(buffer):
    """Write data."""
    output = yield ohneio._get_output
    output.write(buffer)
