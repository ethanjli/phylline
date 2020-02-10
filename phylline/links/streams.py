"""Base classes and utilities for writing communication links.

Links consist of a pair of processors: one processor to process incoming data from
the layer below and surface any resulting data to the layer above, and another to
process incoming data from the layer above and surface any resulting data to the
layer below. Then a link is a pair of independent Kahn process network nodes
which process data in opposite directions.

Provides interfaces to write byte buffer-based stream links or queue-based
discrete event links.
"""

# Builtins

from abc import abstractmethod

# Packages

from phylline.processors import read, stream_processor, write


class StreamLinkAbove(object):
    """Interface for exposing a StreamLink-like interface for the layer above.

    This interface is analogous to the interface for conn1 of a duplex
    multiprocessing.Pipe.
    """

    @abstractmethod
    def read(self):
        """Return available received bytes, while consuming them."""
        pass

    @abstractmethod
    def write(self, event):
        """Write data on the link."""
        pass


class StreamLinkBelow(object):
    """Interface for exposing a StreamLink-like interface for the layer below.

    This interface is analogous to the interface for conn2 of a duplex
    multiprocessing.Pipe.
    """

    @abstractmethod
    def to_read(self, event):
        """Read data on the link."""
        pass

    @abstractmethod
    def to_write(self):
        """Return available bytes to write from the link, while consuming them."""
        pass


# Link base classes


class StreamLink(StreamLinkBelow, StreamLinkAbove):
    """A link which sends and receives bytes of data over a stream.

    The default behavior is to pass the bytes through in both directions without
    any transformation. To change this behavior, either override the class with
    a custom sender and receiver processor, or (not recommended) provide a custom
    processor upon construction.

    Note that each StreamLink adds a bit of performance overhead - it is better
    to use EventLinks and/or SimpleStreamLinks instead.

    Interface:
    Above: sends and receives bytestrings.
    Below: to_send and to_receive bytestrings.
    """

    def __init__(
        self, name=None, reader_processor=None, writer_processor=None,
        reader_processor_args=(), reader_processor_kwargs={},
        writer_processor_args=(), writer_processor_kwargs={},
    ):
        """Initialize processors."""
        super().__init__()
        self.name = name
        if reader_processor is None:
            reader_processor = self.reader_processor
        self._reader = reader_processor(
            *reader_processor_args, **reader_processor_kwargs
        )
        if writer_processor is None:
            writer_processor = self.writer_processor
        self._writer = writer_processor(
            *writer_processor_args, **writer_processor_kwargs
        )

    def __repr__(self):
        """Return a string representation of the link."""
        return '⇌~ {} ~⇌'.format(self.__class__.__qualname__)

    # Implement StreamLinkAbove

    def read(self):
        """Implement StreamLinkAbove.read."""
        return self._reader.read()

    def write(self, bytes_data):
        """Implement StreamLinkAbove.write."""
        # print('{} writing: {}'.format(self.__class__.__qualname__, bytes_data))
        self._writer.send(bytes_data)

    # Implement StreamLinkBelow

    def to_read(self, bytes_data):
        """Implement StreamLinkBelow.to_read."""
        self._reader.send(bytes_data)

    def to_write(self):
        """Implement StreamLinkBelow.to_write."""
        return self._writer.read()

    # Utilities for writing read and write processors

    def after_read(self, buffer):
        """Enqueue the buffer for the layer above for consumption by that layer.

        This gets overridden to change the receiver's behavior to send the event.
        Note that this gets monkey-patched by other links and link utilities
        which combine links, such as ChunkedStreamLink and AutomaticPipe!
        """
        # print('Stream link after_read: {}'.format(buffer))
        yield from write(buffer)

    def after_write(self, buffer):
        """Enqueue the buffer for the layer below for consumption by that layer.

        This gets overridden to change the receiver's behavior to send the event.
        Note that this gets monkey-patched by other links and link utilities
        which combine links, such as ChunkedStreamLink and AutomaticPipe!
        """
        # print('Stream link after_write: {}'.format(buffer))
        yield from write(buffer)

    # Read and write processors

    @stream_processor
    def reader_processor(self):
        """Stream reader processor."""
        while True:
            buffer = yield from read()
            # print('Stream Link read: {}'.format(buffer))
            yield from self.after_read(buffer)

    @stream_processor
    def writer_processor(self):
        """Stream writer processor."""
        while True:
            buffer = yield from read()
            # print('Stream Link writing: {}'.format(buffer))
            yield from self.after_write(buffer)
