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

# Packages

from phylline.links.events import DataEventLink, EventLink, EventLinkAbove, EventLinkBelow
from phylline.links.streams import StreamLink, StreamLinkAbove, StreamLinkBelow
from phylline.processors import event_processor, receive, send
from phylline.processors import read_until, stream_processor, write


class GenericLinkBelow(EventLinkBelow, StreamLinkBelow):
    """Interface for exposing a generic Link interface for the layer below."""

    pass


class GenericLinkAbove(EventLinkAbove, StreamLinkAbove):
    """Interface for exposing a generic Link interface for the layer above."""

    pass


# Utility links


class ChunkedStreamLink(StreamLinkBelow, EventLinkAbove, DataEventLink):
    """Processor which sends and receives delimited chunks of data over a stream.

    Adapts byte buffer-based stream processors to event queue processors by
    discretizing the stream into chunks delimited by a chunk separator in the stream.
    When begin_chunk_separator is enabled, each chunk will be delimited by the
    chunk separator at both the start and end of the chunk; otherwise, it will
    only be delimited by a chunk separator at the end of the chunk.

    Omits empty chunks, which will be neither sent nor received.

    Interface:
    Above: sends and receives bytestrings of chunks.
    Below: to_send and to_receive delimited bytestrings (not following chunk boundaries).
    """

    def __init__(self, name=None, chunk_separator=b'\0', begin_chunk_separator=True):
        """Initialize processors."""
        self._event_link = EventLink(
            sender_processor=self.sender_processor,
            sender_processor_args=(chunk_separator, begin_chunk_separator),
            receiver_event_passthrough=True
        )
        self._event_link.after_receive = self.__after_receive
        self._stream_link = StreamLink(
            reader_processor=self.reader_processor,
            reader_processor_args=(chunk_separator,)
        )
        self._stream_link.after_write = self.__after_write
        self.name = name

    def __repr__(self):
        """Return a string representation of the link."""
        if self.name is not None:
            return '⇌~ {}({}) □⇌'.format(self.__class__.__qualname__, self.name)
        else:
            return '⇌~ {} □⇌'.format(self.__class__.__qualname__)

    # Implement EventLinkAbove

    def receive(self):
        """Implement EventLinkAbove.receive."""
        return self._event_link.receive()

    def has_receive(self):
        """Implement EventLinkAbove.has_receive."""
        return self._event_link.has_receive()

    def send(self, chunk):
        """Implement EventLinkAbove.send."""
        self._event_link.send(chunk)

    # Implement StreamLinkBelow

    def to_read(self, bytes_data):
        """Implement StreamLinkBelow.to_read."""
        self._stream_link.to_read(bytes_data)

    def to_write(self):
        """Implement StreamLinkBelow.to_write."""
        return self._stream_link.to_write()

    # Utilities for writing receive and send processors

    def __after_receive(self, event):
        yield from self.after_receive(event)

    def __after_write(self, event):
        yield from self.after_write(event)

    def after_receive(self, event):
        """Enqueue the event for the layer above for consumption by that layer.

        This gets overridden to change the receiver's behavior to send the event.
        Note that this gets monkey-patched by other links and link utilities
        which combine links, such as ChunkedStreamLink and AutomaticPipe!
        """
        # print('Event link after_receive: {}'.format(event))
        yield from send(event)

    def after_write(self, buffer):
        """Enqueue the buffer for the layer below for consumption by that layer.

        This gets overridden to change the receiver's behavior to send the event.
        Note that this gets monkey-patched by other links and link utilities
        which combine links, such as ChunkedStreamLink and AutomaticPipe!
        """
        # print('Stream link after_write: {}'.format(buffer))
        yield from write(buffer)

    # Receive and send processors

    @stream_processor
    def reader_processor(self, chunk_separator):
        """Stream reader processor."""
        while True:
            buffer = yield from read_until(chunk_separator)
            if len(buffer) == 0:
                continue
            # print(hex_bytes(buffer))
            data_event = self.make_link_data(buffer, 'up', None)
            self._event_link.to_receive(data_event)

    @event_processor
    def sender_processor(self, chunk_separator, begin_chunk_separator):
        """Event sender processor."""
        while True:
            event = yield from receive()
            data_event = self.get_link_data(event, 'down')
            event = data_event.data
            if len(event) == 0:
                continue
            stream_contents = event + chunk_separator
            if begin_chunk_separator:
                stream_contents = chunk_separator + stream_contents
            self._stream_link.write(stream_contents)
