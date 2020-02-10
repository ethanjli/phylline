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

from phylline.links.events import DataEventLink, EventLink, LinkException
from phylline.links.links import GenericLinkAbove, GenericLinkBelow
from phylline.links.streams import StreamLink
from phylline.processors import event_processor, receive
from phylline.processors import read, stream_processor
from phylline.processors import wait


class TopLoopbackLink(GenericLinkBelow, DataEventLink):
    """A data sink to echo events and stream buffers received from below to send back down.

    Note that the events received by to_receive are only echoed back on to_send,
    and buffers read by to_read are only echoed back on to_write.
    """

    def __init__(self, name=None):
        """Initialize members."""
        self.name = name
        self._event_link = EventLink(receiver_processor=self.receiver_processor)
        self._stream_link = StreamLink(reader_processor=self.reader_processor)

    def __repr__(self):
        """Return a string representation of the link."""
        if self.name is not None:
            return '⇌* {}({})'.format(self.__class__.__qualname__, self.name)
        else:
            return '⇌* {}'.format(self.__class__.__qualname__)

    # Implement EventLinkBelow

    def to_receive(self, event):
        """Implement EventLinkBelow."""
        self._event_link.to_receive(event)

    def to_send(self):
        """Implement EventLinkBelow."""
        return self._event_link.to_send()

    def has_to_send(self):
        """Implement EventLinkBelow."""
        return self._event_link.has_to_send()

    # Implement StreamLinkBelow

    def to_read(self, bytes_data):
        """Implement StreamLinkBelow."""
        self._stream_link.to_read(bytes_data)

    def to_write(self):
        """Implement StreamLinkBelow."""
        return self._stream_link.to_write()

    # Read and write processors

    def after_send(self, event):
        """Do nothing."""
        yield from wait()

    def after_write(self, buffer):
        """Do nothing."""
        yield from wait()

    @event_processor
    def receiver_processor(self):
        """Event receiver echo processor."""
        while True:
            event = yield from receive()
            if isinstance(event, LinkException):
                print('Ignoring exception: {}'.format(event))
                continue
            data_event = self.get_link_data(event, 'loopback')
            # print('Echoing: {}'.format(data_event))
            data_event = self.make_link_data(data_event.data, 'loopback', event)
            self._event_link._sender.send(data_event)
            yield from self.after_send(event)  # for correctness in automatic pipeline

    @stream_processor
    def reader_processor(self):
        """Stream reader echo processor."""
        while True:
            buffer = yield from read()
            # print('Echoing: {}'.format(buffer))
            self._stream_link._writer.send(buffer)
            yield from self.after_write(buffer)  # for correctness in automatic pipeline


class BottomLoopbackLink(GenericLinkAbove, DataEventLink):
    """A data sink to echo events and stream buffers sent from above to receive back up.

    Note that the events sent by send are only echoed back on receive,
    and buffers read by write are only echoed back on read.
    """

    def __init__(self, name=None):
        """Initialize members."""
        self.name = name
        self._event_link = EventLink(sender_processor=self.sender_processor)
        self._stream_link = StreamLink(writer_processor=self.writer_processor)

    def __repr__(self):
        """Return a string representation of the link."""
        if self.name is not None:
            return '{}({}) *⇌'.format(self.__class__.__qualname__, self.name)
        else:
            return '{} *⇌'.format(self.__class__.__qualname__)

    # Implement EventLinkAbove

    def receive(self):
        """Implement EventLinkAbove.receive."""
        return self._event_link.receive()

    def has_receive(self):
        """Implement EventLinkAbove.has_receive."""
        return self._event_link.has_receive()

    def send(self, event):
        """Implement EventLinkAbove.send."""
        self._event_link.send(event)

    # Implement StreamLinkAbove

    def read(self):
        """Implement StreamLinkAbove.read."""
        return self._stream_link.read()

    def write(self, bytes_data):
        """Implement StreamLinkAbove.write."""
        self._stream_link.write(bytes_data)

    # Read and write processors

    def after_receive(self, event):
        """Do nothing."""
        yield from wait()

    def after_read(self, buffer):
        """Do nothing."""
        yield from wait()

    @event_processor
    def sender_processor(self):
        """Event sender echo processor."""
        while True:
            event = yield from receive()
            data_event = self.get_link_data(event, 'loopback')
            data_event = self.make_link_data(data_event.data, 'loopback', event)
            self._event_link._receiver.send(data_event)
            yield from self.after_receive(data_event)  # for correctness in automatic pipeline

    @stream_processor
    def writer_processor(self):
        """Stream writer echo processor."""
        while True:
            buffer = yield from read()
            self._stream_link._reader.send(buffer)
            yield from self.after_read(buffer)  # needed for correctness in automatic pipeline
