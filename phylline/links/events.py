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

from phylline.processors import event_processor, receive, send
from phylline.util.logging import hex_bytes


# Events

class LinkEvent(object):
    """Base class to represent any event an event link might produce or consume.

    context contains metadata as specified by the link.
    instance should be a reference to the link instance which produced the event.
    previous should be a reference to the previous LinkEvent or data which caused
        this event to be created.
    """

    def __init__(self, context=None, instance=None, previous=None):
        """Initialize members."""
        if context is None:
            context = {}
        self.context = context
        self.link = str(instance)
        self.previous = previous

    def __str__(self):
        """Represent as a string."""
        return '{} from ({}) {}{}'.format(
            self.__class__.__qualname__, self.link,
            'with context({})'.format(self.context) if self.context else '',
            ', due to ({})'.format(self.previous) if self.previous is not None else ''
        )

    def __repr__(self):
        """Represent as a string."""
        return '{}(context={}, instance={}, previous={})'.format(
            self.__class__.__qualname__, self.context, self.link, self.previous
        )


class LinkData(LinkEvent):
    """Event indicating a unit of data which was passed up or down a layer.

    data should be a reference to the data being passed.
    direction should be a string indicating the direction in which the data is
        being passed.
    """

    def __init__(
        self, data, direction='up', context=None, instance=None, previous=None
    ):
        """Initialize members."""
        super().__init__(context=context, instance=instance, previous=previous)
        self.data = data
        self.direction = direction

    def __str__(self):
        """Represent as a string."""
        return '{} passed {} in ({}){}: {}{}'.format(
            self.__class__.__qualname__,
            self.direction, self.link,
            ' with context({})'.format(self.context) if self.context else '',
            hex_bytes(self.data) if isinstance(self.data, (bytes, bytearray)) else self.data,
            ', due to ({})'.format(self.previous) if self.previous is not None else ''
        )

    def __repr__(self):
        """Represent as a string."""
        return '{}({}, direction={}, context={}, instance={}, previous={})'.format(
            self.__class__.__qualname__, self.data, self.direction, self.context,
            self.link, self.previous
        )


class LinkException(LinkEvent):
    """Event indicating an exception passed up or down a layer.

    exception should be a reference to the exception being passed.
    direction should be a string indicating the direction in which the data is
        being passed.
    """

    def __init__(
        self, exception, direction='up', context=None, instance=None, previous=None
    ):
        """Initialize members."""
        super().__init__(context=context, instance=instance, previous=previous)
        self.exception = exception
        self.direction = direction

    def __str__(self):
        """Represent as a string."""
        return '{} passed {} in ({}){}: {}{}'.format(
            self.__class__.__qualname__,
            self.direction, self.link,
            ' with context({})'.format(self.context) if self.context else '',
            self.exception,
            ', due to ({})'.format(self.previous) if self.previous is not None else ''
        )

    def __repr__(self):
        """Represent as a string."""
        return '{}({}, direction={}, context={}, instance={}, previous={})'.format(
            self.__class__.__qualname__, self.exception, self.direction, self.context,
            self.link, self.previous
        )


# Link interfaces


class EventLinkAbove(object):
    """Interface for exposing an EventLink-like interface for the layer above.

    This interface is analogous to the interface for conn1 of a duplex
    multiprocessing.Pipe.
    """

    @abstractmethod
    def receive(self):
        """Return the next received event, while consuming them."""
        pass

    def receive_all(self):
        """Generate all received events, while consuming them."""
        while self.has_receive():
            yield self.receive()

    @abstractmethod
    def has_receive(self):
        """Return whether received events are available, without consuming them."""
        pass

    @abstractmethod
    def send(self, event):
        """Send event on the link."""
        pass


class EventLinkBelow(object):
    """Interface for exposing an EventLink-like interface for the layer below.

    This interface is analogous to the interface for conn2 of a duplex
    multiprocessing.Pipe.
    """

    @abstractmethod
    def to_receive(self, event):
        """Receive an event on the link."""
        pass

    @abstractmethod
    def to_send(self):
        """Return the next event to send, while consuming it."""
        pass

    def to_send_all(self):
        """Generate all events to send, while consuming them."""
        while self.has_to_send():
            yield self.to_send()

    @abstractmethod
    def has_to_send(self):
        """Return available events to send, without consuming them."""
        pass


# Link base classes


class DataEventLink(object):
    """Support for exposing an EventLink-like interface supporting LinkData events."""

    def send_data(self, data, direction='down', context=None):
        """Send data from above as a LinkData event."""
        self._sender.send(LinkData(
            data, direction=direction, context=context, instance=self
        ))

    def get_link_data(self, event, direction, context=None):
        """Return a LinkData event from the provided event.

        The provided event must be either a LinkData event or some kind of data
        which is not a LinkEvent.
        """
        if isinstance(event, LinkData):
            return LinkData(
                event.data, direction=direction,
                context=event.context.copy() if context is None else context,
                instance=self, previous=event
            )
        elif not isinstance(event, LinkEvent):
            return LinkData(event, direction=direction, context=context, instance=self)

    def make_link_data(self, data, direction, previous, context=None):
        """Return a LinkData event from the provided data."""
        return LinkData(
            data, direction=direction, context=context, instance=self,
            previous=previous
        )

    def make_link_exception(self, exception, direction, previous, context=None):
        """Return a LinkException event."""
        return LinkException(
            exception, direction=direction, context=context, instance=self,
            previous=previous
        )


class EventLink(EventLinkBelow, EventLinkAbove, DataEventLink):
    """A duplex link for sending and receiving events.

    EventLinks have a pair of event queues: one for transforming data from
    the layer below the link for consumption by the layer above the link, and
    one for transforming data from the layer above the link for transmission by
    the layer below the link.
    When the layer below the link has an event ready for consumption by the
    EventLink, it calls the to_receive method with this event. The EventLink then
    processes this event, and when it is complete, the layer above the link can
    process any events surfaced from this processing by calling the receive method
    as an iterable.
    Vice versa, the layer above the link can provide an event for EventLink to
    process for sending (by the layer below the link) by calling its send method,
    and the layer below the link can call the to_send method as an iterable to
    get any events it needs to process for sending.

    The default behavior is to pass the event through in both directions, but
    data is wrapped in a DataEvent. To pass the event through without any
    transformation, enable the receiver_event_passthrough and
    sender_event_passthrough flags.  To change this behavior further, either
    override the class with a custom sender and receiver processor, or provide a
    custom processor upon construction.
    """

    def __init__(
        self, name=None, receiver_processor=None, sender_processor=None,
        receiver_processor_args=(), receiver_processor_kwargs={},
        sender_processor_args=(), sender_processor_kwargs={},
        receiver_event_passthrough=False, sender_event_passthrough=False
    ):
        """Initialize reader and writer processors."""
        super().__init__()
        self.name = name
        if receiver_processor is None:
            receiver_processor = self.receiver_processor
        self._receiver = receiver_processor(
            *receiver_processor_args, **receiver_processor_kwargs
        )
        if sender_processor is None:
            sender_processor = self.sender_processor
        self._sender = sender_processor(
            *sender_processor_args, **sender_processor_kwargs
        )
        self.receiver_event_passthrough = receiver_event_passthrough
        self.sender_event_passthrough = sender_event_passthrough

    @property
    def _using_own_receiver_processor(self):
        return (
            self.receiver_processor.__qualname__
            == '{}.receiver_processor'.format(self.__class__.__qualname__)
        )

    @property
    def _using_own_sender_processor(self):
        return (
            self.sender_processor.__qualname__
            == '{}.sender_processor'.format(self.__class__.__qualname__)
        )

    def __repr__(self):
        """Return a string representation of the link."""
        if self.name is not None:
            return '⇌□ {}({}) □⇌'.format(self.__class__.__qualname__, self.name)
        else:
            return '⇌□ {} □⇌'.format(self.__class__.__qualname__)

    # Implement EventLinkAbove

    def receive(self):
        """Implement EventLinkAbove.receive."""
        return self._receiver.read()

    def has_receive(self):
        """Implement EventLinkAbove.has_receive."""
        return self._receiver.has_read()

    def send(self, event):
        """Implement EventLinkAbove.send."""
        self._sender.send(event)

    # Implement EventLinkBelow

    def to_receive(self, event):
        """Implement EventLinkBelow.to_receive."""
        self._receiver.send(event)

    def to_send(self):
        """Implement EventLinkBelow.to_send."""
        return self._sender.read()

    def has_to_send(self):
        """Implement EventLinkBelow.has_to_send."""
        return self._sender.has_read()

    # Utilities for writing receive and send processors

    def directly_receive(self, event):
        """Add an event directly to the receive queue.

        Intended for use by the sender processor to short-circuit the receiver
        processor and directly add something to the receive queue.
        """
        self._receiver.directly_to_read(event)

    def directly_to_send(self, event):
        """Add an event directly to the receive queue.

        Intended for use by the receiver processor to short-circuit the sender
        processor and directly add something to the to_send queue.
        """
        self._sender.directly_to_read(event)

    def after_receive(self, event):
        """Enqueue the event for the layer above for consumption by that layer.

        This gets overridden to change the receiver's behavior to send the event.
        Note that this gets monkey-patched by other links and link utilities
        which combine links, such as ChunkedStreamLink and AutomaticPipe!
        """
        # print('Event link after_receive: {}'.format(event))
        yield from send(event)

    def after_send(self, event):
        """Enqueue the event for the layer below for consumption by that layer.

        This gets overridden to change the receiver's behavior to send the event.
        Note that this gets monkey-patched by other links and link utilities
        which combine links, such as ChunkedStreamLink and AutomaticPipe!
        """
        # print('Event link after_send: {}'.format(event))
        yield from send(event)

    # Receive and send processors

    @event_processor
    def receiver_processor(self):
        """Event receiver processor.

        Make sure to yield from after_receive() with any generated event to expose
        it for for consumption by the layer above.
        """
        while True:
            event = yield from receive()
            if self.receiver_event_passthrough or isinstance(event, LinkException):
                # print('Event Link received: {}'.format(event))
                yield from self.after_receive(event)
            else:
                data_event = self.get_link_data(event, 'up')
                # print('Event Link received: {}'.format(data_event))
                data_event = self.make_link_data(data_event.data, 'up', event)
                yield from self.after_receive(data_event)

    @event_processor
    def sender_processor(self):
        """Event sender processor.

        Make sure to yield from after_send() with any generated event to expose
        it for consumption by the layer below.
        """
        while True:
            event = yield from receive()
            if self.sender_event_passthrough:
                # print('Event Link sending: {}'.format(event))
                yield from self.after_send(event)
            else:
                data_event = self.get_link_data(event, 'down')
                # print('Event Link sending: {}'.format(data_event))
                data_event = self.make_link_data(data_event.data, 'down', event)
                yield from self.after_send(data_event)
