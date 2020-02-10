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

import collections
import math

# Packages

from phylline.links.events import EventLink, LinkEvent, LinkException
from phylline.processors import event_processor, receive
from phylline.util.timing import Clock, TimeoutTimer


# Events


class LinkClockTime(LinkEvent):
    """Event indicating the current time to advance the event link's clock to.

    clock_time should be the current time to advance the clock to.
    """

    def __init__(self, clock_time, context=None, instance=None, previous=None):
        """Initialize members."""
        super().__init__(context=context, instance=instance, previous=previous)
        self.clock_time = clock_time

    def __str__(self):
        """Represent as a string."""
        return '{} for ({}){}: {}{}'.format(
            self.__class__.__qualname__, self.link,
            ' with context({})'.format(self.context) if self.context else '',
            self.clock_time,
            ', due to ({})'.format(self.previous) if self.previous is not None else ''
        )

    def __repr__(self):
        """Represent as a string."""
        return '{}({}, context={}, instance={}, previous={})'.format(
            self.__class__.__qualname__, self.clock_time, self.context, self.link, self.previous
        )


class LinkClockRequest(LinkEvent):
    """Event indicating the next time by which the event link needs a clock update."""

    def __init__(self, requested_time, context=None, instance=None, previous=None):
        """Initialize members."""
        super().__init__(context=context, instance=instance, previous=previous)
        self.requested_time = requested_time

    def __str__(self):
        """Represent as a string."""
        return '{} for ({}){}: {}{}'.format(
            self.__class__.__qualname__, self.link,
            ' with context({})'.format(self.context) if self.context else '',
            self.requested_time,
            ', due to ({})'.format(self.previous) if self.previous is not None else ''
        )

    def __repr__(self):
        """Represent as a string."""
        return '{}({}, context={}, instance={}, previous={})'.format(
            self.__class__.__qualname__, self.requested_time, self.context, self.link, self.previous
        )

    # Comparison operators

    def __eq__(self, other):
        """Check equality by the value of the requested time.

        Any clock request is not equal to None.
        """
        if other is None:
            return False
        if isinstance(other, LinkClockRequest):
            return self.requested_time == other.requested_time
        else:
            return self.requested_time == other

    def __ne__(self, other):
        """Check not-equality by the value of the requested time."""
        return not (self == other)

    def __lt__(self, other):
        """Check less-than by the value of the requested time.

        Any clock request is less than None.
        """
        if other is None:
            return True
        if isinstance(other, LinkClockRequest):
            return self.requested_time < other.requested_time
        else:
            return self.requested_time < other

    def __gt__(self, other):
        """Check greater-than by the value of the requested time.

        No clock request is greater than None.
        """
        if other is None:
            return False
        if isinstance(other, LinkClockRequest):
            return self.requested_time > other.requested_time
        else:
            return self.requested_time > other

    def __le__(self, other):
        """Check less-than-or-equal-to by the value of the requested time."""
        return self < other or self == other

    def __ge__(self, other):
        """Check greater-than-or-equal-to by the value of the requested time."""
        return self > other or self == other


# Clocked Links


class ClockedLink(object):
    """Support for implementing an EventLink which runs on an internal clock."""

    def __init__(self, *args, clock_start=0.0, **kwargs):
        """Initialize members."""
        super().__init__(*args, **kwargs)
        self.clock = Clock(time=clock_start)
        self.next_clock_request = TimeoutTimer(clock=self.clock)

    # Public interface

    def update_clock(self, time):
        """Update the clock of the link and do any necessary processing."""
        self.to_receive(LinkClockTime(time, instance=self))
        self.send(LinkClockTime(time, instance=self))

    # Internal methods for implementers

    def make_timer(self, delay):
        """Make a timer on the processor clock."""
        return TimeoutTimer(timeout=delay, clock=self.clock)

    def get_clock_time(self, event):
        """Extract any provided clock time from the event."""
        if isinstance(event, LinkClockTime):
            return event.clock_time
        return None  # No clock time provided

    def update_clock_time(self, event):
        """Return the new clock time, updated with any clock time in the event."""
        clock_time = self.get_clock_time(event)
        if clock_time is None:
            return
        # print('Updating clock to: {}'.format(clock_time))
        self.clock.update(clock_time)
        if self.next_clock_request.timed_out:
            self.next_clock_request.reset_and_stop()

    def make_clock_request(self, time, context={}, previous=None):
        """Return a LinkClockRequest if time is different from the last request."""
        if (
            self.next_clock_request.running
            and math.isclose(self.next_clock_request.timeout_time, time)
        ):
            return None
        self.next_clock_request.start(timeout=time - self.clock.time)
        return LinkClockRequest(
            time, context={'time': self.clock.time, **context}, instance=self,
            previous=previous
        )


class EventDelayer(object):
    """A helper class which passes events through after a time delay."""

    def __init__(self, processor, clock, delay):
        """Initialize members."""
        self.processor = processor
        self.clock = clock
        self.delay = delay
        self.in_flight = collections.deque()

    def make_timer(self):
        """Make a timer on the clock."""
        return TimeoutTimer(timeout=self.delay, clock=self.clock)

    def enqueue_event(self, event):
        """Add event to the in-flight queue of delayed events."""
        event.context['to_{}_time'.format(self.processor)] = self.clock.time
        timer = self.make_timer()
        timer.start()
        event.context['intended_{}_time'.format(self.processor)] = timer.timeout_time
        # print('EventDelayer for {} to receive at {}: {}'.format(
        #     self.processor, timer.timeout_time, event
        # ))
        self.in_flight.append({'event': event, 'timer': timer})

    def flush_events(self):
        """Dequeue and yield all events which have satisfied their delays."""
        # print(
        #     'In-flight send events at {}: {}'
        #     .format(self.clock.time, self.in_flight)
        # )
        while self.in_flight and self.in_flight[0]['timer'].timed_out:
            event = self.in_flight.popleft()['event']
            event.context['actual_{}_time'.format(self.processor)] = self.clock.time
            # print(
            #     'EventDelayer {}s at {}: {}'
            #     .format(self.processor, self.clock.time, event)
            # )
            yield event
        # print(
        #     'Post-flush in-flight send events at {}: {}'
        #     .format(self.clock.time, self.in_flight)
        # )

    @property
    def next_in_flight_event(self):
        """Return the next in-flight event to send after the appropriate delay."""
        return self.in_flight[0]


class DelayedEventLink(ClockedLink, EventLink):
    """An EventLink which passes events through only after a time delay.

    The time delay for the receive depends on regularly receiving timing events
    from to_receive. The time delay for the send depends on regularly receiving
    timing events from send.
    """

    def __init__(self, clock_start=0.0, receive_delay=1.0, send_delay=1.0):
        """Initialize members."""
        super().__init__(clock_start=clock_start)
        self._delayers = {
            'up': EventDelayer('receive', self.clock, receive_delay),
            'down': EventDelayer('send', self.clock, send_delay)
        }

    # Receive and send processors

    def _enqueue_data_event(self, event, direction):
        if isinstance(event, LinkException):
            return
        data_event = self.get_link_data(event, direction)
        if data_event is None:
            return

        data_event = self.make_link_data(data_event.data, direction, event)
        self._delayers[direction].enqueue_event(data_event)

    def _flush_event_queue(self, direction):
        delayer = self._delayers[direction]
        # print('Flushing {}...'.format(delayer.processor))
        for data_event in delayer.flush_events():
            yield from getattr(self, 'after_{}'.format(delayer.processor))(data_event)
            # print('Finished yielding from after_{}!'.format(processor))
        # print('Done flushing {}!'.format(processor))

    def _issue_clock_request(self, direction):
        delayer = self._delayers[direction]
        if not delayer.in_flight:
            return

        next_in_flight = delayer.next_in_flight_event
        clock_request = self.make_clock_request(
            next_in_flight['timer'].timeout_time,
            previous=next_in_flight['event'].previous
        )
        if clock_request is not None:
            yield from getattr(self, 'after_{}'.format(delayer.processor))(clock_request)

    @event_processor
    def receiver_processor(self):
        """Event receiver processor.

        Make sure to yield from after_receive() at the end of the loop to expose
        any generated events for consumption by the layer above.
        """
        while True:
            event = yield from receive()
            self.update_clock_time(event)
            self._enqueue_data_event(event, 'up')
            yield from self._flush_event_queue('up')
            yield from self._issue_clock_request('up')

    @event_processor
    def sender_processor(self):
        """Event sender processor.

        Make sure to yield from after_send() at the end of the loop to expose
        any generated events for consumption by the layer below.
        """
        while True:
            event = yield from receive()
            self.update_clock_time(event)
            self._enqueue_data_event(event, 'down')
            yield from self._flush_event_queue('down')
            yield from self._issue_clock_request('down')
