"""Test the links.clocked module."""

# Builtins

# Packages

from phylline.links.clocked import DelayedEventLink, LinkClockRequest

from tests.unit.links.streams import HIGHER_EVENTS, LOWER_EVENTS


def assert_clock_request_event_received(delayed_event_link, time):
    """Assert that the DelayedEventLink has exactly one event on each end, a LinkClockRequest."""
    assert delayed_event_link.has_receive()
    received_events = list(delayed_event_link.receive_all())
    assert len(received_events) == 1
    assert isinstance(received_events[0], LinkClockRequest)
    assert received_events[0].requested_time == time


def assert_clock_request_event_to_send(delayed_event_link, time):
    """Assert that the DelayedEventLink has exactly one event on each end, a LinkClockRequest."""
    assert delayed_event_link.has_to_send()
    to_send_events = list(delayed_event_link.to_send_all())
    assert len(to_send_events) == 1
    assert isinstance(to_send_events[0], LinkClockRequest)
    assert to_send_events[0].requested_time == time


def test_delayed_event_link():
    """Exercise DelayedEventLink's timing interface."""
    print('Testing Delayed Event Link:')
    delayed_event_link = DelayedEventLink()
    # Receiving times by clock updates with LinkClockTime events
    print('On-time clock test:')
    delayed_event_link.update_clock(0)
    for event in LOWER_EVENTS:
        delayed_event_link.to_receive(event)
    for event in HIGHER_EVENTS:
        delayed_event_link.send(event)
    assert_clock_request_event_received(delayed_event_link, 1)
    delayed_event_link.update_clock(0.5)
    assert not delayed_event_link.has_receive()
    assert not delayed_event_link.has_to_send()
    delayed_event_link.update_clock(0.99)
    assert not delayed_event_link.has_receive()
    assert not delayed_event_link.has_to_send()
    delayed_event_link.update_clock(1.0)
    assert delayed_event_link.has_receive()
    for (i, event) in enumerate(delayed_event_link.receive_all()):
        print('Event Link received from queue: {}'.format(event))
        assert event.data == LOWER_EVENTS[i]
    assert delayed_event_link.has_to_send()
    for (i, event) in enumerate(delayed_event_link.to_send_all()):
        print('Event Link sent to queue: {}'.format(event))
        assert event.data == HIGHER_EVENTS[i]

    print('Late clock test:')
    delayed_event_link.update_clock(0)
    for event in HIGHER_EVENTS:
        delayed_event_link.send(event)
    for event in LOWER_EVENTS:
        delayed_event_link.to_receive(event)
    assert_clock_request_event_to_send(delayed_event_link, 1)
    delayed_event_link.update_clock(1.5)
    assert delayed_event_link.has_receive()
    for (i, event) in enumerate(delayed_event_link.receive_all()):
        print('Event Link received from queue: {}'.format(event))
        assert event.data == LOWER_EVENTS[i]
    assert delayed_event_link.has_to_send()
    for (i, event) in enumerate(delayed_event_link.to_send_all()):
        print('Event Link sent to queue: {}'.format(event))
        assert event.data == HIGHER_EVENTS[i]

    print('Staggered clock test:')
    delayed_event_link.update_clock(0)
    delayed_event_link.to_receive(LOWER_EVENTS[0])
    delayed_event_link.send(HIGHER_EVENTS[0])
    assert_clock_request_event_received(delayed_event_link, 1)
    assert not delayed_event_link.has_to_send()
    delayed_event_link.update_clock(0.5)
    for event in LOWER_EVENTS[1:]:
        delayed_event_link.to_receive(event)
    for event in HIGHER_EVENTS[1:]:
        delayed_event_link.send(event)
    delayed_event_link.update_clock(1.0)
    assert delayed_event_link.has_receive()
    received_events = list(delayed_event_link.receive_all())
    print(received_events)
    assert len(received_events) == 2
    assert received_events[0].data == LOWER_EVENTS[0]
    assert isinstance(received_events[1], LinkClockRequest)
    assert received_events[1].requested_time == 1.5
    assert delayed_event_link.has_to_send()
    to_send_events = list(delayed_event_link.to_send_all())
    assert len(to_send_events) == 1
    for (i, event) in enumerate(to_send_events):
        print('Event Link sent to queue: {}'.format(event))
        assert event.data == HIGHER_EVENTS[i]
    delayed_event_link.update_clock(1.5)
    assert delayed_event_link.has_receive()
    received_events = list(delayed_event_link.receive_all())
    assert len(received_events) == 2
    for (i, event) in enumerate(received_events):
        print('Event Link received from queue: {}'.format(event))
        assert event.data == LOWER_EVENTS[i + 1]
    assert delayed_event_link.has_to_send()
    to_send_events = list(delayed_event_link.to_send_all())
    assert len(to_send_events) == 1
    for (i, event) in enumerate(to_send_events):
        print('Event Link sent to queue: {}'.format(event))
        assert event.data == HIGHER_EVENTS[i + 1]
