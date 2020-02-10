"""Test the links.events module."""

# Builtins

# Packages

from phylline.links.events import EventLink

from tests.unit.links.streams import HIGHER_EVENTS, LOWER_EVENTS


def test_event_link():
    """Exercise EventLink's interface."""
    print('Testing Event Link:')
    event_link = EventLink()
    for event in LOWER_EVENTS:
        event_link.to_receive(event)
    assert event_link.has_receive()
    for (i, event) in enumerate(event_link.receive_all()):
        print('Event Link received from queue: {}'.format(event))
        assert event.data == LOWER_EVENTS[i]
    for event in HIGHER_EVENTS:
        event_link.send(event)
    assert event_link.has_to_send()
    for (i, event) in enumerate(event_link.to_send_all()):
        print('Event Link sent to queue: {}'.format(event))
        assert event.data == HIGHER_EVENTS[i]
