"""Test the links.loopback module."""

# Builtins

# Packages

from phylline.links.loopback import BottomLoopbackLink, TopLoopbackLink

from tests.unit.links.streams import LOWER_BUFFERS, LOWER_EVENTS


def test_top_loopback_link():
    """Exercise TopLoopbackLink's interface."""
    print('Testing Top Loopback Link:')
    top_loopback_link = TopLoopbackLink()
    for event in LOWER_EVENTS:
        top_loopback_link.to_receive(event)
        assert top_loopback_link.has_to_send()
    for (i, event) in enumerate(top_loopback_link.to_send_all()):
        print('Top Loopback Link echoed event: {}'.format(event))
        assert event.data == LOWER_EVENTS[i]
    top_loopback_link.to_read(LOWER_BUFFERS[0])
    top_loopback_link.to_read(LOWER_BUFFERS[1] + LOWER_BUFFERS[2])
    result = top_loopback_link.to_write()
    print('Top Loopback Link echoed from stream: {}'.format(result))
    assert result == b'foo,bar,foobar!'


def test_bottom_loopback_link():
    """Exercise BottomLoopbackLink's interface."""
    print('Testing Bottom Loopback Link:')
    bottom_loopback_link = BottomLoopbackLink()
    for event in LOWER_EVENTS:
        bottom_loopback_link.send(event)
        assert bottom_loopback_link.has_receive()
    for (i, event) in enumerate(bottom_loopback_link.receive_all()):
        print('Bottom Loopback Link echoed event: {}'.format(event))
        assert event.data == LOWER_EVENTS[i]
    bottom_loopback_link.write(LOWER_BUFFERS[0])
    bottom_loopback_link.write(LOWER_BUFFERS[1] + LOWER_BUFFERS[2])
    result = bottom_loopback_link.read()
    print('Top Loopback Link echoed from stream: {}'.format(result))
    assert result == b'foo,bar,foobar!'
