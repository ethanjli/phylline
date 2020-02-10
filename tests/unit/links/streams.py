"""Test the links.streams module."""

# Builtins

# Packages

from phylline.links.streams import StreamLink


LOWER_EVENTS = ['foo,', 'bar,', 'foobar!']
LOWER_BUFFERS = [event.encode('utf-8') for event in LOWER_EVENTS]
HIGHER_EVENTS = ['Hello,', 'world!']
HIGHER_BUFFERS = [event.encode('utf-8') for event in HIGHER_EVENTS]
HIGHER_STREAM = ''.join(HIGHER_EVENTS).encode('utf-8')


def test_stream_link():
    """Exercise StreamLink's interface."""
    print('Testing Stream Link:')
    stream_link = StreamLink()
    assert repr(stream_link) == '⇌~ StreamLink ~⇌'
    for buffer in LOWER_BUFFERS:
        stream_link.to_read(buffer)
    result = stream_link.read()
    assert result == b'foo,bar,foobar!'
    for buffer in HIGHER_BUFFERS:
        stream_link.write(buffer)
    result = stream_link.to_write()
    assert result == HIGHER_STREAM
