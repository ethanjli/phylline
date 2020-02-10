"""Test the links.events module."""

# Builtins

# Packages

from phylline.links.links import ChunkedStreamLink

from tests.unit.links.streams import HIGHER_BUFFERS, LOWER_BUFFERS


LOWER_CHUNKED_BUFFERS = [b'\0' + event + b'\0' for event in LOWER_BUFFERS]
LOWER_CHUNKED_STREAM = ''.join(
    buffer.decode('utf-8') for buffer in LOWER_CHUNKED_BUFFERS
).encode('utf-8')
HIGHER_CHUNKED_BUFFERS = [b'\0' + event + b'\0' for event in HIGHER_BUFFERS]
HIGHER_CHUNKED_BUFFERS_MINIMAL = [event + b'\0' for event in HIGHER_BUFFERS]
HIGHER_CHUNKED_STREAM = ''.join(
    buffer.decode('utf-8') for buffer in HIGHER_CHUNKED_BUFFERS
).encode('utf-8')
HIGHER_CHUNKED_STREAM_MINIMAL = ''.join(
    buffer.decode('utf-8') for buffer in HIGHER_CHUNKED_BUFFERS_MINIMAL
).encode('utf-8')


def test_chunked_stream_link():
    """Exercise ChunkedStreamLink's interface."""
    print('Testing Chunked Stream Link:')
    chunked_stream_link = ChunkedStreamLink()
    chunked_stream_link.to_read(LOWER_CHUNKED_BUFFERS[0])
    chunked_stream_link.to_read(LOWER_CHUNKED_BUFFERS[1] + LOWER_CHUNKED_BUFFERS[2])
    assert chunked_stream_link.has_receive()
    for (i, event) in enumerate(chunked_stream_link.receive_all()):
        print('Chunked Stream Link received from stream: {}'.format(event))
        assert event.data == LOWER_BUFFERS[i]
    for event in HIGHER_BUFFERS:
        chunked_stream_link.send(event)
    result = chunked_stream_link.to_write()
    print('Chunked Stream Link wrote to stream: {}'.format(result))
    assert result == HIGHER_CHUNKED_STREAM

    print('Testing Chunked Stream Link without chunk starting delimiters:')
    chunked_stream_link = ChunkedStreamLink(begin_chunk_separator=False)
    chunked_stream_link.to_read(LOWER_CHUNKED_BUFFERS[0])
    chunked_stream_link.to_read(LOWER_CHUNKED_BUFFERS[1] + LOWER_CHUNKED_BUFFERS[2])
    assert chunked_stream_link.has_receive()
    for (i, event) in enumerate(chunked_stream_link.receive_all()):
        print('Chunked Stream Link received from stream: {}'.format(event))
        assert event.data == LOWER_BUFFERS[i]
    for event in HIGHER_BUFFERS:
        chunked_stream_link.send(event)
    result = chunked_stream_link.to_write()
    print('Chunked Stream Link wrote to stream: {}'.format(result))
    assert result == HIGHER_CHUNKED_STREAM_MINIMAL
