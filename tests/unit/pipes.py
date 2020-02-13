"""Test the pipes module."""

# Builtins

# Packages

from phylline.links.clocked import DelayedEventLink
from phylline.links.events import EventLink
from phylline.links.links import ChunkedStreamLink
from phylline.pipes import AutomaticPipe, ManualPipe

from tests.unit.links.clocked import assert_clock_request_event_received
from tests.unit.links.links import HIGHER_CHUNKED_STREAM, LOWER_CHUNKED_BUFFERS
from tests.unit.links.streams import HIGHER_BUFFERS, LOWER_BUFFERS


def write_bottom_chunked_buffers(stream_link_below):
    """Write the bottom chunked buffers to the link with StreamLinkBelow."""
    stream_link_below.to_read(LOWER_CHUNKED_BUFFERS[0])
    stream_link_below.to_read(LOWER_CHUNKED_BUFFERS[1] + LOWER_CHUNKED_BUFFERS[2])


def assert_bottom_events(event_link_above):
    """Receive the bottom buffers as events from the link with EventLinkAbove."""
    assert event_link_above.has_receive()
    for (i, event) in enumerate(event_link_above.receive_all()):
        assert event.data == LOWER_BUFFERS[i]


def write_top_events(event_link_above):
    """Receive the top events on the link with EventLinkAbove."""
    for event in HIGHER_BUFFERS:
        event_link_above.send(event)


def test_manual_pipe():
    """Exercise ManualPipe's interface."""
    print('Testing Piped Event Links with Manual Synchronization:')
    chunked_stream_link = ChunkedStreamLink()
    event_link = EventLink()
    pipe = ManualPipe(chunked_stream_link, event_link)

    # Read/write on links
    write_bottom_chunked_buffers(chunked_stream_link)
    pipe.sync()
    assert_bottom_events(event_link)
    write_top_events(event_link)
    pipe.sync()
    result = chunked_stream_link.to_write()
    print('Chunked Stream Link wrote to stream: {}'.format(result))
    assert result == HIGHER_CHUNKED_STREAM

    # Read/write on pipe
    write_bottom_chunked_buffers(pipe)
    pipe.sync()
    assert_bottom_events(pipe)
    write_top_events(pipe)
    pipe.sync()
    result = pipe.to_write()
    print('Chunked Stream Link wrote to stream: {}'.format(result))
    assert result == HIGHER_CHUNKED_STREAM


def test_manual_singular():
    """Exercise ManualPipe's single-link handling."""
    print('Testing Piped Singular Event Link with Manual Synchronization:')
    chunked_stream_link = ChunkedStreamLink()
    pipe = ManualPipe(chunked_stream_link, chunked_stream_link)

    # Read/write on links
    write_bottom_chunked_buffers(chunked_stream_link)
    pipe.sync()
    assert_bottom_events(chunked_stream_link)
    write_top_events(chunked_stream_link)
    pipe.sync()
    result = chunked_stream_link.to_write()
    print('Chunked Stream Link wrote to stream: {}'.format(result))
    assert result == HIGHER_CHUNKED_STREAM

    # Read/write on pipe
    write_bottom_chunked_buffers(pipe)
    pipe.sync()
    assert_bottom_events(pipe)
    write_top_events(pipe)
    pipe.sync()
    result = pipe.to_write()
    print('Chunked Stream Link wrote to stream: {}'.format(result))
    assert result == HIGHER_CHUNKED_STREAM


def test_manual_pipe_clocked():
    """Exercise ManualPipe's clock functionality."""
    print('Testing Piped Clocked Event Links with Manual Synchronization:')
    pipe = ManualPipe(ChunkedStreamLink(), DelayedEventLink())

    print('On deadline:')
    assert pipe.update_clock(0) is None
    write_bottom_chunked_buffers(pipe)
    assert pipe.sync() is None
    assert_clock_request_event_received(pipe, 1.0)
    assert pipe.update_clock(0.5) is None
    assert not pipe.has_receive()
    assert pipe.update_clock(0.99) is None
    assert not pipe.has_receive()
    assert pipe.update_clock(1.0) is None
    assert_bottom_events(pipe)

    print('After deadline:')
    assert pipe.update_clock(0) is None
    write_top_events(pipe)
    assert pipe.update_clock(0.5) == 1.0
    assert not pipe.to_write()
    assert pipe.update_clock(0.75) == 1.0
    assert not pipe.to_write()
    assert pipe.update_clock(1.5) is None
    result = pipe.to_write()
    assert result == HIGHER_CHUNKED_STREAM


def test_manual_pipe_composition():
    """Exercise ManualPipe nested composition."""
    print('Testing Nesting of Piped Event Links with Manual Synchronization:')
    chunked_stream_link = ChunkedStreamLink()
    upper_event_link = EventLink()
    outer_pipe = ManualPipe(
        ManualPipe(chunked_stream_link, EventLink()),
        ManualPipe(EventLink(), upper_event_link)
    )

    # Read/write on links
    write_bottom_chunked_buffers(chunked_stream_link)
    outer_pipe.sync()
    assert_bottom_events(upper_event_link)
    write_top_events(upper_event_link)
    outer_pipe.sync()
    result = chunked_stream_link.to_write()
    print('Chunked Stream Link wrote to stream: {}'.format(result))
    assert result == HIGHER_CHUNKED_STREAM

    # Read/write on pipe
    write_bottom_chunked_buffers(outer_pipe)
    outer_pipe.sync()
    assert_bottom_events(outer_pipe)
    write_top_events(outer_pipe)
    outer_pipe.sync()
    result = outer_pipe.to_write()
    print('Chunked Stream Link wrote to stream: {}'.format(result))
    assert result == HIGHER_CHUNKED_STREAM


def test_automatic_pipe():
    """Exercise AutomaticPipe's interface."""
    print('Testing Piped Event Links with Automatic Synchronization:')
    chunked_stream_link = ChunkedStreamLink()
    event_link = EventLink()
    pipe = AutomaticPipe(chunked_stream_link, event_link)

    # Read/write on links
    write_bottom_chunked_buffers(chunked_stream_link)
    assert_bottom_events(event_link)
    write_top_events(event_link)
    result = chunked_stream_link.to_write()
    print('Chunked Stream Link wrote to stream: {}'.format(result))
    assert result == HIGHER_CHUNKED_STREAM

    # Read/write on pipe
    write_bottom_chunked_buffers(pipe)
    assert_bottom_events(pipe)
    write_top_events(pipe)
    result = pipe.to_write()
    print('Chunked Stream Link wrote to stream: {}'.format(result))
    assert result == HIGHER_CHUNKED_STREAM


def test_automatic_singular():
    """Exercise AutomaticPipe's single-link handling."""
    print('Testing Piped Singular Event Link with Automatic Synchronization:')
    chunked_stream_link = ChunkedStreamLink()
    pipe = AutomaticPipe(chunked_stream_link, chunked_stream_link)

    # Read/write on links
    write_bottom_chunked_buffers(chunked_stream_link)
    assert_bottom_events(chunked_stream_link)
    write_top_events(chunked_stream_link)
    result = chunked_stream_link.to_write()
    print('Chunked Stream Link wrote to stream: {}'.format(result))
    assert result == HIGHER_CHUNKED_STREAM

    # Read/write on pipe
    write_bottom_chunked_buffers(pipe)
    assert_bottom_events(pipe)
    write_top_events(pipe)
    result = pipe.to_write()
    print('Chunked Stream Link wrote to stream: {}'.format(result))
    assert result == HIGHER_CHUNKED_STREAM


def test_automatic_pipe_clocked():
    """Exercise AutomaticPipe's clock functionality."""
    print('Testing Piped Clocked Event Links with Automatic Synchronization:')
    pipe = AutomaticPipe(ChunkedStreamLink(), DelayedEventLink())
    assert pipe.update_clock(0) is None
    write_bottom_chunked_buffers(pipe)
    assert_clock_request_event_received(pipe, 1.0)
    assert pipe.update_clock(0.5) is None
    assert not pipe.has_receive()
    assert pipe.update_clock(0.99) is None
    assert not pipe.has_receive()
    assert pipe.update_clock(1.0) is None
    assert_bottom_events(pipe)
    print('Resetting clock...')
    assert pipe.update_clock(0) is None
    print('Writing top events...')
    write_top_events(pipe)
    print('Wrote top events!')
    assert pipe.update_clock(0.5) == 1.0
    assert not pipe.to_write()
    assert pipe.update_clock(0.75) == 1.0
    assert not pipe.to_write()
    assert pipe.update_clock(1.5) is None
    result = pipe.to_write()
    assert result == HIGHER_CHUNKED_STREAM


def test_automatic_pipe_composition():
    """Exercise AutomaticPipe nested composition."""
    print('Testing Nesting of Piped Event Links with Automatic Synchronization:')
    chunked_stream_link = ChunkedStreamLink()
    upper_event_link = EventLink()
    outer_pipe = AutomaticPipe(
        AutomaticPipe(chunked_stream_link, EventLink()),
        AutomaticPipe(EventLink(), upper_event_link)
    )

    # Read/write on links
    write_bottom_chunked_buffers(chunked_stream_link)
    assert_bottom_events(upper_event_link)
    write_top_events(upper_event_link)
    result = chunked_stream_link.to_write()
    print('Chunked Stream Link wrote to stream: {}'.format(result))
    assert result == HIGHER_CHUNKED_STREAM

    # Read/write on pipe
    write_bottom_chunked_buffers(outer_pipe)
    assert_bottom_events(outer_pipe)
    write_top_events(outer_pipe)
    result = outer_pipe.to_write()
    print('Chunked Stream Link wrote to stream: {}'.format(result))
    assert result == HIGHER_CHUNKED_STREAM
