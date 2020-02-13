"""Test the pipelines module."""

# Builtins

# Packages

from phylline.links.clocked import DelayedEventLink
from phylline.links.events import EventLink
from phylline.links.links import ChunkedStreamLink
from phylline.links.loopback import TopLoopbackLink
from phylline.links.streams import StreamLink
from phylline.pipelines import AutomaticPipeline, ManualPipeline, PipelineBottomCoupler
from phylline.pipes import AutomaticPipe

import pytest

from tests.unit.links.links import HIGHER_CHUNKED_STREAM, LOWER_CHUNKED_STREAM
from tests.unit.pipes import (
    assert_bottom_events,
    write_bottom_chunked_buffers,
    write_top_events
)


def make_pipeline_singular(pipeline_type):
    """Make a singular pipeline."""
    pipeline = pipeline_type(
        ChunkedStreamLink()
    )
    assert isinstance(pipeline.bottom, ChunkedStreamLink)
    assert isinstance(pipeline.top, ChunkedStreamLink)
    return pipeline


def make_pipeline_short(pipeline_type):
    """Make a short pipeline."""
    pipeline = pipeline_type(
        ChunkedStreamLink(),
        EventLink(),
    )
    assert isinstance(pipeline.bottom, ChunkedStreamLink)
    assert isinstance(pipeline.top, EventLink)
    return pipeline


def make_pipeline_long(pipeline_type):
    """Make a long pipeline."""
    pipeline = pipeline_type(
        StreamLink(),
        StreamLink(),
        StreamLink(),
        ChunkedStreamLink(),
        EventLink(),
        EventLink(),
        EventLink()
    )
    assert isinstance(pipeline.bottom, StreamLink)
    assert isinstance(pipeline.top, EventLink)
    return pipeline


def make_pipeline_delayed(pipeline_type):
    """Make a short pipeline."""
    pipeline = pipeline_type(
        StreamLink(),
        ChunkedStreamLink(),
        DelayedEventLink(),
        EventLink(),
        EventLink()
    )
    assert isinstance(pipeline.bottom, StreamLink)
    assert isinstance(pipeline.top, EventLink)
    return pipeline


def make_pipeline_events(pipeline_type):
    """Make a events-only pipeline."""
    pipeline = pipeline_type(
        EventLink(),
        EventLink(),
        EventLink(),
        EventLink()
    )
    assert isinstance(pipeline.bottom, EventLink)
    assert isinstance(pipeline.top, EventLink)
    return pipeline


def make_pipeline_nested(pipeline_type):
    """Make a nested pipeline."""
    pipeline = pipeline_type(
        make_pipeline_singular(pipeline_type),
        make_pipeline_events(pipeline_type),
        EventLink(),
        AutomaticPipe(EventLink(), EventLink()),
        make_pipeline_events(pipeline_type)
    )
    return pipeline


@pytest.mark.parametrize('pipeline_factory', [
    make_pipeline_singular,
    make_pipeline_short,
    make_pipeline_long,
    make_pipeline_nested
])
def test_manual_pipeline(pipeline_factory):
    """Exercise ManualPipeline's interface."""
    print('Testing Manual Pipeline with factory {}:'.format(pipeline_factory.__name__))
    pipeline = pipeline_factory(ManualPipeline)
    print(pipeline)

    # Read/write on links with directional sync
    write_bottom_chunked_buffers(pipeline.bottom)
    assert pipeline.sync_up() is None
    assert_bottom_events(pipeline.top)
    write_top_events(pipeline.top)
    assert pipeline.sync_down() is None
    result = pipeline.bottom.to_write()
    print('Pipeline bottom wrote to stream: {}'.format(result))
    assert result == HIGHER_CHUNKED_STREAM

    # Read/write on links with bidirectional sync
    write_bottom_chunked_buffers(pipeline.bottom)
    assert pipeline.sync() is None
    assert_bottom_events(pipeline.top)
    write_top_events(pipeline.top)
    assert pipeline.sync() is None
    result = pipeline.bottom.to_write()
    print('Pipeline bottom wrote to stream: {}'.format(result))
    assert result == HIGHER_CHUNKED_STREAM

    # Read/write on pipe with bidirectionl sync
    write_bottom_chunked_buffers(pipeline)
    assert pipeline.sync() is None
    assert_bottom_events(pipeline)
    write_top_events(pipeline)
    assert pipeline.sync() is None
    result = pipeline.to_write()
    print('Pipeline bottom wrote to stream: {}'.format(result))
    assert result == HIGHER_CHUNKED_STREAM


def test_manual_pipeline_clocked():
    """Exercise ManualPipeline's clock functionality."""
    print('Testing Manual Pipeline with factory make_pipeline_delayed:')
    pipeline = make_pipeline_delayed(ManualPipeline)
    print(pipeline)

    assert pipeline.update_clock(0) is None
    write_bottom_chunked_buffers(pipeline.bottom)
    assert pipeline.sync() == 1.0
    assert not pipeline.top.has_receive()
    assert pipeline.update_clock(0.5) == 1.0
    assert not pipeline.top.has_receive()
    assert pipeline.update_clock(0.99) == 1.0
    assert not pipeline.top.has_receive()
    assert pipeline.update_clock(1.0) is None
    assert_bottom_events(pipeline.top)

    print('Resetting clock...')
    assert pipeline.update_clock(0) is None
    write_top_events(pipeline.top)
    assert pipeline.sync() == 1.0
    assert pipeline.update_clock(0.5) == 1.0
    assert not pipeline.bottom.to_write()
    assert pipeline.update_clock(0.75) == 1.0
    assert not pipeline.bottom.to_write()
    assert pipeline.update_clock(1.5) is None
    result = pipeline.bottom.to_write()
    assert result == HIGHER_CHUNKED_STREAM


@pytest.mark.parametrize('pipeline_factory', [
    make_pipeline_singular,
    make_pipeline_short,
    make_pipeline_long,
    make_pipeline_nested
])
def test_automatic_pipeline(pipeline_factory):
    """Exercise AutomaticPipeline's interface."""
    print('Testing Automatic Pipeline with factory {}:'.format(pipeline_factory.__name__))
    automatic_pipeline = pipeline_factory(AutomaticPipeline)
    print(automatic_pipeline)

    # Read/write on links
    write_bottom_chunked_buffers(automatic_pipeline.bottom)
    assert_bottom_events(automatic_pipeline.top)
    write_top_events(automatic_pipeline.top)
    result = automatic_pipeline.bottom.to_write()
    print('Pipeline bottom wrote to stream: {}'.format(result))
    assert result == HIGHER_CHUNKED_STREAM

    # Read/write on pipeline
    write_bottom_chunked_buffers(automatic_pipeline)
    assert_bottom_events(automatic_pipeline)
    write_top_events(automatic_pipeline)
    result = automatic_pipeline.to_write()
    print('Pipeline bottom wrote to stream: {}'.format(result))
    assert result == HIGHER_CHUNKED_STREAM


def test_automatic_pipeline_clocked():
    """Exercise AutomaticPipeline's clock functionality."""
    print('Testing Automatic Pipeline with factory make_pipeline_delayed:')
    pipeline = make_pipeline_delayed(AutomaticPipeline)
    print(pipeline)

    assert pipeline.update_clock(0) is None
    write_bottom_chunked_buffers(pipeline.bottom)
    assert pipeline.update_clock(0) == 1.0
    assert not pipeline.top.has_receive()
    assert pipeline.update_clock(0.5) == 1.0
    assert not pipeline.top.has_receive()
    assert pipeline.update_clock(0.99) == 1.0
    assert not pipeline.top.has_receive()
    assert pipeline.update_clock(1.0) is None
    assert_bottom_events(pipeline.top)

    print('Resetting clock...')
    assert pipeline.update_clock(0) is None
    write_top_events(pipeline.top)
    assert pipeline.update_clock(0) == 1.0
    assert pipeline.update_clock(0.5) == 1.0
    assert not pipeline.bottom.to_write()
    assert pipeline.update_clock(0.75) == 1.0
    assert not pipeline.bottom.to_write()
    assert pipeline.update_clock(1.5) is None
    result = pipeline.bottom.to_write()
    assert result == HIGHER_CHUNKED_STREAM


def make_pipeline_loopback(pipeline_factory):
    """Make a long pipeline with a loopback at the top."""
    manual_pipeline = pipeline_factory(
        StreamLink(),
        StreamLink(),
        StreamLink(),
        ChunkedStreamLink(),
        EventLink(),
        EventLink(),
        EventLink(),
        TopLoopbackLink()
    )
    assert isinstance(manual_pipeline.bottom, StreamLink)
    assert isinstance(manual_pipeline.top, TopLoopbackLink)
    return manual_pipeline


def test_manual_pipeline_loopback():
    """Exercise ManualPipeline's interface."""
    print('Testing Manual Pipeline with factory make_pipeline_loopback:')
    manual_pipeline = make_pipeline_loopback(ManualPipeline)
    print(manual_pipeline)

    write_bottom_chunked_buffers(manual_pipeline.bottom)
    assert manual_pipeline.sync() is None
    result = manual_pipeline.bottom.to_write()
    print('Pipeline bottom wrote to stream: {}'.format(result))
    assert result == LOWER_CHUNKED_STREAM


def test_automatic_pipeline_loopback():
    """Exercise ManualPipeline's interface."""
    print('Testing Automatic Pipeline with factory make_pipeline_loopback:')
    automatic_pipeline = make_pipeline_loopback(AutomaticPipeline)
    print(automatic_pipeline)

    write_bottom_chunked_buffers(automatic_pipeline.bottom)
    result = automatic_pipeline.bottom.to_write()
    print('Pipeline bottom wrote to stream: {}'.format(result))
    assert result == LOWER_CHUNKED_STREAM


def assert_loopback_below(stack, payload):
    """Asset that the stack has correct below-loopback behavior."""
    stack.send(payload)
    assert stack.has_receive()
    result = stack.receive()
    print('Loopback received: {}'.format(result))
    assert result.data == payload


def test_loopback_pipeline_bottom_coupler_stream():
    """Test for pipeline bottom coupling on streams."""
    print('Testing byte buffer loopback with PipelineBottomCoupler...')
    pipeline_one = make_pipeline_nested(AutomaticPipeline)
    pipeline_two = make_pipeline_loopback(AutomaticPipeline)
    coupler = PipelineBottomCoupler(pipeline_one, pipeline_two)
    print(coupler)
    payload = b'\1\2\3\4'
    assert_loopback_below(pipeline_one.top, payload)
    assert_loopback_below(pipeline_one, payload)
    assert_loopback_below(coupler.pipeline_one, payload)


def test_loopback_pipeline_bottom_coupler_event():
    """Test for pipeline bottom coupling on events."""
    print('Testing byte buffer loopback with PipelineBottomCoupler...')
    pipeline_one = make_pipeline_events(AutomaticPipeline)
    pipeline_two = AutomaticPipeline(
        make_pipeline_events(AutomaticPipeline), TopLoopbackLink()
    )
    coupler = PipelineBottomCoupler(pipeline_one, pipeline_two)
    print(coupler)
    payload = b'\1\2\3\4'
    assert_loopback_below(pipeline_one.top, payload)
    assert_loopback_below(pipeline_one, payload)
    assert_loopback_below(coupler.pipeline_one, payload)
