"""Test the util.timing module."""

# Builtins

import math
import time

# Packages

from phylline.util.timing import Clock, TimeoutTimer


def test_clock_realtime():
    """Test real-time clock functionality."""
    clock = Clock()
    assert clock.realtime
    old_time = clock.time
    time.sleep(0.1)
    assert clock.time - old_time >= 0.1
    assert clock.time - old_time < 0.2
    clock.update()
    assert clock.time - old_time >= 0.1
    assert clock.time - old_time < 0.2
    time.sleep(0.1)
    assert clock.time - old_time >= 0.2
    assert clock.time - old_time < 0.3
    clock.update(time=0.0)
    assert clock.time - old_time >= 0.2
    assert clock.time - old_time < 0.3
    clock.reset(time=0)
    assert not clock.realtime
    assert clock.time == 0.0


def test_clock_external():
    """Test externally-clocked clock functionality."""
    clock = Clock(time=0.0)
    assert not clock.realtime
    assert clock.time == 0.0
    clock.update()
    assert clock.time == 0.0
    clock.update(time=0.1)
    assert clock.time == 0.1
    clock.update()
    assert clock.time == 0.1
    clock.update(time=0.0)
    assert clock.time == 0.0
    clock.reset()
    assert clock.realtime
    assert clock.time > 0


def assert_timer_stopped(timer):
    """Check whether the timer is stopped."""
    assert not timer.enabled
    assert timer.elapsed is None
    assert not timer.running
    assert not timer.timed_out
    assert timer.remaining is None
    assert repr(timer) == 'TimeoutTimer(stopped timeout={})'.format(timer.timeout)


def assert_timer_halfway(timer):
    """Check whether the timer has correct state when running halfway to timeout."""
    if timer.clock.realtime:
        assert timer.elapsed >= 0.5 * timer.timeout
        assert timer.elapsed < timer.timeout
    else:
        assert math.isclose(timer.elapsed, 0.5 * timer.timeout)
    assert timer.running
    assert not timer.timed_out
    assert timer.remaining > 0
    if timer.clock.realtime:
        assert timer.remaining <= 0.5 * timer.timeout
    else:
        assert math.isclose(timer.remaining, 0.5 * timer.timeout)


def assert_timer_finished(timer):
    """Check whether the timer has correct state after it has timed out."""
    if timer.clock.realtime:
        assert timer.elapsed >= timer.timeout
    else:
        assert timer.elapsed >= timer.timeout or math.isclose(timer.elapsed, timer.timeout)
    assert not timer.running
    assert timer.timed_out
    assert timer.remaining == 0


def assert_timer_running_realtime(timer):
    """Check whether the timer is running correctly in real-time mode."""
    time.sleep(0.5 * timer.timeout)
    assert_timer_halfway(timer)
    time.sleep(0.5 * timer.timeout)
    assert_timer_finished(timer)
    time.sleep(0.5 * timer.timeout)
    assert_timer_finished(timer)


def test_timer_realtime():
    """Test real-time timer functionality."""
    timer = TimeoutTimer(timeout=0.2)
    assert timer.timeout_time is None
    assert timer.elapsed is None
    timer.enabled = True
    assert timer.elapsed is None  # no start time was set due to invalid state change
    timer.enabled = False
    assert_timer_stopped(timer)

    # Basic test
    timer.start()
    assert_timer_running_realtime(timer)

    # Stop-and-start test
    timer.reset_and_stop()
    assert_timer_stopped(timer)
    timer.reset()
    assert_timer_stopped(timer)
    timer.start()
    assert_timer_running_realtime(timer)

    # Reset test
    timer.reset()
    assert_timer_running_realtime(timer)
    timer.reset()
    assert_timer_running_realtime(timer)

    # Start test
    timer.start(timeout=0.1)
    assert_timer_running_realtime(timer)
    timer.reset()
    assert_timer_running_realtime(timer)
    timer.start()
    assert_timer_running_realtime(timer)

    # Externally-clocked mode-switching test
    timer.clock.reset(0.0)
    timer.start(timeout=0.2)
    timer.clock.update(0.1)
    assert_timer_halfway(timer)
    timer.clock.update(0.2)
    assert_timer_finished(timer)


def test_timer_external():
    """Test externally-clocked timer functionality."""
    timer = TimeoutTimer(timeout=0.2, clock=Clock(time=0.0))
    assert timer.timeout_time is None
    assert timer.elapsed is None
    assert_timer_stopped(timer)

    # Basic test
    timer.clock.update(1.0)
    timer.start()
    assert repr(timer) == 'TimeoutTimer(running from 1.0, timeout=0.2)'
    assert timer.timeout_time == 1.2
    timer.clock.update(1.1)
    assert_timer_halfway(timer)
    timer.clock.update(1.2)
    assert_timer_finished(timer)
    timer.clock.update(1.3)
    assert_timer_finished(timer)
    timer.clock.update(0.0)
    timer.reset()
    timer.clock.update(0.1)
    assert_timer_halfway(timer)
    timer.clock.update(0.2)
    assert_timer_finished(timer)

    # Shared-clock test
    timer.clock.update(1.0)
    timer.reset()
    timer_slow = TimeoutTimer(timeout=0.4, clock=timer.clock)
    timer_slow.start()
    timer.clock.update(1.1)
    assert_timer_halfway(timer)
    timer.clock.update(1.2)
    assert_timer_finished(timer)
    assert_timer_halfway(timer_slow)
    timer.clock.update(1.4)
    assert_timer_finished(timer)
    assert_timer_finished(timer_slow)
