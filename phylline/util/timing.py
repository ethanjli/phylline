"""Timed execution of tasks."""

# Builtins

import math
import time

# Packages


class Clock(object):
    """Clock which runs in either real-time or on an external clock.

    To use the clock in real-time mode, simply call __init__/reset with no
    arguments. Then everything will use the system clock, and no external times
    need to be input (so that update_clock does not need to be called).
    To use the timer in externally clocked mode, call __init__/reset with the
    current clock time, and call update_clock every time the timer needs to be
    checked. Then everything will use the external clock times given by reset
    """

    def __init__(self, time=None):
        """Initialize members."""
        self._time = time

    def reset(self, time=None):
        """Reset the clock."""
        self._time = time

    def update(self, time=None):
        """Update the clock time.

        Only needs to be caled when the clock runs in externally clocked mode,
        which is done by calling reset/start with the current clock time. If
        called when the clock runs in real-time mode, does nothing.
        """
        if self.realtime:
            return
        if time is None:  # clock in externally-clocked mode, need valid time
            return
        self._time = time

    @property
    def time(self):
        """Clock time."""
        if self.realtime:
            return time.time()
        return self._time

    @property
    def realtime(self):
        """Whether the clock is running in real-time mode."""
        return self._time is None


class TimeoutTimer(object):
    """Timer which counts down to timeout.

    Note that the clock is checked against timeout up to floating-point precision,
    so if the elapsed time is nominally just before the timeout but actually within
    floating-point error of it, the timer will consider itself to have timed out.
    """

    def __init__(self, timeout=0, clock=None):
        """Initialize members."""
        self.enabled = False
        self.timeout = timeout
        self.start_time = None
        if clock is None:
            clock = Clock()
        self.clock = clock

    def start(self, timeout=None):
        """Start the timer from beginning."""
        if timeout is not None:
            self.timeout = timeout
        self.enabled = True
        self.reset()

    def reset(self):
        """Reset the timer."""
        if not self.enabled:
            return
        self.start_time = self.clock.time

    def reset_and_stop(self):
        """Reset the timer and stop it."""
        self.enabled = False
        self.start_time = None

    @property
    def timeout_time(self):
        """Clock time when timeout will occur."""
        if self.start_time is None:
            return None
        return self.start_time + self.timeout

    @property
    def elapsed(self):
        """Amount of time elapsed since start."""
        if not self.enabled:
            return None
        if self.start_time is None:
            return None
        return self.clock.time - self.start_time

    @property
    def running(self):
        """Whether the timer is running to timeout."""
        return (
            self.enabled and (self.elapsed < self.timeout)
            and not math.isclose(self.elapsed, self.timeout)
        )

    @property
    def timed_out(self):
        """Whether timeout has occurred."""
        return self.enabled and (
            self.elapsed >= self.timeout
            or math.isclose(self.elapsed, self.timeout)
        )

    @property
    def remaining(self):
        """Amount of time remaining before timeout."""
        if not self.enabled:
            return None
        duration = self.timeout - self.elapsed
        if self.timed_out:  # check timed_out after duration for real-time correctness
            return 0
        return duration

    def __repr__(self):
        """Return a string representation of the timer."""
        return '{}({}{}{})'.format(
            self.__class__.__qualname__,
            'running ' if self.enabled else 'stopped ',
            'from {}, '.format(self.start_time) if self.start_time is not None else '',
            'timeout={}'.format(self.timeout)
        )
