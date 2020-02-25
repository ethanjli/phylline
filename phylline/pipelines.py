"""Classes for building pipelines of layered links."""

# Builtins

# Packages

from phylline.links.links import GenericLinkAbove, GenericLinkBelow
from phylline.pipes import AutomaticPipe, ManualPipe
from phylline.processors import proceed, wait
from phylline.util.interfaces import SetterProperty
from phylline.util.iterables import remove_none


# Pipelines


class Pipeline(GenericLinkBelow, GenericLinkAbove):
    """A linear pipeline which connects layers of links with Pipes."""

    def __init__(self, pipe_factory, *layers, name=None, **pipe_factory_kwargs):
        """Initialize the pipeline."""
        self.name = name
        self.layers = layers
        if len(layers) > 1:
            self.pipes = [
                pipe_factory(below, above, **pipe_factory_kwargs)
                for (below, above) in zip(self.layers[:-1], self.layers[1:])
            ]
        elif len(layers) == 1:
            self.pipes = [pipe_factory(self.layers[0], self.layers[0])]
        else:
            raise NotImplementedError('Empty pipeline is not supported!')
        self.next_clock_request = None
        self.last_clock_update = None

    @property
    def bottom(self):
        """Return the bottom layer."""
        return self.layers[0]

    @property
    def top(self):
        """Return the bottom layer."""
        return self.layers[-1]

    def __repr__(self):
        """Return a string representation of the pipeline.

        Left-to-right order is from bottom to top.
        """
        layers = '|'.join('{}'.format(layer) for layer in self.layers)
        if self.name is not None:
            return '[{}: {}]'.format(self.name, layers)
        else:
            return '[{}]'.format(layers)

    # Clocks

    def update_clock(self, time):
        """Update the clock of any ClockedLink and do any necessary processing."""
        self.last_clock_update = time
        if self.clock_update_requested(time):
            self.next_clock_request = None
            # print('Reset next clock request to None!')
        for pipe in self.pipes:
            pipe.update_clock(time)

    def clock_update_requested(self, time):
        """Return whether a clock request has been requested by the given time."""
        return (
            self.next_clock_request is not None and time >= self.next_clock_request
        )

    def update_clock_request(self, event):
        """Update the next clock request based on the event."""
        if self.next_clock_request is None:
            self.next_clock_request = event
        else:
            self.next_clock_request = min(self.next_clock_request, event)

    # EventLink/StreamLink-like interface

    @SetterProperty
    def after_receive(self, handler):
        """Mimic EventLink.after_receive."""
        self.pipes[-1].after_receive = handler

    @SetterProperty
    def directly_receive(self, handler):
        """Mimic EventLink.directly_receive."""
        self.pipes[-1].directly_receive = handler

    @SetterProperty
    def after_send(self, handler):
        """Mimic EventLink.after_receive."""
        self.pipes[0].after_send = handler

    @SetterProperty
    def directly_to_send(self, handler):
        """Mimic EventLink.directly_to_send."""
        self.pipes[0].directly_to_send = handler

    @SetterProperty
    def after_read(self, handler):
        """Mimic StreamLink.after_read."""
        self.pipes[-1].after_read = handler

    @SetterProperty
    def after_write(self, handler):
        """Mimic StreamLink.after_write."""
        self.pipes[0].after_write = handler

    # Implement GenericLinkBelow

    def to_receive(self, event):
        """Implement EventLinkBelow.to_receive."""
        return self.pipes[0].to_receive(event)

    def to_send(self):
        """Implement EventLinkBelow.to_send."""
        return self.pipes[0].to_send()

    def has_to_send(self):
        """Implement EventLinkBelow.has_to_send."""
        return self.pipes[0].has_to_send()

    def to_read(self, event):
        """Implement StreamLinkBelow.to_read."""
        return self.pipes[0].to_read(event)

    def to_write(self):
        """Implement StreamLinkBelow.to_write."""
        return self.pipes[0].to_write()

    # Implement GenericLinkAbove

    def receive(self):
        """Implement EventLinkAbove.receive."""
        return self.pipes[-1].receive()

    def has_receive(self):
        """Implement EventLinkAbove.has_receive."""
        return self.pipes[-1].has_receive()

    def send(self, event):
        """Implement EventLinkAbove.send."""
        return self.pipes[-1].send(event)

    def read(self):
        """Implement StreamLinkAbove.read."""
        return self.pipes[-1].read()

    def write(self, event):
        """Implement StreamLinkAbove.write."""
        return self.pipes[-1].write(event)


class ManualPipeline(Pipeline):
    """A manually synchronized pipeline."""

    def __init__(self, *layers, pipe_factory=ManualPipe, **kwargs):
        """Initialize the pipeline."""
        super().__init__(pipe_factory, *layers, **kwargs)

    # Synchronization

    def sync(self):
        """Sync data from the lowest layer to the highest, then backwards.

        Returns the earliest clock update requested by any link in the pipeline.
        """
        # print('Sync up...')
        self.sync_up()
        # print('Sync down...')
        self.sync_down()
        return self.next_clock_request

    def sync_up(self):
        """Sync data from the lowest layer to the highest.

        Returns the earliest clock update requested by any bottom link in the pipeline.
        """
        clock_requests = list(remove_none(pipe.sync_up() for pipe in self.pipes))
        if clock_requests:
            next_clock_request = min(clock_requests)
            self.update_clock_request(next_clock_request)
        return self.next_clock_request

    def sync_down(self):
        """Sync data from the highest layer to the lowest.

        Returns the earliest clock update requested by any top link in the pipeline.
        """
        clock_requests = list(remove_none(pipe.sync_down() for pipe in reversed(self.pipes)))
        if clock_requests:
            next_clock_request = min(clock_requests)
            self.update_clock_request(next_clock_request)
        return self.next_clock_request

    # Clocks

    def update_clock(self, time):
        """Update the clock of any ClockedLink and do any necessary processing.

        Returns the earliest clock update requested by any processor within the pipeline.
        """
        super().update_clock(time)
        return self.sync()


class AutomaticPipeline(Pipeline):
    """A automatically synchronized pipeline."""

    def __init__(self, *layers, pipe_factory=AutomaticPipe, **kwargs):
        """Initialize the pipeline."""
        super().__init__(pipe_factory, *layers, **kwargs)
        self.pipes_clocked = [pipe for pipe in self.pipes if pipe.clocked]
        self.clocked = len(self.pipes_clocked) > 0

    # Clocks

    def update_clock(self, time):
        """Update the clock of any ClockedLink and do any necessary processing."""
        self.last_clock_update = time
        if not self.clocked:
            return
        if self.next_clock_request is not None and time >= self.next_clock_request:
            self.next_clock_request = None
            # print('Reset next clock request to None!')
        for pipe in self.pipes_clocked:
            next_clock_request = pipe.update_clock_send(time)
            self.update_clock_request(next_clock_request)
        for pipe in reversed(self.pipes_clocked):
            next_clock_request = pipe.update_clock_receive(time)
            self.update_clock_request(next_clock_request)
        return self.next_clock_request


class PipelineBottomCoupler(object):
    """A pipeline-to-pipeline coupler which connects two pipelines by their bottoms."""

    def __init__(self, pipeline_one, pipeline_two):
        """Initialize members."""
        self.pipeline_one = pipeline_one
        self.pipeline_two = pipeline_two
        self.is_manual_one = isinstance(self.pipeline_one, ManualPipeline)
        self.is_manual_two = isinstance(self.pipeline_two, ManualPipeline)
        if hasattr(self.pipeline_one, 'after_write'):
            self.pipeline_one.after_write = self._write_one
        if hasattr(self.pipeline_one, 'after_send'):
            self.pipeline_one.after_send = self._send_one
        if hasattr(self.pipeline_two, 'after_write'):
            self.pipeline_two.after_write = self._write_two
        if hasattr(self.pipeline_two, 'after_send'):
            self.pipeline_two.after_send = self._send_two

    def __repr__(self):
        """Represent the coupler as a string."""
        return '╔{}\n╚{}'.format(self.pipeline_one, self.pipeline_two)

    def update_clock(self, clock_time):
        """Update the pipeline's clock.

        If the pipeline is a manual pipeline, it will also sync the pipeline and write
        any data at the bottom to the connection.
        """
        self.pipeline_one.update_clock(clock_time)
        self.pipeline_two.update_clock(clock_time)
        if self.is_manual_one:
            self.write_one()
            self.send_one()
        if self.is_manual_two:
            self.write_two()
            self.send_two()

    def _write_one(self, buffer):
        """Write the buffer to the connection.

        This is used to overwrite the after_write of the bottom of the pipeline
        if it's an automatic pipeline.
        """
        # print('Sending to pipeline two: {}'.format(buffer))
        self.pipeline_two.to_read(buffer)
        yield from wait()

    def _write_two(self, buffer):
        """Write the buffer to the connection.

        This is used to overwrite the after_write of the bottom of the pipeline
        if it's an automatic pipeline.
        """
        # print('Sending to pipeline one: {}'.format(buffer))
        self.pipeline_one.to_read(buffer)
        yield from wait()

    def write_one(self):
        """Update the write side of the coupler.

        This is used for manual writing from the bottom of the pipeline if it's not
        an automatic pipeline.
        """
        data = self.pipeline_one.to_write()
        if data:
            self.pipeline_two.to_read(data)
        return data

    def write_two(self):
        """Update the write side of the coupler.

        This is used for manual writing from the bottom of the pipeline if it's not
        an automatic pipeline.
        """
        data = self.pipeline_two.to_write()
        if data:
            self.pipeline_one.to_read(data)
        return data

    def _send_one(self, event):
        """Send the event to the connection.

        This is used to overwrite the after_send of the bottom of the pipeline
        if it's an automatic pipeline.
        """
        # print('Sending to pipeline two: {}'.format(event))
        self.pipeline_two.to_receive(event)
        yield from proceed()

    def _send_two(self, event):
        """Send the event to the connection.

        This is used to overwrite the after_send of the bottom of the pipeline
        if it's an automatic pipeline.
        """
        # print('Sending to pipeline one: {}'.format(event))
        self.pipeline_one.to_receive(event)
        yield from proceed()

    def send_one(self):
        """Update the send side of the coupler.

        This is used for manual writing from the bottom of the pipeline if it's not
        an automatic pipeline.
        """
        data = self.pipeline_one.to_send()
        if data:
            self.pipeline_two.to_receive(data)
        return data

    def send_two(self):
        """Update the send side of the coupler.

        This is used for manual writing from the bottom of the pipeline if it's not
        an automatic pipeline.
        """
        data = self.pipeline_two.to_send()
        if data:
            self.pipeline_one.to_receive(data)
        return data
