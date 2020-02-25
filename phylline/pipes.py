"""Classes for building pipelines of layered links."""

# Builtins
import itertools

# Packages

from phylline.links.clocked import LinkClockRequest, LinkClockTime
from phylline.links.links import GenericLinkAbove, GenericLinkBelow
from phylline.processors import proceed, wait
from phylline.util.interfaces import SetterProperty
from phylline.util.iterables import make_collection, remove_none


# Pipes


class Pipe(GenericLinkBelow, GenericLinkAbove):
    """Link to join two layers of links together in a pipeline.

    This makes it possible to implement Kahn process networks with links.

    This is just a base class without any implementation of how data is pipelined
    between the layers.

    Note that the pipe only passes events between EventLinks and streams between
    StreamLinks, because there is no standardized way to convert between streams
    and arbitrary events!
    """

    def __init__(self, bottom, top):
        """Initialize members.

        Instantiate the pipe before passing any data to bottom or top, or else
        you will probably lose data!

        If you pass in an iterable of multiple links in bottom and/or top, then
        anything from receive() of any bottom link will be passed to to_read of
        every link in top, and anything from to_send of any top link will
        be passed to send of every link in bottom.
        """
        self.bottom = make_collection(bottom)
        self.top = make_collection(top)
        self.name = None

    def __repr__(self):
        """Return a string representation of the pipeline.

        Left-to-right order is from bottom to top.
        """
        if len(self.bottom) == 1:
            bottom = str(self.bottom[0])
        else:
            bottom = repr(self.bottom)
        if len(self.top) == 1:
            top = str(self.top[0])
        else:
            top = repr(self.top)
        return '[{} | {}]'.format(bottom, top)

    # Utilities for implementing Pipes

    def receive_up(self, event):
        """Pass an event up to all top links which receive events."""
        # print('Passing event up: {}'.format(event))
        for top in self.top:
            try:
                top.to_receive(event)
            except AttributeError:
                pass

    def read_up(self, buffer):
        """Pass a stream buffer up to all top links which read streams."""
        # print('Passing buffer up: {}'.format(buffer))
        for top in self.top:
            try:
                top.to_read(buffer)
            except AttributeError:
                pass

    def send_down(self, event):
        """Pass an event down to all bottom links which send events."""
        # print('Passing event down: {}'.format(event))
        for bottom in self.bottom:
            try:
                bottom.send(event)
            except AttributeError:
                pass

    def write_down(self, buffer):
        """Pass a stream buffer down to all bottom links which write streams."""
        # print('Passing buffer down: {}'.format(buffer))
        for bottom in self.bottom:
            try:
                bottom.write(buffer)
            except AttributeError:
                pass

    # EventLink/StreamLink-like interface

    @SetterProperty
    def after_receive(self, handler):
        """Mimic EventLink.after_receive."""
        for top in self.top:
            if hasattr(top, 'after_receive'):
                top.after_receive = handler

    @SetterProperty
    def directly_receive(self, handler):
        """Mimic EventLink.directly_receive."""
        for top in self.top:
            if hasattr(top, 'directly_receive'):
                top.directly_receive = handler

    @SetterProperty
    def after_send(self, handler):
        """Mimic EventLink.after_receive."""
        for bottom in self.bottom:
            if hasattr(bottom, 'after_send'):
                bottom.after_send = handler

    @SetterProperty
    def directly_to_send(self, handler):
        """Mimic EventLink.directly_to_receive."""
        for bottom in self.bottom:
            if hasattr(bottom, 'directly_to_send'):
                bottom.directly_to_send = handler

    @SetterProperty
    def after_read(self, handler):
        """Mimic StreamLink.after_read."""
        for top in self.top:
            if hasattr(top, 'after_read'):
                top.after_read = handler

    @SetterProperty
    def after_write(self, handler):
        """Mimic StreamLink.after_write."""
        for bottom in self.bottom:
            if hasattr(bottom, 'after_write'):
                bottom.after_write = handler

    # Clocks

    def update_clock(self, time):
        """Update the clock of any ClockedLink and do any necessary processing."""
        for link in itertools.chain(self.bottom, self.top):
            try:
                link.update_clock(time)
            except AttributeError:
                pass

    # Implement GenericLinkBelow

    def to_receive(self, event):
        """Implement EventLinkBelow.to_receive."""
        for bottom in self.bottom:
            try:
                bottom.to_receive(event)
            except AttributeError:
                pass

    def to_send(self):
        """Implement EventLinkBelow.to_send."""
        for bottom in self.bottom:
            try:
                if bottom.has_to_send():
                    return bottom.to_send()
            except AttributeError:
                pass

    def has_to_send(self):
        """Implement EventLinkBelow.has_to_send."""
        for bottom in self.bottom:
            try:
                if bottom.has_to_send():
                    return True
            except AttributeError:
                pass
        return False

    def to_read(self, event):
        """Implement StreamLinkBelow.to_read."""
        for bottom in self.bottom:
            try:
                bottom.to_read(event)
            except AttributeError:
                pass

    def to_write(self):
        """Implement StreamLinkBelow.to_write."""
        all_to_write = b''
        for bottom in self.bottom:
            try:
                if all_to_write == b'':
                    all_to_write = bottom.to_write()
                else:
                    all_to_write += bottom.to_write()
            except AttributeError:
                pass
        return all_to_write

    # Implement GenericLinkAbove

    def receive(self):
        """Implement EventLinkAbove.receive."""
        for top in self.top:
            try:
                if top.has_receive():
                    return top.receive()
            except AttributeError:
                pass
        return False

    def has_receive(self):
        """Implement EventLinkAbove.has_receive."""
        for top in self.top:
            try:
                if top.has_receive():
                    return True
            except AttributeError:
                pass
        return False

    def send(self, event):
        """Implement EventLinkAbove.send."""
        for top in self.top:
            try:
                top.send(event)
            except AttributeError:
                pass

    def read(self):
        """Implement StreamLinkAbove.read."""
        all_read = None
        for top in self.top:
            try:
                if all_read is None:
                    all_read = top.read()
                else:
                    all_read += top.read()
            except AttributeError:
                pass
        return all_read

    def write(self, event):
        """Implement StreamLinkAbove.write."""
        for top in self.top:
            try:
                top.write(event)
            except AttributeError:
                pass


class ManualPipe(Pipe):
    """Link to join two layers of EventLinks or StreamLinks together in a pipeline.

    This one requires manual synchronization between the links, by calling the
    sync method.
    """

    def __init__(self, *args):
        """Initialize members."""
        super().__init__(*args)
        self.next_clock_request = None
        self.last_clock_update = None

    # Synchronization

    def sync_bottom_links(self):
        """If any bottom links are also ManualPipes, sync them."""
        for link in self.bottom:
            try:
                link.sync()
            except AttributeError:  # link is not a ManualPipe
                pass

    def sync_top_links(self):
        """If any top links are also ManualPipes, sync them."""
        for link in self.top:
            try:
                link.sync()
            except AttributeError:  # link is not a ManualPipe
                pass

    def sync(self):
        """Synchronize the queues or buffers between the two layers.

        The receive() of bottom will be passed up to the to_receive() of top, and the
        to_send() of top will be passed down to the send() of bottom.

        Returns the earliest clock update requested by any processor within the
        pipe. This consists of the receivers in the links in the bottom layer
        and the senders in the links in the top layer.
        """
        self.sync_up()
        self.sync_down()
        return self.next_clock_request

    def sync_up(self):
        """Synchronize the queues or buffers from the bottom layer to the top layer.

        The receive() of bottom will be passed up to the to_receive() of top.

        Returns the earliest clock update requested by the receiver of any link
        in the bottom layer.
        """
        clock_requests = list(remove_none(self._sync_up(bottom) for bottom in self.bottom))
        if clock_requests:
            next_clock_request = min(clock_requests)
            self.update_clock_request(next_clock_request)
        return self.next_clock_request

    def sync_down(self):
        """Synchronize the queues or buffers from the top layer to the bottom layer.

        The to_send() of top will be passed down to the send() of bottom.

        Returns the earliest clock update requested by the sender of any link
        in the top layer.
        """
        clock_requests = list(remove_none(self._sync_down(top) for top in self.top))
        if clock_requests:
            next_clock_request = min(clock_requests)
            self.update_clock_request(next_clock_request)
        return self.next_clock_request

    def _sync_up(self, bottom):
        """Synchronize from a bottom link to the top layer.

        Returns the earliest clock update requested by the receiver of the bottom link.
        """
        earliest_clock_request = None
        self.sync_bottom_links()
        if self.bottom == self.top:
            return None

        try:
            for event in bottom.receive_all():
                if isinstance(event, LinkClockRequest):
                    earliest_clock_request = min(earliest_clock_request, event)
                else:
                    self.receive_up(event)
        except AttributeError:
            pass
        try:
            self.read_up(bottom.read())
        except AttributeError:
            pass
        self.sync_top_links()
        return earliest_clock_request

    def _sync_down(self, top):
        """Synchronize from a top link to the bottom layer.

        Returns the earliest clock update requested by the sender of the top link.
        """
        earliest_clock_request = None
        self.sync_top_links()
        if self.bottom == self.top:
            return None

        try:
            for event in top.to_send_all():
                if isinstance(event, LinkClockRequest):
                    earliest_clock_request = min(earliest_clock_request, event)
                else:
                    self.send_down(event)
        except AttributeError:
            pass
        try:
            self.write_down(top.to_write())
        except AttributeError:
            pass
        self.sync_bottom_links()
        return earliest_clock_request

    # Clocks

    def update_clock(self, time):
        """Update the clock of any ClockedLink and do any necessary processing."""
        if self.next_clock_request is not None and time >= self.next_clock_request:
            self.next_clock_request = None
        self.last_clock_update = time
        super().update_clock(time)
        return self.sync()

    def update_clock_request(self, event):
        """Update the next clock request based on the event."""
        if self.next_clock_request is None:
            self.next_clock_request = event
        else:
            self.next_clock_request = min(self.next_clock_request, event)
        # print(
        #     'Updated next_clock_request to {}!'
        #     .format(self.next_clock_request)
        # )


class AutomaticPipe(Pipe):
    """Link to join the queues of two EventLinks together in a pipeline.

    This one provides automatic synchronization between the links, by modifying
    the protocols.

    Warning: this works by monkey-patching the after_receive and after_send methods
    of EventLink, and the after_read and after_write methods of StreamLink! So
    if you want to use this pipe, don't put any custom logic in there.
    Warning: after you make the pipe, you shouldn't modify the bottom or top
    members; instead, make a new pipe to add new connections.
    """

    def __init__(self, bottom, top):
        """Initialize protocols.

        Instantiate this before sending any data to bottom or top, or else you will probably
        lose data!
        """
        super().__init__(bottom, top)
        self.connected_up = True
        self.connected_down = True
        if self.bottom != self.top:
            for bottom in self.bottom:
                if hasattr(bottom, 'after_receive'):
                    bottom.after_receive = self._after_receive
                if hasattr(bottom, 'after_read'):
                    bottom.after_read = self._after_read
                if hasattr(bottom, 'directly_receive'):
                    bottom.directly_receive = self._directly_receive
            for top in self.top:
                if hasattr(top, 'after_send'):
                    top.after_send = self._after_send
                if hasattr(top, 'after_write'):
                    top.after_write = self._after_write
                if hasattr(top, 'directly_to_send'):
                    top.directly_to_send = self._directly_to_send
        self.bottom_clocked = [
            link for link in self.bottom if hasattr(link, 'update_clock')
        ]
        self.top_clocked = [
            link for link in self.top if hasattr(link, 'update_clock')
        ]
        self.clocked = self.bottom_clocked or self.top_clocked
        self.next_clock_request = None
        self.last_clock_update = None

    def _directly_receive(self, event):
        """Pass the processed event up to the top layer."""
        if isinstance(event, LinkClockRequest):
            self.update_clock_request(event)
            return
        if not self.connected_up:
            return
        # print('AutomaticPipe passing event up: {}'.format(event))
        self.receive_up(event)

    def _after_receive(self, event):
        """Pass the processed event up to the top layer."""
        self._directly_receive(event)
        yield from proceed()  # proceed to process any additional events
        # print('AutomaticPipe after_receive finished yielding from proceed!')

    def _directly_to_send(self, event):
        """Pass the processed event down to the bottom layer."""
        if isinstance(event, LinkClockRequest):
            self.update_clock_request(event)
            return
        if not self.connected_down:
            return
        # print('AutomaticPipe passing event down: {}'.format(event))
        self.send_down(event)

    def _after_send(self, event):
        """Pass the processed event down to the bottom layer."""
        self._directly_to_send(event)
        yield from proceed()  # proceed to process any additional events
        # print('AutomaticPipe after_send finished yielding from proceed!')

    def _after_read(self, buffer):
        """Pass the processed buffer up to the top layer."""
        if not self.connected_up:
            return
        # print('AutomaticPipe passing buffer up: {}'.format(buffer))
        self.read_up(buffer)
        yield from wait()  # wait for more buffer activity

    def _after_write(self, buffer):
        """Pass the processed buffer down to the bottom layer."""
        if not self.connected_down:
            return
        # print('AutomaticPipe passing buffer down: {}'.format(buffer))
        self.write_down(buffer)
        yield from wait()  # wait for more buffer activity

    # Clocks

    def update_clock(self, time):
        """Update the clock of any ClockedLink and do any necessary processing."""
        self.last_clock_update = time
        self.update_clock_send(time)
        self.update_clock_receive(time)
        return self.next_clock_request

    def update_clock_send(self, time):
        """Update the clocks of any ClockedLink and do any necessary processing, for send.

        Updates the bottom clocks first, because automatic piping needs the bottom
        send processor to have the updated time before it automatically gets
        events from the top send processor.
        """
        if not self.clocked:
            return
        if self.next_clock_request is not None and time >= self.next_clock_request:
            self.next_clock_request = None
        for link in itertools.chain(self.bottom_clocked, self.top_clocked):
            link.send(LinkClockTime(time))
        return self.next_clock_request

    def update_clock_receive(self, time):
        """Update the clocks of any ClockedLink and do any necessary processing, for receive.

        Updates the top clocks first, because automatic piping needs the top
        receive processor to have the updated time before it automatically gets
        events from the bottom receive processor.
        """
        if not self.clocked:
            return
        if self.next_clock_request is not None and time >= self.next_clock_request:
            self.next_clock_request = None
        for link in itertools.chain(self.top_clocked, self.bottom_clocked):
            link.to_receive(LinkClockTime(time))
        return self.next_clock_request

    def update_clock_request(self, event):
        """Update the next clock request based on the event."""
        if self.next_clock_request is None:
            self.next_clock_request = event
        else:
            self.next_clock_request = min(self.next_clock_request, event)
        # print(
        #     'Updated next_clock_request to {}!'
        #     .format(self.next_clock_request)
        # )
