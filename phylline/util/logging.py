"""Support for nicer logging."""

# Builtins

# Packages


def hex_bytes(buffer, truncate=10):
    """Return a hex string representation of a byte buffer."""
    if truncate is not None and truncate < 0:
        raise ValueError('Invalid truncation length is negative: {}'.format(truncate))

    if len(buffer) == 0:
        return '(0 bytes)'

    truncated_length = 0
    if truncate is not None and len(buffer) > truncate:
        truncated_length = len(buffer) - truncate
        buffer = buffer[:truncate]
    hex_string = ' '.join('0x{:02x}'.format(b) for b in buffer)
    if truncated_length > 0:
        if truncated_length == 1:
            unit = 'byte'
        else:
            unit = 'bytes'
        if hex_string:
            hex_string += ' ...({} more {})'.format(truncated_length, unit)
        else:
            hex_string = ('({} {})'.format(truncated_length, unit))
    return hex_string
