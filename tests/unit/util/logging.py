"""Test the util.logging module."""

# Builtins

# Packages

from phylline.util.logging import hex_bytes

import pytest


def test_hex_bytes_negative_truncate():
    """Test whether the hex_bytes correctly handles invalid truncate parameter values."""
    with pytest.raises(ValueError):
        hex_bytes(bytes([0x00, 0x01, 0x02]), truncate=-1)
    with pytest.raises(ValueError):
        hex_bytes(bytes([]), truncate=-1)


def test_hex_bytes_empty():
    """Test whether the hex_bytes correctly renders empty buffers."""
    assert hex_bytes(bytes([]), truncate=1) == '(0 bytes)'
    assert hex_bytes(bytes([]), truncate=0) == '(0 bytes)'


def test_hex_bytes_untruncated():
    """Test whether the hex_bytes correctly renders buffers without truncation."""
    short = bytes([0x00, 0x01, 0x02])
    assert(hex_bytes(short, truncate=None)) == '0x00 0x01 0x02'
    buffer = bytes(range(5))
    assert hex_bytes(buffer, truncate=6) == '0x00 0x01 0x02 0x03 0x04'
    assert hex_bytes(buffer, truncate=5) == '0x00 0x01 0x02 0x03 0x04'


def test_hex_bytes_truncated():
    """Test whether the hex_bytes correctly renders buffers with truncation."""
    buffer = bytes(range(5))
    assert hex_bytes(buffer, truncate=4) == '0x00 0x01 0x02 0x03 ...(1 more byte)'
    assert hex_bytes(buffer, truncate=3) == '0x00 0x01 0x02 ...(2 more bytes)'
    assert hex_bytes(buffer, truncate=2) == '0x00 0x01 ...(3 more bytes)'
    assert hex_bytes(buffer, truncate=1) == '0x00 ...(4 more bytes)'
    assert hex_bytes(buffer, truncate=0) == '(5 bytes)'
    assert hex_bytes(bytes([0x00]), truncate=0) == '(1 byte)'
