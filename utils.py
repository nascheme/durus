"""$URL$
$Id$
"""

import struct

def format_oid(oid):
    """(oid:str) -> str
    Returns a nice representation of an 8-byte oid.
    """
    return str(oid and u64(oid))

def p64(v):
    """Pack an integer or long into a 8-byte string"""
    return struct.pack(">Q", v)

def u64(v):
    """Unpack an 8-byte string into a 64-bit long integer."""
    return struct.unpack(">Q", v)[0]

def p32(v):
    """Pack an integer or long into a 4-byte string"""
    return struct.pack(">L", v)

def u32(v):
    """Unpack an 8-byte string into a 32-bit long integer."""
    return struct.unpack(">L", v)[0]

