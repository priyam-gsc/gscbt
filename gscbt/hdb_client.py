import socket
import struct
import ctypes
import sys

from pathlib import Path


BUFFER_SIZE = 4096
KEY_SIZE = 24
SEPARATOR = "||"


SIZE_INT   = ctypes.sizeof(ctypes.c_int)
SIZE_LONG  = ctypes.sizeof(ctypes.c_long)
BYTEORDER  = "little"  # 'little' or 'big'

# ——————————————————————————————————————————————————————————————————
# Helpers to unpack C++-style int and long
# ——————————————————————————————————————————————————————————————————
SIZE_INT  = 4
SIZE_LONG = 8
BYTEORDER = sys.byteorder  # 'little' or 'big'

def unpack_c_int(data: bytes) -> int:
    """Unpack a C 'int' from bytes using native byte order."""
    if len(data) != SIZE_INT:
        raise ValueError(f"Expected {SIZE_INT} bytes for C 'int', got {len(data)}")
    return int.from_bytes(data, byteorder=BYTEORDER, signed=True)

def unpack_c_long(data: bytes) -> int:
    """Unpack a C 'long' from bytes using native byte order."""
    if len(data) != SIZE_LONG:
        raise ValueError(f"Expected {SIZE_LONG} bytes for C 'long', got {len(data)}")
    return int.from_bytes(data, byteorder=BYTEORDER, signed=True)


def prep_hdb_key(*args) -> str:
    combined = SEPARATOR.join(args)  
    padded = combined.encode('utf-8')           
    padded = padded.ljust(KEY_SIZE, b'\x00')[:KEY_SIZE] 
    return padded

def hdb_download(
    HDB_IP_PORT : str,
    data_key : str, 
    path_with_filename : Path
) -> int:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            ip, port = HDB_IP_PORT.split(":",1)
            port = int(port)

            sock.connect((ip, port))
            sock.sendall(data_key)

            raw_flag = sock.recv(SIZE_INT)
            flag = unpack_c_int(raw_flag)
            if flag != 1:
                return flag

            raw_size = sock.recv(SIZE_LONG)
            size = unpack_c_long(raw_size)

            bytes_recd = 0
            if size > 0:
                with open(path_with_filename, "wb") as f:
                    while bytes_recd < size:
                        chunk = sock.recv(min(BUFFER_SIZE, size - bytes_recd))
                        if not chunk:
                            raise ConnectionError("Connection closed prematurely")
                        f.write(chunk)
                        bytes_recd += len(chunk)

            return 1

    except Exception as e:
        if path_with_filename.exists():
            path_with_filename.unlink()
        return 0
    