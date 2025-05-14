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
BYTEORDER  = sys.byteorder  # 'little' or 'big'

# Native C struct formats
# '@i' = C int, '@l' = C long, native order & alignment
_pack_int  = struct.Struct('@i').pack
_unpack_int = struct.Struct('@i').unpack
_pack_long = struct.Struct('@l').pack
_unpack_long = struct.Struct('@l').unpack


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
            flag = _unpack_int(raw_flag)[0]
            if flag != 1:
                return flag

            raw_size = sock.recv(SIZE_LONG)
            size = _unpack_long(raw_size)[0]

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
    