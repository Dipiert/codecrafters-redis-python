from enum import Enum

class Constants(Enum):
    
    SEPARATOR = '\r\n'
    ARRAY_FIRST_BYTE = '*'
    RDB_KV_HASH_TABLE_SIZE_START = b"\xfb"