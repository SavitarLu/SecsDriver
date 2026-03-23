"""
SECS-II 数据类型定义

定义 SECS-II 协议支持的所有数据类型及其编码规则。
"""

from enum import IntEnum
from typing import Any, List, Optional, Union
import struct


class SECSType(IntEnum):
    """SECS-II 数据类型枚举"""

    LIST = 0x00
    BINARY = 0x08
    BOOLEAN = 0x09
    ASCII = 0x10
    JIS8 = 0x11
    INT8 = 0x18
    INT1 = 0x19
    INT2 = 0x1A
    INT4 = 0x1C
    FLOAT8 = 0x20
    FLOAT4 = 0x24
    UINT8 = 0x28
    UINT1 = 0x29
    UINT2 = 0x2A
    UINT4 = 0x2C

    # 以下为扩展类型，用于内部处理
    UNKNOWN = 0xFF


class SECSTypeInfo:
    """SECS-II 数据类型信息"""

    # 类型名称映射
    TYPE_NAMES = {
        SECSType.LIST: "L",
        SECSType.BOOLEAN: "BOOLEAN",
        SECSType.ASCII: "A",
        SECSType.INT8: "I8",
        SECSType.INT1: "I1",
        SECSType.INT4: "I4",
        SECSType.INT2: "I2",
        SECSType.UINT8: "U8",
        SECSType.UINT1: "U1",
        SECSType.UINT4: "U4",
        SECSType.UINT2: "U2",
        SECSType.FLOAT8: "F8",
        SECSType.FLOAT4: "F4",
        SECSType.JIS8: "J",
        SECSType.BINARY: "B",
    }

    # 类型字节数
    TYPE_SIZES = {
        SECSType.BOOLEAN: 1,
        SECSType.INT8: 8,
        SECSType.INT1: 1,
        SECSType.INT4: 4,
        SECSType.INT2: 2,
        SECSType.UINT8: 8,
        SECSType.UINT1: 1,
        SECSType.UINT4: 4,
        SECSType.UINT2: 2,
        SECSType.FLOAT8: 8,
        SECSType.FLOAT4: 4,
    }

    @classmethod
    def get_name(cls, secs_type: SECSType) -> str:
        """获取类型名称"""
        return cls.TYPE_NAMES.get(secs_type, "UNKNOWN")

    @classmethod
    def get_size(cls, secs_type: SECSType) -> Optional[int]:
        """获取固定大小类型的字节数"""
        return cls.TYPE_SIZES.get(secs_type)

    @classmethod
    def is_fixed_size(cls, secs_type: SECSType) -> bool:
        """判断是否为固定大小类型"""
        return secs_type in cls.TYPE_SIZES


def encode_length(length: int) -> bytes:
    """
    编码长度字段

    SECS-II 使用 1-3 字节编码长度值：
    - 0-63: 单字节
    - 64-16383: 双字节
    - 16384-4194303: 三字节
    """
    if length < 0:
        raise ValueError("Length cannot be negative")

    if length <= 0x3F:  # 63
        return bytes([length])
    elif length <= 0x3FFF:  # 16383
        return struct.pack(">H", 0x8000 | length)
    elif length <= 0x3FFFFF:  # 4194303
        b1 = (length >> 16) & 0x3F
        b2 = (length >> 8) & 0xFF
        b3 = length & 0xFF
        return bytes([0xC0 | b1, b2, b3])
    else:
        raise ValueError(f"Length {length} exceeds maximum (4194303)")


def decode_length(data: bytes, offset: int) -> tuple[int, int]:
    """
    解码长度字段

    Args:
        data: 数据缓冲区
        offset: 起始偏移

    Returns:
        (length, bytes_consumed): 解码的长度值和消耗的字节数
    """
    if offset >= len(data):
        raise ValueError("Buffer underflow")

    first_byte = data[offset]

    if first_byte & 0x80 == 0:  # 0xxxxxxx: 单字节
        return first_byte, 1
    elif first_byte & 0xC0 == 0x80:  # 10xxxxxx: 双字节
        if offset + 1 >= len(data):
            raise ValueError("Buffer underflow")
        length = ((first_byte & 0x3F) << 8) | data[offset + 1]
        return length, 2
    else:  # 11xxxxxx: 三字节
        if offset + 2 >= len(data):
            raise ValueError("Buffer underflow")
        length = ((first_byte & 0x3F) << 16) | (data[offset + 1] << 8) | data[offset + 2]
        return length, 3


def encode_format(secs_type: SECSType, length: int) -> bytes:
    """
    编码格式字段

    格式字段 = Type << 2 | Length bytes indicator
    """
    if length < 0:
        raise ValueError("Length cannot be negative")
    if length <= 0xFF:
        length_bytes = 1
    elif length <= 0xFFFF:
        length_bytes = 2
    elif length <= 0xFFFFFF:
        length_bytes = 3
    else:
        raise ValueError(f"Length {length} exceeds maximum (16777215)")

    format_byte = (secs_type << 2) | length_bytes
    return bytes([format_byte]) + length.to_bytes(length_bytes, byteorder="big")


def decode_format(data: bytes, offset: int) -> tuple[SECSType, int, int]:
    """
    解码格式字段

    Args:
        data: 数据缓冲区
        offset: 起始偏移

    Returns:
        (secs_type, length, bytes_consumed)
    """
    if offset >= len(data):
        raise ValueError("Buffer underflow")

    first_byte = data[offset]

    secs_type = SECSType(first_byte >> 2)
    length_bytes = first_byte & 0x03
    if length_bytes == 0:
        raise ValueError("Invalid length-byte count in item header")
    if offset + 1 + length_bytes > len(data):
        raise ValueError("Buffer underflow")
    length = int.from_bytes(data[offset + 1 : offset + 1 + length_bytes], byteorder="big")
    return secs_type, length, 1 + length_bytes
