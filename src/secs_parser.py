"""
SECS-II 解析器和编码器。

这里实现的是标准 HSMS Data Message 中使用的 SECS-II 10 字节消息头
加数据项文本（items/text）部分，不包含 HSMS 最外层 4 字节长度。
"""

import struct
from typing import Any, Optional
import logging

from .secs_types import (
    SECSType,
    decode_format,
    encode_format,
)
from .secs_message import SECSItem, SECSMessage


logger = logging.getLogger(__name__)


class SECSParser:
    """
    SECS-II 消息解析器。

    输入是标准 HSMS Data Message 中的 10 字节消息头和后续 items/text。
    """

    HEADER_SIZE = 10
    HEADER_STRUCT = struct.Struct(">HBBBBL")

    def feed(self, data: bytes) -> list[SECSMessage]:
        """
        解析完整 SECS-II 消息。

        Args:
            data: 10 字节消息头 + items/text

        Returns:
            解析出的消息列表。当前实现一次只解析一条完整消息。
        """
        if len(data) < self.HEADER_SIZE:
            return []

        return [self._parse_message(data)]

    def _parse_message(self, data: bytes) -> SECSMessage:
        """解析单条完整消息。"""
        if len(data) < self.HEADER_SIZE:
            raise ValueError("Message too short")

        device_id, stream_byte, function, p_type, s_type, system_value = self.HEADER_STRUCT.unpack(
            data[: self.HEADER_SIZE]
        )

        if p_type != 0 or s_type != 0:
            raise ValueError("Not a SECS data message")

        w_bit = (stream_byte & 0x80) != 0
        stream = stream_byte & 0x7F
        system_bytes = system_value.to_bytes(4, byteorder="big")

        items = []
        if len(data) > self.HEADER_SIZE:
            items, consumed = self._parse_items(data, self.HEADER_SIZE)
            expected_end = self.HEADER_SIZE + consumed
            if expected_end != len(data):
                raise ValueError("Unexpected trailing bytes in SECS message")

        return SECSMessage(
            stream=stream,
            function=function,
            w_bit=w_bit,
            device_id=device_id,
            system_bytes=system_bytes,
            items=items,
        )

    def _parse_items(self, data: bytes, offset: int) -> tuple[list[SECSItem], int]:
        """递归解析数据项列表。"""
        items = []
        start = offset

        while offset < len(data):
            item, item_consumed = self._parse_single_item(data, offset)
            if item is None or item_consumed == 0:
                raise ValueError("Incomplete SECS item")
            items.append(item)
            offset += item_consumed

        return items, offset - start

    def _parse_single_item(self, data: bytes, offset: int) -> tuple[Optional[SECSItem], int]:
        """解析单个数据项。"""
        if offset >= len(data):
            return None, 0

        secs_type, length, header_size = decode_format(data, offset)
        item_offset = offset + header_size

        if secs_type == SECSType.LIST:
            children = []
            cursor = item_offset
            for _ in range(length):
                child, child_consumed = self._parse_single_item(data, cursor)
                if child is None or child_consumed == 0:
                    raise ValueError("Incomplete LIST item")
                children.append(child)
                cursor += child_consumed
            return SECSItem(type=SECSType.LIST, children=children), cursor - offset

        if item_offset + length > len(data):
            raise ValueError("Item payload exceeds buffer")

        value = self._decode_value(data, item_offset, secs_type, length)
        return SECSItem(type=secs_type, value=value), header_size + length

    def _decode_value(
        self, data: bytes, offset: int, secs_type: SECSType, size: int
    ) -> Any:
        """解码原子类型值。"""
        if size == 0:
            return None

        if secs_type == SECSType.BOOLEAN:
            return data[offset] != 0
        if secs_type == SECSType.INT8:
            return struct.unpack(">q", data[offset : offset + 8])[0]
        if secs_type == SECSType.INT1:
            return struct.unpack(">b", data[offset : offset + 1])[0]
        if secs_type == SECSType.UINT8:
            return struct.unpack(">Q", data[offset : offset + 8])[0]
        if secs_type == SECSType.UINT1:
            return struct.unpack(">B", data[offset : offset + 1])[0]
        if secs_type == SECSType.INT4:
            return struct.unpack(">i", data[offset : offset + 4])[0]
        if secs_type == SECSType.UINT4:
            return struct.unpack(">I", data[offset : offset + 4])[0]
        if secs_type == SECSType.INT2:
            return struct.unpack(">h", data[offset : offset + 2])[0]
        if secs_type == SECSType.UINT2:
            return struct.unpack(">H", data[offset : offset + 2])[0]
        if secs_type == SECSType.FLOAT8:
            return struct.unpack(">d", data[offset : offset + 8])[0]
        if secs_type == SECSType.FLOAT4:
            return struct.unpack(">f", data[offset : offset + 4])[0]
        if secs_type in (SECSType.ASCII, SECSType.JIS8):
            return data[offset : offset + size].decode("latin-1").rstrip("\x00")
        if secs_type == SECSType.BINARY:
            return bytes(data[offset : offset + size])

        return bytes(data[offset : offset + size])


class SECSEncoder:
    """
    SECS-II 消息编码器。

    输出标准 HSMS Data Message 中的 10 字节消息头和后续 items/text。
    """

    HEADER_STRUCT = struct.Struct(">HBBBBL")

    @staticmethod
    def encode(message: SECSMessage) -> bytes:
        """编码完整 SECS-II 数据消息。"""
        body = SECSEncoder._encode_items(message.items)
        header = SECSEncoder._encode_header(message)
        return header + body

    @staticmethod
    def _encode_header(message: SECSMessage) -> bytes:
        """编码标准 10 字节消息头。"""
        stream_byte = message.stream & 0x7F
        if message.w_bit:
            stream_byte |= 0x80

        system_value = int.from_bytes(
            message.system_bytes[:4].ljust(4, b"\x00"), byteorder="big"
        )

        return SECSEncoder.HEADER_STRUCT.pack(
            message.device_id & 0xFFFF,
            stream_byte,
            message.function & 0xFF,
            0x00,  # PType
            0x00,  # SType, data message
            system_value,
        )

    @staticmethod
    def _encode_items(items: list[SECSItem]) -> bytes:
        result = b""
        for item in items:
            result += SECSEncoder._encode_single_item(item)
        return result

    @staticmethod
    def _encode_single_item(item: SECSItem) -> bytes:
        """编码单个数据项。"""
        if item.type == SECSType.LIST:
            body = SECSEncoder._encode_items(item.children)
            return encode_format(item.type, len(item.children)) + body

        if item.type in (SECSType.ASCII, SECSType.JIS8, SECSType.BINARY):
            if item.value is None:
                value_bytes = b""
            elif isinstance(item.value, bytes):
                value_bytes = item.value
            elif isinstance(item.value, str):
                if item.type == SECSType.JIS8:
                    value_bytes = item.value.encode("iso-2022-jp")
                else:
                    value_bytes = item.value.encode("latin-1")
            else:
                value_bytes = bytes(item.value)
            return encode_format(item.type, len(value_bytes)) + value_bytes

        value_bytes = SECSEncoder._encode_value(item.type, item.value)
        return encode_format(item.type, len(value_bytes)) + value_bytes

    @staticmethod
    def _encode_value(secs_type: SECSType, value: Any) -> bytes:
        """编码原子值。"""
        if value is None:
            return b""

        if secs_type == SECSType.BOOLEAN:
            return bytes([0x01 if value else 0x00])
        if secs_type == SECSType.INT8:
            return struct.pack(">q", int(value))
        if secs_type == SECSType.INT1:
            return struct.pack(">b", int(value))
        if secs_type == SECSType.UINT8:
            return struct.pack(">Q", int(value))
        if secs_type == SECSType.UINT1:
            return struct.pack(">B", int(value))
        if secs_type == SECSType.INT4:
            return struct.pack(">i", int(value))
        if secs_type == SECSType.UINT4:
            return struct.pack(">I", int(value))
        if secs_type == SECSType.INT2:
            return struct.pack(">h", int(value))
        if secs_type == SECSType.UINT2:
            return struct.pack(">H", int(value))
        if secs_type == SECSType.FLOAT8:
            return struct.pack(">d", float(value))
        if secs_type == SECSType.FLOAT4:
            return struct.pack(">f", float(value))
        return b""
