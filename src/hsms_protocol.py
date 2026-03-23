"""
HSMS 协议处理器。

实现标准 HSMS-SS（SEMI E37）报文格式：
- 4 字节 Length
- 10 字节 Header
"""

from enum import IntEnum
from dataclasses import dataclass
from typing import Callable, Optional
import logging
import struct

from .secs_message import SECSMessage
from .secs_parser import SECSEncoder, SECSParser


logger = logging.getLogger(__name__)


class HSMSMessageType(IntEnum):
    """标准 HSMS SType。"""

    DATA_MESSAGE = 0x00
    SELECT_REQUEST = 0x01
    SELECT_RESPONSE = 0x02
    DESELECT_REQUEST = 0x03
    DESELECT_RESPONSE = 0x04
    LINKTEST_REQUEST = 0x05
    LINKTEST_RESPONSE = 0x06
    REJECT_REQUEST = 0x07
    SEPARATE_REQUEST = 0x09


class HSMSConnectionState(IntEnum):
    """HSMS 连接状态。"""

    NOT_CONNECTED = 0
    CONNECTED = 1
    NOT_SELECTED = 2
    WAIT_SELECT = 3
    SELECTED = 4
    COMMUNICATION_ACTIVE = 5


@dataclass
class HSMSMessage:
    """一个完整的 HSMS 报文。"""

    session_id: int = 0
    message_type: HSMSMessageType = HSMSMessageType.DATA_MESSAGE
    secs_message: Optional[SECSMessage] = None
    p_type: int = 0
    stream: int = 0
    function: int = 0
    w_bit: bool = False
    raw_data: bytes = b""
    system_bytes_data: bytes = b"\x00\x00\x00\x00"

    @property
    def is_control_message(self) -> bool:
        return self.message_type != HSMSMessageType.DATA_MESSAGE

    @property
    def is_data_message(self) -> bool:
        return self.message_type == HSMSMessageType.DATA_MESSAGE

    @property
    def system_bytes(self) -> bytes:
        if self.secs_message is not None:
            return self.secs_message.system_bytes[:4].ljust(4, b"\x00")
        return self.system_bytes_data[:4].ljust(4, b"\x00")


@dataclass
class HSMSConfig:
    """HSMS 协议配置。"""

    t3_timeout: float = 45.0
    t5_timeout: float = 10.0
    t6_timeout: float = 5.0
    t7_timeout: float = 10.0
    t8_timeout: float = 5.0


class HSMSProtocolHandler:
    """
    标准 HSMS 协议处理器。

    负责收发外层 4 字节长度和 10 字节 HSMS Header。
    """

    LENGTH_BYTES = 4
    HEADER_SIZE = 10
    HEADER_STRUCT = struct.Struct(">HBBBBL")
    CONTROL_SESSION_ID = 0xFFFF

    def __init__(self, config: HSMSConfig = None):
        self.config = config or HSMSConfig()
        self.parser = SECSParser()
        self.state = HSMSConnectionState.NOT_CONNECTED
        self._callbacks = {}
        self._buffer = bytearray()
        self._next_control_system = 1

    def set_callback(self, event: str, callback: Callable) -> None:
        self._callbacks[event] = callback

    def _trigger_callback(self, event: str, *args, **kwargs) -> None:
        if event in self._callbacks:
            try:
                self._callbacks[event](*args, **kwargs)
            except Exception as e:
                logger.error(f"Callback error for {event}: {e}")

    def encode_message(self, message: HSMSMessage) -> bytes:
        """编码为标准 HSMS 报文。"""
        if message.is_control_message:
            payload = self._encode_control_message(message)
        else:
            payload = self._encode_data_message(message)
        return struct.pack(">I", len(payload)) + payload

    def _encode_control_message(self, message: HSMSMessage) -> bytes:
        """编码控制消息。"""
        system_value = int.from_bytes(message.system_bytes, byteorder="big")
        return self.HEADER_STRUCT.pack(
            self.CONTROL_SESSION_ID,
            0x00,
            0x00,
            message.p_type & 0xFF,
            int(message.message_type) & 0xFF,
            system_value,
        )

    def _encode_data_message(self, message: HSMSMessage) -> bytes:
        """编码数据消息。"""
        if message.secs_message is None:
            raise ValueError("Data message requires SECS message")
        return SECSEncoder.encode(message.secs_message)

    def feed(self, data: bytes) -> list[HSMSMessage]:
        """
        将原始 TCP 数据分帧成 HSMS 报文。
        """
        self._buffer.extend(data)
        messages = []

        while len(self._buffer) >= self.LENGTH_BYTES:
            length = struct.unpack(">I", self._buffer[: self.LENGTH_BYTES])[0]
            total_length = self.LENGTH_BYTES + length
            if len(self._buffer) < total_length:
                break

            payload = bytes(self._buffer[self.LENGTH_BYTES : total_length])
            del self._buffer[:total_length]

            decoded = self._decode_payload(payload)
            if decoded is not None:
                messages.append(decoded)

        return messages

    def decode_message(self, data: bytes) -> Optional[HSMSMessage]:
        """
        兼容旧调用，解析一段完整数据。
        """
        messages = self.feed(data)
        return messages[0] if messages else None

    def _decode_payload(self, payload: bytes) -> Optional[HSMSMessage]:
        """解析单个完整 HSMS payload（不含 4 字节 length）。"""
        if len(payload) < self.HEADER_SIZE:
            return None

        try:
            session_id, stream_byte, function, p_type, s_type, system_value = self.HEADER_STRUCT.unpack(
                payload[: self.HEADER_SIZE]
            )
            system_bytes = system_value.to_bytes(4, byteorder="big")
            stream = stream_byte & 0x7F
            w_bit = (stream_byte & 0x80) != 0

            if s_type == HSMSMessageType.DATA_MESSAGE:
                secs_messages = self.parser.feed(payload)
                if not secs_messages:
                    return None
                secs_message = secs_messages[0]
                return HSMSMessage(
                    session_id=session_id,
                    message_type=HSMSMessageType.DATA_MESSAGE,
                    secs_message=secs_message,
                    p_type=p_type,
                    stream=secs_message.stream,
                    function=secs_message.function,
                    w_bit=secs_message.w_bit,
                    raw_data=payload,
                    system_bytes_data=system_bytes,
                )

            return HSMSMessage(
                session_id=session_id,
                message_type=HSMSMessageType(s_type),
                p_type=p_type,
                stream=stream,
                function=function,
                w_bit=w_bit,
                raw_data=payload,
                system_bytes_data=system_bytes,
            )

        except (struct.error, ValueError) as e:
            logger.error(f"Error decoding HSMS message: {e}")
            return None

    def create_select_request(self, system_bytes: bytes = None) -> HSMSMessage:
        return HSMSMessage(
            session_id=self.CONTROL_SESSION_ID,
            message_type=HSMSMessageType.SELECT_REQUEST,
            system_bytes_data=system_bytes or self._next_control_system_bytes(),
        )

    def create_select_response(self, system_bytes: bytes = None) -> HSMSMessage:
        return HSMSMessage(
            session_id=self.CONTROL_SESSION_ID,
            message_type=HSMSMessageType.SELECT_RESPONSE,
            system_bytes_data=system_bytes or self._next_control_system_bytes(),
        )

    def create_deselect_request(self, system_bytes: bytes = None) -> HSMSMessage:
        return HSMSMessage(
            session_id=self.CONTROL_SESSION_ID,
            message_type=HSMSMessageType.DESELECT_REQUEST,
            system_bytes_data=system_bytes or self._next_control_system_bytes(),
        )

    def create_deselect_response(self, system_bytes: bytes = None) -> HSMSMessage:
        return HSMSMessage(
            session_id=self.CONTROL_SESSION_ID,
            message_type=HSMSMessageType.DESELECT_RESPONSE,
            system_bytes_data=system_bytes or self._next_control_system_bytes(),
        )

    def create_linktest_request(self, system_bytes: bytes = None) -> HSMSMessage:
        return HSMSMessage(
            session_id=self.CONTROL_SESSION_ID,
            message_type=HSMSMessageType.LINKTEST_REQUEST,
            system_bytes_data=system_bytes or self._next_control_system_bytes(),
        )

    def create_linktest_response(self, system_bytes: bytes = None) -> HSMSMessage:
        return HSMSMessage(
            session_id=self.CONTROL_SESSION_ID,
            message_type=HSMSMessageType.LINKTEST_RESPONSE,
            system_bytes_data=system_bytes or self._next_control_system_bytes(),
        )

    def create_separate_request(self, system_bytes: bytes = None) -> HSMSMessage:
        return HSMSMessage(
            session_id=self.CONTROL_SESSION_ID,
            message_type=HSMSMessageType.SEPARATE_REQUEST,
            system_bytes_data=system_bytes or self._next_control_system_bytes(),
        )

    def update_state(self, new_state: HSMSConnectionState) -> None:
        old_state = self.state
        self.state = new_state
        logger.debug(f"HSMS state change: {old_state} -> {new_state}")
        self._trigger_callback("state_changed", old_state, new_state)

    def _next_control_system_bytes(self) -> bytes:
        value = self._next_control_system & 0xFFFFFFFF
        if value == 0:
            value = 1
        self._next_control_system = (value + 1) & 0xFFFFFFFF
        return value.to_bytes(4, byteorder="big")


class HSMSConnectionStateMachine:
    """HSMS 连接状态机。"""

    def __init__(self, handler: HSMSProtocolHandler):
        self.handler = handler
        self.handler.state = HSMSConnectionState.NOT_CONNECTED

    def handle_tcp_connected(self) -> None:
        if self.handler.state == HSMSConnectionState.NOT_CONNECTED:
            self.handler.update_state(HSMSConnectionState.CONNECTED)

    def handle_tcp_disconnected(self) -> None:
        self.handler.update_state(HSMSConnectionState.NOT_CONNECTED)

    def handle_select_request(self, system_bytes: bytes) -> HSMSMessage:
        self.handler.update_state(HSMSConnectionState.SELECTED)
        return self.handler.create_select_response(system_bytes=system_bytes)

    def handle_select_response(self) -> bool:
        if self.handler.state in (
            HSMSConnectionState.WAIT_SELECT,
            HSMSConnectionState.CONNECTED,
            HSMSConnectionState.NOT_SELECTED,
        ):
            self.handler.update_state(HSMSConnectionState.SELECTED)
            return True
        return False

    def initiate_select(self) -> Optional[HSMSMessage]:
        if self.handler.state in (
            HSMSConnectionState.CONNECTED,
            HSMSConnectionState.NOT_SELECTED,
        ):
            self.handler.update_state(HSMSConnectionState.WAIT_SELECT)
            return self.handler.create_select_request()
        return None

    def handle_deselect_request(self, system_bytes: bytes) -> HSMSMessage:
        self.handler.update_state(HSMSConnectionState.NOT_SELECTED)
        return self.handler.create_deselect_response(system_bytes=system_bytes)

    def handle_deselect_response(self) -> None:
        if self.handler.state in (
            HSMSConnectionState.SELECTED,
            HSMSConnectionState.COMMUNICATION_ACTIVE,
        ):
            self.handler.update_state(HSMSConnectionState.NOT_SELECTED)

    def handle_separate_request(self) -> None:
        self.handler.update_state(HSMSConnectionState.NOT_CONNECTED)

    def handle_linktest(self, system_bytes: bytes) -> HSMSMessage:
        return self.handler.create_linktest_response(system_bytes=system_bytes)
