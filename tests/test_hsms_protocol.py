"""HSMS 分帧、控制消息与状态机测试。"""

import struct

import pytest

from secs_driver.config import HSMSConfig
from secs_driver.hsms_protocol import (
    HSMSConnectionState,
    HSMSConnectionStateMachine,
    HSMSMessage,
    HSMSMessageType,
    HSMSProtocolHandler,
    HSMSRejectReason,
)
from secs_driver.secs_message import SECSItem, SECSMessage


def _data_frame() -> bytes:
    message = SECSMessage(stream=1, function=1, w_bit=True, device_id=1)
    message.add_item(SECSItem.ascii("PING"))
    hsms = HSMSMessage(
        message_type=HSMSMessageType.DATA_MESSAGE, secs_message=message
    )
    return HSMSProtocolHandler().encode_message(hsms)


def test_feed_reassembles_split_frames() -> None:
    handler = HSMSProtocolHandler()
    raw = _data_frame()
    mid = len(raw) // 2

    assert handler.feed(raw[:mid]) == []

    messages = handler.feed(raw[mid:])
    assert len(messages) == 1
    assert messages[0].message_type == HSMSMessageType.DATA_MESSAGE
    assert messages[0].secs_message.sf == "S1F1"
    assert messages[0].secs_message.items[0].value == "PING"


def test_feed_multiple_frames_in_one_chunk() -> None:
    handler = HSMSProtocolHandler()
    messages = handler.feed(_data_frame() * 3)
    assert len(messages) == 3
    assert all(m.secs_message.sf == "S1F1" for m in messages)


def test_feed_rejects_oversized_frame_length() -> None:
    errors = []
    handler = HSMSProtocolHandler(HSMSConfig(max_frame_size=1024))
    handler.set_callback("frame_error", errors.append)

    out = handler.feed(struct.pack(">I", 4096) + b"\x00" * 10)

    assert out == []
    assert len(handler._buffer) == 0
    assert len(errors) == 1
    assert "exceeds" in str(errors[0])


def test_feed_rejects_short_frame_length() -> None:
    errors = []
    handler = HSMSProtocolHandler()
    handler.set_callback("frame_error", errors.append)

    assert handler.feed(struct.pack(">I", 1) + b"\x00") == []

    assert len(handler._buffer) == 0
    assert len(errors) == 1
    assert "shorter than header" in str(errors[0])


def test_partial_frame_t8_timeout_clears_buffer() -> None:
    errors = []
    handler = HSMSProtocolHandler(HSMSConfig(t8_timeout=0.1))
    handler.set_callback("frame_error", errors.append)

    assert handler.feed(struct.pack(">I", 20) + b"\x00") == []
    assert handler._partial_started_at is not None

    assert handler.check_t8_timeout(handler._partial_started_at + 0.2) is True
    assert len(handler._buffer) == 0
    assert len(errors) == 1
    assert "timed out" in str(errors[0])


@pytest.mark.parametrize(
    "message_type",
    [
        HSMSMessageType.SELECT_REQUEST,
        HSMSMessageType.SELECT_RESPONSE,
        HSMSMessageType.DESELECT_REQUEST,
        HSMSMessageType.DESELECT_RESPONSE,
        HSMSMessageType.LINKTEST_REQUEST,
        HSMSMessageType.LINKTEST_RESPONSE,
        HSMSMessageType.SEPARATE_REQUEST,
    ],
)
def test_control_message_roundtrip(message_type: HSMSMessageType) -> None:
    handler = HSMSProtocolHandler()
    raw = handler.encode_message(HSMSMessage(message_type=message_type))

    decoded = handler.feed(raw)
    assert len(decoded) == 1
    assert decoded[0].message_type == message_type
    assert decoded[0].is_control_message


def test_select_response_carries_status_and_system_bytes() -> None:
    handler = HSMSProtocolHandler()
    request = handler.create_select_request()
    response = handler.create_select_response(
        system_bytes=request.system_bytes, status=1
    )

    decoded = handler.feed(handler.encode_message(response))[0]

    assert decoded.message_type == HSMSMessageType.SELECT_RESPONSE
    assert decoded.stream == 1  # SelectStatus
    assert decoded.system_bytes == request.system_bytes


def test_reject_request_carries_reason_and_system_bytes() -> None:
    handler = HSMSProtocolHandler()
    request = handler.create_select_request()
    reject = handler.create_reject_request(
        HSMSRejectReason.ENTITY_NOT_SELECTED, system_bytes=request.system_bytes
    )

    decoded = handler.feed(handler.encode_message(reject))[0]

    assert decoded.message_type == HSMSMessageType.REJECT_REQUEST
    assert decoded.stream == int(HSMSRejectReason.ENTITY_NOT_SELECTED)
    assert decoded.system_bytes == request.system_bytes


def test_select_response_must_match_system_bytes() -> None:
    handler = HSMSProtocolHandler()
    machine = HSMSConnectionStateMachine(handler)
    machine.handle_tcp_connected()

    request = machine.initiate_select()
    assert request is not None
    assert handler.state == HSMSConnectionState.WAIT_SELECT

    # 不匹配的 system bytes 必须被忽略
    assert machine.handle_select_response(b"\xde\xad\xbe\xef", 0) is False
    assert handler.state == HSMSConnectionState.WAIT_SELECT

    assert machine.handle_select_response(request.system_bytes, 0) is True
    assert handler.state == HSMSConnectionState.SELECTED


def test_select_response_rejects_error_status() -> None:
    handler = HSMSProtocolHandler()
    machine = HSMSConnectionStateMachine(handler)
    machine.handle_tcp_connected()

    request = machine.initiate_select()
    assert machine.handle_select_response(request.system_bytes, 2) is False
    assert handler.state == HSMSConnectionState.WAIT_SELECT


def test_tcp_disconnect_resets_state_machine() -> None:
    handler = HSMSProtocolHandler()
    machine = HSMSConnectionStateMachine(handler)
    machine.handle_tcp_connected()
    request = machine.initiate_select()
    assert machine.handle_select_response(request.system_bytes, 0) is True

    machine.handle_tcp_disconnected()
    assert handler.state == HSMSConnectionState.NOT_CONNECTED
