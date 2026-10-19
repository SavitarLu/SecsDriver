"""会话层事务测试（含事务超时 P0 回归）。"""

import asyncio

from secs_driver.config import HSMSConfig
from secs_driver.hsms_protocol import (
    HSMSConnectionState,
    HSMSMessage,
    HSMSMessageType,
    HSMSProtocolHandler,
    HSMSRejectReason,
)
from secs_driver.secs_message import SECSItem, SECSMessage
from secs_driver.session import SessionManager


def _selected_session(t3: float = 0.2) -> SessionManager:
    manager = SessionManager(HSMSConfig(t3_timeout=t3))
    manager.protocol.update_state(HSMSConnectionState.SELECTED)
    return manager


def _data_wire(manager: SessionManager, message: SECSMessage) -> bytes:
    return manager.protocol.encode_message(
        HSMSMessage(message_type=HSMSMessageType.DATA_MESSAGE, secs_message=message)
    )


async def test_timeout_completes_callback_with_none() -> None:
    """P0 回归：T3 超时后事务必须完成，等待方不能悬挂。"""
    manager = _selected_session(t3=0.2)
    results = []

    data = await manager.send_message(
        SECSMessage(1, 1, w_bit=True), wait_reply=True, callback=results.append
    )

    assert data is not None
    await asyncio.sleep(0.5)

    assert results == [None]
    assert manager._transactions == {}
    assert manager._timeout_tasks == {}


async def test_reply_completes_transaction_and_preserves_system_bytes() -> None:
    manager = _selected_session(t3=5.0)
    results = []
    request = SECSMessage(1, 1, w_bit=True)

    await manager.send_message(request, wait_reply=True, callback=results.append)
    assert len(manager._transactions) == 1

    reply = request.create_reply([SECSItem.ascii("OK")])
    received = await manager.receive_message(_data_wire(manager, reply))

    assert received is not None
    assert len(results) == 1
    got = results[0]
    assert got is not None
    assert got.sf == "S1F2"
    assert got.system_bytes == request.system_bytes
    assert got.items[0].value == "OK"
    assert manager._transactions == {}
    assert manager._timeout_tasks == {}


async def test_cancel_all_transactions_unblocks_waiters() -> None:
    manager = _selected_session(t3=30.0)
    results = []

    await manager.send_message(
        SECSMessage(1, 1, w_bit=True), wait_reply=True, callback=results.append
    )
    assert len(manager._transactions) == 1

    await manager.cancel_all_transactions("test disconnect")

    assert results == [None]
    assert manager._transactions == {}
    assert manager._timeout_tasks == {}


async def test_message_while_not_selected_is_rejected() -> None:
    manager = SessionManager(HSMSConfig())
    manager.protocol.update_state(HSMSConnectionState.NOT_SELECTED)
    sent = []
    manager.set_callback("send_control_message", sent.append)

    request = SECSMessage(1, 1, w_bit=True)
    await manager.receive_message(_data_wire(manager, request))

    assert len(sent) == 1
    frames = HSMSProtocolHandler().feed(sent[0])
    assert len(frames) == 1
    assert frames[0].message_type == HSMSMessageType.REJECT_REQUEST
    assert frames[0].stream == int(HSMSRejectReason.ENTITY_NOT_SELECTED)
    assert frames[0].system_bytes == request.system_bytes


async def test_selected_data_message_delivered_to_application() -> None:
    manager = _selected_session()
    received = []
    manager.set_callback("message_received", received.append)

    request = SECSMessage(5, 1, w_bit=True)
    await manager.receive_message(_data_wire(manager, request))

    assert len(received) == 1
    assert received[0].sf == "S5F1"
    assert manager._transactions == {}


async def test_send_message_while_not_selected_returns_none() -> None:
    manager = SessionManager(HSMSConfig())
    manager.protocol.update_state(HSMSConnectionState.NOT_SELECTED)

    data = await manager.send_message(SECSMessage(1, 1, w_bit=True), wait_reply=True)
    assert data is None
