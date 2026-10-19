"""消息处理器与队列测试（含 timeout=None 挂起 P0 回归）。"""

import asyncio

import pytest

from secs_driver.config import MessageQueueConfig
from secs_driver.message_handler import MessageHandler, MessageQueue, QueueFullError
from secs_driver.secs_message import SECSMessage


class FakeSessionManager:
    """模拟会话层：可配置回复、超时或拒绝发送。"""

    def __init__(
        self,
        t3: float = 0.2,
        reply: SECSMessage = None,
        drop: bool = False,
        selected: bool = True,
    ):
        self.t3 = t3
        self.reply = reply
        self.drop = drop
        self.selected = selected
        self.sent = []

    async def send_message(
        self, message, wait_reply=False, timeout=None, callback=None
    ):
        if not self.selected:
            return None

        self.sent.append(message)
        data = b"wire:" + message.system_bytes

        if callback is not None and (wait_reply or message.w_bit):
            if self.reply is not None:
                asyncio.get_running_loop().call_soon(callback, self.reply)
            elif self.drop:
                delay = timeout if timeout is not None else self.t3
                asyncio.get_running_loop().create_task(self._timeout_later(callback, delay))

        return data

    async def _timeout_later(self, callback, delay: float) -> None:
        await asyncio.sleep(delay)
        callback(None)


async def test_send_with_timeout_none_completes_on_session_timeout() -> None:
    """P0 回归：timeout=None 时必须由会话 T3 兜底，不能永久挂起。"""
    handler = MessageHandler(MessageQueueConfig())
    handler.set_session_manager(FakeSessionManager(t3=0.2, drop=True))

    loop = asyncio.get_running_loop()
    started = loop.time()
    reply = await handler.send(
        SECSMessage(1, 1, w_bit=True), wait_reply=True, timeout=None
    )
    elapsed = loop.time() - started

    assert reply is None
    assert elapsed < 1.0


async def test_send_returns_reply_object() -> None:
    reply_message = SECSMessage(1, 2, w_bit=False)
    handler = MessageHandler(MessageQueueConfig())
    handler.set_session_manager(FakeSessionManager(reply=reply_message))

    got = await handler.send(SECSMessage(1, 1, w_bit=True), wait_reply=True)
    assert got is reply_message


async def test_send_returns_none_when_not_selected() -> None:
    handler = MessageHandler(MessageQueueConfig())
    handler.set_session_manager(FakeSessionManager(selected=False))

    reply = await asyncio.wait_for(
        handler.send(SECSMessage(1, 1, w_bit=True), wait_reply=True), timeout=1.0
    )
    assert reply is None


async def test_try_send_reports_success_and_failure() -> None:
    handler = MessageHandler(MessageQueueConfig())
    handler.set_session_manager(FakeSessionManager())
    assert await handler.try_send(SECSMessage(1, 1)) is True

    handler.set_session_manager(FakeSessionManager(selected=False))
    assert await handler.try_send(SECSMessage(1, 1)) is False


async def test_queue_priority_order() -> None:
    queue = MessageQueue(MessageQueueConfig())
    await queue.enqueue(SECSMessage(3, 1), priority=5)
    await queue.enqueue(SECSMessage(1, 1), priority=1)
    await queue.enqueue(SECSMessage(2, 1), priority=3)

    order = []
    for _ in range(3):
        queued = await queue.dequeue(timeout=0.1)
        assert queued is not None
        order.append(queued.secs_message.stream)

    assert order == [1, 2, 3]


async def test_queue_size_limit() -> None:
    queue = MessageQueue(MessageQueueConfig(max_queue_size=2))
    await queue.enqueue(SECSMessage(1, 1))
    await queue.enqueue(SECSMessage(1, 1))

    with pytest.raises(QueueFullError):
        await queue.enqueue(SECSMessage(1, 1))


async def test_requeue_respects_max_retry() -> None:
    queue = MessageQueue(MessageQueueConfig(max_retry=1))
    await queue.enqueue(SECSMessage(1, 1))
    queued = await queue.dequeue(timeout=0.1)
    assert queued is not None

    assert await queue.requeue(queued) is True
    retried = await queue.dequeue(timeout=0.1)
    assert retried is queued
    assert retried.retry_count == 1
    assert await queue.requeue(retried) is False


async def test_send_loop_dispatches_without_head_of_line_blocking() -> None:
    """等待回复的消息不应阻塞后续消息的发送。"""
    handler = MessageHandler(MessageQueueConfig(max_in_flight=4))
    session = FakeSessionManager(t3=0.3, drop=True)
    handler.set_session_manager(session)

    await handler.start()
    try:
        # 第一条需要等待回复（会超时），第二条应立即发出
        await handler.send_async(SECSMessage(1, 1, w_bit=True))
        await asyncio.sleep(0)

        await handler.send_async(SECSMessage(2, 1))
        await asyncio.sleep(0.1)

        assert [message.sf for message in session.sent] == ["S1F1", "S2F1"]
    finally:
        await handler.stop()
