"""主动/被动双端集成测试（本机回环）。"""

import asyncio
import socket

from secs_driver.config import (
    ConnectionConfig,
    DriverConfig,
    HSMSConfig,
    LoggingConfig,
)
from secs_driver.secs_driver import SECSEventHandler, SECSDriver
from secs_driver.secs_message import SECSItem
from secs_driver.secs_types import SECSType

SILENT_LOGGING = LoggingConfig(level="CRITICAL", console=False)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


async def _wait_until(predicate, timeout: float = 3.0, interval: float = 0.02) -> bool:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return predicate()


class Recorder(SECSEventHandler):
    """记录事件，供断言使用。"""

    def __init__(self) -> None:
        self.connected = 0
        self.disconnected = 0
        self.selected = 0
        self.deselected = 0
        self.separated = 0
        self.messages = []
        self.timeouts = []
        self.errors = []

    async def on_connected(self, session_id: int) -> None:
        self.connected += 1

    async def on_disconnected(self, session_id: int, reason: str) -> None:
        self.disconnected += 1

    async def on_selected(self) -> None:
        self.selected += 1

    async def on_deselected(self) -> None:
        self.deselected += 1

    async def on_separated(self) -> None:
        self.separated += 1

    async def on_message_received(self, message) -> None:
        self.messages.append(message)

    async def on_timeout(self, message) -> None:
        self.timeouts.append(message)

    async def on_error(self, error) -> None:
        self.errors.append(error)


class PassiveEndpoint(Recorder):
    """被动端：对 S1F1 自动回复 S1F2；其余不回复（用于超时场景）。"""

    def __init__(self) -> None:
        super().__init__()
        self.driver = None

    async def on_message_received(self, message) -> None:
        self.messages.append(message)
        if message.w_bit and message.stream == 1:
            await self.driver.send_reply(message, [SECSItem(SECSType.ASCII, "OK")])


def _make_drivers(port: int, passive_handler: PassiveEndpoint):
    passive = SECSDriver(
        DriverConfig(
            name="passive",
            connection=ConnectionConfig(
                mode="passive", host="127.0.0.1", port=port, timeout=2.0
            ),
            hsms=HSMSConfig(
                t3_timeout=1.0, t5_timeout=10.0, t6_timeout=1.0, t7_timeout=3.0
            ),
            logging=SILENT_LOGGING,
        )
    )
    active = SECSDriver(
        DriverConfig(
            name="active",
            connection=ConnectionConfig(
                mode="active",
                host="127.0.0.1",
                port=port,
                timeout=2.0,
                retry_interval=0.2,
                max_retry=20,
            ),
            hsms=HSMSConfig(
                t3_timeout=1.0, t5_timeout=10.0, t6_timeout=1.0, t7_timeout=3.0
            ),
            logging=SILENT_LOGGING,
        )
    )
    passive_handler.driver = passive
    return passive, active


async def test_active_passive_roundtrip_and_timeout() -> None:
    port = _free_port()
    passive_handler = PassiveEndpoint()
    active_recorder = Recorder()
    passive, active = _make_drivers(port, passive_handler)
    passive.set_event_handler(passive_handler)
    active.set_event_handler(active_recorder)

    try:
        assert await passive.connect() is True
        assert await active.connect() is True

        assert await _wait_until(
            lambda: active.is_selected and passive.is_selected
        ), "Select 握手未完成"

        # S1F1 / S1F2：被动端自动回复
        reply = await active.send_message(
            1, 1, [SECSItem(SECSType.ASCII, "PING")], wait_reply=True
        )
        assert reply is not None
        assert reply.sf == "S1F2"
        assert reply.items[0].value == "OK"
        assert passive_handler.messages[-1].sf == "S1F1"

        # S2F1：被动端不回复，主动端应收到 T3 超时（timeout=None 走默认 T3）
        loop = asyncio.get_running_loop()
        started = loop.time()
        no_reply = await active.send_message(2, 1, wait_reply=True)
        elapsed = loop.time() - started

        assert no_reply is None
        assert 0.5 <= elapsed < 3.0
        # 超时事件由后台任务派发，轮询等待
        assert await _wait_until(
            lambda: len(active_recorder.timeouts) == 1
        ), "未收到 on_timeout 事件"

    finally:
        await active.disconnect()
        await passive.disconnect()


async def test_active_connect_fails_fast_without_server() -> None:
    port = _free_port()  # 保证没有服务端监听
    active = SECSDriver(
        DriverConfig(
            name="active",
            connection=ConnectionConfig(
                mode="active", host="127.0.0.1", port=port, timeout=1.0
            ),
            logging=SILENT_LOGGING,
        )
    )

    loop = asyncio.get_running_loop()
    started = loop.time()
    assert await active.connect() is False
    assert loop.time() - started < 2.0

    await active.disconnect()


async def test_deselect_flow() -> None:
    port = _free_port()
    passive_handler = PassiveEndpoint()
    active_recorder = Recorder()
    passive, active = _make_drivers(port, passive_handler)
    passive.set_event_handler(passive_handler)
    active.set_event_handler(active_recorder)

    try:
        assert await passive.connect() is True
        assert await active.connect() is True
        assert await _wait_until(
            lambda: active.is_selected and passive.is_selected
        ), "Select 握手未完成"

        assert await active.deselect() is True
        assert await _wait_until(
            lambda: not active.is_selected and not passive.is_selected
        ), "Deselect 未生效"
        assert await _wait_until(
            lambda: active_recorder.deselected >= 1
        ), "未收到 on_deselected 事件"

        # 未 Selected 时发送应立即失败返回 None，而不是等待 T3
        loop = asyncio.get_running_loop()
        started = loop.time()
        result = await active.send_message(1, 1, wait_reply=True)
        assert result is None
        assert loop.time() - started < 0.5

    finally:
        await active.disconnect()
        await passive.disconnect()


async def test_separate_closes_then_auto_reconnect() -> None:
    port = _free_port()
    passive_handler = PassiveEndpoint()
    active_recorder = Recorder()
    passive, active = _make_drivers(port, passive_handler)
    passive.set_event_handler(passive_handler)
    active.set_event_handler(active_recorder)

    try:
        assert await passive.connect() is True
        assert await active.connect() is True
        assert await _wait_until(
            lambda: active.is_selected and passive.is_selected
        ), "Select 握手未完成"

        assert await active.separate() is True

        assert await _wait_until(
            lambda: passive_handler.separated >= 1, timeout=3.0
        ), "被动端未收到 Separate.req"

        # 主动端应按 auto_reconnect 重新连接并重新 Select
        assert await _wait_until(
            lambda: active.is_selected and passive.is_selected, timeout=5.0
        ), "断开后未能自动重连并重新 Select"
        assert active_recorder.connected >= 2

    finally:
        await active.disconnect()
        await passive.disconnect()
