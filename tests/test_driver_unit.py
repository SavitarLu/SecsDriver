"""驱动内部逻辑单元测试（链路维护等）。"""

import asyncio

from secs_driver.config import DriverConfig, LoggingConfig
from secs_driver.hsms_protocol import HSMSMessage, HSMSMessageType, HSMSProtocolHandler
from secs_driver.secs_driver import SECSDriver

SILENT_LOGGING = LoggingConfig(level="CRITICAL", console=False)


def _driver() -> SECSDriver:
    return SECSDriver(DriverConfig(logging=SILENT_LOGGING))


async def test_linktest_response_clears_pending() -> None:
    driver = _driver()
    protocol = HSMSProtocolHandler()
    request = protocol.create_linktest_request()

    driver._linktest_pending = (0.0, request.system_bytes)
    response = HSMSMessage(
        message_type=HSMSMessageType.LINKTEST_RESPONSE,
        system_bytes_data=request.system_bytes,
    )

    await driver._on_linktest_response(response)
    assert driver._linktest_pending is None


async def test_linktest_response_with_mismatched_bytes_keeps_pending() -> None:
    driver = _driver()
    protocol = HSMSProtocolHandler()
    request = protocol.create_linktest_request()

    driver._linktest_pending = (0.0, request.system_bytes)
    response = HSMSMessage(
        message_type=HSMSMessageType.LINKTEST_RESPONSE,
        system_bytes_data=b"\xde\xad\xbe\xef",
    )

    await driver._on_linktest_response(response)
    assert driver._linktest_pending is not None


async def test_unexpected_linktest_response_is_ignored() -> None:
    driver = _driver()
    response = HSMSMessage(
        message_type=HSMSMessageType.LINKTEST_RESPONSE,
        system_bytes_data=b"\x00\x00\x00\x01",
    )

    await driver._on_linktest_response(response)
    assert driver._linktest_pending is None


async def test_select_timeout_task_cancelled_on_state_change() -> None:
    """离开 WAIT_SELECT 状态时应取消 T7 计时任务。"""
    driver = _driver()
    driver._start_select_timeout()
    assert driver._select_timeout_task is not None

    driver._cancel_select_timeout()
    assert driver._select_timeout_task is None
    await asyncio.sleep(0)  # 让取消生效
