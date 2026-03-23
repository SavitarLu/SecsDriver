"""
会话管理器

管理 HSMS 会话的生命周期、事务和超时。
"""

import asyncio
import logging
import time
from typing import Dict, Optional, Callable, Any
from dataclasses import dataclass, field

from .hsms_protocol import (
    HSMSProtocolHandler,
    HSMSMessage,
    HSMSMessageType,
    HSMSConfig,
    HSMSConnectionState,
)
from .secs_message import SECSMessage


logger = logging.getLogger(__name__)


@dataclass
class Transaction:
    """HSMS 事务"""

    tid: int  # 事务 ID
    request: Optional[SECSMessage] = None
    reply: Optional[SECSMessage] = None
    created_at: float = field(default_factory=time.time)
    completed: bool = False
    timeout: float = 45.0
    future: Optional[asyncio.Future] = None
    callback: Optional[Callable] = None


class SessionManager:
    """
    HSMS 会话管理器

    管理 HSMS 会话状态、事务和超时处理。
    """

    def __init__(self, config: HSMSConfig = None):
        """
        初始化会话管理器

        Args:
            config: HSMS 配置
        """
        self.config = config or HSMSConfig()
        self.protocol = HSMSProtocolHandler(self.config)
        self._state_machine = None  # 稍后初始化

        # 事务管理
        self._transactions: Dict[int, Transaction] = {}
        self._next_tid: int = 1
        self._tid_lock = asyncio.Lock()

        # 超时管理
        self._timeout_tasks: Dict[int, asyncio.Task] = {}

        # 回调
        self._callbacks: Dict[str, Callable] = {}

    def set_callback(self, event: str, callback: Callable) -> None:
        """设置事件回调"""
        self._callbacks[event] = callback

    def _trigger_callback(self, event: str, *args, **kwargs) -> None:
        """触发回调"""
        if event in self._callbacks:
            try:
                result = self._callbacks[event](*args, **kwargs)
                if asyncio.iscoroutine(result):
                    asyncio.create_task(result)
            except Exception as e:
                logger.error(f"Callback error for {event}: {e}")

    async def initialize(self, state_machine) -> None:
        """初始化会话管理器"""
        self._state_machine = state_machine

        # 设置协议回调
        self.protocol.set_callback("state_changed", self._on_state_changed)

    def _on_state_changed(self, old_state: HSMSConnectionState, new_state: HSMSConnectionState) -> None:
        """状态变化回调"""
        logger.info(f"Session state changed: {old_state} -> {new_state}")
        self._trigger_callback("state_changed", old_state, new_state)

    async def generate_tid(self) -> int:
        """生成唯一事务 ID"""
        async with self._tid_lock:
            tid = self._next_tid
            self._next_tid = (self._next_tid + 1) & 0xFFFFFFFF  # 32 位循环
            if self._next_tid == 0:
                self._next_tid = 1
            return tid

    async def send_message(
        self,
        secs_message: SECSMessage,
        wait_reply: bool = False,
        timeout: Optional[float] = None,
        callback: Optional[Callable[[SECSMessage], None]] = None,
    ) -> Optional[SECSMessage]:
        """
        发送 SECS 消息

        Args:
            secs_message: SECS 消息
            wait_reply: 是否等待回复
            timeout: 超时时间（秒）
            callback: 异步回调函数

        Returns:
            回复消息（如果 wait_reply=True）
        """
        if self.protocol.state not in (
            HSMSConnectionState.SELECTED,
            HSMSConnectionState.COMMUNICATION_ACTIVE,
        ):
            logger.warning(f"Cannot send message in state: {self.protocol.state}")
            return None

        expect_reply = wait_reply or secs_message.w_bit
        preserve_tid = (
            secs_message.is_reply
            and not expect_reply
            and self._bytes_to_tid(secs_message.system_bytes) != 0
        )

        # Secondary reply 必须沿用原 Primary 的 system bytes，
        # 否则外部监视器不会把它识别成同一笔事务。
        if preserve_tid:
            tid = self._bytes_to_tid(secs_message.system_bytes)
        else:
            tid = await self.generate_tid()
            secs_message.system_bytes = self._tid_to_bytes(tid)

        # 创建 HSMS 消息
        hsms_message = HSMSMessage(
            session_id=secs_message.device_id & 0xFFFF,
            message_type=HSMSMessageType.DATA_MESSAGE,
            secs_message=secs_message,
        )

        # 编码消息
        data = self.protocol.encode_message(hsms_message)

        # 如果需要等待回复，创建事务
        if expect_reply:
            transaction = Transaction(
                tid=tid,
                request=secs_message,
                timeout=timeout or self.config.t3_timeout,
                future=asyncio.get_running_loop().create_future(),
                callback=callback,
            )

            self._transactions[tid] = transaction

            # 启动超时计时器
            self._start_timeout_timer(tid, transaction.timeout)

            self._trigger_callback("send_with_reply", data, secs_message, tid)
        else:
            # 无需回复，直接发送
            self._trigger_callback("send_no_reply", data, secs_message, tid)

        # 返回消息数据供发送
        return data

    def _start_timeout_timer(self, tid: int, timeout: float) -> None:
        """启动超时计时器"""
        if tid in self._timeout_tasks:
            self._timeout_tasks[tid].cancel()

        loop = asyncio.get_event_loop()
        task = loop.create_task(self._handle_timeout(tid, timeout))
        self._timeout_tasks[tid] = task

    async def _handle_timeout(self, tid: int, timeout: float) -> None:
        """处理事务超时"""
        await asyncio.sleep(timeout)

        if tid in self._transactions:
            transaction = self._transactions[tid]
            if not transaction.completed:
                logger.warning(f"Transaction {tid} timed out")
                transaction.future.set_exception(TimeoutError(f"Transaction {tid} timed out"))
                self._trigger_callback("timeout", tid)
                await self._cleanup_transaction(tid)

    async def receive_message(self, data: bytes) -> Optional[SECSMessage]:
        """
        处理接收到的数据

        Args:
            data: 接收到的二进制数据

        Returns:
            SECS 消息（如果有）
        """
        last_message = None

        for hsms_message in self.protocol.feed(data):
            if hsms_message.is_control_message:
                await self._handle_control_message(hsms_message)
                continue

            if hsms_message.secs_message:
                secs_message = hsms_message.secs_message
                tid = self._bytes_to_tid(secs_message.system_bytes)

                if tid in self._transactions:
                    await self._complete_transaction(tid, secs_message)
                else:
                    self._trigger_callback("message_received", secs_message)

                last_message = secs_message

        return last_message

    async def _handle_control_message(self, hsms_message: HSMSMessage) -> None:
        """处理 HSMS 控制消息"""
        msg_type = hsms_message.message_type

        logger.debug(f"Received control message: {msg_type}")

        if msg_type == HSMSMessageType.SELECT_REQUEST:
            # 处理 Select 请求
            if self._state_machine:
                response = self._state_machine.handle_select_request(hsms_message.system_bytes)
                if response:
                    data = self.protocol.encode_message(response)
                    self._trigger_callback("send_control_message", data)

        elif msg_type == HSMSMessageType.SELECT_RESPONSE:
            # 处理 Select 响应
            if self._state_machine:
                accepted = self._state_machine.handle_select_response()
                if accepted:
                    self._trigger_callback("selected")

        elif msg_type == HSMSMessageType.DESELECT_REQUEST:
            # 处理 Deselect 请求
            if self._state_machine:
                response = self._state_machine.handle_deselect_request(hsms_message.system_bytes)
                if response:
                    data = self.protocol.encode_message(response)
                    self._trigger_callback("send_control_message", data)
                self._trigger_callback("deselected")

        elif msg_type == HSMSMessageType.DESELECT_RESPONSE:
            # 处理 Deselect 响应
            if self._state_machine:
                self._state_machine.handle_deselect_response()

        elif msg_type == HSMSMessageType.LINKTEST_REQUEST:
            # 处理 Linktest 请求
            if self._state_machine:
                response = self._state_machine.handle_linktest(hsms_message.system_bytes)
                if response:
                    data = self.protocol.encode_message(response)
                    self._trigger_callback("send_control_message", data)

        elif msg_type == HSMSMessageType.LINKTEST_RESPONSE:
            # 处理 Linktest 响应
            self._trigger_callback("linktest_response", hsms_message)

        elif msg_type == HSMSMessageType.SEPARATE_REQUEST:
            # 处理 Separate 请求
            if self._state_machine:
                self._state_machine.handle_separate_request()
            self._trigger_callback("separated")

    async def _complete_transaction(self, tid: int, reply: SECSMessage) -> None:
        """完成事务"""
        if tid not in self._transactions:
            return

        transaction = self._transactions[tid]
        transaction.reply = reply
        transaction.completed = True

        # 取消超时计时器
        if tid in self._timeout_tasks:
            self._timeout_tasks[tid].cancel()
            del self._timeout_tasks[tid]

        # 完成 Future 或调用回调
        if transaction.future and not transaction.future.done():
            transaction.future.set_result(reply)

        if transaction.callback:
            try:
                transaction.callback(reply)
            except Exception as e:
                logger.error(f"Transaction callback error: {e}")

        self._trigger_callback("reply_received", reply, tid)

        await self._cleanup_transaction(tid)

    async def _cleanup_transaction(self, tid: int) -> None:
        """清理事务"""
        if tid in self._transactions:
            del self._transactions[tid]

        if tid in self._timeout_tasks:
            del self._timeout_tasks[tid]

    async def cancel_transaction(self, tid: int) -> bool:
        """取消事务"""
        if tid in self._transactions:
            transaction = self._transactions[tid]
            if transaction.future and not transaction.future.done():
                transaction.future.cancel()
            await self._cleanup_transaction(tid)
            return True
        return False

    def get_pending_transactions(self) -> Dict[int, Transaction]:
        """获取待处理事务"""
        return {
            tid: txn for tid, txn in self._transactions.items() if not txn.completed
        }

    @staticmethod
    def _tid_to_bytes(tid: int) -> bytes:
        """事务 ID 转为字节"""
        return tid.to_bytes(4, byteorder="big")

    @staticmethod
    def _bytes_to_tid(system_bytes: bytes) -> int:
        """字节转事务 ID"""
        if len(system_bytes) >= 4:
            return int.from_bytes(system_bytes[:4], byteorder="big")
        return 0

    @property
    def state(self) -> HSMSConnectionState:
        """获取当前状态"""
        return self.protocol.state

    @property
    def is_connected(self) -> bool:
        """检查是否已连接"""
        return self.protocol.state != HSMSConnectionState.NOT_CONNECTED

    @property
    def is_selected(self) -> bool:
        """检查是否已选中"""
        return self.protocol.state in (
            HSMSConnectionState.SELECTED,
            HSMSConnectionState.COMMUNICATION_ACTIVE,
        )
