"""
会话管理器

管理 HSMS 会话的生命周期、事务和超时。
"""

import asyncio
import logging
import time
from typing import Dict, Optional, Callable
from dataclasses import dataclass, field

from .config import HSMSConfig
from .hsms_protocol import (
    HSMSProtocolHandler,
    HSMSMessage,
    HSMSMessageType,
    HSMSRejectReason,
    HSMSConnectionState,
)
from .secs_message import SECSMessage
from .task_utils import spawn


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
    # 完成通知：收到回复时传回复消息，超时/断开时传 None
    callback: Optional[Callable[[Optional[SECSMessage]], None]] = None


class SessionManager:
    """
    HSMS 会话管理器

    管理 HSMS 会话状态、事务和超时处理。
    """

    def __init__(self, config: HSMSConfig = None, device_id: Optional[int] = None):
        """
        初始化会话管理器

        Args:
            config: HSMS 配置
            device_id: 期望的 HSMS session/device id；None 表示不校验
        """
        self.config = config or HSMSConfig()
        self.device_id = device_id
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
        callback = self._callbacks.get(event)
        if callback is None:
            return
        try:
            result = callback(*args, **kwargs)
            if asyncio.iscoroutine(result):
                spawn(result, name=f"SessionManager.{event}")
        except Exception as e:
            logger.error(f"Callback error for {event}: {e}")

    async def _trigger_callback_wait(self, event: str, *args, **kwargs) -> bool:
        """触发发送类回调并等待完成；False 表示实际发送失败。"""
        callback = self._callbacks.get(event)
        if callback is None:
            return True
        try:
            result = callback(*args, **kwargs)
            if asyncio.iscoroutine(result):
                result = await result
            return result is not False
        except Exception as e:
            logger.error(f"Callback error for {event}: {e}")
            return False

    async def initialize(self, state_machine) -> None:
        """初始化会话管理器"""
        self._state_machine = state_machine

        # 设置协议回调
        self.protocol.set_callback("state_changed", self._on_state_changed)
        self.protocol.set_callback("unsupported_ptype", self._on_unsupported_protocol_type)
        self.protocol.set_callback("unsupported_stype", self._on_unsupported_protocol_type)

    def _on_state_changed(self, old_state: HSMSConnectionState, new_state: HSMSConnectionState) -> None:
        """状态变化回调"""
        logger.info(f"Session state changed: {old_state} -> {new_state}")
        self._trigger_callback("state_changed", old_state, new_state)

    def _on_unsupported_protocol_type(
        self,
        value: int,
        system_bytes: bytes,
        reason: HSMSRejectReason,
    ) -> None:
        """收到不支持的 PType/SType 时按 HSMS 规范回 Reject.req。"""
        logger.warning(f"Unsupported protocol type value {value}; sending Reject.req")
        self._send_reject(reason, system_bytes)

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
                timeout=timeout if timeout is not None else self.config.t3_timeout,
                future=asyncio.get_running_loop().create_future(),
                callback=callback,
            )

            self._transactions[tid] = transaction

            sent = await self._trigger_callback_wait("send_with_reply", data, secs_message, tid)
            if not sent:
                await self.cancel_transaction(tid)
                return None

            # 实际写入成功后再启动 T3，避免排队/发送耗时侵占事务等待时间。
            self._start_timeout_timer(tid, transaction.timeout)
        else:
            # 无需回复，直接发送
            sent = await self._trigger_callback_wait("send_no_reply", data, secs_message, tid)
            if not sent:
                return None

        # 返回消息数据供发送
        return data

    def _start_timeout_timer(self, tid: int, timeout: float) -> None:
        """启动超时计时器"""
        if tid in self._timeout_tasks:
            self._timeout_tasks[tid].cancel()

        loop = asyncio.get_running_loop()
        task = loop.create_task(self._handle_timeout(tid, timeout))
        self._timeout_tasks[tid] = task

    async def _handle_timeout(self, tid: int, timeout: float) -> None:
        """处理事务超时：通知等待方并清理。"""
        await asyncio.sleep(timeout)

        transaction = self._transactions.get(tid)
        if transaction is None or transaction.completed:
            return

        transaction.completed = True
        logger.warning(f"Transaction {tid} timed out")
        self._timeout_tasks.pop(tid, None)

        # 完成 future 与回调，保证等待方（MessageHandler.send）能及时返回
        self._complete_future(transaction, None)
        self._invoke_callback(transaction, None)
        self._trigger_callback("timeout", tid)

        await self._cleanup_transaction(tid)

    def _complete_future(
        self, transaction: Transaction, result: Optional[SECSMessage]
    ) -> None:
        """完成事务 future（如果有等待者）。"""
        if transaction.future is not None and not transaction.future.done():
            transaction.future.set_result(result)

    def _invoke_callback(
        self, transaction: Transaction, result: Optional[SECSMessage]
    ) -> None:
        """调用事务回调；result 为 None 表示超时/断开。"""
        if transaction.callback is None:
            return
        try:
            transaction.callback(result)
        except Exception as e:
            logger.error(f"Transaction callback error: {e}")

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

            secs_message = hsms_message.secs_message
            if secs_message is None:
                continue

            if not self._is_expected_device(hsms_message, secs_message):
                logger.warning(
                    "Data message device/session id mismatch: "
                    f"expected={self.device_id}, got session={hsms_message.session_id}, "
                    f"device={secs_message.device_id}"
                )
                continue

            # 未 Selected 时收到数据消息：按 HSMS 规范回 Reject.req
            if not self.is_selected:
                self._reject_not_selected(hsms_message)
                continue

            tid = self._bytes_to_tid(secs_message.system_bytes)

            if tid in self._transactions:
                await self._complete_transaction(tid, secs_message)
            elif secs_message.is_reply:
                self._reject_transaction_not_open(hsms_message)
            else:
                self._trigger_callback("message_received", secs_message)

            last_message = secs_message

        return last_message

    def _reject_not_selected(self, hsms_message: HSMSMessage) -> None:
        """未 Selected 时收到数据消息，回 Reject.req (reason=4)。"""
        logger.warning(
            "Data message received while not selected; sending Reject.req "
            f"(system_bytes={hsms_message.system_bytes.hex()})"
        )
        self._send_reject(HSMSRejectReason.ENTITY_NOT_SELECTED, hsms_message.system_bytes)

    def _reject_transaction_not_open(self, hsms_message: HSMSMessage) -> None:
        """收到无法匹配的 Secondary，回 Reject.req (reason=3)。"""
        logger.warning(
            "Secondary message has no open transaction; sending Reject.req "
            f"(system_bytes={hsms_message.system_bytes.hex()})"
        )
        self._send_reject(HSMSRejectReason.TRANSACTION_NOT_OPEN, hsms_message.system_bytes)

    def _send_reject(self, reason: HSMSRejectReason, system_bytes: bytes) -> None:
        """发送 Reject.req。"""
        reject = self.protocol.create_reject_request(reason, system_bytes=system_bytes)
        data = self.protocol.encode_message(reject)
        self._trigger_callback("send_control_message", data)

    def _is_expected_device(
        self, hsms_message: HSMSMessage, secs_message: SECSMessage
    ) -> bool:
        """校验 incoming data message 的 session/device id。"""
        if self.device_id is None:
            return True
        expected = self.device_id & 0xFFFF
        return hsms_message.session_id == expected and secs_message.device_id == expected

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
            # 处理 Select 响应（校验 system bytes 与 SelectStatus）
            if self._state_machine:
                accepted = self._state_machine.handle_select_response(
                    system_bytes=hsms_message.system_bytes,
                    status=hsms_message.stream,
                )
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

        elif msg_type == HSMSMessageType.REJECT_REQUEST:
            # 对端拒绝：记录原因码（Byte2）
            logger.warning(f"Received Reject.req, reason={hsms_message.stream}")
            self._trigger_callback("reject_received", hsms_message)

    async def _complete_transaction(self, tid: int, reply: SECSMessage) -> None:
        """完成事务"""
        if tid not in self._transactions:
            return

        transaction = self._transactions[tid]
        if not self._is_valid_reply(transaction, reply):
            logger.warning(
                f"Unexpected reply for transaction {tid}: "
                f"request={transaction.request.sf if transaction.request else '<unknown>'}, "
                f"reply={reply.sf}"
            )
            return

        transaction.reply = reply
        transaction.completed = True

        # 取消超时计时器
        if tid in self._timeout_tasks:
            self._timeout_tasks[tid].cancel()
            del self._timeout_tasks[tid]

        # 完成 Future 并通知回调
        self._complete_future(transaction, reply)
        self._invoke_callback(transaction, reply)
        self._trigger_callback("reply_received", reply, tid)

        await self._cleanup_transaction(tid)

    def _is_valid_reply(self, transaction: Transaction, reply: SECSMessage) -> bool:
        """校验 Secondary 是否匹配原事务。"""
        request = transaction.request
        if request is None:
            return True
        if not reply.is_reply or reply.w_bit:
            return False
        return reply.stream == request.stream and reply.function == request.function + 1

    async def _cleanup_transaction(self, tid: int) -> None:
        """清理事务"""
        if tid in self._transactions:
            del self._transactions[tid]

        if tid in self._timeout_tasks:
            del self._timeout_tasks[tid]

    async def cancel_transaction(self, tid: int) -> bool:
        """取消事务（等待方会收到 None 通知）。"""
        transaction = self._transactions.get(tid)
        if transaction is None:
            return False

        transaction.completed = True
        task = self._timeout_tasks.pop(tid, None)
        if task is not None:
            task.cancel()

        self._complete_future(transaction, None)
        self._invoke_callback(transaction, None)
        await self._cleanup_transaction(tid)
        return True

    async def cancel_all_transactions(self, reason: str = "connection lost") -> None:
        """取消全部未完成事务（断开连接时调用，避免等待方悬挂）。"""
        if not self._transactions:
            return

        logger.info(f"Cancelling {len(self._transactions)} pending transaction(s): {reason}")
        for tid in list(self._transactions.keys()):
            await self.cancel_transaction(tid)

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
