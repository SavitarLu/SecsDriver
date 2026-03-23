"""
消息处理器

处理 SECS 消息的发送、接收、队列管理和流量控制。
"""

import asyncio
import logging
from typing import Dict, Optional, Callable, Any, List
from dataclasses import dataclass, field
from collections import deque
import time

from .secs_message import SECSMessage, SECSItem


logger = logging.getLogger(__name__)


@dataclass
class MessageQueueConfig:
    """消息队列配置"""

    max_queue_size: int = 1000  # 最大队列大小
    max_retry: int = 3  # 最大重试次数
    retry_delay: float = 1.0  # 重试延迟（秒）


@dataclass
class QueuedMessage:
    """队列中的消息"""

    secs_message: SECSMessage
    priority: int = 0  # 优先级，数值越小优先级越高
    created_at: float = field(default_factory=time.time)
    retry_count: int = 0
    callback: Optional[Callable[[SECSMessage], None]] = None
    future: Optional[asyncio.Future] = None


class MessageQueue:
    """
    SECS 消息队列

    管理和调度待发送的消息，支持优先级和流量控制。
    """

    def __init__(self, config: MessageQueueConfig = None):
        """
        初始化消息队列

        Args:
            config: 队列配置
        """
        self.config = config or MessageQueueConfig()
        self._queue: deque = deque()
        self._pending: Dict[int, QueuedMessage] = {}  # tid -> message
        self._lock = asyncio.Lock()
        self._not_empty = asyncio.Condition(self._lock)

    async def enqueue(
        self,
        message: SECSMessage,
        priority: int = 0,
        callback: Optional[Callable[[SECSMessage], None]] = None,
    ) -> QueuedMessage:
        """
        将消息加入队列

        Args:
            message: SECS 消息
            priority: 优先级
            callback: 回调函数

        Returns:
            队列中的消息对象
        """
        async with self._lock:
            # 检查队列大小
            if len(self._queue) >= self.config.max_queue_size:
                raise QueueFullError("Message queue is full")

            queued = QueuedMessage(
                secs_message=message,
                priority=priority,
                callback=callback,
            )

            # 按优先级插入队列
            inserted = False
            for i, qm in enumerate(self._queue):
                if priority < qm.priority:
                    self._queue.insert(i, queued)
                    inserted = True
                    break

            if not inserted:
                self._queue.append(queued)

            # 通知等待的消费者
            self._not_empty.notify()

            return queued

    async def dequeue(self, timeout: Optional[float] = None) -> Optional[QueuedMessage]:
        """
        从队列取出消息

        Args:
            timeout: 等待超时（秒）

        Returns:
            队列消息对象，如果队列为空则返回 None
        """
        async with self._not_empty:
            while len(self._queue) == 0:
                try:
                    await asyncio.wait_for(self._not_empty.wait(), timeout=timeout)
                except asyncio.TimeoutError:
                    return None

            return self._queue.popleft()

    async def peek(self) -> Optional[QueuedMessage]:
        """
        查看下一条消息（不取出）

        Returns:
            队列消息对象，如果队列为空则返回 None
        """
        async with self._lock:
            if len(self._queue) > 0:
                return self._queue[0]
            return None

    def mark_pending(self, tid: int, message: QueuedMessage) -> None:
        """标记消息为待回复状态"""
        self._pending[tid] = message

    def mark_completed(self, tid: int) -> None:
        """标记消息为已完成"""
        if tid in self._pending:
            del self._pending[tid]

    async def requeue(self, message: QueuedMessage) -> bool:
        """
        重新加入队列（用于重试）

        Returns:
            是否成功重试
        """
        if message.retry_count >= self.config.max_retry:
            logger.warning(f"Message exceeded max retry count: {message.retry_count}")
            return False

        message.retry_count += 1
        await self.enqueue(message.secs_message, message.priority, message.callback)
        return True

    @property
    def size(self) -> int:
        """获取队列大小"""
        return len(self._queue)

    @property
    def pending_count(self) -> int:
        """获取待回复消息数量"""
        return len(self._pending)

    def is_empty(self) -> bool:
        """检查队列是否为空"""
        return len(self._queue) == 0

    def is_full(self) -> int:
        """检查队列是否已满"""
        return len(self._queue) >= self.config.max_queue_size


class QueueFullError(Exception):
    """队列已满异常"""
    pass


class MessageHandler:
    """
    SECS 消息处理器

    协调消息队列、会话管理和发送接收操作。
    """

    def __init__(self, queue_config: MessageQueueConfig = None):
        """
        初始化消息处理器

        Args:
            queue_config: 队列配置
        """
        self._queue = MessageQueue(queue_config)
        self._send_task: Optional[asyncio.Task] = None
        self._running = False

        # 回调
        self._callbacks: Dict[str, Callable] = {}

        # 会话管理器引用（稍后设置）
        self._session_manager = None

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

    def set_session_manager(self, session_manager) -> None:
        """设置会话管理器"""
        self._session_manager = session_manager

    async def start(self) -> None:
        """启动消息处理器"""
        if self._running:
            return

        self._running = True
        self._send_task = asyncio.create_task(self._send_loop())
        logger.info("Message handler started")

    async def stop(self) -> None:
        """停止消息处理器"""
        self._running = False

        if self._send_task:
            self._send_task.cancel()
            try:
                await self._send_task
            except asyncio.CancelledError:
                pass
            self._send_task = None

        logger.info("Message handler stopped")

    async def send(
        self,
        message: SECSMessage,
        wait_reply: bool = False,
        timeout: Optional[float] = None,
        priority: int = 0,
        callback: Optional[Callable[[SECSMessage], None]] = None,
    ) -> Optional[SECSMessage]:
        """
        发送 SECS 消息

        Args:
            message: SECS 消息
            wait_reply: 是否等待回复
            timeout: 超时时间（秒）
            priority: 优先级
            callback: 回调函数

        Returns:
            回复消息（如果 wait_reply=True）
        """
        if not self._session_manager:
            raise RuntimeError("Session manager not set")

        # 创建异步 Future
        future = asyncio.get_running_loop().create_future()

        def on_reply(reply: SECSMessage) -> None:
            if not future.done():
                future.set_result(reply)

        # 发送消息
        data = await self._session_manager.send_message(
            message, wait_reply=wait_reply, timeout=timeout, callback=on_reply
        )

        if data is None:
            return None

        # 触发发送回调
        self._trigger_callback("message_sent", message)

        # 等待回复
        if wait_reply or message.w_bit:
            try:
                reply = await asyncio.wait_for(future, timeout=timeout)
                self._trigger_callback("reply_received", reply)
                return reply
            except asyncio.TimeoutError:
                logger.warning(f"Message timeout: {message.sf}")
                self._trigger_callback("timeout", message)
                return None

        return None

    async def send_async(
        self,
        message: SECSMessage,
        priority: int = 0,
        callback: Optional[Callable[[SECSMessage], None]] = None,
    ) -> None:
        """
        异步发送消息（通过队列）

        Args:
            message: SECS 消息
            priority: 优先级
            callback: 回调函数
        """
        await self._queue.enqueue(message, priority, callback)

    async def _send_loop(self) -> None:
        """消息发送循环"""
        while self._running:
            try:
                # 从队列获取消息
                queued = await self._queue.dequeue(timeout=1.0)

                if queued is None:
                    continue

                # 发送消息
                message = queued.secs_message
                wait_reply = queued.callback is not None or message.w_bit

                try:
                    reply = await self.send(
                        message,
                        wait_reply=wait_reply,
                        callback=queued.callback,
                    )

                    # 如果有回复，触发回调
                    if reply and queued.callback:
                        queued.callback(reply)

                except Exception as e:
                    logger.error(f"Error sending message: {e}")

                    # 尝试重试
                    if await self._queue.requeue(queued):
                        logger.info(f"Message requeued for retry: {queued.retry_count}")
                    else:
                        # 超过最大重试次数
                        self._trigger_callback("send_failed", message, e)

            except asyncio.CancelledError:
                break

            except Exception as e:
                logger.error(f"Send loop error: {e}")

    async def handle_received_message(self, message: SECSMessage) -> None:
        """
        处理接收到的消息

        Args:
            message: SECS 消息
        """
        self._trigger_callback("message_received", message)

    def get_queue_size(self) -> int:
        """获取队列大小"""
        return self._queue.size

    def get_pending_count(self) -> int:
        """获取待回复消息数量"""
        return self._queue.pending_count
