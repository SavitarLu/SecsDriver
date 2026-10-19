"""
消息处理器

处理 SECS 消息的发送、队列管理和流量控制。
"""

import asyncio
import inspect
import logging
import time
from typing import Callable, Dict, Optional, Set
from dataclasses import dataclass, field
from collections import deque

from .config import MessageQueueConfig
from .secs_message import SECSMessage
from .task_utils import spawn


logger = logging.getLogger(__name__)


@dataclass
class QueuedMessage:
    """队列中的消息"""

    secs_message: SECSMessage
    priority: int = 0  # 优先级，数值越小优先级越高
    created_at: float = field(default_factory=time.time)
    retry_count: int = 0
    # 完成通知：收到回复时传回复消息，超时/断开时传 None
    callback: Optional[Callable[[Optional[SECSMessage]], None]] = None


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
        self._lock = asyncio.Lock()
        self._not_empty = asyncio.Condition(self._lock)

    async def enqueue(
        self,
        message: SECSMessage,
        priority: int = 0,
        callback: Optional[Callable[[Optional[SECSMessage]], None]] = None,
    ) -> QueuedMessage:
        """
        将消息加入队列

        Args:
            message: SECS 消息
            priority: 优先级
            callback: 完成回调

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

    def is_empty(self) -> bool:
        """检查队列是否为空"""
        return len(self._queue) == 0

    def is_full(self) -> bool:
        """检查队列是否已满"""
        return len(self._queue) >= self.config.max_queue_size


class QueueFullError(Exception):
    """队列已满异常"""
    pass


class MessageHandler:
    """
    SECS 消息处理器

    协调消息队列、会话管理和发送接收操作。

    队列中的消息通过并发任务派发（受 max_in_flight 限制），
    等待回复不会阻塞后续消息的发送。
    """

    def __init__(self, queue_config: MessageQueueConfig = None):
        """
        初始化消息处理器

        Args:
            queue_config: 队列配置
        """
        self._queue = MessageQueue(queue_config)
        self._send_task: Optional[asyncio.Task] = None
        self._inflight: Set[asyncio.Task] = set()
        self._semaphore: Optional[asyncio.Semaphore] = None
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
        callback = self._callbacks.get(event)
        if callback is None:
            return
        try:
            result = callback(*args, **kwargs)
            if inspect.isawaitable(result):
                spawn(result, name=f"MessageHandler.{event}")
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
        max_in_flight = max(1, int(self._queue.config.max_in_flight))
        self._semaphore = asyncio.Semaphore(max_in_flight)
        self._send_task = asyncio.create_task(self._send_loop())
        logger.info(f"Message handler started (max_in_flight={max_in_flight})")

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

        inflight = [task for task in self._inflight if not task.done()]
        for task in inflight:
            task.cancel()
        if inflight:
            await asyncio.gather(*inflight, return_exceptions=True)
        self._inflight.clear()

        self._semaphore = None
        logger.info("Message handler stopped")

    async def send(
        self,
        message: SECSMessage,
        wait_reply: bool = False,
        timeout: Optional[float] = None,
    ) -> Optional[SECSMessage]:
        """
        发送 SECS 消息

        超时时间统一由会话层（T3）保证，本方法只等待结果：
        - 正常收到回复：返回回复消息
        - 超时 / 连接断开：返回 None

        Args:
            message: SECS 消息
            wait_reply: 是否等待回复
            timeout: 超时时间（秒），None 时使用会话默认 T3

        Returns:
            回复消息（如果 wait_reply=True）
        """
        if not self._session_manager:
            raise RuntimeError("Session manager not set")

        expect_reply = bool(wait_reply or message.w_bit)

        future: Optional[asyncio.Future] = None
        callback = None
        if expect_reply:
            future = asyncio.get_running_loop().create_future()

            def callback(reply: Optional[SECSMessage]) -> None:
                if not future.done():
                    future.set_result(reply)

        data = await self._session_manager.send_message(
            message, wait_reply=wait_reply, timeout=timeout, callback=callback
        )

        if data is None:
            logger.warning(f"Message not sent (session not selected?): {message.sf}")
            return None

        self._trigger_callback("message_sent", message)

        if future is None:
            return None

        reply = await future
        if reply is None:
            logger.warning(f"No reply for {message.sf} (timeout or disconnected)")
            self._trigger_callback("timeout", message)
            return None

        self._trigger_callback("reply_received", reply)
        return reply

    async def try_send(self, message: SECSMessage) -> bool:
        """
        只发送、不等待回复。

        Returns:
            消息是否实际发出（未 Selected/未连接时返回 False）
        """
        if not self._session_manager:
            raise RuntimeError("Session manager not set")

        data = await self._session_manager.send_message(message, wait_reply=False)
        if data is None:
            return False

        self._trigger_callback("message_sent", message)
        return True

    async def send_async(
        self,
        message: SECSMessage,
        priority: int = 0,
        callback: Optional[Callable[[Optional[SECSMessage]], None]] = None,
    ) -> None:
        """
        异步发送消息（通过队列）

        Args:
            message: SECS 消息
            priority: 优先级
            callback: 完成回调；收到回复时传回复消息，超时/断开时传 None
        """
        await self._queue.enqueue(message, priority, callback)

    async def _send_loop(self) -> None:
        """消息发送循环：取出队列消息并并发派发，避免队头阻塞。"""
        while self._running:
            try:
                queued = await self._queue.dequeue(timeout=1.0)
                if queued is None:
                    continue

                await self._semaphore.acquire()
                task = asyncio.create_task(self._dispatch(queued))
                self._inflight.add(task)
                task.add_done_callback(self._inflight.discard)

            except asyncio.CancelledError:
                break

            except Exception as e:
                logger.error(f"Send loop error: {e}")

    async def _dispatch(self, queued: QueuedMessage) -> None:
        """发送单条队列消息，失败时按配置重试。"""
        message = queued.secs_message
        try:
            wait_reply = queued.callback is not None or message.w_bit
            reply = await self.send(message, wait_reply=wait_reply)

            if queued.callback:
                try:
                    result = queued.callback(reply)
                    if inspect.isawaitable(result):
                        await result
                except Exception as e:
                    logger.error(f"Queued message callback error: {e}")

        except Exception as e:
            logger.error(f"Error sending message: {e}")

            # 尝试重试（按配置延迟）
            if self._queue.config.retry_delay > 0:
                await asyncio.sleep(self._queue.config.retry_delay)

            if await self._queue.requeue(queued):
                logger.info(f"Message requeued for retry: {queued.retry_count}")
            else:
                # 超过最大重试次数
                self._trigger_callback("send_failed", message, e)

        finally:
            self._semaphore.release()

    def get_queue_size(self) -> int:
        """获取队列大小"""
        return self._queue.size

    def get_pending_count(self) -> int:
        """获取进行中的发送任务数量（含等待回复）"""
        return len(self._inflight)
