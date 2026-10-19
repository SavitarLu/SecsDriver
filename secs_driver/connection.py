"""
TCP 连接管理器

实现主动模式和被动模式的 TCP 连接管理。

两种模式统一采用"内部读取任务 + data_received 回调"的数据流，
调用方（SECSDriver）不需要也不应该轮询 receive()。
"""

import asyncio
import inspect
import logging
from abc import ABC, abstractmethod
from typing import Callable, Optional

from .config import ConnectionConfig
from .task_utils import spawn

logger = logging.getLogger(__name__)


class TCPConnection(ABC):
    """
    TCP 连接基类

    定义 TCP 连接的标准接口。
    """

    def __init__(self, config: ConnectionConfig):
        self.config = config
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._connected = False
        self._callbacks = {}

    def set_callback(self, event: str, callback: Callable) -> None:
        """设置事件回调"""
        self._callbacks[event] = callback

    def _trigger_callback(self, event: str, *args, **kwargs) -> None:
        """触发回调（协程回调以后台任务方式运行）。"""
        callback = self._callbacks.get(event)
        if callback is None:
            return
        try:
            result = callback(*args, **kwargs)
            if inspect.isawaitable(result):
                spawn(result, name=f"{type(self).__name__}.{event}")
        except Exception as e:
            logger.error(f"Callback error for {event}: {e}")

    async def _emit_async(self, event: str, *args) -> None:
        """串行等待回调完成，保证 data_received 等事件的顺序性。"""
        callback = self._callbacks.get(event)
        if callback is None:
            return
        try:
            result = callback(*args)
            if inspect.isawaitable(result):
                await result
        except Exception as e:
            logger.error(f"Callback error for {event}: {e}")

    @property
    def is_connected(self) -> bool:
        """检查连接状态"""
        return self._connected

    @property
    def peer_address(self) -> Optional[tuple]:
        """获取对端地址"""
        if self._writer:
            return self._writer.get_extra_info("peername")
        return None

    @abstractmethod
    async def connect(self) -> bool:
        """建立连接"""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """断开连接"""
        pass

    @abstractmethod
    async def force_reconnect(self) -> None:
        """链路异常时重建连接（按各自策略保持监听或自动重连）"""
        pass

    @abstractmethod
    async def send(self, data: bytes) -> None:
        """发送数据"""
        pass


class TCPClient(TCPConnection):
    """
    TCP 客户端（主动模式）

    主动连接到设备服务器，断开后按配置自动重连。
    """

    def __init__(self, config: ConnectionConfig):
        super().__init__(config)
        self._retry_count = 0
        self._retry_task: Optional[asyncio.Task] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._should_reconnect = False

    async def connect(self) -> bool:
        """
        建立初始连接。

        只尝试一次，失败立即返回 False（不阻塞、不隐式后台重试）；
        连接建立后的意外断开才进入自动重连流程。
        """
        self._should_reconnect = self.config.auto_reconnect
        self._retry_count = 0
        return await self._do_connect()

    async def _do_connect(self) -> bool:
        """执行一次连接操作"""
        try:
            logger.info(f"Connecting to {self.config.host}:{self.config.port}")

            self._reader, self._writer = await asyncio.wait_for(
                self._open_connection(),
                timeout=self.config.timeout,
            )

            self._connected = True
            self._retry_count = 0
            logger.info(f"Connected to {self.peer_address}")

            self._reader_task = asyncio.create_task(self._read_loop())
            self._trigger_callback("connected")
            return True

        except asyncio.TimeoutError:
            logger.warning(f"Connection timeout to {self.config.host}:{self.config.port}")
        except ConnectionRefusedError:
            logger.warning(f"Connection refused by {self.config.host}:{self.config.port}")
        except OSError as e:
            logger.error(f"Connection error: {e}")

        self._connected = False
        return False

    def _schedule_reconnect(self) -> None:
        """调度一次重连（max_retry <= 0 表示无限重连）。"""
        if not self._should_reconnect:
            return

        current = asyncio.current_task()
        if (
            self._retry_task is not None
            and not self._retry_task.done()
            and self._retry_task is not current
        ):
            return  # 已有重连在途

        if self.config.max_retry > 0 and self._retry_count >= self.config.max_retry:
            logger.error(f"Reconnect gave up after {self._retry_count} attempts")
            self._trigger_callback("connection_failed", self._retry_count)
            return

        self._retry_count += 1
        limit = f"/{self.config.max_retry}" if self.config.max_retry > 0 else "(无限)"
        logger.info(
            f"Scheduling reconnect attempt {self._retry_count}{limit} "
            f"in {self.config.retry_interval}s"
        )
        self._retry_task = spawn(self._reconnect_after_delay(), name="TCPClient.reconnect")

    async def _reconnect_after_delay(self) -> None:
        """等待重连间隔后重试连接。"""
        try:
            await asyncio.sleep(self.config.retry_interval)
            if not self._should_reconnect:
                return
            if not await self._do_connect():
                self._schedule_reconnect()
        finally:
            if self._retry_task is asyncio.current_task():
                self._retry_task = None

    async def _read_loop(self) -> None:
        """持续读取数据并推送；连接结束时触发清理与重连。"""
        try:
            while self._connected and self._reader is not None:
                data = await self._reader.read(4096)
                if not data:
                    logger.info("Connection closed by remote")
                    break
                await self._emit_async("data_received", data)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Read loop error: {e}")
        finally:
            if self._reader_task is asyncio.current_task():
                self._reader_task = None
                # 独立任务完成清理，避免在自身 finally 中取消自己
                spawn(self._handle_connection_lost(), name="TCPClient.connection_lost")

    async def _handle_connection_lost(self) -> None:
        """处理连接意外断开。"""
        if not self._connected:
            return
        await self._teardown()
        self._schedule_reconnect()

    async def force_reconnect(self) -> None:
        """断开当前连接并按策略重连（链路超时等异常场景使用）。"""
        self._retry_count = 0
        await self._teardown()
        self._schedule_reconnect()

    async def _teardown(self) -> None:
        """关闭当前连接（不改变重连策略）。"""
        was_connected = self._connected or self._writer is not None
        self._connected = False

        task, self._reader_task = self._reader_task, None
        if task is not None and task is not asyncio.current_task():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.debug(f"Reader task cleanup: {e}")

        self._reader = None
        writer, self._writer = self._writer, None
        if writer is not None:
            try:
                writer.close()
                await asyncio.wait_for(writer.wait_closed(), timeout=5.0)
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")

        if was_connected:
            logger.info("Disconnected")
            self._trigger_callback("disconnected")

    async def disconnect(self) -> None:
        """主动断开：取消重连并关闭连接。"""
        self._should_reconnect = False

        retry_task, self._retry_task = self._retry_task, None
        if retry_task is not None:
            retry_task.cancel()
            try:
                await retry_task
            except asyncio.CancelledError:
                pass

        await self._teardown()

    async def _open_connection(self) -> tuple:
        """兼容不同 Python 版本的 open_connection 调用。"""
        kwargs = {}
        if self.config.keepalive:
            kwargs["keepalive"] = True

        try:
            return await asyncio.open_connection(self.config.host, self.config.port, **kwargs)
        except TypeError:
            return await asyncio.open_connection(self.config.host, self.config.port)

    async def send(self, data: bytes) -> None:
        """发送数据；失败时关闭链路并进入重连流程。"""
        if not self._connected or not self._writer:
            raise ConnectionError("Not connected")

        try:
            self._writer.write(data)
            await asyncio.wait_for(self._writer.drain(), timeout=self.config.timeout)

        except Exception as e:
            logger.error(f"Send error: {e}")
            await self._teardown()
            self._schedule_reconnect()
            raise


class TCPServer(TCPConnection):
    """
    TCP 服务器（被动模式）

    监听设备连接；仅维护一个客户端会话，
    多余的连接会被立即关闭。
    """

    def __init__(self, config: ConnectionConfig):
        super().__init__(config)
        self._server: Optional[asyncio.AbstractServer] = None
        self._client_reader: Optional[asyncio.StreamReader] = None
        self._client_writer: Optional[asyncio.StreamWriter] = None

    async def connect(self) -> bool:
        """
        启动服务器并监听连接

        Returns:
            服务器是否成功启动
        """
        try:
            logger.info(f"Starting server on {self.config.host}:{self.config.port}")

            self._server = await self._start_server()

            addr = self._server.sockets[0].getsockname()
            logger.info(f"Server listening on {addr}")

            return True

        except OSError as e:
            logger.error(f"Server error: {e}")
            return False

    async def _start_server(self):
        """兼容不同 Python 版本的 start_server 调用。"""
        kwargs = {}
        if self.config.keepalive:
            kwargs["keepalive"] = True

        try:
            return await asyncio.start_server(
                self._handle_client,
                self.config.host,
                self.config.port,
                **kwargs,
            )
        except TypeError:
            return await asyncio.start_server(
                self._handle_client,
                self.config.host,
                self.config.port,
            )

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """处理客户端连接（单会话）。"""
        peername = writer.get_extra_info("peername")

        if self._client_writer is not None:
            logger.warning(
                f"Rejecting extra client {peername}: only one session is supported"
            )
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            return

        self._client_reader = reader
        self._client_writer = writer
        self._connected = True

        logger.info(f"Client connected from {peername}")
        await self._emit_async("client_connected", peername)

        try:
            while self._connected and self._client_reader is reader:
                data = await reader.read(4096)
                if not data:
                    break
                await self._emit_async("data_received", data)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Client handler error: {e}")

        finally:
            if self._client_reader is reader:
                self._client_reader = None
            if self._client_writer is writer:
                self._client_writer = None
                self._connected = False

            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

            logger.info(f"Client disconnected: {peername}")
            self._trigger_callback("client_disconnected", peername)

    async def force_reconnect(self) -> None:
        """关闭当前客户端连接，继续监听等待对端重连。"""
        writer = self._client_writer
        if writer is not None:
            logger.info("Closing client connection to force peer reconnect")
            writer.close()

    async def disconnect(self) -> None:
        """停止服务器并断开客户端。"""
        self._connected = False

        writer, self._client_writer = self._client_writer, None
        self._client_reader = None
        if writer is not None:
            try:
                writer.close()
                await asyncio.wait_for(writer.wait_closed(), timeout=5.0)
            except Exception:
                pass

        server, self._server = self._server, None
        if server is not None:
            server.close()
            try:
                await asyncio.wait_for(server.wait_closed(), timeout=2.0)
            except Exception:
                pass

        logger.info("Server stopped")

    async def send(self, data: bytes) -> None:
        """发送数据到客户端"""
        if not self._connected or not self._client_writer:
            raise ConnectionError("No client connected")

        try:
            self._client_writer.write(data)
            await asyncio.wait_for(self._client_writer.drain(), timeout=self.config.timeout)

        except Exception as e:
            logger.error(f"Send error: {e}")
            raise

    @property
    def has_client(self) -> bool:
        """检查是否有客户端连接"""
        return self._connected and self._client_writer is not None


def create_connection(config: ConnectionConfig) -> TCPConnection:
    """
    创建连接实例

    Args:
        config: 连接配置

    Returns:
        TCP 连接实例
    """
    if config.mode.lower() == "active":
        return TCPClient(config)
    else:
        return TCPServer(config)
