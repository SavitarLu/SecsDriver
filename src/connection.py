"""
TCP 连接管理器

实现主动模式和被动模式的 TCP 连接管理。
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Optional, Callable
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class ConnectionConfig:
    """连接配置"""

    mode: str = "active"  # "active" 或 "passive"
    host: str = "127.0.0.1"
    port: int = 5000
    timeout: float = 30.0  # 连接超时（秒）
    retry_interval: float = 5.0  # 重连间隔（秒）
    max_retry: int = 3  # 最大重试次数
    keepalive: bool = True  # 是否启用 TCP Keepalive


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
        """触发回调"""
        if event in self._callbacks:
            try:
                result = self._callbacks[event](*args, **kwargs)
                if asyncio.iscoroutine(result):
                    asyncio.create_task(result)
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
    async def send(self, data: bytes) -> None:
        """发送数据"""
        pass

    @abstractmethod
    async def receive(self, num_bytes: int = 4096) -> Optional[bytes]:
        """接收数据"""
        pass


class TCPClient(TCPConnection):
    """
    TCP 客户端（主动模式）

    主动连接到设备服务器。
    """

    def __init__(self, config: ConnectionConfig):
        super().__init__(config)
        self._retry_count = 0
        self._retry_task: Optional[asyncio.Task] = None
        self._should_reconnect = False

    async def connect(self) -> bool:
        """
        连接到服务器

        Returns:
            连接是否成功
        """
        self._should_reconnect = True
        return await self._do_connect()

    async def _do_connect(self) -> bool:
        """执行连接操作"""
        try:
            logger.info(f"Connecting to {self.config.host}:{self.config.port}")

            self._reader, self._writer = await asyncio.wait_for(
                self._open_connection(),
                timeout=self.config.timeout,
            )

            self._connected = True
            self._retry_count = 0
            logger.info(f"Connected to {self.peer_address}")

            self._trigger_callback("connected")
            return True

        except asyncio.TimeoutError:
            logger.warning(f"Connection timeout to {self.config.host}:{self.config.port}")
            self._handle_connection_failure()
            return False

        except ConnectionRefusedError:
            logger.warning(f"Connection refused by {self.config.host}:{self.config.port}")
            self._handle_connection_failure()
            return False

        except OSError as e:
            logger.error(f"Connection error: {e}")
            self._handle_connection_failure()
            return False

    def _handle_connection_failure(self) -> None:
        """处理连接失败"""
        self._connected = False

        if self._should_reconnect and self._retry_count < self.config.max_retry:
            self._retry_count += 1
            logger.info(
                f"Scheduling reconnect attempt {self._retry_count}/{self.config.max_retry} "
                f"in {self.config.retry_interval}s"
            )
            self._retry_task = asyncio.create_task(self._schedule_reconnect())
        else:
            self._trigger_callback("connection_failed", self._retry_count)

    async def _schedule_reconnect(self) -> None:
        """调度重连"""
        await asyncio.sleep(self.config.retry_interval)
        if self._should_reconnect:
            await self._do_connect()

    async def _open_connection(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """兼容不同 Python 版本的 open_connection 调用。"""
        kwargs = {}
        if self.config.keepalive:
            kwargs["keepalive"] = True

        try:
            return await asyncio.open_connection(self.config.host, self.config.port, **kwargs)
        except TypeError:
            return await asyncio.open_connection(self.config.host, self.config.port)

    async def disconnect(self) -> None:
        """断开连接"""
        self._should_reconnect = False

        if self._retry_task:
            self._retry_task.cancel()
            self._retry_task = None

        if self._writer:
            try:
                self._writer.close()
                await asyncio.wait_for(self._writer.wait_closed(), timeout=5.0)
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")

        self._connected = False
        self._reader = None
        self._writer = None

        logger.info("Disconnected")
        self._trigger_callback("disconnected")

    async def send(self, data: bytes) -> None:
        """发送数据"""
        if not self._connected or not self._writer:
            raise ConnectionError("Not connected")

        try:
            self._writer.write(data)
            await asyncio.wait_for(self._writer.drain(), timeout=self.config.timeout)

        except Exception as e:
            logger.error(f"Send error: {e}")
            await self.disconnect()
            raise

    async def receive(self, num_bytes: int = 4096) -> Optional[bytes]:
        """接收数据"""
        if not self._connected or not self._reader:
            return None

        try:
            data = await asyncio.wait_for(
                self._reader.read(num_bytes), timeout=self.config.timeout
            )

            if not data:
                logger.info("Connection closed by remote")
                await self.disconnect()
                return None

            return data

        except asyncio.TimeoutError:
            logger.warning("Receive timeout")
            return None

        except Exception as e:
            logger.error(f"Receive error: {e}")
            await self.disconnect()
            return None


class TCPServer(TCPConnection):
    """
    TCP 服务器（被动模式）

    监听设备连接。
    """

    def __init__(self, config: ConnectionConfig):
        super().__init__(config)
        self._server: Optional[asyncio.Server] = None
        self._client_reader: Optional[asyncio.StreamReader] = None
        self._client_writer: Optional[asyncio.StreamWriter] = None
        self._server_task: Optional[asyncio.Task] = None

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

            self._server_task = asyncio.create_task(self._server.serve_forever())

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
        """处理客户端连接"""
        self._client_reader = reader
        self._client_writer = writer
        self._connected = True

        peername = writer.get_extra_info("peername")
        logger.info(f"Client connected from {peername}")

        self._trigger_callback("client_connected", peername)

        try:
            while self._connected:
                try:
                    data = await asyncio.wait_for(
                        reader.read(4096), timeout=self.config.timeout
                    )
                except asyncio.TimeoutError:
                    continue

                if not data:
                    break

                self._trigger_callback("data_received", data)

        except Exception as e:
            logger.error(f"Client handler error: {e}")

        finally:
            self._connected = False
            self._client_reader = None
            self._client_writer = None

            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

            logger.info(f"Client disconnected: {peername}")
            self._trigger_callback("client_disconnected", peername)

    async def disconnect(self) -> None:
        """停止服务器"""
        self._connected = False

        if self._client_writer:
            try:
                self._client_writer.close()
                await asyncio.wait_for(self._client_writer.wait_closed(), timeout=5.0)
            except Exception:
                pass

        if self._server:
            self._server.close()
            if self._server_task:
                self._server_task.cancel()
                try:
                    await self._server_task
                except asyncio.CancelledError:
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

    async def receive(self, num_bytes: int = 4096) -> Optional[bytes]:
        """从客户端接收数据"""
        if not self._connected or not self._client_reader:
            return None

        try:
            data = await asyncio.wait_for(
                self._client_reader.read(num_bytes), timeout=self.config.timeout
            )

            if not data:
                return None

            return data

        except asyncio.TimeoutError:
            return None

        except Exception as e:
            logger.error(f"Receive error: {e}")
            return None

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
