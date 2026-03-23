"""
SECS Driver 主类

整合所有组件，提供统一的 SECS 通信接口。
"""

import asyncio
import logging
from typing import Optional, Callable, Dict, Any, List

from .config import DriverConfig, ConnectionConfig, HSMSConfig, MessageQueueConfig, LoggingConfig
from .connection import TCPConnection, TCPClient, TCPServer, create_connection
from .session import SessionManager
from .message_handler import MessageHandler
from .secs_message import SECSMessage, SECSItem
from .secs_parser import SECSEncoder, SECSParser
from .hsms_protocol import HSMSConnectionState, HSMSProtocolHandler, HSMSConnectionStateMachine


logger = logging.getLogger(__name__)


class SECSEventHandler:
    """
    SECS 事件回调接口

    用户应继承此类并实现回调方法。
    """

    def on_connected(self, session_id: int) -> None:
        """TCP 连接建立"""
        pass

    def on_disconnected(self, session_id: int, reason: str) -> None:
        """TCP 连接断开"""
        pass

    def on_selected(self) -> None:
        """会话选择成功"""
        pass

    def on_deselected(self) -> None:
        """会话取消选择"""
        pass

    def on_separated(self) -> None:
        """对方发起断开连接"""
        pass

    def on_message_received(self, message: SECSMessage) -> None:
        """收到 SECS-II 消息"""
        pass

    def on_message_sent(self, message: SECSMessage) -> None:
        """消息发送成功"""
        pass

    def on_timeout(self, message: SECSMessage) -> None:
        """消息超时"""
        pass

    def on_error(self, error: Exception) -> None:
        """发生错误"""
        pass


class SECSDriver:
    """
    SECS Driver 主类

    提供完整的 SECS/HSMS 通信功能，用于 EAP 与半导体设备通信。
    """

    def __init__(self, config: DriverConfig = None):
        """
        初始化 SECS Driver

        Args:
            config: 驱动配置
        """
        self.config = config or DriverConfig()
        self._event_handler: Optional[SECSEventHandler] = None

        # 组件初始化
        self._connection: Optional[TCPConnection] = None
        self._session_manager: Optional[SessionManager] = None
        self._message_handler: Optional[MessageHandler] = None
        self._state_machine: Optional[HSMSConnectionStateMachine] = None

        # 状态
        self._running = False
        self._receive_task: Optional[asyncio.Task] = None
        self._linktest_task: Optional[asyncio.Task] = None

        # 日志配置
        self._setup_logging()

        logger.info(f"SECS Driver initialized: {self.config.name}")

    def _setup_logging(self) -> None:
        """配置日志"""
        log_config = self.config.logging

        # 创建日志格式
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        # 配置根日志器
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, log_config.level.upper(), logging.INFO))

        # 清除现有处理器
        root_logger.handlers.clear()

        # 添加控制台处理器
        if log_config.console:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            root_logger.addHandler(console_handler)

        # 添加文件处理器
        if log_config.file:
            try:
                from logging.handlers import RotatingFileHandler

                file_handler = RotatingFileHandler(
                    log_config.file,
                    maxBytes=log_config.max_size,
                    backupCount=log_config.backup_count,
                )
                file_handler.setFormatter(formatter)
                root_logger.addHandler(file_handler)
            except Exception as e:
                logger.warning(f"Failed to setup file logging: {e}")

    def set_event_handler(self, handler: SECSEventHandler) -> None:
        """
        设置事件回调处理器

        Args:
            handler: SECSEventHandler 子类实例
        """
        self._event_handler = handler

    async def connect(self) -> bool:
        """
        建立连接

        Returns:
            连接是否成功
        """
        if self._running:
            logger.warning("Driver already running")
            return True

        logger.info("Starting SECS Driver...")

        try:
            # 创建连接
            self._connection = create_connection(self.config.connection)

            # 创建会话管理器
            self._session_manager = SessionManager(self.config.hsms)

            # 创建消息处理器
            self._message_handler = MessageHandler(self.config.message_queue)
            self._message_handler.set_session_manager(self._session_manager)

            # 创建状态机
            self._state_machine = HSMSConnectionStateMachine(self._session_manager.protocol)
            await self._session_manager.initialize(self._state_machine)

            # 设置连接回调
            self._connection.set_callback("connected", self._on_connected)
            self._connection.set_callback("disconnected", self._on_disconnected)
            self._connection.set_callback("data_received", self._on_data_received)
            self._connection.set_callback("client_connected", self._on_client_connected)
            self._connection.set_callback("client_disconnected", self._on_client_disconnected)

            # 设置会话回调
            self._session_manager.set_callback("state_changed", self._on_state_changed)
            self._session_manager.set_callback("message_received", self._on_message_received)
            self._session_manager.set_callback("send_control_message", self._on_send_control)
            self._session_manager.set_callback("send_with_reply", self._on_send_message)
            self._session_manager.set_callback("send_no_reply", self._on_send_message)
            self._session_manager.set_callback("separated", self._on_separated)

            # 设置消息处理器回调
            self._message_handler.set_callback("message_sent", self._on_message_sent)
            self._message_handler.set_callback("timeout", self._on_timeout)

            # 建立 TCP 连接
            connected = await self._connection.connect()

            if not connected:
                logger.error("Failed to establish TCP connection")
                return False

            # 启动消息处理器
            await self._message_handler.start()

            # 启动接收循环
            self._running = True
            self._receive_task = asyncio.create_task(self._receive_loop())

            # 启动链路测试
            self._linktest_task = asyncio.create_task(self._linktest_loop())

            logger.info("SECS Driver started successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to start driver: {e}")
            await self.disconnect()
            return False

    async def disconnect(self) -> None:
        """断开连接"""
        logger.info("Stopping SECS Driver...")

        self._running = False

        # 停止链路测试
        if self._linktest_task:
            self._linktest_task.cancel()
            try:
                await self._linktest_task
            except asyncio.CancelledError:
                pass

        # 停止接收循环
        if self._receive_task:
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass

        # 停止消息处理器
        if self._message_handler:
            await self._message_handler.stop()

        # 断开连接
        if self._connection:
            await self._connection.disconnect()

        logger.info("SECS Driver stopped")

    async def _on_connected(self) -> None:
        """连接建立回调"""
        logger.info("TCP connection established")
        if self._state_machine:
            self._state_machine.handle_tcp_connected()
        self._trigger_event("on_connected", 0)
        await self._send_select_request_if_needed()

    async def _on_disconnected(self) -> None:
        """连接断开回调"""
        logger.info("TCP connection lost")
        if self._state_machine:
            self._state_machine.handle_tcp_disconnected()
        self._trigger_event("on_disconnected", 0, "Connection lost")

    async def _on_client_connected(self, address: tuple) -> None:
        """客户端连接回调（被动模式）"""
        logger.info(f"Client connected from {address}")
        if self._state_machine:
            self._state_machine.handle_tcp_connected()
        self._trigger_event("on_connected", 0)

    async def _on_client_disconnected(self, address: tuple) -> None:
        """客户端断开回调（被动模式）"""
        logger.info(f"Client disconnected: {address}")
        if self._state_machine:
            self._state_machine.handle_tcp_disconnected()
        self._trigger_event("on_disconnected", 0, f"Client disconnected: {address}")

    async def _on_data_received(self, data: bytes) -> None:
        """数据接收回调"""
        if self._session_manager:
            await self._session_manager.receive_message(data)

    async def _on_state_changed(
        self, old_state: HSMSConnectionState, new_state: HSMSConnectionState
    ) -> None:
        """状态变化回调"""
        logger.info(f"State changed: {old_state} -> {new_state}")

        if new_state == HSMSConnectionState.SELECTED:
            self._trigger_event("on_selected")
        elif new_state == HSMSConnectionState.NOT_SELECTED:
            self._trigger_event("on_deselected")

    async def _on_message_received(self, message: SECSMessage) -> None:
        """消息接收回调"""
        self._trigger_event("on_message_received", message)

    async def _on_separated(self) -> None:
        """收到对端 Separate 请求。"""
        self._trigger_event("on_separated")

    async def _on_send_control(self, data: bytes) -> None:
        """发送控制消息"""
        if self._connection and self._connection.is_connected:
            try:
                await self._connection.send(data)
            except Exception as e:
                logger.error(f"Failed to send control message: {e}")

    async def _on_send_message(
        self, data: bytes, message: SECSMessage, tid: int
    ) -> None:
        """发送数据消息。"""
        if not self._connection or not self._connection.is_connected:
            logger.warning(f"Skip sending {message.sf}, transport is not connected")
            return

        try:
            await self._connection.send(data)
            logger.debug(f"Sent {message.sf} (tid={tid})")
        except Exception as e:
            logger.error(f"Failed to send {message.sf}: {e}")

    async def _on_message_sent(self, message: SECSMessage) -> None:
        """消息发送回调"""
        self._trigger_event("on_message_sent", message)

    async def _on_timeout(self, message: SECSMessage) -> None:
        """消息超时回调"""
        self._trigger_event("on_timeout", message)

    async def _receive_loop(self) -> None:
        """接收数据循环"""
        while self._running:
            try:
                if self._connection and self._connection.is_connected:
                    # 对于被动模式，检查是否有客户端
                    if isinstance(self._connection, TCPServer):
                        if not self._connection.has_client:
                            await asyncio.sleep(0.1)
                        else:
                            # 被动模式的数据由 TCPServer 内部回调驱动，避免双重读取同一 socket
                            await asyncio.sleep(0.1)
                        continue

                    data = await self._connection.receive(4096)
                    if data:
                        await self._on_data_received(data)
                else:
                    await asyncio.sleep(0.1)

            except asyncio.CancelledError:
                break

            except Exception as e:
                logger.error(f"Receive loop error: {e}")
                await asyncio.sleep(1)

    async def _linktest_loop(self) -> None:
        """链路测试循环"""
        interval = self.config.hsms.t5_timeout / 2  # T5 超时的一半

        while self._running:
            await asyncio.sleep(interval)

            if (
                self._running
                and self._connection
                and self._connection.is_connected
                and self._session_manager
                and self._session_manager.is_selected
            ):
                try:
                    # 发送 Linktest 请求
                    msg = self._session_manager.protocol.create_linktest_request()
                    data = self._session_manager.protocol.encode_message(msg)
                    await self._connection.send(data)
                    logger.debug("Linktest request sent")
                except Exception as e:
                    logger.warning(f"Linktest failed: {e}")

    async def _send_select_request_if_needed(self) -> None:
        """主动模式下在 TCP 建立后发起 Select。"""
        if (
            self.config.connection.mode.lower() != "active"
            or not self._state_machine
            or not self._connection
            or not self._connection.is_connected
        ):
            return

        request = self._state_machine.initiate_select()
        if not request:
            return

        try:
            data = self._session_manager.protocol.encode_message(request)
            await self._connection.send(data)
            logger.info("Select request sent")
        except Exception as e:
            logger.error(f"Failed to send Select request: {e}")

    def _trigger_event(self, method_name: str, *args, **kwargs) -> None:
        """触发事件回调"""
        if self._event_handler:
            method = getattr(self._event_handler, method_name, None)
            if method:
                try:
                    if asyncio.iscoroutinefunction(method):
                        asyncio.create_task(method(*args, **kwargs))
                    else:
                        method(*args, **kwargs)
                except Exception as e:
                    logger.error(f"Event handler error in {method_name}: {e}")
                    self._trigger_event("on_error", e)

    # ==================== 公开 API ====================

    async def send_message(
        self,
        stream: int,
        function: int,
        items: List[SECSItem] = None,
        wait_reply: bool = True,
        timeout: Optional[float] = None,
    ) -> Optional[SECSMessage]:
        """
        发送 SECS 消息并等待回复

        Args:
            stream: Stream 编号
            function: Function 编号
            items: 消息数据项列表
            wait_reply: 是否等待回复
            timeout: 超时时间（秒）

        Returns:
            回复消息
        """
        if not self._message_handler:
            raise RuntimeError("Driver not connected")

        # 创建消息
        message = SECSMessage(
            stream=stream,
            function=function,
            w_bit=wait_reply,
            device_id=self.config.device_id,
        )

        if items:
            for item in items:
                message.add_item(item)

        # 发送消息
        reply = await self._message_handler.send(
            message,
            wait_reply=wait_reply,
            timeout=timeout,
        )

        return reply

    async def send_message_async(
        self,
        stream: int,
        function: int,
        items: List[SECSItem] = None,
        callback: Optional[Callable[[SECSMessage], None]] = None,
    ) -> None:
        """
        异步发送 SECS 消息

        Args:
            stream: Stream 编号
            function: Function 编号
            items: 消息数据项列表
            callback: 回复回调函数
        """
        if not self._message_handler:
            raise RuntimeError("Driver not connected")

        # 创建消息
        message = SECSMessage(
            stream=stream,
            function=function,
            w_bit=callback is not None,
            device_id=self.config.device_id,
        )

        if items:
            for item in items:
                message.add_item(item)

        # 异步发送
        await self._message_handler.send_async(message, callback=callback)

    async def send_reply(
        self, original_message: SECSMessage, items: List[SECSItem] = None
    ) -> bool:
        """
        发送回复消息

        Args:
            original_message: 原消息
            items: 回复数据项列表

        Returns:
            是否发送成功
        """
        if not self._message_handler:
            return False

        # 创建回复
        reply = original_message.create_reply(items)

        try:
            await self._message_handler.send(reply, wait_reply=False)
            return True
        except Exception as e:
            logger.error(f"Failed to send reply: {e}")
            return False

    def encode_message(self, message: SECSMessage) -> bytes:
        """
        编码 SECS 消息为二进制

        Args:
            message: SECS 消息

        Returns:
            二进制数据
        """
        return SECSEncoder.encode(message)

    def decode_message(self, data: bytes) -> Optional[SECSMessage]:
        """
        解码 SECS 消息

        Args:
            data: 二进制数据

        Returns:
            SECS 消息
        """
        parser = SECSParser()
        messages = parser.feed(data)
        return messages[0] if messages else None

    # ==================== 属性 ====================

    @property
    def is_connected(self) -> bool:
        """检查是否已连接"""
        return self._connection is not None and self._connection.is_connected

    @property
    def is_selected(self) -> bool:
        """检查会话是否已选中"""
        return self._session_manager is not None and self._session_manager.is_selected

    @property
    def state(self) -> HSMSConnectionState:
        """获取当前状态"""
        if self._session_manager:
            return self._session_manager.state
        return HSMSConnectionState.NOT_CONNECTED

    @property
    def queue_size(self) -> int:
        """获取消息队列大小"""
        if self._message_handler:
            return self._message_handler.get_queue_size()
        return 0

    @property
    def pending_count(self) -> int:
        """获取待回复消息数量"""
        if self._message_handler:
            return self._message_handler.get_pending_count()
        return 0


# ==================== 便捷函数 ====================


async def create_driver(
    mode: str = "active",
    host: str = "127.0.0.1",
    port: int = 5000,
    device_id: int = 0,
    **kwargs,
) -> SECSDriver:
    """
    创建 SECS Driver 的便捷函数

    Args:
        mode: 连接模式 ("active" 或 "passive")
        host: 主机地址
        port: 端口
        device_id: 设备 ID
        **kwargs: 其他配置参数

    Returns:
        SECSDriver 实例
    """
    # 构建配置
    config = DriverConfig(
        name=kwargs.get("name", f"SECS_Driver_{host}_{port}"),
        device_id=device_id,
        connection=ConnectionConfig(
            mode=mode,
            host=host,
            port=port,
            timeout=kwargs.get("timeout", 30.0),
            retry_interval=kwargs.get("retry_interval", 5.0),
            max_retry=kwargs.get("max_retry", 3),
        ),
        hsms=HSMSConfig(
            t3_timeout=kwargs.get("t3_timeout", 45.0),
            t5_timeout=kwargs.get("t5_timeout", 10.0),
            t6_timeout=kwargs.get("t6_timeout", 5.0),
            t7_timeout=kwargs.get("t7_timeout", 10.0),
            t8_timeout=kwargs.get("t8_timeout", 5.0),
        ),
    )

    # 创建驱动
    driver = SECSDriver(config)

    return driver
