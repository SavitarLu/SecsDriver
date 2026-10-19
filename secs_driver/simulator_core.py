"""
SECS/HSMS Simulator 核心能力。

提供给桌面 GUI 使用的端点封装、SxFy 解析和 JSON 数据项转换。
"""

from __future__ import annotations

import asyncio
import ast
import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from .config import ConnectionConfig, DriverConfig, LoggingConfig
from .secs_driver import SECSEventHandler, SECSDriver
from .secs_message import SECSItem, SECSMessage
from .secs_types import SECSType


SF_PATTERN = re.compile(r"^\s*S?(?P<stream>\d+)\s*F(?P<function>\d+)\s*$", re.IGNORECASE)

TYPE_ALIASES: Dict[str, SECSType] = {
    "L": SECSType.LIST,
    "LIST": SECSType.LIST,
    "BOOLEAN": SECSType.BOOLEAN,
    "BOOL": SECSType.BOOLEAN,
    "A": SECSType.ASCII,
    "ASCII": SECSType.ASCII,
    "J": SECSType.JIS8,
    "JIS8": SECSType.JIS8,
    "B": SECSType.BINARY,
    "BINARY": SECSType.BINARY,
    "I2": SECSType.INT2,
    "I1": SECSType.INT1,
    "I4": SECSType.INT4,
    "I8": SECSType.INT8,
    "U2": SECSType.UINT2,
    "U1": SECSType.UINT1,
    "U4": SECSType.UINT4,
    "U8": SECSType.UINT8,
    "F4": SECSType.FLOAT4,
    "F8": SECSType.FLOAT8,
}


@dataclass
class SimulatorEvent:
    """Simulator 发给 GUI 的事件。"""

    endpoint: str
    kind: str
    text: str
    status: Optional[str] = None
    message: Optional[SECSMessage] = None
    created_at: datetime = field(default_factory=datetime.now)

    @property
    def timestamp(self) -> str:
        return self.created_at.strftime("%H:%M:%S")


def parse_sf(value: str) -> Tuple[int, int]:
    """解析 SxFy 文本。"""
    match = SF_PATTERN.match(value)
    if not match:
        raise ValueError("SxFy 格式无效，例如 S1F1")
    return int(match.group("stream")), int(match.group("function"))


def parse_items_text(text: str) -> List[SECSItem]:
    """将 SECS 结构文本转换为 SECSItem 列表。"""
    if not text.strip():
        return []

    stripped = text.lstrip()
    if stripped.startswith("{") or stripped.startswith("["):
        payload = json.loads(text)
        if isinstance(payload, dict):
            payload = [payload]
        if not isinstance(payload, list):
            raise ValueError("消息体必须是对象数组")
        return [spec_to_item(item) for item in payload]

    lines = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#") or line.startswith("//"):
            continue
        lines.append(line)
    items, next_index = _parse_item_lines(lines, 0, stop_at_end=False)
    if next_index != len(lines):
        raise ValueError("消息体存在未解析的多余内容")
    return items


def spec_to_item(spec: Any) -> SECSItem:
    """将 JSON 结构转换为单个 SECSItem。"""
    if not isinstance(spec, dict):
        raise ValueError("每个数据项都必须是对象，例如 {\"type\": \"A\", \"value\": \"PING\"}")

    type_name = str(spec.get("type", "")).strip().upper()
    if type_name not in TYPE_ALIASES:
        raise ValueError(f"不支持的数据类型: {type_name or '<empty>'}")

    secs_type = TYPE_ALIASES[type_name]

    if secs_type == SECSType.LIST:
        children = spec.get("items", spec.get("children", []))
        if not isinstance(children, list):
            raise ValueError("LIST 类型必须包含 items 数组")
        return SECSItem.list_([spec_to_item(child) for child in children])

    value = spec.get("value")
    if secs_type == SECSType.BOOLEAN:
        return SECSItem.boolean(bool(value))
    if secs_type == SECSType.ASCII:
        return SECSItem.ascii("" if value is None else str(value))
    if secs_type == SECSType.JIS8:
        return SECSItem.jis8("" if value is None else str(value))
    if secs_type == SECSType.BINARY:
        return SECSItem.binary(_parse_binary_value(value))
    if secs_type == SECSType.INT1:
        return SECSItem.int1(int(value))
    if secs_type == SECSType.INT2:
        return SECSItem.int2(int(value))
    if secs_type == SECSType.INT4:
        return SECSItem.int4(int(value))
    if secs_type == SECSType.INT8:
        return SECSItem.int8(int(value))
    if secs_type == SECSType.UINT1:
        return SECSItem.uint1(int(value))
    if secs_type == SECSType.UINT2:
        return SECSItem.uint2(int(value))
    if secs_type == SECSType.UINT4:
        return SECSItem.uint4(int(value))
    if secs_type == SECSType.UINT8:
        return SECSItem.uint8(int(value))
    if secs_type == SECSType.FLOAT4:
        return SECSItem.float4(float(value))
    if secs_type == SECSType.FLOAT8:
        return SECSItem.float8(float(value))

    raise ValueError(f"未实现的数据类型: {type_name}")


def items_to_json(items: List[SECSItem]) -> str:
    """将 SECSItem 列表转成便于再次编辑的 JSON。"""
    return json.dumps([item_to_spec(item) for item in items], ensure_ascii=False, indent=2)


def item_to_spec(item: SECSItem) -> Dict[str, Any]:
    """将 SECSItem 转成 JSON 对象。"""
    type_name = _secs_type_name(item.type)
    if item.type == SECSType.LIST:
        return {
            "type": type_name,
            "items": [item_to_spec(child) for child in item.children],
        }

    value = item.value
    if item.type == SECSType.BINARY and isinstance(value, (bytes, bytearray)):
        value = bytes(value).hex().upper()

    return {
        "type": type_name,
        "value": value,
    }


def message_to_text(message: SECSMessage) -> str:
    """格式化日志中的消息，使用 SECS 结构表示。"""
    header = message.sf + (" W" if message.w_bit else "")
    if not message.items:
        return header
    lines = [header]
    for item in message.items:
        lines.extend(_item_to_secs_lines(item, 0))
    return "\n".join(lines)


class SimulatorEndpoint(SECSEventHandler):
    """供 GUI 控制的单个端点。"""

    def __init__(self, name: str, mode: str, event_sink: Callable[[SimulatorEvent], None]):
        self.name = name
        self.mode = mode
        self._event_sink = event_sink
        self._driver: Optional[SECSDriver] = None
        self._auto_reply_enabled = True
        self._auto_reply_payload = ""
        self._auto_reply_templates: Dict[str, str] = {}
        self._status = "STOPPED"

    async def start(
        self,
        host: str,
        port: int,
        device_id: int = 0,
        auto_reply: bool = True,
        auto_reply_payload: str = "",
        auto_reply_templates: Optional[Dict[str, str]] = None,
    ) -> bool:
        """启动端点。"""
        if self._driver is not None:
            await self.stop()

        if auto_reply_payload.strip():
            parse_items_text(auto_reply_payload)

        self._auto_reply_enabled = auto_reply
        self._auto_reply_payload = auto_reply_payload
        self._auto_reply_templates = {
            key.strip().upper(): value
            for key, value in (auto_reply_templates or {}).items()
        }
        self._set_status("STARTING")

        config = DriverConfig(
            name=f"Simulator_{self.name}",
            device_id=device_id,
            connection=ConnectionConfig(
                mode=self.mode,
                host=host,
                port=port,
                timeout=10.0,
                retry_interval=1.0,
                max_retry=10,
            ),
            logging=LoggingConfig(level="ERROR", console=False),
        )

        driver = SECSDriver(config)
        driver.set_event_handler(self)
        ok = await driver.connect()

        if not ok:
            self._driver = None
            self._set_status("ERROR")
            self._emit("error", f"启动失败: {host}:{port}")
            return False

        self._driver = driver
        if self.mode == "passive":
            self._set_status("LISTENING")
            self._emit("log", f"监听中 {host}:{port}")
        else:
            self._set_status("CONNECTING")
            self._emit("log", f"正在连接 {host}:{port}")
        return True

    async def stop(self) -> None:
        """停止端点。"""
        driver = self._driver
        self._driver = None
        if driver is not None:
            await driver.disconnect()
        self._set_status("STOPPED")
        self._emit("log", "已停止")

    async def send(
        self,
        sf: str,
        payload_text: str = "",
        wait_reply: bool = False,
        timeout: float = 10.0,
    ) -> Optional[SECSMessage]:
        """发送一条消息。"""
        if self._driver is None:
            raise RuntimeError("端点尚未启动")

        stream, function = parse_sf(sf)
        items = parse_items_text(payload_text)
        reply = await self._driver.send_message(
            stream,
            function,
            items=items,
            wait_reply=wait_reply,
            timeout=timeout,
        )

        if wait_reply:
            if reply is None:
                self._emit("log", f"{sf.upper()} 等待回复超时")
            else:
                self._emit("reply", message_to_text(reply), message=reply)

        return reply

    async def update_auto_reply(
        self,
        enabled: bool,
        payload_text: str = "",
        template_payloads: Optional[Dict[str, str]] = None,
    ) -> None:
        """动态更新自动回复配置。"""
        if payload_text.strip():
            parse_items_text(payload_text)

        self._auto_reply_enabled = enabled
        self._auto_reply_payload = payload_text
        if template_payloads is not None:
            self._auto_reply_templates = {
                key.strip().upper(): value
                for key, value in template_payloads.items()
            }
        state = "启用" if enabled else "关闭"
        self._emit("log", f"自动回复已{state}")

    async def update_auto_reply_templates(self, template_payloads: Dict[str, str]) -> None:
        """仅同步模板回复映射，不改动当前开关和手填回复。"""
        self._auto_reply_templates = {
            key.strip().upper(): value
            for key, value in template_payloads.items()
        }

    def on_connected(self, session_id: int) -> None:
        self._set_status("CONNECTED")
        self._emit("log", "TCP 已连接")

    def on_disconnected(self, session_id: int, reason: str) -> None:
        next_status = "LISTENING" if self.mode == "passive" and self._driver is not None else "DISCONNECTED"
        self._set_status(next_status)
        self._emit("log", f"连接断开: {reason}")

    def on_selected(self) -> None:
        self._set_status("SELECTED")
        self._emit("log", "HSMS 已 Selected")

    def on_deselected(self) -> None:
        self._set_status("NOT_SELECTED")
        self._emit("log", "HSMS 已 Deselect")

    def on_separated(self) -> None:
        next_status = "LISTENING" if self.mode == "passive" and self._driver is not None else "DISCONNECTED"
        self._set_status(next_status)
        self._emit("log", "收到 Separate 请求")

    def on_message_received(self, message: SECSMessage) -> None:
        self._emit("rx", message_to_text(message), message=message)
        if message.w_bit and self._driver is not None and self._auto_reply_enabled:
            asyncio.create_task(self._send_auto_reply(message))

    def on_message_sent(self, message: SECSMessage) -> None:
        self._emit("tx", message_to_text(message), message=message)

    def on_timeout(self, message: SECSMessage) -> None:
        self._emit("error", f"等待回复失败（超时或连接断开）: {message.sf}")

    def on_error(self, error: Exception) -> None:
        self._emit("error", f"{type(error).__name__}: {error}")

    async def _send_auto_reply(self, request: SECSMessage) -> None:
        """对 W-bit 消息发送自动回复。"""
        if self._driver is None:
            return

        try:
            reply_items = self._resolve_auto_reply_items(request)
            ok = await self._driver.send_reply(request, reply_items)
            if ok:
                self._emit("log", f"已自动回复 {request.sf}")
            else:
                self._emit("error", f"自动回复 {request.sf} 未发出（会话未 Selected）")
        except Exception as exc:
            self._emit("error", f"自动回复失败: {exc}")

    def _resolve_auto_reply_items(self, request: SECSMessage) -> List[SECSItem]:
        """优先使用手填回复体，留空时回退到模板 Secondary。"""
        payload_text = self._auto_reply_payload.strip()
        if not payload_text:
            payload_text = self._auto_reply_templates.get(request.sf.upper(), "").strip()
        if not payload_text:
            return []
        return parse_items_text(payload_text)

    def _emit(
        self,
        kind: str,
        text: str,
        *,
        status: Optional[str] = None,
        message: Optional[SECSMessage] = None,
    ) -> None:
        self._event_sink(
            SimulatorEvent(
                endpoint=self.name,
                kind=kind,
                text=text,
                status=status,
                message=message,
            )
        )

    def _set_status(self, status: str) -> None:
        self._status = status
        self._emit("status", status, status=status)


def _secs_type_name(secs_type: SECSType) -> str:
    """返回适合 GUI 输入的短类型名。"""
    mapping = {
        SECSType.LIST: "L",
        SECSType.BOOLEAN: "BOOLEAN",
        SECSType.ASCII: "A",
        SECSType.JIS8: "J",
        SECSType.BINARY: "B",
        SECSType.INT1: "I1",
        SECSType.INT2: "I2",
        SECSType.INT4: "I4",
        SECSType.INT8: "I8",
        SECSType.UINT1: "U1",
        SECSType.UINT2: "U2",
        SECSType.UINT4: "U4",
        SECSType.UINT8: "U8",
        SECSType.FLOAT4: "F4",
        SECSType.FLOAT8: "F8",
    }
    return mapping.get(secs_type, secs_type.name)


def _item_to_secs_lines(item: SECSItem, indent: int) -> List[str]:
    """将数据项格式化为 SECS 结构文本。"""
    prefix = "  " * indent
    type_name = _secs_type_name(item.type)

    if item.type == SECSType.LIST:
        lines = [f"{prefix}<L [{len(item.children)}]"]
        for child in item.children:
            lines.extend(_item_to_secs_lines(child, indent + 1))
        lines.append(f"{prefix}>")
        return lines

    if item.type == SECSType.ASCII:
        value = "" if item.value is None else str(item.value)
        return [f'{prefix}<{type_name} "{value}">']

    if item.type == SECSType.JIS8:
        value = "" if item.value is None else str(item.value)
        return [f'{prefix}<{type_name} "{value}">']

    if item.type == SECSType.BINARY:
        if isinstance(item.value, (bytes, bytearray)):
            value = bytes(item.value).hex().upper()
        elif item.value is None:
            value = ""
        else:
            value = str(item.value)
        return [f"{prefix}<{type_name} {value}>"]

    if item.value is None:
        return [f"{prefix}<{type_name}>"]

    return [f"{prefix}<{type_name} {item.value}>"]


def _parse_item_lines(
    lines: List[str],
    index: int,
    *,
    stop_at_end: bool,
) -> Tuple[List[SECSItem], int]:
    """递归解析结构化文本行。"""
    items: List[SECSItem] = []

    while index < len(lines):
        line = lines[index]
        if SF_PATTERN.match(line):
            index += 1
            continue
        if line == ">":
            if stop_at_end:
                return items, index + 1
            raise ValueError("检测到多余的结束符 >")
        if line.upper().startswith("<L"):
            item, index = _parse_list_item(lines, index)
            items.append(item)
            continue
        items.append(_parse_scalar_item(line))
        index += 1

    if stop_at_end:
        raise ValueError("LIST 缺少结束符 >")
    return items, index


def _parse_list_item(lines: List[str], index: int) -> Tuple[SECSItem, int]:
    """解析 LIST 项。"""
    line = lines[index]
    match = re.match(r"^<L(?:\s*\[(?P<count>\d+)\])?\s*(?P<closed>>)?$", line, re.IGNORECASE)
    if not match:
        raise ValueError(f"LIST 语法无效: {line}")

    count_text = match.group("count")
    expected_count = int(count_text) if count_text is not None else None
    closed = match.group("closed") is not None

    if closed:
        if expected_count not in (None, 0):
            raise ValueError("单行闭合 LIST 只能用于空列表")
        return SECSItem.list_([]), index + 1

    children, next_index = _parse_item_lines(lines, index + 1, stop_at_end=True)
    if expected_count is not None and expected_count != len(children):
        raise ValueError(
            f"LIST 子项数量不匹配，声明为 {expected_count}，实际为 {len(children)}"
        )
    return SECSItem.list_(children), next_index


def _parse_scalar_item(line: str) -> SECSItem:
    """解析原子项。"""
    match = re.match(r"^<(?P<type>[A-Za-z0-9]+)(?:\s+(?P<value>.*))?>$", line)
    if not match:
        raise ValueError(f"数据项语法无效: {line}")

    type_name = match.group("type").upper()
    if type_name not in TYPE_ALIASES:
        raise ValueError(f"不支持的数据类型: {type_name}")

    secs_type = TYPE_ALIASES[type_name]
    raw_value = (match.group("value") or "").strip()

    if secs_type == SECSType.LIST:
        raise ValueError("LIST 必须使用多行结构表示")

    if secs_type == SECSType.BOOLEAN:
        if not raw_value:
            return SECSItem.boolean(False)
        normalized = raw_value.upper()
        if normalized in ("TRUE", "T", "1"):
            return SECSItem.boolean(True)
        if normalized in ("FALSE", "F", "0"):
            return SECSItem.boolean(False)
        raise ValueError(f"BOOLEAN 值无效: {raw_value}")

    if secs_type in (SECSType.ASCII, SECSType.JIS8):
        value = _parse_string_value(raw_value)
        return SECSItem.ascii(value) if secs_type == SECSType.ASCII else SECSItem.jis8(value)

    if secs_type == SECSType.BINARY:
        return SECSItem.binary(_parse_binary_value(raw_value))

    if secs_type == SECSType.INT1:
        return SECSItem.int1(int(raw_value or "0"))
    if secs_type == SECSType.INT2:
        return SECSItem.int2(int(raw_value or "0"))
    if secs_type == SECSType.INT4:
        return SECSItem.int4(int(raw_value or "0"))
    if secs_type == SECSType.INT8:
        return SECSItem.int8(int(raw_value or "0"))
    if secs_type == SECSType.UINT1:
        return SECSItem.uint1(int(raw_value or "0"))
    if secs_type == SECSType.UINT2:
        return SECSItem.uint2(int(raw_value or "0"))
    if secs_type == SECSType.UINT4:
        return SECSItem.uint4(int(raw_value or "0"))
    if secs_type == SECSType.UINT8:
        return SECSItem.uint8(int(raw_value or "0"))
    if secs_type == SECSType.FLOAT4:
        return SECSItem.float4(float(raw_value or "0"))
    if secs_type == SECSType.FLOAT8:
        return SECSItem.float8(float(raw_value or "0"))

    raise ValueError(f"未实现的数据类型: {type_name}")


def _parse_binary_value(value: Any) -> bytes:
    """支持 HEX 字符串或整数数组。"""
    if value is None:
        return b""
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if isinstance(value, list):
        return bytes(int(item) & 0xFF for item in value)
    if isinstance(value, str):
        normalized = value.replace(" ", "").replace("0x", "").replace("0X", "")
        if len(normalized) % 2 == 1:
            normalized = f"0{normalized}"
        if not normalized:
            return b""
        return bytes.fromhex(normalized)
    raise ValueError("BINARY 仅支持 HEX 字符串或整数数组")


def _parse_string_value(raw_value: str) -> str:
    """解析 ASCII/JIS8 字符串。"""
    if not raw_value:
        return ""
    if raw_value[0] in ('"', "'"):
        try:
            parsed = ast.literal_eval(raw_value)
        except (SyntaxError, ValueError) as exc:
            raise ValueError(f"字符串字面量无效: {raw_value}") from exc
        if not isinstance(parsed, str):
            raise ValueError(f"字符串字面量无效: {raw_value}")
        return parsed
    return raw_value
