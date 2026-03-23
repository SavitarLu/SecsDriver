"""
SECS-II 消息和数据项定义

定义 SECSMessage 和 SECSItem 类，用于表示 SECS-II 协议的消息结构。
"""

from typing import Any, List, Optional, Union
from dataclasses import dataclass, field
import uuid

from .secs_types import SECSType, SECSTypeInfo, encode_format, encode_length


@dataclass
class SECSItem:
    """
    SECS-II 数据项

    表示 SECS-II 协议中的一个数据元素，可以是原子类型（数值、字符串等）
    或嵌套的 LIST 类型。
    """

    type: SECSType
    value: Any = None
    children: List["SECSItem"] = field(default_factory=list)

    def __post_init__(self):
        """验证数据项的有效性"""
        if self.type == SECSType.LIST:
            # LIST 类型不需要 value，使用 children
            if self.children is None:
                self.children = []
        else:
            # 非 LIST 类型需要 value
            if self.value is None and self.type not in (SECSItem.get_placeholder_type()):
                pass  # 允许创建空值占位符

    @staticmethod
    def get_placeholder_type() -> List[SECSType]:
        """获取需要 value 的类型列表"""
        return [
            SECSType.BOOLEAN,
            SECSType.ASCII,
            SECSType.INT8,
            SECSType.INT1,
            SECSType.INT4,
            SECSType.INT2,
            SECSType.UINT8,
            SECSType.UINT1,
            SECSType.UINT4,
            SECSType.UINT2,
            SECSType.FLOAT8,
            SECSType.FLOAT4,
            SECSType.JIS8,
            SECSType.BINARY,
        ]

    @staticmethod
    def list_(children: List["SECSItem"] = None) -> "SECSItem":
        """创建 LIST 类型数据项"""
        return SECSItem(type=SECSType.LIST, children=children or [])

    @staticmethod
    def boolean(value: bool) -> "SECSItem":
        """创建 BOOLEAN 类型数据项"""
        return SECSItem(type=SECSType.BOOLEAN, value=value)

    @staticmethod
    def ascii(value: str) -> "SECSItem":
        """创建 ASCII 类型数据项"""
        return SECSItem(type=SECSType.ASCII, value=value)

    @staticmethod
    def jis8(value: str) -> "SECSItem":
        """创建 JIS8 类型数据项"""
        return SECSItem(type=SECSType.JIS8, value=value)

    @staticmethod
    def binary(value: bytes) -> "SECSItem":
        """创建 BINARY 类型数据项"""
        return SECSItem(type=SECSType.BINARY, value=value)

    @staticmethod
    def int1(value: int) -> "SECSItem":
        """创建 1 字节有符号整数类型数据项"""
        return SECSItem(type=SECSType.INT1, value=value)

    @staticmethod
    def int2(value: int) -> "SECSItem":
        """创建 2 字节有符号整数类型数据项"""
        return SECSItem(type=SECSType.INT2, value=value)

    @staticmethod
    def int4(value: int) -> "SECSItem":
        """创建 4 字节有符号整数类型数据项"""
        return SECSItem(type=SECSType.INT4, value=value)

    @staticmethod
    def int8(value: int) -> "SECSItem":
        """创建 8 字节有符号整数类型数据项"""
        return SECSItem(type=SECSType.INT8, value=value)

    @staticmethod
    def uint1(value: int) -> "SECSItem":
        """创建 1 字节无符号整数类型数据项"""
        return SECSItem(type=SECSType.UINT1, value=value)

    @staticmethod
    def uint2(value: int) -> "SECSItem":
        """创建 2 字节无符号整数类型数据项"""
        return SECSItem(type=SECSType.UINT2, value=value)

    @staticmethod
    def uint4(value: int) -> "SECSItem":
        """创建 4 字节无符号整数类型数据项"""
        return SECSItem(type=SECSType.UINT4, value=value)

    @staticmethod
    def uint8(value: int) -> "SECSItem":
        """创建 8 字节无符号整数类型数据项"""
        return SECSItem(type=SECSType.UINT8, value=value)

    @staticmethod
    def float4(value: float) -> "SECSItem":
        """创建 4 字节浮点数类型数据项"""
        return SECSItem(type=SECSType.FLOAT4, value=value)

    @staticmethod
    def float8(value: float) -> "SECSItem":
        """创建 8 字节浮点数类型数据项"""
        return SECSItem(type=SECSType.FLOAT8, value=value)

    def add_child(self, child: "SECSItem") -> None:
        """添加子元素（仅用于 LIST 类型）"""
        if self.type != SECSType.LIST:
            raise TypeError("Only LIST type can have children")
        self.children.append(child)

    def get_child(self, index: int) -> Optional["SECSItem"]:
        """获取指定索引的子元素"""
        if self.type != SECSType.LIST:
            return None
        if 0 <= index < len(self.children):
            return self.children[index]
        return None

    def __repr__(self) -> str:
        """返回可读表示"""
        if self.type == SECSType.LIST:
            return f"L[{len(self.children)}]"
        elif self.value is not None:
            type_name = SECSTypeInfo.get_name(self.type)
            if isinstance(self.value, bytes):
                return f"{type_name}:{self.value.hex()}"
            return f"{type_name}:{self.value}"
        else:
            return SECSTypeInfo.get_name(self.type)


@dataclass
class SECSMessage:
    """
    SECS-II 消息

    表示一个完整的 SECS-II 消息，包含 Stream、Function、W-bit 等信息，
    以及消息体的数据项列表。
    """

    stream: int
    function: int
    w_bit: bool = False  # Wait bit: True 表示需要等待回复
    device_id: int = 0
    system_bytes: bytes = field(default_factory=lambda: uuid.uuid4().bytes[:4])
    items: List[SECSItem] = field(default_factory=list)

    @property
    def s(self) -> int:
        """获取 Stream 编号"""
        return self.stream

    @property
    def f(self) -> int:
        """获取 Function 编号"""
        return self.function

    @property
    def sf(self) -> str:
        """获取 S-F 表示"""
        return f"S{self.stream}F{self.function}"

    @property
    def is_reply(self) -> bool:
        """判断是否为回复消息"""
        return self.function % 2 == 0

    @property
    def is_primary(self) -> bool:
        """判断是否为主消息（需要回复）"""
        return self.w_bit

    def add_item(self, item: SECSItem) -> None:
        """添加数据项"""
        self.items.append(item)

    def get_item(self, index: int) -> Optional[SECSItem]:
        """获取指定索引的数据项"""
        if 0 <= index < len(self.items):
            return self.items[index]
        return None

    def create_reply(self, items: List[SECSItem] = None) -> "SECSMessage":
        """
        创建回复消息

        创建一个对应的回复消息，Function 编号加 1。
        """
        return SECSMessage(
            stream=self.stream,
            function=self.function + 1,
            w_bit=False,  # 回复消息不需要 W-bit
            device_id=self.device_id,
            system_bytes=self.system_bytes,
            items=items or [],
        )

    def __repr__(self) -> str:
        """返回可读表示"""
        w = " W" if self.w_bit else ""
        items_str = ""
        if self.items:
            items_str = " " + " ".join(repr(item) for item in self.items)
        return f"S{self.stream}F{self.function}{w}{items_str}"


def format_secs_message(message: SECSMessage, indent: int = 0) -> str:
    """
    格式化 SECS-II 消息为可读字符串

    用于调试和日志输出。
    """
    prefix = "  " * indent
    lines = [f"{prefix}S{message.stream}F{message.function}"]

    for item in message.items:
        lines.append(_format_item(item, indent + 1))

    return "\n".join(lines)


def _format_item(item: SECSItem, indent: int) -> str:
    """格式化单个数据项"""
    prefix = "  " * indent
    type_name = SECSTypeInfo.get_name(item.type)

    if item.type == SECSType.LIST:
        lines = [f"{prefix}L[{len(item.children)}]"]
        for child in item.children:
            lines.append(_format_item(child, indent + 1))
        return "\n".join(lines)
    else:
        value_str = ""
        if item.value is not None:
            if isinstance(item.value, bytes):
                value_str = f" {item.value.hex()}"
            elif isinstance(item.value, str):
                value_str = f' "{item.value}"'
            else:
                value_str = f" {item.value}"
        return f"{prefix}{type_name}{value_str}"
