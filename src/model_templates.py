"""
从 EAP 的 Model.xml 读取 SxFx 模板骨架。
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List
import xml.etree.ElementTree as ET


DEFAULT_MODEL_XML = "/Users/luxinyu/work/个人/EAP/Model.xml"


@dataclass
class MessageTemplate:
    """单条 SxFx 模板。"""

    name: str
    description: str
    stream: int
    function: int
    reply_expected: bool
    primary_text: str
    secondary_text: str

    @property
    def sf(self) -> str:
        return f"S{self.stream}F{self.function}"

    @property
    def secondary_sf(self) -> str:
        return f"S{self.stream}F{self.function + 1}"

    @property
    def label(self) -> str:
        if self.description:
            return f"{self.sf} - {self.description}"
        return self.sf


def load_model_templates(xml_path: str) -> List[MessageTemplate]:
    """读取 Model.xml 并转换为模板列表。"""
    path = Path(xml_path).expanduser()
    root = ET.parse(path).getroot()

    templates: List[MessageTemplate] = []
    for txn in root.findall("Transaction"):
        name = (txn.findtext("Name") or "").strip()
        if not name:
            continue

        stream = int((txn.findtext("Stream") or "0").strip())
        function = int((txn.findtext("Function") or "0").strip())
        description = (txn.findtext("Description") or "").strip()
        reply_expected = (txn.findtext("ReplyExpected") or "").strip().lower() == "true"

        primary = _section_to_text(txn.find("Primary"))
        secondary = _section_to_text(txn.find("Secondary"))
        templates.append(
            MessageTemplate(
                name=name,
                description=description,
                stream=stream,
                function=function,
                reply_expected=reply_expected,
                primary_text=primary,
                secondary_text=secondary,
            )
        )

    templates.sort(key=lambda item: (item.stream, item.function))
    return templates


def _section_to_text(section: ET.Element | None) -> str:
    """将 Primary/Secondary 节点转成可编辑的 SECS 结构文本。"""
    if section is None:
        return ""

    lines: List[str] = []
    for item in section.findall("Item"):
        lines.extend(_item_to_lines(item, 0))
    return "\n".join(lines)


def _item_to_lines(item: ET.Element, indent: int) -> List[str]:
    """递归渲染单个 Item。"""
    prefix = "  " * indent
    name = (item.findtext("Name") or "").strip()
    description = (item.findtext("Description") or "").strip()
    format_name = (item.findtext("Format") or "").strip()
    children = item.findall("Item")
    lines: List[str] = []

    if name or description:
        comment = name or "Item"
        if description:
            comment = f"{comment} - {description}"
        lines.append(f"{prefix}# {comment}")

    if format_name.lower() == "list":
        lines.append(f"{prefix}<L [{len(children)}]")
        for child in children:
            lines.extend(_item_to_lines(child, indent + 1))
        lines.append(f"{prefix}>")
        return lines

    lines.append(f"{prefix}{_format_scalar(format_name)}")
    return lines


def _format_scalar(format_name: str) -> str:
    """将 XML 中的格式名转成 SECS 结构骨架。"""
    normalized = format_name.strip().upper()
    mapping = {
        "ASCII": '<A "">',
        "BINARY": "<B 00>",
        "BOOLEAN": "<BOOLEAN FALSE>",
        "U1": "<U1 0>",
        "U2": "<U2 0>",
        "U4": "<U4 0>",
        "I1": "<I1 0>",
        "I2": "<I2 0>",
        "I4": "<I4 0>",
        "I8": "<I8 0>",
        "F4": "<F4 0>",
        "F8": "<F8 0>",
    }
    return mapping.get(normalized, f"<{normalized}>")


def _ensure_default_binary_values(text: str) -> str:
    """把空的二进制骨架补成 00，避免 ACK 类字段默认为空。"""
    if not text:
        return text
    return text.replace("<B>", "<B 00>")
