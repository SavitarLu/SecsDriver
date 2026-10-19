"""SECS-II 编解码回归测试。"""

import pytest

from secs_driver.secs_message import SECSItem, SECSMessage
from secs_driver.secs_parser import SECSEncoder, SECSParser
from secs_driver.secs_types import SECSType


def _roundtrip(message: SECSMessage) -> SECSMessage:
    data = SECSEncoder.encode(message)
    messages = SECSParser().feed(data)
    assert len(messages) == 1
    return messages[0]


@pytest.mark.parametrize(
    "item",
    [
        pytest.param(SECSItem.boolean(True), id="boolean"),
        pytest.param(SECSItem.ascii("HELLO"), id="ascii"),
        pytest.param(SECSItem.jis8("ABC"), id="jis8"),
        pytest.param(SECSItem.binary(b"\x01\x02\xff"), id="binary"),
        pytest.param(SECSItem.int1(-5), id="int1"),
        pytest.param(SECSItem.int2(-300), id="int2"),
        pytest.param(SECSItem.int4(-70000), id="int4"),
        pytest.param(SECSItem.int8(-(2**40)), id="int8"),
        pytest.param(SECSItem.uint1(200), id="uint1"),
        pytest.param(SECSItem.uint2(60000), id="uint2"),
        pytest.param(SECSItem.uint4(4000000000), id="uint4"),
        pytest.param(SECSItem.uint8(10**18), id="uint8"),
        pytest.param(SECSItem.float4(1.25), id="float4"),
        pytest.param(SECSItem.float8(3.5), id="float8"),
    ],
)
def test_scalar_roundtrip(item: SECSItem) -> None:
    message = SECSMessage(stream=1, function=1, w_bit=True, device_id=7)
    message.add_item(item)

    decoded = _roundtrip(message)

    assert decoded.stream == 1
    assert decoded.function == 1
    assert decoded.w_bit is True
    assert decoded.device_id == 7
    assert len(decoded.items) == 1
    assert decoded.items[0].type == item.type
    assert decoded.items[0].value == item.value


def test_nested_list_roundtrip() -> None:
    inner = SECSItem.list_([SECSItem.ascii("X"), SECSItem.uint2(9)])
    outer = SECSItem.list_([inner, SECSItem.binary(b"\x00\x01")])

    message = SECSMessage(stream=5, function=1, w_bit=True)
    message.add_item(outer)

    decoded = _roundtrip(message)

    root = decoded.items[0]
    assert root.type == SECSType.LIST
    assert len(root.children) == 2
    assert root.children[0].type == SECSType.LIST
    assert root.children[0].children[0].value == "X"
    assert root.children[0].children[1].value == 9
    assert root.children[1].value == b"\x00\x01"


def test_empty_message_roundtrip() -> None:
    message = SECSMessage(stream=9, function=9, w_bit=False, device_id=1)
    decoded = _roundtrip(message)
    assert decoded.stream == 9
    assert decoded.function == 9
    assert decoded.items == []


def test_secs_parser_returns_empty_for_incomplete_data() -> None:
    parser = SECSParser()
    assert parser.feed(b"") == []
    assert parser.feed(b"\x00\x01") == []
    # 分帧（半包重组）由 HSMS 层负责，见 test_hsms_protocol
