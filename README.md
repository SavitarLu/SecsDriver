---
AIGC:
    ContentProducer: Minimax Agent AI
    ContentPropagator: Minimax Agent AI
    Label: AIGC
    ProduceID: "00000000000000000000000000000000"
    PropagateID: "00000000000000000000000000000000"
    ReservedCode1: 304402201c384fdcc2dbdda160291ca2dd9424231ed33a21bf409998fedf89fa3a63575502200adb400fbfae06e97cc7faa1bef3cc26c4c68520bddc3670a5cdde0b153628d2
    ReservedCode2: 30430220010258cff3746758096d5a68eceb83d16e6f22c9b9039242ad51e5307690ebfc021f2b6d9383d8f147d797126978adb9aad31d47f0537e2c00346e766c622d6e7e
---

# SEMI SECS Driver 设计方案

## 一、项目概述

### 1.1 项目背景

在半导体制造行业中，EAP（Equipment Automation Package，设备自动化包）是连接工厂MES系统与生产设备的核心中间件。SECS（Semiconductor Equipment Communication Standard，半导体设备通信标准）是由SEMI国际半导体产业协会制定的设备通信协议标准，是半导体 fab 自动化通信的基础协议。本项目旨在为 EAP 提供一个高性能、高可靠性的 SECS Driver 实现，实现与半导体设备的下层消息收发功能。

SECS 协议体系包含两个主要层次：SECS-I 是基于 RS-232 的点对点通信协议，虽然历史悠久但目前已较少使用；SECS-II 是消息格式标准，定义了设备通信的消息结构和数据类型；HSMS（High-Speed SECS Message Services）是基于 TCP/IP 的现代高速通信协议，取代了早期基于 RS-232 的 SECS-I；GEM（Generic Equipment Model）是 SECS-II 的实现指南，定义了半导体设备的标准行为模型。本 Driver 将完整实现 HSMS 协议和 SECS-II 消息格式，同时兼容 GEM 标准。

### 1.2 设计目标

本 Driver 的设计目标是实现一个生产级别的 SECS 通信组件，具体包括以下几个方面。首先是完整协议支持，需要实现 HSMS 协议的所有消息类型，包括连接管理（Select、Deselect、Linktest、Separate）和数据传输，支持主动模式（设备作为服务器）和被动模式（设备作为客户端）。其次是高性能处理，采用异步 I/O 和零拷贝技术，支持高吞吐量的消息处理，设计消息队列进行流量控制，避免消息丢失。第三是高可靠性保证，实现自动重连机制和心跳检测，支持连接超时和消息超时处理，具备完善的错误恢复能力。第四是易用性设计，提供简洁的 API 接口和清晰的事件回调机制，支持灵活的配置管理。最后是可维护性保障，模块化设计便于维护和扩展，完善的日志记录便于问题诊断。

### 1.3 术语表

| 术语 | 英文全称 | 说明 |
|------|----------|------|
| SECS | Semiconductor Equipment Communication Standard | SEMI 制定的设备通信标准 |
| HSMS | High-Speed SECS Message Services | 基于 TCP/IP 的高速 SECS 消息服务 |
| EAP | Equipment Automation Package | 设备自动化包 |
| GEM | Generic Equipment Model | 通用设备模型 |
| MES | Manufacturing Execution System | 制造执行系统 |
| TNS | Transaction Number | 事务编号 |
| Stream | - | SECS 消息的功能组 |
| Function | - | SECS 消息的具体功能 |
| SxFy | Stream x Function y | SECS 消息的表示方法 |

## Simulator GUI

项目现在包含一个本地调试用的桌面 Simulator，可以同时起两个端点：

- `Passive (Server)`：监听本地端口
- `Host (Active)`：主动连到 Passive

功能包括：

- 启动、停止两个端点
- 手工输入 `SxFy`
- 用 JSON 输入消息体并发送
- 查看发送、接收、回复日志
- 对收到的 `W-bit` 主消息自动回复 `F+1`

启动方式：

```bash
python3 simulator.py
```

消息体 JSON 示例：

```json
[
  {"type": "A", "value": "PING"},
  {"type": "U4", "value": 100},
  {"type": "L", "items": [{"type": "I2", "value": -1}]}
]
```

支持的类型简写包括：`L`、`BOOLEAN`、`A`、`J`、`B`、`I2`、`I4`、`I8`、`U2`、`U4`、`U8`、`F4`、`F8`。

## 二、系统架构

### 2.1 整体架构

```
┌────────────────────────────────────────────────────────────────────┐
│                           EAP Application                           │
└────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────┐
│                        SECS Driver Interface                        │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │                     SECS Driver Core                          │  │
│  │  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐   │  │
│  │  │   Connection   │  │    Session    │  │   Message      │   │  │
│  │  │    Manager     │  │    Manager     │  │    Handler     │   │  │
│  │  └────────────────┘  └────────────────┘  └────────────────┘   │  │
│  │  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐   │  │
│  │  │  HSMS Protocol│  │  SECS-II Parse │  │    Event       │   │  │
│  │  │    Handler    │  │     /Encode    │  │   Dispatcher  │   │  │
│  │  └────────────────┘  └────────────────┘  └────────────────┘   │  │
│  └──────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────┐
│                        TCP/IP Network Layer                          │
└────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────┐
│                        Semiconductor Equipment                     │
└────────────────────────────────────────────────────────────────────┘
```

### 2.2 核心组件

SECS Driver 由以下核心组件构成，各组件职责明确，相互协作完成通信功能。Connection Manager 负责 TCP 连接管理，支持主动连接和被动监听两种模式，管理连接生命周期，处理连接断开和重连。Session Manager 负责 HSMS 会话管理，实现 Select 和 Deselect 握手流程，维护会话状态，管理事务编号（TID）。Message Handler 负责消息的发送和接收处理，维护发送队列和接收缓冲区，处理消息超时和重试。HSMS Protocol Handler 负责 HSMS 协议实现，解析和构造 HSMS 消息头，处理控制消息（Select、Linktest 等）。SECS-II Parser/Encoder 负责 SECS-II 数据格式的解析和编码，将二进制数据转换为对象结构，将对象结构编码为二进制格式。Event Dispatcher 负责事件分发，将接收到的消息通过回调函数分发给上层应用，支持多种事件类型。

### 2.3 数据流

消息接收的数据流如下：TCP 层接收到字节流后，首先进入 HSMS Protocol Handler 进行协议解析，提取消息头和消息体；然后 SECS-II Parser 对消息体进行解码，将 SECS-II 格式转换为 SECSMessage 对象；Event Dispatcher 根据消息类型触发相应的事件回调；上层应用通过事件回调接收消息并进行业务处理。消息发送的数据流如下：应用层构造 SECSMessage 对象并调用发送接口；Message Handler 将消息放入发送队列；SECS-II Encoder 将对象编码为 SECS-II 格式；HSMS Protocol Handler 添加 HSMS 消息头；TCP 层将数据发送到设备。

## 三、SECS-II 协议规范

### 3.1 数据类型定义

SECS-II 定义了丰富的数据类型，用于在设备和主机之间传递各种格式的信息。

| 类型名称 | 格式符 | 字节数 | 取值范围 | 说明 |
|----------|--------|--------|----------|------|
| LIST | L | 可变 | 0-255 个元素 | 嵌套列表，可包含任意类型 |
| BOOLEAN | BOOLEAN | 1 | True/False | 布尔值 |
| ASCII | A | 可变 | 0-255 字节 | ASCII 字符串 |
| JIS8 | J | 可变 | 0-255 字节 | JIS8 编码字符串 |
| INT8 | I8 | 8 | -9,223,372,036,854,775,808 ~ 9,223,372,036,854,775,807 | 8 字节有符号整数 |
| INT4 | I4 | 4 | -2,147,483,648 ~ 2,147,483,647 | 4 字节有符号整数 |
| INT2 | I2 | 2 | -32,768 ~ 32,767 | 2 字节有符号整数 |
| UINT8 | U8 | 8 | 0 ~ 18,446,744,073,709,551,615 | 8 字节无符号整数 |
| UINT4 | U4 | 4 | 0 ~ 4,294,967,295 | 4 字节无符号整数 |
| UINT2 | U2 | 2 | 0 ~ 65,535 | 2 字节无符号整数 |
| FLOAT4 | F4 | 4 | IEEE 754 单精度 | 4 字节浮点数 |
| FLOAT8 | F8 | 8 | IEEE 754 双精度 | 8 字节浮点数 |
| BINARY | B | 可变 | 0-255 字节 | 二进制数据 |

### 3.2 消息格式

SECS-II 消息由消息头和消息体两部分组成。消息头为 10 字节，包含以下字段：Session ID（2 字节），标识通信会话；Header Bits（1 字节），包含消息类型和结束位信息；Message Length（2 字节），表示消息体的字节数；Stream Number（1 字节），功能组编号；Function Number（1 字节），具体功能编号；以及 System Bytes（3 字节），用于事务匹配。消息体采用层次化的树形结构，以 LIST 类型为根，包含各种数据类型元素。

### 3.3 消息表示

SECS-II 消息通常使用特定的文本格式表示，便于阅读和交流。例如，一条 S1F1（Are You There Request）消息的文本表示为 `S1F1 W .`，其中 W 表示需要等待回复。而 S2F29（Formatted Equipment Status Request）消息可以表示为 `S2F29 W <L [3] [A "SFCD"] [A "sfrcv"] [U4 100]>`，表示请求三个数据项。

## 四、HSMS 协议规范

### 4.1 通信模式

HSMS 支持两种通信模式。主动模式（Active Mode）下，Driver 作为客户端主动连接到设备，通常用于设备作为服务器的场景。被动模式（Passive Mode）下，Driver 作为服务器监听设备连接，通常用于设备作为客户端主动连接驱动的场景。

### 4.2 连接状态机

HSMS 连接有多种状态，包括 NOT_CONNECTED（未连接）、CONNECTED（已建立 TCP 连接）、SELECTED（会话已选中）、NOT_SELECTED（会话未选中）、WAIT_SELECT（等待 Select 响应）和 COMMUNICATION_ACTIVE（通信活跃）等状态。

### 4.3 HSMS 消息类型

HSMS 定义了控制消息和数据消息两类。控制消息用于会话管理，不承载 SECS-II 数据，包括 Select.request（S1F1）、Select.acknowledge（S1F2）、Deselect.request（S1F3）、Deselect.acknowledge（S1F4）、Linktest.request（S1F13）、Linktest.response（S1F14）和 Separate.request（S1F15）等。数据消息用于传输 SECS-II 数据，包含 HSMS 消息头和 SECS-II 消息数据。

### 4.4 HSMS 消息头

HSMS 消息头为 12 字节，包含以下字段：Session ID（4 字节），通常为 0xFFFF 表示控制消息；Header Bits（1 字节），包含消息类型和属性；Message Length（4 字节），表示后续数据的字节数；以及 Protocol Type（3 字节），固定为 0x00。

## 五、接口设计

### 5.1 核心接口定义

```python
class ISECSEventHandler:
    """SECS 事件回调接口"""

    def on_connected(self, session_id: int) -> None:
        """TCP 连接建立"""
        pass

    def on_disconnected(self, session_id: int, reason: str) -> None:
        """TCP 连接断开"""
        pass

    def on_selected(self, session_id: int) -> None:
        """会话选择成功"""
        pass

    def on_deselected(self, session_id: int) -> None:
        """会话取消选择"""
        pass

    def on_message_received(self, session_id: int, message: SECSMessage) -> None:
        """收到 SECS-II 消息"""
        pass

    def on_message_sent(self, session_id: int, message: SECSMessage, tid: int) -> None:
        """消息发送成功"""
        pass

    def on_timeout(self, session_id: int, tid: int) -> None:
        """事务超时"""
        pass

    def on_error(self, session_id: int, error: SECSError) -> None:
        """发生错误"""
        pass
```

### 5.2 消息结构定义

```python
class SECSItem:
    """SECS-II 数据项"""
    type: SECSType
    value: Any
    children: List['SECSItem']

class SECSMessage:
    """SECS-II 消息"""
    stream: int
    function: int
    w_bit: bool  # Wait bit, True 表示需要回复
    device_id: int
    system_bytes: bytes
    items: List[SECSItem]
```

### 5.3 主驱动接口

```python
class SECSDriver:
    """SECS Driver 主类"""

    def __init__(self, config: DriverConfig):
        """初始化 Driver"""
        pass

    def connect(self) -> bool:
        """建立连接"""
        pass

    def disconnect(self) -> None:
        """断开连接"""
        pass

    def send_message(self, message: SECSMessage,
                     timeout: float = 10.0) -> Optional[SECSMessage]:
        """同步发送消息并等待回复"""
        pass

    def send_message_async(self, message: SECSMessage,
                           callback: Callable[[SECSMessage], None] = None,
                           timeout: float = 10.0) -> int:
        """异步发送消息"""
        pass

    def send_reply(self, original: SECSMessage,
                   reply: SECSMessage) -> bool:
        """发送回复消息"""
        pass
```

## 六、配置管理

### 6.1 配置文件结构

```yaml
secs_driver:
  # 连接配置
  connection:
    mode: "active"  # active 或 passive
    host: "192.168.1.100"
    port: 5000
    timeout: 30.0  # 连接超时（秒）
    retry_interval: 5.0  # 重连间隔（秒）
    max_retry: 3  # 最大重试次数

  # HSMS 配置
  hsms:
    t3_timeout: 45.0  # 事务超时（秒）
    t5_timeout: 10.0  # 连接分隔超时（秒）
    t6_timeout: 5.0  # 控制会话超时（秒）
    t7_timeout: 10.0  # 等待 Select 超时（秒）
    t8_timeout: 5.0  # 网络重试超时（秒）

  # 日志配置
  logging:
    level: "INFO"
    file: "/var/log/secs_driver.log"
    max_size: 10485760  # 10MB
    backup_count: 5
```

## 七、错误处理

### 7.1 错误类型

| 错误码 | 名称 | 说明 |
|--------|------|------|
| 0x01 | ERR_CONNECTION_FAILED | 连接失败 |
| 0x02 | ERR_CONNECTION_TIMEOUT | 连接超时 |
| 0x03 | ERR_SESSION_SELECT_FAILED | 会话选择失败 |
| 0x04 | ERR_MESSAGE_TIMEOUT | 消息超时 |
| 0x05 | ERR_PARSE_ERROR | 解析错误 |
| 0x06 | ERR_ENCODE_ERROR | 编码错误 |
| 0x07 | ERR_PROTOCOL_ERROR | 协议错误 |
| 0x08 | ERR_QUEUE_FULL | 队列已满 |
| 0x09 | ERR_INVALID_MESSAGE | 无效消息 |

### 7.2 错误恢复策略

连接失败时，Driver 应自动尝试重连，默认重试 3 次，间隔 5 秒。会话选择失败时，等待对方发起选择或重新建立连接。消息超时时，根据消息类型决定是否重试，S 系列消息可重试，R 系列消息不重试。解析错误时，记录错误日志并丢弃该消息，触发错误回调。

## 八、测试用例

### 8.1 单元测试

```python
def test_parse_list():
    """测试 LIST 解析"""
    data = b'\x01\x01\x00\x01\x04\x00\x00\x00\x02\x41\x00\x41\x01'
    item = SECSParser.parse(data)
    assert item.type == SECSType.LIST
    assert len(item.children) == 2

def test_encode_message():
    """测试消息编码"""
    msg = SECSMessage(stream=1, function=1, w_bit=False)
    msg.items = [SECSItem(SECSType.LIST, [])]
    encoded = SECSEncoder.encode(msg)
    assert len(encoded) > 0
```

### 8.2 集成测试

```python
def test_full_communication():
    """测试完整通信流程"""
    # 建立连接
    driver.connect()
    # 等待会话选中
    time.sleep(2)
    # 发送消息
    reply = driver.send_message(create_s1f1_message())
    assert reply is not None
```

## 九、项目结构

```
secs_driver/
├── src/
│   ├── __init__.py
│   ├── secs_driver.py          # 主驱动类
│   ├── connection/
│   │   ├── __init__.py
│   │   ├── connection_manager.py
│   │   ├── tcp_client.py
│   │   └── tcp_server.py
│   ├── session/
│   │   ├── __init__.py
│   │   └── session_manager.py
│   ├── protocol/
│   │   ├── __init__.py
│   │   ├── hsms_protocol.py
│   │   └── secs_parser.py
│   ├── message/
│   │   ├── __init__.py
│   │   ├── secs_message.py
│   │   ├── secs_item.py
│   │   └── message_handler.py
│   └── config/
│       ├── __init__.py
│       └── driver_config.py
├── tests/
│   ├── __init__.py
│   ├── test_parser.py
│   ├── test_encoder.py
│   ├── test_protocol.py
│   └── test_integration.py
├── docs/
│   └── design.md
├── README.md
├── requirements.txt
└── setup.py
```

## 十、后续工作

### 10.1 第一阶段（MVP）

实现 SECS-II 解析器和编码器，完成 HSMS 协议基础实现，支持主动和被动连接模式，提供基本的消息收发功能。

### 10.2 第二阶段（增强）

添加完整的错误处理和自动重连机制，实现消息队列和流量控制，优化性能和内存使用。

### 10.3 第三阶段（生产就绪）

添加完整的日志和监控支持，性能测试和优化，文档完善和示例代码。

---

**作者**：MiniMax Agent
**版本**：1.0.0
**日期**：2026-03-20
