"""
配置管理

定义 SECS Driver 的配置类。
"""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any
import json

try:
    import yaml
except ImportError:  # pragma: no cover - 兼容无 PyYAML 的轻量环境
    yaml = None


@dataclass
class ConnectionConfig:
    """TCP 连接配置"""

    mode: str = "active"  # "active" 或 "passive"
    host: str = "127.0.0.1"
    port: int = 5000
    timeout: float = 30.0  # 连接超时（秒）
    retry_interval: float = 5.0  # 重连间隔（秒）
    max_retry: int = 3  # 最大重试次数
    keepalive: bool = True  # 是否启用 TCP Keepalive

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "mode": self.mode,
            "host": self.host,
            "port": self.port,
            "timeout": self.timeout,
            "retry_interval": self.retry_interval,
            "max_retry": self.max_retry,
            "keepalive": self.keepalive,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ConnectionConfig":
        """从字典创建"""
        return cls(**{k: v for k, v in data.items() if k in cls.__annotations__})


@dataclass
class HSMSConfig:
    """HSMS 协议配置"""

    t3_timeout: float = 45.0  # 事务超时（秒）
    t5_timeout: float = 10.0  # 连接分隔超时（秒）
    t6_timeout: float = 5.0  # 控制会话超时（秒）
    t7_timeout: float = 10.0  # 等待 Select 超时（秒）
    t8_timeout: float = 5.0  # 网络重试超时（秒）

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "t3_timeout": self.t3_timeout,
            "t5_timeout": self.t5_timeout,
            "t6_timeout": self.t6_timeout,
            "t7_timeout": self.t7_timeout,
            "t8_timeout": self.t8_timeout,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "HSMSConfig":
        """从字典创建"""
        return cls(**{k: v for k, v in data.items() if k in cls.__annotations__})


@dataclass
class MessageQueueConfig:
    """消息队列配置"""

    max_queue_size: int = 1000  # 最大队列大小
    max_retry: int = 3  # 最大重试次数
    retry_delay: float = 1.0  # 重试延迟（秒）

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "max_queue_size": self.max_queue_size,
            "max_retry": self.max_retry,
            "retry_delay": self.retry_delay,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MessageQueueConfig":
        """从字典创建"""
        return cls(**{k: v for k, v in data.items() if k in cls.__annotations__})


@dataclass
class LoggingConfig:
    """日志配置"""

    level: str = "INFO"  # DEBUG, INFO, WARNING, ERROR
    file: Optional[str] = None  # 日志文件路径
    max_size: int = 10 * 1024 * 1024  # 10MB
    backup_count: int = 5
    console: bool = True

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "level": self.level,
            "file": self.file,
            "max_size": self.max_size,
            "backup_count": self.backup_count,
            "console": self.console,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LoggingConfig":
        """从字典创建"""
        return cls(**{k: v for k, v in data.items() if k in cls.__annotations__})


@dataclass
class DriverConfig:
    """SECS Driver 配置"""

    name: str = "SECS_Driver"  # 驱动名称
    device_id: int = 0  # 设备 ID

    # 连接配置
    connection: ConnectionConfig = field(default_factory=ConnectionConfig)

    # HSMS 配置
    hsms: HSMSConfig = field(default_factory=HSMSConfig)

    # 消息队列配置
    message_queue: MessageQueueConfig = field(default_factory=MessageQueueConfig)

    # 日志配置
    logging: LoggingConfig = field(default_factory=LoggingConfig)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "name": self.name,
            "device_id": self.device_id,
            "connection": self.connection.to_dict(),
            "hsms": self.hsms.to_dict(),
            "message_queue": self.message_queue.to_dict(),
            "logging": self.logging.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DriverConfig":
        """从字典创建"""
        return cls(
            name=data.get("name", "SECS_Driver"),
            device_id=data.get("device_id", 0),
            connection=ConnectionConfig.from_dict(data.get("connection", {})),
            hsms=HSMSConfig.from_dict(data.get("hsms", {})),
            message_queue=MessageQueueConfig.from_dict(data.get("message_queue", {})),
            logging=LoggingConfig.from_dict(data.get("logging", {})),
        )

    @classmethod
    def from_yaml(cls, yaml_str: str) -> "DriverConfig":
        """从 YAML 字符串创建"""
        if yaml is None:
            raise RuntimeError("PyYAML is required to load YAML configuration")
        data = yaml.safe_load(yaml_str)
        return cls.from_dict(data.get("secs_driver", data))

    @classmethod
    def from_json(cls, json_str: str) -> "DriverConfig":
        """从 JSON 字符串创建"""
        data = json.loads(json_str)
        return cls.from_dict(data.get("secs_driver", data))

    @classmethod
    def from_file(cls, file_path: str) -> "DriverConfig":
        """
        从文件加载配置

        Args:
            file_path: 配置文件路径（支持 .yaml, .yml, .json）

        Returns:
            DriverConfig 对象
        """
        with open(file_path, "r", encoding="utf-8") as f:
            if file_path.endswith(".json"):
                return cls.from_json(f.read())
            else:
                if yaml is None:
                    raise RuntimeError("PyYAML is required to load YAML configuration")
                return cls.from_yaml(f.read())

    def save(self, file_path: str) -> None:
        """
        保存配置到文件

        Args:
            file_path: 配置文件路径
        """
        with open(file_path, "w", encoding="utf-8") as f:
            if file_path.endswith(".json"):
                json.dump(self.to_dict(), f, indent=2)
            else:
                if yaml is None:
                    raise RuntimeError("PyYAML is required to save YAML configuration")
                yaml.dump(self.to_dict(), f, default_flow_style=False)


# 默认配置
DEFAULT_CONFIG = DriverConfig()


# 配置示例（YAML 格式）
CONFIG_EXAMPLE = """
secs_driver:
  name: "SECS_Driver_1"
  device_id: 0

  connection:
    mode: "active"          # active 或 passive
    host: "192.168.1.100"
    port: 5000
    timeout: 30.0
    retry_interval: 5.0
    max_retry: 3
    keepalive: true

  hsms:
    t3_timeout: 45.0         # 事务超时
    t5_timeout: 10.0         # 连接分隔超时
    t6_timeout: 5.0          # 控制会话超时
    t7_timeout: 10.0         # 等待 Select 超时
    t8_timeout: 5.0          # 网络重试超时

  message_queue:
    max_queue_size: 1000
    max_retry: 3
    retry_delay: 1.0

  logging:
    level: "INFO"
    file: "/var/log/secs_driver.log"
    max_size: 10485760       # 10MB
    backup_count: 5
    console: true
"""


if __name__ == "__main__":
    # 测试配置
    config = DriverConfig()
    if yaml is not None:
        print(yaml.dump(config.to_dict(), default_flow_style=False))
    else:
        print(json.dumps(config.to_dict(), indent=2))
