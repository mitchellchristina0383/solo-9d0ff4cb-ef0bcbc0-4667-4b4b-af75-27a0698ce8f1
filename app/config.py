"""运行配置（全部可通过环境变量覆盖，便于测试与容器编排）。"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class NodeConfig:
    node_id: str
    name: str
    base_url: str
    capacity_mb: int


def _nodes_from_env() -> list[NodeConfig]:
    """格式: id|名称|base_url|容量MB，多个用逗号分隔。"""
    raw = os.environ.get(
        "PLATFORM_NODES",
        "node-a|模拟节点A|http://node-a:9001|8192,"
        "node-b|模拟节点B|http://node-b:9002|8192",
    )
    nodes: list[NodeConfig] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        nid, name, url, cap = item.split("|")
        nodes.append(NodeConfig(nid, name, url.rstrip("/"), int(cap)))
    return nodes


DB_PATH = os.environ.get("PLATFORM_DB", os.path.join("data", "platform.db"))
# 单次节点 HTTP 调用超时（失联/慢响应判定）
CALL_TIMEOUT = float(os.environ.get("NODE_CALL_TIMEOUT", "1.5"))
# 等待节点确认加载完成的上限，超时后保留占用转待核
LOAD_TIMEOUT = float(os.environ.get("LOAD_TIMEOUT", "30"))
# 后台轮询间隔
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "1.0"))
# 节点健康探测间隔
HEALTH_INTERVAL = float(os.environ.get("HEALTH_INTERVAL", "10.0"))

NODES: list[NodeConfig] = _nodes_from_env()
NODE_MAP: dict[str, NodeConfig] = {n.node_id: n for n in NODES}

# 仍占用显存的副本状态集合
RESERVED_STATUSES = (
    "PENDING", "LOADING", "LOADED", "TIMEOUT", "UNVERIFIED", "RELEASING",
)
# 活跃加载中的状态
ACTIVE_STATUSES = ("PENDING", "LOADING")
