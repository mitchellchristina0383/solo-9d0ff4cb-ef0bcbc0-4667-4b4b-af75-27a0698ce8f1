"""状态常量与中文展示映射。"""
from __future__ import annotations

# 部署状态
DEP_PENDING = "PENDING"        # 调度中
DEP_AVAILABLE = "AVAILABLE"    # 全部副本确认加载，可用
DEP_FAILED = "FAILED"          # 失败（可重试）
DEP_UNVERIFIED = "UNVERIFIED"  # 待核：超时/失联/对账不一致，保留占用

DEP_STATUS_CN = {
    DEP_PENDING: "调度中",
    DEP_AVAILABLE: "可用",
    DEP_FAILED: "失败",
    DEP_UNVERIFIED: "待核",
}

# 副本状态
REP_PENDING = "PENDING"        # 已预留显存，待下发加载
REP_LOADING = "LOADING"        # 已下发，等待节点确认
REP_LOADED = "LOADED"          # 节点确认加载完成
REP_LOAD_FAILED = "LOAD_FAILED"      # 节点明确返回加载失败（不保留占用）
REP_TIMEOUT = "TIMEOUT"              # 加载超时/调用失联，保留占用待核
REP_UNVERIFIED = "UNVERIFIED"        # 对账不一致，保留占用待核
REP_RELEASING = "RELEASING"          # 已请求卸载/撤销旧加载，等待确认释放
REP_RELEASED = "RELEASED"            # 对账确认已释放，显存可重新分配

REP_STATUS_CN = {
    REP_PENDING: "待加载",
    REP_LOADING: "加载中",
    REP_LOADED: "已加载",
    REP_LOAD_FAILED: "加载失败",
    REP_TIMEOUT: "超时待核",
    REP_UNVERIFIED: "待核",
    REP_RELEASING: "释放中",
    REP_RELEASED: "已释放",
}

REP_BADGE = {
    REP_PENDING: "secondary",
    REP_LOADING: "info",
    REP_LOADED: "success",
    REP_LOAD_FAILED: "danger",
    REP_TIMEOUT: "warning",
    REP_UNVERIFIED: "warning",
    REP_RELEASING: "info",
    REP_RELEASED: "secondary",
}
DEP_BADGE = {
    DEP_PENDING: "info",
    DEP_AVAILABLE: "success",
    DEP_FAILED: "danger",
    DEP_UNVERIFIED: "warning",
}
