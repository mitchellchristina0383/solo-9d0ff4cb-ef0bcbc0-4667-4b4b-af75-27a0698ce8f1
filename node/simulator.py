"""本地推理节点模拟器（应用工厂，每个节点拥有独立状态）。

模拟能力：
- 固定显存容量（仅作信息上报，调度器以自身账本为准做预留）
- 可配置加载/卸载延迟、回执延迟（模拟慢节点与迟到回执）
- 故障模式：normal / slow（高延迟）/ fail（HTTP 503 故障）/ unreachable（超时失联）
- load_fail：加载请求直接返回 500
- 每条副本记录带 epoch，旧 epoch 的迟到回调不会覆盖新状态（防复活）
"""
from __future__ import annotations

import asyncio
import os
import time
from typing import Any, Dict

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field


class ControlBody(BaseModel):
    mode: str | None = Field(None, pattern="^(normal|slow|fail|unreachable)$")
    load_delay: float | None = Field(None, ge=0, le=60)
    unload_delay: float | None = Field(None, ge=0, le=60)
    ack_delay: float | None = Field(None, ge=0, le=60)
    load_fail: bool | None = None


def create_app(
    node_id: str,
    name: str,
    capacity_mb: int,
    default_load_delay: float = 0.4,
    default_unload_delay: float = 0.2,
    unreachable_sleep: float = 3600.0,
) -> FastAPI:
    app = FastAPI(title=f"节点模拟器 {node_id}")

    # rid -> {model, mem_mb, epoch, status: loading|loaded|unloaded|failed}
    replicas: Dict[str, Dict[str, Any]] = {}
    control: Dict[str, Any] = {
        "mode": "normal",
        "load_delay": default_load_delay,
        "unload_delay": default_unload_delay,
        "ack_delay": 0.0,
        "load_fail": False,
    }
    tasks: set[asyncio.Task] = set()

    def schedule(delay: float, coro):
        task = asyncio.create_task(coro)
        tasks.add(task)
        task.add_done_callback(tasks.discard)
        return task

    @app.middleware("http")
    async def fault_middleware(request: Request, call_next):
        path = request.url.path
        # 控制与带外健康通道在任何故障模式下都保持可用（模拟独立管理网口），
        # 否则健康监测无法观测到“失联 -> 恢复”的状态边沿
        if path in ("/control", "/reset", "/health", "/docs", "/openapi.json"):
            return await call_next(request)

        mode = control["mode"]
        if mode == "unreachable":
            # 模拟数据面失联：挂起不响应，直到客户端超时（真实服务器下中间件抛异常
            # 会被兜底成 500，无法区分于显式故障；长挂起才能稳定复现超时语义）。
            # unreachable_sleep<=0 时退化为直接抛连接异常（ASGI 测试传输用）。
            if unreachable_sleep <= 0:
                raise ConnectionError(f"节点 {node_id} 模拟失联：连接被拒绝")
            await asyncio.sleep(unreachable_sleep)
            raise ConnectionError(f"节点 {node_id} 模拟失联：无响应后连接中断")
        if mode == "fail":
            return JSONResponse({"detail": f"节点 {node_id} 处于故障模式"}, status_code=503)
        if mode == "slow":
            await asyncio.sleep(max(control["load_delay"], 1.5))
        return await call_next(request)

    @app.get("/health")
    async def health():
        # 健康端点反映数据面故障模式：
        # unreachable -> 长挂起直至探测超时（与数据面失联语义一致）；
        # fail -> 503 显式故障；控制通道(/control /reset)始终独立可用
        if control["mode"] == "unreachable":
            if unreachable_sleep <= 0:
                raise ConnectionError(f"节点 {node_id} 健康探测失败：数据面失联")
            await asyncio.sleep(unreachable_sleep)
            raise ConnectionError(f"节点 {node_id} 健康探测失败：数据面失联")
        if control["mode"] == "fail":
            return JSONResponse(
                {"node_id": node_id, "name": name, "status": "error",
                 "capacity_mb": capacity_mb, "mode": control["mode"]}, status_code=503)
        return {
            "node_id": node_id, "name": name, "status": "online",
            "capacity_mb": capacity_mb, "mode": control["mode"], "ts": time.time(),
        }

    async def finish_load(rid: str, epoch: int, delay: float):
        await asyncio.sleep(delay)
        rec = replicas.get(rid)
        # 关键：仅当记录仍属于本次 epoch 且仍在 loading 时才落地，迟到回调不复活旧副本
        if rec and rec["epoch"] == epoch and rec["status"] == "loading":
            rec["status"] = "loaded"

    async def finish_unload(rid: str, epoch: int, delay: float):
        await asyncio.sleep(delay)
        rec = replicas.get(rid)
        if rec and rec["epoch"] == epoch and rec["status"] != "unloaded":
            rec["status"] = "unloaded"

    @app.post("/models/{model_name}/load")
    async def load(model_name: str, request: Request):
        params = request.query_params
        rid = params.get("replica")
        epoch = int(params.get("epoch", "1"))
        mem_mb = int(params.get("mem_mb", "0"))
        if not rid:
            return JSONResponse({"detail": "缺少 replica 参数"}, status_code=400)

        if control["load_fail"]:
            replicas[rid] = {
                "model": model_name, "mem_mb": mem_mb, "epoch": epoch, "status": "failed",
            }
            await asyncio.sleep(control["ack_delay"])
            return JSONResponse(
                {"detail": "节点模拟加载失败", "replica": rid}, status_code=500
            )

        existing = replicas.get(rid)
        if existing and existing["epoch"] == epoch and existing["status"] == "loaded":
            return {"replica": rid, "epoch": epoch, "status": "loaded"}

        # 同一 rid 的新 epoch 直接覆盖，节点已“忘记”旧副本
        replicas[rid] = {
            "model": model_name, "mem_mb": mem_mb, "epoch": epoch, "status": "loading",
        }
        schedule(control["load_delay"], finish_load(rid, epoch, control["load_delay"]))
        await asyncio.sleep(control["ack_delay"])
        return JSONResponse(
            {"replica": rid, "epoch": epoch, "status": "loading"}, status_code=202
        )

    @app.post("/models/{model_name}/unload")
    async def unload(model_name: str, request: Request):
        rid = request.query_params.get("replica")
        epoch = int(request.query_params.get("epoch", "1"))
        rec = replicas.get(rid)
        if not rec or rec["epoch"] != epoch:
            # 节点已无该 epoch 的记录（重启/已释放），对账时可直接视为不存在
            return {"replica": rid, "epoch": epoch, "status": "unknown"}
        if rec["status"] in ("loaded", "loading"):
            rec["status"] = "unloading"
            schedule(control["unload_delay"],
                     finish_unload(rid, epoch, control["unload_delay"]))
        await asyncio.sleep(control["ack_delay"])
        return JSONResponse(
            {"replica": rid, "epoch": epoch, "status": "unloading"}, status_code=202
        )

    @app.get("/models/by-replica/{rid}")
    async def query_replica(rid: str, epoch: int = 1):
        rec = replicas.get(rid)
        if not rec or rec["epoch"] != epoch:
            return JSONResponse(
                {"replica": rid, "epoch": epoch, "status": "unknown"}, status_code=404
            )
        return {"replica": rid, "epoch": epoch, **rec}

    @app.get("/state")
    async def full_state():
        return {"node_id": node_id, "control": control, "replicas": replicas}

    @app.post("/control")
    async def set_control(body: ControlBody):
        for key, value in body.model_dump(exclude_none=True).items():
            control[key] = value
        return {"node_id": node_id, "control": control}

    @app.post("/reset")
    async def reset():
        replicas.clear()
        control.update(
            mode="normal",
            load_delay=default_load_delay,
            unload_delay=default_unload_delay,
            ack_delay=0.0,
            load_fail=False,
        )
        return {"node_id": node_id, "reset": True}

    # 暴露内部句柄，供测试直接检查/重置
    app.state.replicas = replicas
    app.state.control = control
    return app


# uvicorn node.simulator:app 入口：单节点容器按环境变量构建
app = create_app(
    node_id=os.environ.get("NODE_ID", "node-a"),
    name=os.environ.get("NODE_NAME", "模拟节点"),
    capacity_mb=int(os.environ.get("CAPACITY_MB", "8192")),
    default_load_delay=float(os.environ.get("DEFAULT_LOAD_DELAY", "0.4")),
    default_unload_delay=float(os.environ.get("DEFAULT_UNLOAD_DELAY", "0.2")),
    unreachable_sleep=float(os.environ.get("UNREACHABLE_SLEEP", "3600")),
)
