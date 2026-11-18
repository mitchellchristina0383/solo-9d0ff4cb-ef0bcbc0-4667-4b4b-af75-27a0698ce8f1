"""本地推理节点模拟器（应用工厂，每个节点拥有独立状态）。

模拟能力：
- 固定显存容量（仅作信息上报，调度器以自身账本为准做预留）
- 可配置加载/卸载延迟、回执延迟（模拟慢节点与迟到回执）
- 故障模式：normal / slow（高延迟）/ fail（HTTP 503 故障）/ unreachable（超时失联）
- load_fail：加载请求直接返回 500
- corrupt_download：从平台拉取制品后损坏字节（一次性），核对摘要失败则拒绝加载
- download_timeout：拉取制品超时（一次性），结果不明 -> 保持 loading 占用显存待核
- 加载前从平台下载制品并逐字节核对 SHA-256，副本记录实际上报的摘要
- 每条副本记录带 epoch，旧 epoch 的迟到回调不会覆盖新状态（防复活）
"""
from __future__ import annotations

import asyncio
import hashlib
import os
import time
from typing import Any, Dict

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field


class ControlBody(BaseModel):
    mode: str | None = Field(None, pattern="^(normal|slow|fail|unreachable)$")
    load_delay: float | None = Field(None, ge=0, le=60)
    unload_delay: float | None = Field(None, ge=0, le=60)
    ack_delay: float | None = Field(None, ge=0, le=60)
    load_fail: bool | None = None
    corrupt_download: bool | None = None
    download_timeout: bool | None = None


# 测试可替换的 HTTP 客户端工厂（ASGI 传输下由测试注入）
_http_client_factory = None


async def _default_client_factory(timeout: float) -> httpx.AsyncClient:
    return httpx.AsyncClient(timeout=timeout)


async def get_http_client(timeout: float = 5.0) -> httpx.AsyncClient:
    factory = _http_client_factory or _default_client_factory
    return await factory(timeout)


def create_app(
    node_id: str,
    name: str,
    capacity_mb: int,
    default_load_delay: float = 0.4,
    default_unload_delay: float = 0.2,
    unreachable_sleep: float = 3600.0,
    download_timeout_sleep: float = 3600.0,
) -> FastAPI:
    app = FastAPI(title=f"节点模拟器 {node_id}")

    # rid -> {model, mem_mb, epoch, status: loading|loaded|unloaded|failed,
    #         artifact_id, expected_sha256, loaded_sha256}
    replicas: Dict[str, Dict[str, Any]] = {}
    control: Dict[str, Any] = {
        "mode": "normal",
        "load_delay": default_load_delay,
        "unload_delay": default_unload_delay,
        "ack_delay": 0.0,
        "load_fail": False,
        "corrupt_download": False,
        "download_timeout": False,
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
            rec["loaded_sha256"] = None

    async def fetch_artifact(download_url: str, expected_sha256: str) -> tuple[bytes | None, str | None, str | None]:
        """从平台拉取制品并核对字节。

        返回 (data, actual_sha256, error)：
        - 成功：data 与 actual_sha256 有值，error 为 None
        - 摘要不符（损坏）：actual_sha256 有值，error 说明期望/实际摘要
        - 下载超时/连接失败：data 为 None，error 说明节点状态不可知
        """
        if control["download_timeout"]:
            control["download_timeout"] = False  # 一次性故障
            if download_timeout_sleep > 0:
                await asyncio.sleep(download_timeout_sleep)
            # 下载在途超时：节点无法确认自己是否持有完整文件，保持 loading 占用待核
            return None, None, "制品下载超时，加载结果未知（保留显存待核）"

        client = await get_http_client()
        try:
            resp = await client.get(download_url)
            if resp.status_code != 200:
                return None, None, f"平台制品下载失败(HTTP {resp.status_code})"
            data = resp.content
        except httpx.TimeoutException as exc:
            # 下载在途超时：平台是否已分发、本地是否落盘均不可知，调用方保持占用待核
            return None, None, f"制品下载超时，加载结果未知（保留显存待核）: {exc.__class__.__name__}"
        except (httpx.HTTPError, OSError) as exc:
            return None, None, f"制品下载连接失败（结果未知，保留显存待核）: {exc.__class__.__name__}"

        if control["corrupt_download"]:
            control["corrupt_download"] = False  # 一次性故障
            if data:
                # 翻转/截断字节，模拟传输或落盘损坏
                mutated = bytearray(data)
                mutated[0] = (mutated[0] ^ 0xFF) & 0xFF
                if len(mutated) > 1:
                    mutated[-1] = (mutated[-1] ^ 0x01) & 0xFF
                data = bytes(mutated)

        actual = hashlib.sha256(data).hexdigest()
        if expected_sha256 and actual != expected_sha256:
            return data, actual, (
                f"制品字节核对失败：实际 sha256={actual[:12]}… 与期望 "
                f"{expected_sha256[:12]}… 不符，拒绝加载损坏文件"
            )
        return data, actual, None

    @app.post("/models/{model_name}/load")
    async def load(model_name: str, request: Request):
        params = request.query_params
        rid = params.get("replica")
        epoch = int(params.get("epoch", "1"))
        mem_mb = int(params.get("mem_mb", "0"))
        artifact_id = params.get("artifact_id", "")
        expected_sha256 = params.get("expected_sha256", "")
        download_url = params.get("download_url", "")
        if not rid:
            return JSONResponse({"detail": "缺少 replica 参数"}, status_code=400)

        if control["load_fail"]:
            replicas[rid] = {
                "model": model_name, "mem_mb": mem_mb, "epoch": epoch, "status": "failed",
                "artifact_id": artifact_id, "expected_sha256": expected_sha256,
                "loaded_sha256": None,
            }
            await asyncio.sleep(control["ack_delay"])
            return JSONResponse(
                {"detail": "节点模拟加载失败", "replica": rid}, status_code=500
            )

        existing = replicas.get(rid)
        if existing and existing["epoch"] == epoch and existing["status"] == "loaded":
            return {"replica": rid, "epoch": epoch, "status": "loaded",
                    "loaded_sha256": existing.get("loaded_sha256")}

        # 先登记 loading（占住显存语义），再从平台拉取制品
        replicas[rid] = {
            "model": model_name, "mem_mb": mem_mb, "epoch": epoch, "status": "loading",
            "artifact_id": artifact_id, "expected_sha256": expected_sha256,
            "loaded_sha256": None,
        }

        if download_url:
            data, actual_sha, dl_error = await fetch_artifact(download_url, expected_sha256)
            if dl_error is not None:
                if actual_sha:
                    # 损坏：节点明确拒绝加载（显式失败，确认未占用可生效副本）
                    replicas[rid]["status"] = "failed"
                    replicas[rid]["loaded_sha256"] = actual_sha
                    await asyncio.sleep(control["ack_delay"])
                    return JSONResponse(
                        {"detail": dl_error, "replica": rid,
                         "expected_sha256": expected_sha256, "actual_sha256": actual_sha},
                        status_code=422,
                    )
                # 下载超时/连接异常：结果不可知，保持 loading 与占用，待平台超时对账
                await asyncio.sleep(control["ack_delay"])
                return JSONResponse(
                    {"replica": rid, "epoch": epoch, "status": "loading",
                     "detail": dl_error}, status_code=202
                )
            replicas[rid]["loaded_sha256"] = actual_sha

        # 同一 rid 的新 epoch 直接覆盖，节点已“忘记”旧副本
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
            corrupt_download=False,
            download_timeout=False,
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
    download_timeout_sleep=float(os.environ.get("DOWNLOAD_TIMEOUT_SLEEP", "3600")),
)
