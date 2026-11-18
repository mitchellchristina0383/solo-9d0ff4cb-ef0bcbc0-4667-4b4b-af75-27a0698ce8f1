"""FastAPI 入口：REST API、后台轮询、节点故障注入代理、网页界面。"""
from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from typing import Literal

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from . import config, node_client, service


class DeployBody(BaseModel):
    model_name: str = Field(min_length=1, max_length=64)
    mem_per_replica_mb: int = Field(gt=0, le=1_000_000)
    replica_count: Literal[1, 2]


class ControlBody(BaseModel):
    mode: Literal["normal", "slow", "fail", "unreachable"] | None = None
    load_delay: float | None = Field(default=None, ge=0, le=60)
    unload_delay: float | None = Field(default=None, ge=0, le=60)
    ack_delay: float | None = Field(default=None, ge=0, le=60)
    load_fail: bool | None = None


async def _poll_loop() -> None:
    while True:
        try:
            await service.poll_loading()
            await service.poll_releasing()
        except Exception as exc:  # 后台循环不能因单次异常退出
            print(f"[poll] {exc.__class__.__name__}: {exc}")
        await asyncio.sleep(config.POLL_INTERVAL)


async def _health_loop() -> None:
    while True:
        try:
            await service.health_monitor_once()
        except Exception as exc:
            print(f"[health] {exc.__class__.__name__}: {exc}")
        await asyncio.sleep(config.HEALTH_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    service.init_db()
    poller = asyncio.create_task(_poll_loop())
    health = asyncio.create_task(_health_loop())
    # 启动即做一次健康探测
    await service.health_monitor_once()
    yield
    poller.cancel()
    health.cancel()
    await node_client.close()


app = FastAPI(title="推理平台副本调度值班台", lifespan=lifespan)

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
async def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


@app.get("/api/nodes")
async def get_nodes():
    return service.node_book()


@app.post("/api/deployments", status_code=201)
async def create_deployment(body: DeployBody):
    return service.create_deployment(
        body.model_name.strip(), body.mem_per_replica_mb, body.replica_count
    )


@app.get("/api/deployments")
async def list_deployments():
    return service.list_deployments()


@app.get("/api/deployments/{dep_id}")
async def get_deployment(dep_id: str):
    return service.get_deployment(dep_id)


@app.post("/api/deployments/{dep_id}/retry")
async def retry_deployment(dep_id: str):
    return service.retry_deployment(dep_id)


@app.post("/api/deployments/{dep_id}/reconcile")
async def reconcile(dep_id: str):
    return await service.reconcile(dep_id)


@app.get("/api/events")
async def events(limit: int = 30):
    return service.recent_events(limit)


@app.post("/api/nodes/{node_id}/control")
async def node_control(node_id: str, body: ControlBody):
    """代理故障注入到对应模拟节点。"""
    if node_id not in config.NODE_MAP:
        raise HTTPException(404, "节点不存在")
    url = config.NODE_MAP[node_id].base_url + "/control"
    payload = {k: v for k, v in body.model_dump().items() if v is not None}
    try:
        c = await node_client.client()
        r = await c.post(url, json=payload)
        return r.json()
    except httpx.HTTPError as exc:
        raise HTTPException(502, f"控制指令送达失败：{exc.__class__.__name__}")


@app.post("/api/nodes/{node_id}/reset")
async def node_reset(node_id: str):
    if node_id not in config.NODE_MAP:
        raise HTTPException(404, "节点不存在")
    url = config.NODE_MAP[node_id].base_url + "/reset"
    try:
        c = await node_client.client()
        r = await c.post(url)
        return r.json()
    except httpx.HTTPError as exc:
        raise HTTPException(502, f"重置指令送达失败：{exc.__class__.__name__}")
