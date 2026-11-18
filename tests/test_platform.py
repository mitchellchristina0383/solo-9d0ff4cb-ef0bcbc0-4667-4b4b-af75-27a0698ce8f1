"""端到端自测：基于 ASGI transport 同时跑调度器与两台节点模拟器。

运行：
  PYTHONPATH=/tmp/pylibs PLATFORM_DB=/tmp/platform-test/test.db \
  python3 -m pytest tests/ -v
"""
from __future__ import annotations

import asyncio
import hashlib
import os
import uuid

import httpx
import pytest

# 必须在导入 app 之前配置环境
TEST_DIR = f"/tmp/platform-test-{uuid.uuid4().hex[:8]}"
os.environ["PLATFORM_DB"] = os.path.join(TEST_DIR, "test.db")
os.environ["ARTIFACT_DIR"] = os.path.join(TEST_DIR, "artifacts")
os.environ["PLATFORM_BASE_URL"] = "http://platform"
os.environ["PLATFORM_NODES"] = (
    "node-a|模拟节点A|http://testnode-a:9001|8192,"
    "node-b|模拟节点B|http://testnode-b:9002|8192"
)
os.environ["NODE_CALL_TIMEOUT"] = "1.0"
os.environ["LOAD_TIMEOUT"] = "4"
os.environ["POLL_INTERVAL"] = "0.2"
os.environ["HEALTH_INTERVAL"] = "3600"  # 测试中手动驱动健康监测

from app.main import app as platform_app          # noqa: E402
from app import config, service                    # noqa: E402
from node import simulator                         # noqa: E402


class RoutingTransport(httpx.AsyncBaseTransport):
    """按 host 把请求路由到对应 ASGI 应用，无需真实端口。"""

    def __init__(self, sim_a, sim_b, platform):
        self._a = httpx.ASGITransport(app=sim_a)
        self._b = httpx.ASGITransport(app=sim_b)
        self._p = httpx.ASGITransport(app=platform)
        self.app = sim_a  # 供用例访问模拟器内部状态

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        host = request.url.host
        if host == "testnode-a":
            return await self._a.handle_async_request(request)
        if host == "testnode-b":
            return await self._b.handle_async_request(request)
        return await self._p.handle_async_request(request)


@pytest.fixture
async def client():
    # 每台模拟器独立实例、独立内存态；每用例全新 SQLite 库
    os.makedirs(TEST_DIR, exist_ok=True)
    if os.path.exists(os.environ["PLATFORM_DB"]):
        os.remove(os.environ["PLATFORM_DB"])
    from app import db as db_mod
    db_mod._conn = None
    # ASGI 测试传输下用 0 表示直接抛连接异常；真实 uvicorn 下用长挂起复现超时
    sim_a = simulator.create_app("node-a", "模拟节点A", 8192, 0.3, 0.15,
                                 unreachable_sleep=0, download_timeout_sleep=0)
    sim_b = simulator.create_app("node-b", "模拟节点B", 8192, 0.3, 0.15,
                                 unreachable_sleep=0, download_timeout_sleep=0)
    transport = RoutingTransport(sim_a, sim_b, platform_app)
    shared_client = httpx.AsyncClient(base_url="http://platform",
                                      transport=transport, timeout=10)

    # 节点模拟器回平台拉取制品时复用同一 ASGI 传输
    async def test_client_factory(timeout: float):
        return shared_client
    simulator._http_client_factory = test_client_factory

    async with shared_client as c:
        # 手动初始化（不走 lifespan），并替换服务里的全局 httpx client
        service.init_db()
        await service.node_client.close()
        service.node_client._client = httpx.AsyncClient(
            base_url="http://platform", transport=transport, timeout=1.0
        )
        # 启动与生产相同的后台轮询（含健康监测，驱动失联检测与恢复自动对账）
        async def poll_loop():
            while True:
                await service.poll_loading()
                await service.poll_loaded_digests()
                await service.poll_releasing()
                await asyncio.sleep(0.2)

        async def health_loop():
            while True:
                await service.health_monitor_once()
                await asyncio.sleep(0.5)

        poller = asyncio.create_task(poll_loop())
        health = asyncio.create_task(health_loop())
        yield c
        poller.cancel()
        health.cancel()
    simulator._http_client_factory = None
    await service.node_client.close()


async def wait_for(client, dep_id, statuses, timeout=12.0):
    deadline = asyncio.get_event_loop().time() + timeout
    last = None
    while asyncio.get_event_loop().time() < deadline:
        d = await client.get(f"/api/deployments/{dep_id}")
        last = d.json()
        if last["status"] in statuses:
            return last
        await asyncio.sleep(0.25)
    raise AssertionError(f"部署 {dep_id} 未进入 {statuses}，最后状态={last['status']} / {last['reason']}")


def node_free(book, nid):
    return next(n["free_mb"] for n in book if n["node_id"] == nid)


# ---------------------------------------------------------------- 制品助手
async def upload_artifact(client, model_name: str, payload: bytes,
                          filename: str = "model.bin"):
    r = await client.post(
        "/api/artifacts",
        data={"model_name": model_name},
        files={"file": (filename, payload, "application/octet-stream")},
    )
    assert r.status_code == 201, r.text
    return r.json()


async def deploy(client, model_name, mem, count, payload=None, **extra):
    """统一部署入口：自动登记制品并按所选版本提交。"""
    if payload is None:
        payload = f"weights-of-{model_name}-{uuid.uuid4().hex[:6]}".encode()
    art = await upload_artifact(client, model_name, payload)
    body = {"model_name": model_name, "mem_per_replica_mb": mem,
            "replica_count": count, "artifact_id": art["id"], **extra}
    r = await client.post("/api/deployments", json=body)
    return r, art


@pytest.mark.asyncio
async def test_01_full_deploy_flow_and_available(client):
    # 两台节点初始各 8192
    book = (await client.get("/api/nodes")).json()
    assert {n["node_id"] for n in book} == {"node-a", "node-b"}
    assert all(n["free_mb"] == 8192 for n in book)

    r, art = await deploy(client, "llama3-8b", 2048, 2)
    assert r.status_code == 201, r.text
    dep_id = r.json()["id"]

    dep = await wait_for(client, dep_id, {"AVAILABLE"})
    # 两个副本必须分处不同节点
    nodes = {rep["node_id"] for rep in dep["replicas"]}
    assert nodes == {"node-a", "node-b"}
    assert all(rep["status"] == "LOADED" for rep in dep["replicas"])
    # 部署固定所选版本，副本上报的实际摘要与版本摘要一致
    assert dep["artifact_version"] == art["version"]
    assert dep["expected_sha256"] == art["sha256"]
    for rep in dep["replicas"]:
        assert rep["loaded_sha256"] == art["sha256"]
        assert rep["digest_match"] is True

    book = (await client.get("/api/nodes")).json()
    assert all(n["used_mb"] == 2048 and n["free_mb"] == 6144 for n in book)


@pytest.mark.asyncio
async def test_02_concurrent_reservation_never_overcommits(client):
    # 每台 8192：每副本 3000，双副本部署每台占 3000；并发 3 个 -> 只有 2 个能成功
    async def submit(i):
        r, _ = await deploy(client, f"m{i}", 3000, 2)
        return r

    results = await asyncio.gather(*(submit(i) for i in range(3)))
    codes = [r.status_code for r in results]
    assert codes.count(201) == 2, codes
    assert codes.count(409) == 1, codes

    book = (await client.get("/api/nodes")).json()
    for n in book:
        assert n["used_mb"] <= n["capacity_mb"]
        assert n["used_mb"] == 6000, n  # 2 个部署 × 3000
        assert n["free_mb"] == 2192

    # 显存不足时再提交应明确拒绝
    r, _ = await deploy(client, "big", 4000, 1)
    assert r.status_code == 409
    assert "显存不足" in r.json()["detail"]


@pytest.mark.asyncio
async def test_03_load_fail_releases_reservation_and_retry(client):
    # 让节点 A 加载失败；单副本部署落到剩余显存最多的节点（A/B 相同，排序后取 A）
    await client.post("/api/nodes/node-a/control", json={"load_fail": True})
    r, art = await deploy(client, "fail-model", 1024, 1)
    dep_id = r.json()["id"]
    dep = await wait_for(client, dep_id, {"FAILED"})
    rep = dep["replicas"][0]
    assert rep["status"] == "LOAD_FAILED"
    # 失败不占显存
    book = (await client.get("/api/nodes")).json()
    assert all(n["used_mb"] == 0 for n in book)

    # 清除故障后重试，应成功；重试固定使用原制品版本与摘要
    await client.post("/api/nodes/node-a/control",
                      json={"load_fail": False, "mode": "normal"})
    r2 = await client.post(f"/api/deployments/{dep_id}/retry")
    assert r2.status_code == 200
    dep2 = await wait_for(client, dep_id, {"AVAILABLE"})
    assert dep2["epoch"] == 2 and dep2["attempts"] == 2
    assert dep2["artifact_id"] == art["id"]
    assert dep2["expected_sha256"] == art["sha256"]
    # 旧 epoch 副本保持终态，不复活
    old = [x for x in dep2["replicas"] if x["epoch"] == 1]
    new = [x for x in dep2["replicas"] if x["epoch"] == 2]
    assert old[0]["status"] == "LOAD_FAILED" and new[0]["status"] == "LOADED"
    assert new[0]["loaded_sha256"] == art["sha256"]


@pytest.mark.asyncio
async def test_04_unreachable_timeout_keeps_hold_then_reconcile_releases(client):
    await client.post("/api/nodes/node-b/control", json={"mode": "unreachable"})
    r, _ = await deploy(client, "ghost", 1500, 2)
    dep_id = r.json()["id"]

    # A 正常加载，B 失联 -> 部署 UNVERIFIED，B 上显存保留占用
    dep = await wait_for(client, dep_id, {"UNVERIFIED"}, timeout=12)
    await service.health_monitor_once()  # 驱动一次探测周期，确认失联被健康监测捕获
    by_node = {x["node_id"]: x for x in dep["replicas"]}
    assert by_node["node-b"]["status"] == "TIMEOUT"
    # A 可能已加载或仍在加载，但绝不能被标记为失败/释放
    assert by_node["node-a"]["status"] in {"LOADING", "LOADED"}

    book = (await client.get("/api/nodes")).json()
    b = next(n for n in book if n["node_id"] == "node-b")
    assert b["used_mb"] == 1500  # 待核期间仍占用，不可被新部署用掉

    # 该显存不能被重新分配：节点 B 剩余不足时提交双副本必被拒
    r2, _ = await deploy(client, "blocked", 7000, 2)
    assert r2.status_code == 409

    # 节点恢复（失联期间请求从未到达模拟器，B 上实际没有该副本）
    await client.post("/api/nodes/node-b/control", json={"mode": "normal"})
    await service.health_monitor_once()  # 触发恢复自动对账
    dep = await wait_for(client, dep_id, {"AVAILABLE", "FAILED"}, timeout=12)
    # B 上实际没有副本 -> 对账释放 -> 该副本 LOAD_FAILED/RELEASED，部署 FAILED
    assert dep["status"] == "FAILED"
    by_node = {x["node_id"]: x for x in dep["replicas"]}
    assert by_node["node-b"]["status"] in {"RELEASED", "LOAD_FAILED"}
    book = (await client.get("/api/nodes")).json()
    b = next(n for n in book if n["node_id"] == "node-b")
    assert b["used_mb"] == 0  # 确认释放后显存可重新分配


@pytest.mark.asyncio
async def test_05_late_ack_must_not_revive_old_replica(client):
    # 场景：节点 A 在加载请求在途时失联，调度器保留占用转待核；
    # 之后节点其实完成了加载（迟到回执/延迟状态变化），恢复后补确认；
    # 再撤销旧加载、重试 epoch=2，旧 epoch 的迟到状态绝不能复活旧副本。

    # 1) 失联期间发起部署 -> TIMEOUT 待核，占用保留
    await client.post("/api/nodes/node-a/control", json={"mode": "unreachable"})
    await service.health_monitor_once()  # 确认健康监测捕获失联（online=False）
    r, art = await deploy(client, "late", 512, 1)
    dep_id = r.json()["id"]
    dep = await wait_for(client, dep_id, {"UNVERIFIED"}, timeout=10)
    rid = dep["replicas"][0]["id"]
    assert dep["replicas"][0]["status"] == "TIMEOUT"
    book = (await client.get("/api/nodes")).json()
    assert node_free(book, "node-a") == 8192 - 512

    # 2) 节点恢复，但它在“失联期间”其实把副本加载好了（模拟迟到回执落地）。
    #    先写入迟到状态再恢复，避免健康循环的自动对账抢先判定为无残留。
    sim_state = client._transport.app.state  # type: ignore[attr-defined]
    sim_state.replicas[rid] = {
        "model": "late", "mem_mb": 512, "epoch": 1, "status": "loaded",
        "artifact_id": art["id"], "expected_sha256": art["sha256"],
        "loaded_sha256": art["sha256"],
    }
    await client.post("/api/nodes/node-a/control", json={"mode": "normal"})
    await service.health_monitor_once()  # 探测恢复边沿，触发自动对账
    # 对账补确认 -> AVAILABLE（同一 epoch 的合法补确认，不是复活）
    (await client.post(f"/api/deployments/{dep_id}/reconcile")).json()
    dep = await wait_for(client, dep_id, {"AVAILABLE"}, timeout=8)
    assert dep["replicas"][0]["status"] == "LOADED"
    assert dep["replicas"][0]["digest_match"] is True

    # 3) 再次失联并人工撤销旧加载，恢复后对账确认释放
    await client.post("/api/nodes/node-a/control", json={"mode": "unreachable"})
    from app import models
    import app.db as db_mod
    with db_mod.tx() as conn:
        service._set_replica(conn, rid, models.REP_UNVERIFIED, error="演练：失联待核")
        service._evaluate(conn, dep_id)
    await service.request_release(rid, "演练：撤销旧加载")
    await client.post("/api/nodes/node-a/control", json={"mode": "normal"})
    deadline = asyncio.get_event_loop().time() + 15
    dep = None
    while asyncio.get_event_loop().time() < deadline:
        await client.post(f"/api/deployments/{dep_id}/reconcile")
        dep = (await client.get(f"/api/deployments/{dep_id}")).json()
        if all(x["status"] in {"RELEASED", "LOAD_FAILED"} for x in dep["replicas"]):
            break
        await asyncio.sleep(0.25)
    assert dep is not None and all(
        x["status"] in {"RELEASED", "LOAD_FAILED"} for x in dep["replicas"])

    # 4) 模拟旧 epoch 的“迟到回执”：节点侧再次出现 epoch=1 loaded
    sim_state.replicas[rid] = {
        "model": "late", "mem_mb": 512, "epoch": 1, "status": "loaded",
        "artifact_id": art["id"], "expected_sha256": art["sha256"],
        "loaded_sha256": art["sha256"],
    }
    # 重试 -> epoch=2；调度器只认当前 epoch，旧副本终态不变
    r3 = await client.post(f"/api/deployments/{dep_id}/retry")
    assert r3.status_code == 200
    dep = await wait_for(client, dep_id, {"AVAILABLE"}, timeout=10)
    assert dep["epoch"] == 2
    old = [x for x in dep["replicas"] if x["epoch"] == 1]
    new = [x for x in dep["replicas"] if x["epoch"] == 2]
    assert all(x["status"] in {"RELEASED", "LOAD_FAILED"} for x in old)  # 不复活
    assert new[0]["status"] == "LOADED"
    # 新副本使用不同 rid，节点侧旧记录与新记录互不干扰
    assert new[0]["id"] != rid


@pytest.mark.asyncio
async def test_06_reconcile_failure_stays_unverified_with_reason(client):
    # B 失联 -> 待核；保持失联时手动对账，必须仍是待核并说明原因
    await client.post("/api/nodes/node-b/control", json={"mode": "unreachable"})
    r, _ = await deploy(client, "nv", 900, 2)
    dep_id = r.json()["id"]
    dep = await wait_for(client, dep_id, {"UNVERIFIED"}, timeout=12)
    rec = (await client.post(f"/api/deployments/{dep_id}/reconcile")).json()
    assert rec["status"] == "UNVERIFIED"
    assert any("失联" in note or "不可达" in note for note in rec["reconcile_notes"])
    # 显存仍保留
    book = (await client.get("/api/nodes")).json()
    b = next(n for n in book if n["node_id"] == "node-b")
    assert b["used_mb"] == 900


@pytest.mark.asyncio
async def test_07_recovery_revokes_stale_load_then_confirms_release(client):
    # 正常部署成功后，节点 B 失联，调度器侧强制转待核并撤销；
    # 恢复后对账必须撤销节点上的旧加载，确认释放后才可重新分配
    r, _ = await deploy(client, "svc", 2500, 2)
    dep_id = r.json()["id"]
    dep = await wait_for(client, dep_id, {"AVAILABLE"}, timeout=10)
    b_rep = next(x for x in dep["replicas"] if x["node_id"] == "node-b")
    rid = b_rep["id"]

    await client.post("/api/nodes/node-b/control", json={"mode": "unreachable"})
    # 直接通过服务层把 B 副本置为待核并请求释放（模拟失联期间人工处置）
    from app import models
    import app.db as db_mod
    with db_mod.tx() as conn:
        service._set_replica(conn, rid, models.REP_UNVERIFIED, error="演练：失联待核")
        service._evaluate(conn, dep_id)
    await service.request_release(rid, "演练：撤销旧加载")

    # 失联中对账不能释放
    rec = (await client.post(f"/api/deployments/{dep_id}/reconcile")).json()
    assert rec["status"] == "UNVERIFIED"
    book = (await client.get("/api/nodes")).json()
    assert node_free(book, "node-b") == 8192 - 2500

    # 恢复：节点上旧加载仍在（loaded），自动/手动对账应撤销并最终确认释放。
    # 自动对账与手动对账可能由不同轮次触发，终态为 RELEASED（确认释放）或
    # LOAD_FAILED（对账确认节点无残留）——两者都不再占用显存，语义等价。
    await client.post("/api/nodes/node-b/control", json={"mode": "normal"})
    await service.health_monitor_once()
    deadline = asyncio.get_event_loop().time() + 15
    while asyncio.get_event_loop().time() < deadline:
        dep = (await client.get(f"/api/deployments/{dep_id}")).json()
        b = next(x for x in dep["replicas"] if x["node_id"] == "node-b")
        if b["status"] in {"RELEASED", "LOAD_FAILED"}:
            break
        await client.post(f"/api/deployments/{dep_id}/reconcile")
        await asyncio.sleep(0.25)
    else:
        raise AssertionError("恢复后副本未在时限内确认释放")
    dep = (await client.get(f"/api/deployments/{dep_id}")).json()
    b = next(x for x in dep["replicas"] if x["node_id"] == "node-b")
    assert b["status"] in {"RELEASED", "LOAD_FAILED"}, dep
    book = (await client.get("/api/nodes")).json()
    assert node_free(book, "node-b") == 8192  # 关键：确认释放后显存全额可用

    # 节点侧旧记录应为 unloaded，且重新部署能拿到显存
    r2, _ = await deploy(client, "new-tenant", 7000, 1)
    assert r2.status_code == 201, r2.text


# ---------------------------------------------------------------- 制品与版本交错
@pytest.mark.asyncio
async def test_08_artifact_registry_immutable_and_versioned(client):
    payload_v1 = b"model weights revision one"
    a1 = await upload_artifact(client, "mix", payload_v1, "mix-v1.bin")
    assert a1["version"] == 1
    assert a1["sha256"] == hashlib.sha256(payload_v1).hexdigest()
    assert a1["size_bytes"] == len(payload_v1)

    # 相同字节重复上传：拒绝并回指既有版本，不产生新版本
    dup = await client.post(
        "/api/artifacts",
        data={"model_name": "mix"},
        files={"file": ("again.bin", payload_v1, "application/octet-stream")},
    )
    assert dup.status_code == 409
    assert "v1" in dup.json()["detail"]

    # 同名新字节 -> v2，v1 记录保持不变（不可改写）
    payload_v2 = b"model weights revision two with more bytes"
    a2 = await upload_artifact(client, "mix", payload_v2, "mix-v2.bin")
    assert a2["version"] == 2 and a2["sha256"] != a1["sha256"]
    listing = (await client.get("/api/artifacts?model_name=mix")).json()
    assert {a["version"] for a in listing} == {1, 2}
    still_v1 = next(a for a in listing if a["version"] == 1)
    assert still_v1["sha256"] == a1["sha256"]  # 旧版本摘要未被改写

    # 内容分发：节点拉到的字节与登记摘要一致
    r = await client.get(f"/api/artifacts/{a1['id']}/content")
    assert r.status_code == 200
    assert r.content == payload_v1
    assert r.headers["x-sha256"] == a1["sha256"]

    # 未登记制品的模型不能部署
    r = await client.post("/api/deployments", json={
        "model_name": "ghost-model", "mem_per_replica_mb": 128, "replica_count": 1})
    assert r.status_code == 409 and "尚未登记" in r.json()["detail"]

    # 显式选择 v1 部署；随后再传 v3，在途/已有部署所选版本不得被改变
    r = await client.post("/api/deployments", json={
        "model_name": "mix", "mem_per_replica_mb": 256, "replica_count": 1,
        "artifact_id": a1["id"]})
    assert r.status_code == 201, r.text
    dep_id = r.json()["id"]
    dep = await wait_for(client, dep_id, {"AVAILABLE"})
    assert dep["artifact_version"] == 1
    assert dep["expected_sha256"] == a1["sha256"]

    a3 = await upload_artifact(client, "mix", b"revision three")
    assert a3["version"] == 3
    dep = (await client.get(f"/api/deployments/{dep_id}")).json()
    # 新版本登记后，既有部署仍锁定 v1，副本实际摘要仍是 v1
    assert dep["artifact_version"] == 1
    assert dep["expected_sha256"] == a1["sha256"]
    assert all(x["loaded_sha256"] == a1["sha256"] for x in dep["replicas"])

    # 新部署默认取最新版 v3，与旧部署互不影响
    r = await client.post("/api/deployments", json={
        "model_name": "mix", "mem_per_replica_mb": 256, "replica_count": 1,
        "version": 3})
    assert r.status_code == 201
    dep2 = await wait_for(client, r.json()["id"], {"AVAILABLE"})
    assert dep2["artifact_version"] == 3
    assert dep2["expected_sha256"] == a3["sha256"]
    dep1 = (await client.get(f"/api/deployments/{dep_id}")).json()
    assert dep1["artifact_version"] == 1  # 旧部署版本不变


@pytest.mark.asyncio
async def test_09_corrupt_download_is_rejected_and_retry_works(client):
    art = await upload_artifact(client, "corrupt-m", b"important weights v1")
    # 节点 A 一次性损坏下载字节（单副本部署落在 A）
    await client.post("/api/nodes/node-a/control", json={"corrupt_download": True})
    r = await client.post("/api/deployments", json={
        "model_name": "corrupt-m", "mem_per_replica_mb": 640, "replica_count": 1,
        "artifact_id": art["id"]})
    assert r.status_code == 201
    dep_id = r.json()["id"]

    # 节点核对字节失败并显式拒绝 -> 加载失败终态，不占显存
    dep = await wait_for(client, dep_id, {"FAILED"}, timeout=10)
    rep = dep["replicas"][0]
    assert rep["status"] == "LOAD_FAILED"
    assert "核对失败" in (rep["last_error"] or "") or "不符" in (rep["last_error"] or "")
    book = (await client.get("/api/nodes")).json()
    assert all(n["used_mb"] == 0 for n in book)

    # 故障为一次性，重试后正常下载并核对通过
    r2 = await client.post(f"/api/deployments/{dep_id}/retry")
    assert r2.status_code == 200
    dep = await wait_for(client, dep_id, {"AVAILABLE"}, timeout=10)
    new = [x for x in dep["replicas"] if x["epoch"] == 2][0]
    assert new["status"] == "LOADED" and new["loaded_sha256"] == art["sha256"]


@pytest.mark.asyncio
async def test_10_download_timeout_keeps_memory_until_verified(client):
    art = await upload_artifact(client, "slow-dl", b"weights needing download")
    # 节点 A 下载超时（一次性）：结果不明，保持 loading 占用显存
    await client.post("/api/nodes/node-a/control", json={"download_timeout": True})
    r = await client.post("/api/deployments", json={
        "model_name": "slow-dl", "mem_per_replica_mb": 700, "replica_count": 1,
        "artifact_id": art["id"]})
    dep_id = r.json()["id"]

    # 加载超时 -> 待核，显存保留（节点真实状态不可知，不能擅自释放）
    dep = await wait_for(client, dep_id, {"UNVERIFIED"}, timeout=12)
    assert dep["replicas"][0]["status"] == "TIMEOUT"
    book = (await client.get("/api/nodes")).json()
    assert node_free(book, "node-a") == 8192 - 700
    # 保留期间该显存不可被新部署使用：双副本要求两台节点都腾出 7800MB，
    # node-a 仅剩 7492MB，必被账本拒绝
    r2, _ = await deploy(client, "want-mem", 7800, 2)
    assert r2.status_code == 409

    # 恢复后手动对账：节点记录停留在 loading（下载结果不明期的真实表现），
    # 首轮对账只能重新纳入轮询、仍不得释放；运维现场确认节点无显存占用后
    # 清除其残留记录，再次对账才允许确认释放
    await client.post("/api/nodes/node-a/control", json={"mode": "normal"})
    rec = (await client.post(f"/api/deployments/{dep_id}/reconcile")).json()
    # 节点仍报 loading：重新纳入轮询，但部署不可用、显存仍保留
    assert rec["status"] in {"UNVERIFIED", "PENDING"}
    assert rec["replicas"][0]["status"] == "LOADING"
    book = (await client.get("/api/nodes")).json()
    assert node_free(book, "node-a") == 8192 - 700

    rid = dep["replicas"][0]["id"]
    client._transport.app.state.replicas.pop(rid, None)  # 现场确认节点无占用
    deadline = asyncio.get_event_loop().time() + 12
    while asyncio.get_event_loop().time() < deadline:
        rec = (await client.post(f"/api/deployments/{dep_id}/reconcile")).json()
        if rec["status"] == "FAILED":
            break
        await asyncio.sleep(0.25)
    dep = (await client.get(f"/api/deployments/{dep_id}")).json()
    assert dep["status"] == "FAILED"
    assert dep["replicas"][0]["status"] in {"RELEASED", "LOAD_FAILED"}
    book = (await client.get("/api/nodes")).json()
    assert node_free(book, "node-a") == 8192  # 确认无占用后显存才释放

    r3 = await client.post(f"/api/deployments/{dep_id}/retry")
    assert r3.status_code == 200
    dep = await wait_for(client, dep_id, {"AVAILABLE"}, timeout=10)
    assert dep["replicas"][-1]["loaded_sha256"] == art["sha256"]


@pytest.mark.asyncio
async def test_11_wrong_digest_never_available_even_with_late_ack(client):
    art = await upload_artifact(client, "interleave", b"v1 bytes")
    r = await client.post("/api/deployments", json={
        "model_name": "interleave", "mem_per_replica_mb": 333, "replica_count": 1,
        "artifact_id": art["id"]})
    dep_id = r.json()["id"]
    dep = await wait_for(client, dep_id, {"AVAILABLE"}, timeout=10)
    rid = dep["replicas"][0]["id"]
    assert dep["replicas"][0]["loaded_sha256"] == art["sha256"]

    # 登记同名新版本，但旧部署仍锁定 v1
    art2 = await upload_artifact(client, "interleave", b"v2 bytes different")
    dep = (await client.get(f"/api/deployments/{dep_id}")).json()
    assert dep["artifact_version"] == 1

    # 节点侧被换成错误版本（实际加载 v2 的字节），模拟迟到/异常回执
    sim_state = client._transport.app.state  # type: ignore[attr-defined]
    sim_state.replicas[rid] = {
        "model": "interleave", "mem_mb": 333, "epoch": 1, "status": "loaded",
        "artifact_id": art2["id"], "expected_sha256": art["sha256"],
        "loaded_sha256": art2["sha256"],
    }

    # 审计发现摘要不符：不得判可用，转待核并保留占用（自动撤销错误版本）
    await service.poll_loaded_digests()
    dep = (await client.get(f"/api/deployments/{dep_id}")).json()
    rep = dep["replicas"][0]
    assert rep["status"] in {"UNVERIFIED", "RELEASING"}  # 均为保留占用的待核处置
    assert rep["loaded_sha256"] == art2["sha256"]
    assert rep["digest_match"] is False
    assert dep["status"] == "UNVERIFIED"

    # 手动对账也不能把错误版本判为可用：无论此时节点仍持有（继续撤销）还是
    # 已被后台撤销（确认释放），错误版本都绝不回到 LOADED/AVAILABLE
    rec = (await client.post(f"/api/deployments/{dep_id}/reconcile")).json()
    assert rec["status"] in {"UNVERIFIED", "FAILED"}
    assert all(r["status"] != "LOADED" for r in rec["replicas"])
    # 待核期间错误事实必须持久化在副本上（不依赖某次回执的措辞），且显存保留
    if rec["status"] == "UNVERIFIED":
        rep = rec["replicas"][0]
        assert rep["digest_match"] is False
        assert rep["loaded_sha256"] == art2["sha256"]
        assert rep["status"] in {"UNVERIFIED", "RELEASING"}
        book = (await client.get("/api/nodes")).json()
        assert node_free(book, "node-a") == 8192 - 333
    else:
        kinds = {e["event_type"] for e in rec["events"]}
        assert {"AUDIT_DIGEST_MISMATCH", "RECONCILE_WRONG_RELEASED"} & kinds

    # 对账驱动撤销，确认节点无占用后释放 -> FAILED，重试固定 v1 并恢复可用
    deadline = asyncio.get_event_loop().time() + 15
    while asyncio.get_event_loop().time() < deadline:
        await client.post(f"/api/deployments/{dep_id}/reconcile")
        dep = (await client.get(f"/api/deployments/{dep_id}")).json()
        if dep["status"] == "FAILED":
            break
        await asyncio.sleep(0.25)
    assert dep["status"] == "FAILED"
    book = (await client.get("/api/nodes")).json()
    assert node_free(book, "node-a") == 8192

    r2 = await client.post(f"/api/deployments/{dep_id}/retry")
    assert r2.status_code == 200
    dep = await wait_for(client, dep_id, {"AVAILABLE"}, timeout=10)
    assert dep["artifact_version"] == 1  # 重试不被新版本带偏
    new = [x for x in dep["replicas"] if x["epoch"] == 2][0]
    assert new["loaded_sha256"] == art["sha256"]
    assert new["digest_match"] is True
