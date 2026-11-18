"""调度核心：并发预留、加载编排、超时待核、恢复对账、失败重试。"""
from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import HTTPException

from . import config, db, models, node_client


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# 节点在线状态（内存态；进程重启后可通过手动对账重新核实）
NODE_RUNTIME: dict[str, dict[str, Any]] = {
    n.node_id: {"online": None, "last_check": None, "detail": "尚未探测", "mode": ""}
    for n in config.NODES
}

_bg_tasks: set[asyncio.Task] = set()


def _spawn(coro) -> None:
    task = asyncio.create_task(coro)
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)

    def _watch(t: asyncio.Task) -> None:
        if t.cancelled():
            return
        exc = t.exception()
        if exc:
            print(f"[background-task] {exc.__class__.__name__}: {exc}")

    task.add_done_callback(_watch)


# ---------------------------------------------------------------- 初始化
def init_db() -> None:
    with db.tx() as conn:
        for n in config.NODES:
            conn.execute(
                "INSERT OR IGNORE INTO nodes(node_id,name,base_url,capacity_mb,created_at) "
                "VALUES(?,?,?,?,?)",
                (n.node_id, n.name, n.base_url, n.capacity_mb, now()),
            )


def log_event(conn, event_type: str, detail: str, deployment_id: str | None = None,
              replica_id: str | None = None, node_id: str | None = None) -> None:
    conn.execute(
        "INSERT INTO events(deployment_id,replica_id,node_id,event_type,detail,created_at) "
        "VALUES(?,?,?,?,?,?)",
        (deployment_id, replica_id, node_id, event_type, detail, now()),
    )


def mark_node_lost(node_id: str, error: str) -> None:
    """业务调用发现传输层失联时联动节点状态（不依赖健康探测恰好撞上故障窗口）。
    仅更新内存态；调用方已在事务内记录事件，此处不自行开事务以免嵌套提交。"""
    rt = NODE_RUNTIME.get(node_id)
    if not rt:
        return
    rt["last_check"] = now()
    rt["online"] = False
    rt["detail"] = error


def mark_node_online(node_id: str, mode: str = "") -> None:
    """业务调用成功时联动恢复；若此前为失联状态则安排自动对账（事务外执行）。"""
    rt = NODE_RUNTIME.get(node_id)
    if not rt:
        return
    was_online = rt["online"]
    rt["online"] = True
    rt["last_check"] = now()
    rt["detail"] = "在线"
    if mode:
        rt["mode"] = mode
    if was_online is False:
        dep_ids = [r["deployment_id"] for r in db.query(
            "SELECT DISTINCT deployment_id FROM replicas WHERE node_id=? AND status IN (%s)"
            % ",".join("?" * len(config.RESERVED_STATUSES)),
            (node_id, *config.RESERVED_STATUSES),
        )]
        for dep_id in dep_ids:
            _spawn(reconcile(dep_id))


# ---------------------------------------------------------------- 显存账本
def _node_usage(conn, node_id: str) -> int:
    row = conn.execute(
        "SELECT COALESCE(SUM(mem_mb),0) AS used FROM replicas "
        "WHERE node_id=? AND status IN (%s)" % ",".join("?" * len(config.RESERVED_STATUSES)),
        (node_id, *config.RESERVED_STATUSES),
    ).fetchone()
    return row["used"]


def node_book() -> list[dict[str, Any]]:
    rows = db.query("SELECT * FROM nodes ORDER BY node_id")
    out = []
    for r in rows:
        used = _node_usage(db.connect(), r["node_id"])
        rt = NODE_RUNTIME.get(r["node_id"], {})
        out.append({
            "node_id": r["node_id"], "name": r["name"], "base_url": r["base_url"],
            "capacity_mb": r["capacity_mb"], "used_mb": used,
            "free_mb": r["capacity_mb"] - used,
            "online": rt.get("online"), "detail": rt.get("detail", ""),
            "mode": rt.get("mode", ""), "last_check": rt.get("last_check"),
        })
    return out


# ---------------------------------------------------------------- 提交部署
def create_deployment(model_name: str, mem_per_replica_mb: int, replica_count: int) -> dict:
    model_name = model_name.strip()
    if not model_name:
        raise HTTPException(400, "模型名不能为空")
    if mem_per_replica_mb <= 0:
        raise HTTPException(400, "每副本显存必须为正整数")
    if replica_count not in (1, 2):
        raise HTTPException(400, "副本数仅支持 1 或 2")

    dep_id = "dep-" + uuid.uuid4().hex[:10]
    with db.tx() as conn:
        nodes = conn.execute("SELECT * FROM nodes ORDER BY node_id").fetchall()
        if replica_count > len(nodes):
            raise HTTPException(409, f"可用节点仅 {len(nodes)} 台，无法满足 {replica_count} 副本分处不同节点")

        # 单节点容量检查
        for n in nodes:
            if mem_per_replica_mb > n["capacity_mb"]:
                raise HTTPException(
                    400, f"副本显存 {mem_per_replica_mb}MB 超过节点 {n['name']} 容量 {n['capacity_mb']}MB"
                )

        # 账本预留：仅统计仍占用显存的副本；整个分配在一个写事务内完成
        candidates = []
        for n in nodes:
            used = _node_usage(conn, n["node_id"])
            free = n["capacity_mb"] - used
            candidates.append((free, n))
        candidates.sort(key=lambda x: x[0], reverse=True)

        chosen: list[Any] = []
        for free, n in candidates:
            if free >= mem_per_replica_mb:
                chosen.append(n)
            if len(chosen) == replica_count:
                break
        if len(chosen) < replica_count:
            book = [
                f"{n['name']} 剩余 {n['capacity_mb'] - _node_usage(conn, n['node_id'])}MB"
                for _, n in candidates
            ]
            raise HTTPException(409, "显存不足，无法并发预留。当前账本：" + "；".join(book))

        ts = now()
        conn.execute(
            "INSERT INTO deployments(id,model_name,mem_per_replica_mb,replica_count,status,"
            "reason,epoch,attempts,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (dep_id, model_name, mem_per_replica_mb, replica_count,
             models.DEP_PENDING, "已预留显存，等待加载", 1, 1, ts, ts),
        )
        replica_ids = []
        for seq, n in enumerate(chosen, start=1):
            rid = f"{dep_id}-r{seq}-e1"
            conn.execute(
                "INSERT INTO replicas(id,deployment_id,node_id,seq,epoch,status,mem_mb,"
                "last_error,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (rid, dep_id, n["node_id"], seq, 1, models.REP_PENDING,
                 mem_per_replica_mb, None, ts, ts),
            )
            replica_ids.append(rid)
        log_event(conn, "RESERVE",
                  f"部署 {model_name}：在 {','.join(n['name'] for n in chosen)} 预留 "
                  f"{replica_count}×{mem_per_replica_mb}MB", dep_id)

    for rid in replica_ids:
        _spawn(_load_replica(rid))
    return get_deployment(dep_id)


# ---------------------------------------------------------------- 状态流转
def _set_replica(conn, rid: str, status: str, error: str | None = None,
                 confirmed: bool = False, released: bool = False) -> None:
    sets = ["status=?", "updated_at=?"]
    params: list[Any] = [status, now()]
    if error is not None:
        sets.append("last_error=?")
        params.append(error)
    if confirmed:
        sets.append("confirmed_at=?")
        params.append(now())
    if released:
        sets.append("released_at=?")
        params.append(now())
    params.append(rid)
    conn.execute(f"UPDATE replicas SET {','.join(sets)} WHERE id=?", params)


def _evaluate(conn, dep_id: str) -> None:
    """依据全部副本状态推导部署状态。"""
    dep = conn.execute("SELECT * FROM deployments WHERE id=?", (dep_id,)).fetchone()
    reps = conn.execute(
        "SELECT * FROM replicas WHERE deployment_id=? ORDER BY seq,epoch", (dep_id,)
    ).fetchall()
    # 只看当前 epoch 的副本
    reps = [r for r in reps if r["epoch"] == dep["epoch"]]
    statuses = {r["status"] for r in reps}

    new_status = dep["status"]
    reason = dep["reason"]
    trouble = {models.REP_TIMEOUT, models.REP_UNVERIFIED, models.REP_RELEASING}
    terminal = {models.REP_RELEASED, models.REP_LOAD_FAILED}
    if statuses & trouble:
        # 任何待核/释放中的副本都要保留占用，部署整体保持待核
        new_status = models.DEP_UNVERIFIED
        pending = [f"{r['node_id']}({models.REP_STATUS_CN[r['status']]})"
                   for r in reps if r["status"] in trouble]
        reason = "存在待核副本，显存保留占用：" + "、".join(pending) + "。恢复后自动/手动对账"
    elif statuses and statuses <= terminal:
        new_status = models.DEP_FAILED
        errs = [f"{r['node_id']}: {r['last_error'] or '未说明'}" for r in reps]
        reason = "所有副本均未成功（可重试）。" + "；".join(errs)
    elif statuses and statuses == {models.REP_LOADED}:
        new_status = models.DEP_AVAILABLE
        reason = f"全部 {len(reps)} 个副本确认加载完成"
    elif models.REP_LOADED in statuses and statuses <= {models.REP_LOADED, *terminal}:
        # 部分副本确认加载、其余已终态失败/释放：本 epoch 无法满足全部副本，判定失败
        new_status = models.DEP_FAILED
        reason = ("部分副本未能加载，未满足全部副本可用的要求（可重试）："
                  + "；".join(f"{r['node_id']}={models.REP_STATUS_CN[r['status']]}" for r in reps))
    else:
        # 仍有 PENDING/LOADING，且无待核/终态失败：继续调度
        new_status = models.DEP_PENDING
        reason = "加载进行中"
        if statuses & terminal:
            reason = "部分副本失败，等待其余副本结束后判定：" + reason

    if new_status != dep["status"] or reason != dep["reason"]:
        conn.execute(
            "UPDATE deployments SET status=?,reason=?,updated_at=? WHERE id=?",
            (new_status, reason, now(), dep_id),
        )
        log_event(conn, "DEP_" + new_status, reason, dep_id)


# ---------------------------------------------------------------- 加载编排
async def _load_replica(rid: str) -> None:
    rep = db.query_one("SELECT * FROM replicas WHERE id=?", (rid,))
    if not rep or rep["status"] != models.REP_PENDING:
        return
    dep = db.query_one("SELECT * FROM deployments WHERE id=?", (rep["deployment_id"],))
    node = config.NODE_MAP[rep["node_id"]]

    with db.tx() as conn:
        _set_replica(conn, rid, models.REP_LOADING)
        log_event(conn, "LOAD_SEND", f"向 {node.name} 下发加载 {dep['model_name']}（epoch={rep['epoch']}）",
                  dep["id"], rid, node.node_id)

    result = await node_client.load(
        node.base_url, dep["model_name"], rid, rep["epoch"], rep["mem_mb"]
    )

    with db.tx() as conn:
        cur = conn.execute("SELECT * FROM replicas WHERE id=?", (rid,)).fetchone()
        if cur["status"] != models.REP_LOADING or cur["epoch"] != rep["epoch"]:
            return  # 已被重试/释放流程取代，迟到结果丢弃，不复活旧副本
        if result.ok:
            log_event(conn, "LOAD_ACK", f"{node.name} 受理加载，等待确认", dep["id"], rid, node.node_id)
            mark_node_online(node.node_id)
        elif result.transport_failed:
            _set_replica(conn, rid, models.REP_TIMEOUT, error=result.error)
            log_event(conn, "LOAD_TIMEOUT", result.error, dep["id"], rid, node.node_id)
            mark_node_lost(node.node_id, result.error)
            _evaluate(conn, dep["id"])
        else:
            # 节点明确拒绝/HTTP 故障：确认未占用，释放本侧预留
            _set_replica(conn, rid, models.REP_LOAD_FAILED, error=result.error, released=True)
            log_event(conn, "LOAD_FAIL", result.error, dep["id"], rid, node.node_id)
            _evaluate(conn, dep["id"])


async def poll_loading() -> None:
    """轮询 LOADING 副本的节点确认情况，执行加载超时判定。"""
    reps = db.query(
        "SELECT * FROM replicas WHERE status=? ORDER BY updated_at", (models.REP_LOADING,)
    )
    for rep in reps:
        node = config.NODE_MAP[rep["node_id"]]
        dep = db.query_one("SELECT * FROM deployments WHERE id=?", (rep["deployment_id"],))
        waited = (datetime.now(timezone.utc) -
                  datetime.fromisoformat(rep["updated_at"])).total_seconds()
        result = await node_client.query_replica(node.base_url, rep["id"], rep["epoch"])

        with db.tx() as conn:
            cur = conn.execute("SELECT * FROM replicas WHERE id=?", (rep["id"],)).fetchone()
            if cur["status"] != models.REP_LOADING or cur["epoch"] != rep["epoch"]:
                continue
            remote = (result.data or {}).get("status") if result.ok else None
            if result.transport_failed:
                mark_node_lost(node.node_id, result.error)
                if waited >= config.LOAD_TIMEOUT:
                    _set_replica(conn, rep["id"], models.REP_TIMEOUT,
                                 error=result.error or "节点失联，加载状态未知")
                    log_event(conn, "LOAD_TIMEOUT",
                              f"等待 {waited:.0f}s 仍无法确认，保留占用待核",
                              dep["id"], rep["id"], node.node_id)
                    _evaluate(conn, dep["id"])
                # 未超时则继续等待，维持 LOADING 与预留
                continue
            if remote == "loaded":
                _set_replica(conn, rep["id"], models.REP_LOADED, confirmed=True)
                log_event(conn, "LOAD_CONFIRMED", f"{node.name} 确认加载完成",
                          dep["id"], rep["id"], node.node_id)
                _evaluate(conn, dep["id"])
            elif remote == "failed":
                _set_replica(conn, rep["id"], models.REP_LOAD_FAILED,
                             error="节点上报加载失败", released=True)
                log_event(conn, "LOAD_FAIL", "节点上报加载失败", dep["id"], rep["id"], node.node_id)
                _evaluate(conn, dep["id"])
            elif remote == "unknown":
                # 节点明确不存在该副本，远端不占显存，本侧预留可释放
                _set_replica(conn, rep["id"], models.REP_LOAD_FAILED,
                             error="对账发现节点上无此副本（加载未生效）", released=True)
                log_event(conn, "LOAD_MISSING", "节点上无此副本，释放预留",
                          dep["id"], rep["id"], node.node_id)
                _evaluate(conn, dep["id"])
            else:  # loading / unloading：仍在途
                if waited >= config.LOAD_TIMEOUT:
                    _set_replica(conn, rep["id"], models.REP_TIMEOUT,
                                 error=f"加载超过 {config.LOAD_TIMEOUT:.0f}s 未确认，保留占用待核")
                    log_event(conn, "LOAD_TIMEOUT", "节点在途过久，转待核",
                              dep["id"], rep["id"], node.node_id)
                    _evaluate(conn, dep["id"])


# ---------------------------------------------------------------- 释放/对账
async def request_release(rid: str, reason: str) -> None:
    """将待核副本转入释放中，并向节点撤销旧加载（幂等，可重复调用）。"""
    rep = db.query_one("SELECT * FROM replicas WHERE id=?", (rid,))
    if not rep or rep["status"] in (models.REP_RELEASED, models.REP_LOAD_FAILED):
        return
    dep = db.query_one("SELECT * FROM deployments WHERE id=?", (rep["deployment_id"],))
    node = config.NODE_MAP[rep["node_id"]]

    with db.tx() as conn:
        if rep["status"] != models.REP_RELEASING:
            _set_replica(conn, rid, models.REP_RELEASING, error=reason)
            log_event(conn, "RELEASE_SEND", f"请求 {node.name} 撤销旧加载：{reason}",
                      dep["id"], rid, node.node_id)
            _evaluate(conn, dep["id"])

    result = await node_client.unload(node.base_url, dep["model_name"], rid, rep["epoch"])
    with db.tx() as conn:
        cur = conn.execute("SELECT * FROM replicas WHERE id=?", (rid,)).fetchone()
        if cur["status"] != models.REP_RELEASING:
            return
        if result.transport_failed:
            conn.execute("UPDATE replicas SET last_error=? WHERE id=?",
                         (f"卸载未确认：{result.error}，保持占用待对账", rid))
        else:
            log_event(conn, "UNLOAD_ACK", "节点受理卸载，等待确认释放", dep["id"], rid, node.node_id)


async def poll_releasing() -> None:
    """轮询释放中的副本，只有节点确认不存在/已卸载才标记 RELEASED。"""
    reps = db.query(
        "SELECT * FROM replicas WHERE status=?", (models.REP_RELEASING,)
    )
    for rep in reps:
        node = config.NODE_MAP[rep["node_id"]]
        dep = db.query_one("SELECT * FROM deployments WHERE id=?", (rep["deployment_id"],))
        result = await node_client.query_replica(node.base_url, rep["id"], rep["epoch"])
        with db.tx() as conn:
            cur = conn.execute("SELECT * FROM replicas WHERE id=?", (rep["id"],)).fetchone()
            if cur["status"] != models.REP_RELEASING:
                continue
            if result.transport_failed:
                conn.execute("UPDATE replicas SET last_error=?,updated_at=? WHERE id=?",
                             (f"节点失联无法对账：{result.error}，继续保留占用", now(), rep["id"]))
                continue
            remote = (result.data or {}).get("status")
            if remote in ("unknown", "unloaded"):
                _set_replica(conn, rep["id"], models.REP_RELEASED,
                             error="节点对账确认已释放", released=True)
                log_event(conn, "RELEASE_CONFIRMED", "对账确认显存已释放，可重新分配",
                          dep["id"], rep["id"], node.node_id)
                _evaluate(conn, dep["id"])
            else:
                # loaded/loading/unloading：旧加载仍在，重新撤销
                log_event(conn, "RELEASE_STILL_HELD",
                          f"节点仍持有副本(remote={remote})，重新下发卸载",
                          dep["id"], rep["id"], node.node_id)
        if result.ok and (result.data or {}).get("status") not in ("unknown", "unloaded"):
            await node_client.unload(node.base_url, dep["model_name"], rep["id"], rep["epoch"])


async def reconcile(dep_id: str) -> dict:
    """对账：逐副本向节点核实真实状态，处置超时/失联后的残留占用。"""
    dep = db.query_one("SELECT * FROM deployments WHERE id=?", (dep_id,))
    if not dep:
        raise HTTPException(404, "部署不存在")
    reps = db.query(
        "SELECT * FROM replicas WHERE deployment_id=? AND epoch=? ORDER BY seq",
        (dep_id, dep["epoch"]),
    )
    notes: list[str] = []
    for rep in reps:
        # 仅终态副本跳过：已确认释放/明确失败的无需再核实
        if rep["status"] in (models.REP_RELEASED, models.REP_LOAD_FAILED):
            continue
        node = config.NODE_MAP[rep["node_id"]]
        result = await node_client.query_replica(node.base_url, rep["id"], rep["epoch"])
        with db.tx() as conn:
            cur = conn.execute("SELECT * FROM replicas WHERE id=?", (rep["id"],)).fetchone()
            if result.transport_failed:
                note = f"{node.name} 仍失联/不可达（{result.error}），副本 {cur['id']} 保持{models.REP_STATUS_CN[cur['status']]}并继续占用"
                notes.append(note)
                log_event(conn, "RECONCILE_BLOCKED", note, dep_id, cur["id"], node.node_id)
                continue
            remote = (result.data or {}).get("status")
            st = cur["status"]
            if remote == "loaded":
                mark_node_online(node.node_id)
                if st in (models.REP_LOADING, models.REP_TIMEOUT, models.REP_UNVERIFIED):
                    # 超时/待核后核实加载其实已成功：同一 epoch 的合法补确认
                    _set_replica(conn, cur["id"], models.REP_LOADED, confirmed=True)
                    notes.append(f"{node.name} 上副本实际已加载，补确认")
                    log_event(conn, "RECONCILE_LOADED",
                              "失联/超时后核实加载其实已成功，补确认",
                              dep_id, cur["id"], node.node_id)
                elif st == models.REP_RELEASING:
                    notes.append(f"{node.name} 仍持有旧加载，继续撤销")
                    log_event(conn, "RECONCILE_STILL_HELD", "旧加载仍在，重新卸载",
                              dep_id, cur["id"], node.node_id)
                else:
                    notes.append(f"{node.name} 上副本正常加载中状态一致")
            elif remote in ("unknown", "unloaded"):
                mark_node_online(node.node_id)
                if st in (models.REP_TIMEOUT, models.REP_UNVERIFIED, models.REP_RELEASING):
                    _set_replica(conn, cur["id"], models.REP_RELEASED,
                                 error=f"对账确认节点无残留（remote={remote}）", released=True)
                    notes.append(f"{node.name} 无残留，副本 {cur['id']} 确认释放")
                    log_event(conn, "RECONCILE_RELEASED", "节点确认无副本，释放显存",
                              dep_id, cur["id"], node.node_id)
                elif st == models.REP_LOADED:
                    # 账本认为已加载，节点却没有——不一致，先待核再释放
                    _set_replica(conn, cur["id"], models.REP_UNVERIFIED,
                                 error="对账不一致：账本为已加载，节点上不存在")
                    notes.append(f"{node.name} 对账不一致，副本转待核并将撤销")
                    log_event(conn, "RECONCILE_MISMATCH", "账本已加载但节点无此副本",
                              dep_id, cur["id"], node.node_id)
                else:  # PENDING/LOADING
                    _set_replica(conn, cur["id"], models.REP_LOAD_FAILED,
                                 error="对账发现节点无此副本，加载未生效", released=True)
                    notes.append(f"{node.name} 无此副本，判定加载失败")
                    log_event(conn, "RECONCILE_MISSING", "加载未生效",
                              dep_id, cur["id"], node.node_id)
            elif remote == "failed":
                _set_replica(conn, cur["id"], models.REP_LOAD_FAILED,
                             error="节点上报 failed", released=True)
                notes.append(f"{node.name} 上报失败，释放预留")
            else:  # loading / unloading
                if st == models.REP_TIMEOUT:
                    # 节点仍在加载：给一次确认机会，维持等待，下轮轮询继续核实
                    notes.append(f"{node.name} 上副本仍在 {remote}，维持待核继续观察")
                else:
                    notes.append(f"{node.name} 上副本处于 {remote}，维持 {models.REP_STATUS_CN[st]}")
            _evaluate(conn, dep_id)

    # 对账中新出现的待核/不一致副本，自动发起撤销
    for rep in db.query(
        "SELECT * FROM replicas WHERE deployment_id=? AND epoch=? AND status IN (?,?)",
        (dep_id, dep["epoch"], models.REP_UNVERIFIED, models.REP_RELEASING),
    ):
        await request_release(rep["id"], "对账后撤销旧加载")

    updated = get_deployment(dep_id)
    updated["reconcile_notes"] = notes or ["所有副本与节点状态一致"]
    return updated


# ---------------------------------------------------------------- 重试
def retry_deployment(dep_id: str) -> dict:
    """失败部署重试：开启新 epoch，旧副本保留历史，不复活旧副本。"""
    with db.tx() as conn:
        dep = conn.execute("SELECT * FROM deployments WHERE id=?", (dep_id,)).fetchone()
        if not dep:
            raise HTTPException(404, "部署不存在")
        if dep["status"] != models.DEP_FAILED:
            raise HTTPException(409, f"仅失败部署可重试，当前状态为 {models.DEP_STATUS_CN[dep['status']]}")

        new_epoch = dep["epoch"] + 1
        old_reps = conn.execute(
            "SELECT * FROM replicas WHERE deployment_id=? AND epoch=? ORDER BY seq",
            (dep_id, dep["epoch"]),
        ).fetchall()
        # 旧副本必须全部不再占显存（FAILED 的定义），双重校验
        for r in old_reps:
            if r["status"] in config.RESERVED_STATUSES:
                raise HTTPException(409, "旧副本仍占用显存，需先对账释放，不能重试")

        nodes = conn.execute("SELECT * FROM nodes ORDER BY node_id").fetchall()
        candidates = []
        for n in nodes:
            free = n["capacity_mb"] - _node_usage(conn, n["node_id"])
            candidates.append((free, n))
        candidates.sort(key=lambda x: x[0], reverse=True)
        chosen = []
        for free, n in candidates:
            if free >= dep["mem_per_replica_mb"]:
                chosen.append(n)
            if len(chosen) == dep["replica_count"]:
                break
        if len(chosen) < dep["replica_count"]:
            raise HTTPException(409, "重试时显存不足，无法预留")

        ts = now()
        new_ids = []
        for seq, n in enumerate(chosen, start=1):
            rid = f"{dep_id}-r{seq}-e{new_epoch}"
            conn.execute(
                "INSERT INTO replicas(id,deployment_id,node_id,seq,epoch,status,mem_mb,"
                "last_error,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (rid, dep_id, n["node_id"], seq, new_epoch, models.REP_PENDING,
                 dep["mem_per_replica_mb"], None, ts, ts),
            )
            new_ids.append(rid)
        conn.execute(
            "UPDATE deployments SET status=?,reason=?,epoch=?,attempts=?,updated_at=? WHERE id=?",
            (models.DEP_PENDING, f"第 {new_epoch} 次尝试，加载进行中", new_epoch,
             dep["attempts"] + 1, ts, dep_id),
        )
        log_event(conn, "RETRY",
                  f"失败后重试，开启 epoch={new_epoch}，旧 epoch={dep['epoch']} 副本保持终态不复活",
                  dep_id)

    for rid in new_ids:
        _spawn(_load_replica(rid))
    return get_deployment(dep_id)


# ---------------------------------------------------------------- 健康监测
async def health_monitor_once() -> None:
    for n in config.NODES:
        result = await node_client.health(n.base_url)
        rt = NODE_RUNTIME[n.node_id]
        was_online = rt["online"]
        rt["last_check"] = now()
        if result.ok:
            mark_node_online(n.node_id, (result.data or {}).get("mode", ""))
            if was_online is False:
                with db.tx() as conn:
                    log_event(conn, "NODE_RECOVERED",
                              f"{n.name} 健康探测恢复，触发自动对账", node_id=n.node_id)
        else:
            rt["online"] = False
            rt["detail"] = result.error
            if was_online is not False:
                with db.tx() as conn:
                    log_event(conn, "NODE_LOST",
                              f"{n.name} 健康探测失联：{result.error}，相关占用保留待核",
                              node_id=n.node_id)


# ---------------------------------------------------------------- 查询
def list_deployments() -> list[dict]:
    deps = db.query("SELECT * FROM deployments ORDER BY created_at DESC")
    return [get_deployment(d["id"]) for d in deps]


def get_deployment(dep_id: str) -> dict:
    dep = db.query_one("SELECT * FROM deployments WHERE id=?", (dep_id,))
    if not dep:
        raise HTTPException(404, "部署不存在")
    reps = db.query(
        "SELECT * FROM replicas WHERE deployment_id=? ORDER BY epoch,seq", (dep_id,)
    )
    events = db.query(
        "SELECT * FROM events WHERE deployment_id=? ORDER BY id DESC LIMIT 20", (dep_id,)
    )
    d = dict(dep)
    d["status_cn"] = models.DEP_STATUS_CN[dep["status"]]
    d["replicas"] = []
    for r in reps:
        rr = dict(r)
        rr["status_cn"] = models.REP_STATUS_CN[r["status"]]
        rr["node_name"] = config.NODE_MAP[r["node_id"]].name
        d["replicas"].append(rr)
    d["events"] = [dict(e) for e in events]
    return d


def recent_events(limit: int = 30) -> list[dict]:
    return [dict(r) for r in db.query(
        "SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,))]
