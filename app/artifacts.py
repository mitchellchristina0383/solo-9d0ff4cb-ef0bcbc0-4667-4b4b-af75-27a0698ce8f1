"""模型制品登记：内容寻址存储 + 不可改写版本。

规则：
- 上传时按字节计算 SHA-256 摘要，文件以摘要落盘（同名同内容天然去重、不可改写）；
- 每个 model_name 下版本号单调递增，版本一经写入即固定其摘要与字节，无更新/删除接口；
- 同一 model_name 重复上传相同字节返回 409 并回指既有版本，不会产生新版本。
"""
from __future__ import annotations

import hashlib
import os
import uuid

from fastapi import HTTPException

from . import config, db
from .service import log_event, now

_CHUNK = 1024 * 1024


def _store_bytes(data: bytes, digest: str) -> str:
    os.makedirs(config.ARTIFACT_DIR, exist_ok=True)
    # 内容寻址：路径只由摘要决定，相同字节永远指向同一份不可变文件
    path = os.path.join(config.ARTIFACT_DIR, digest)
    if not os.path.exists(path):
        tmp = os.path.join(config.ARTIFACT_DIR, f".tmp-{uuid.uuid4().hex}")
        with open(tmp, "wb") as f:
            f.write(data)
        os.replace(tmp, path)
    return path


def register_upload(model_name: str, filename: str, content: bytes) -> dict:
    model_name = (model_name or "").strip()
    if not model_name:
        raise HTTPException(400, "模型名不能为空")
    if not content:
        raise HTTPException(400, "制品文件为空，拒绝登记")
    if len(content) > config.ARTIFACT_MAX_BYTES:
        raise HTTPException(
            413,
            f"制品 {len(content)} 字节超过上限 {config.ARTIFACT_MAX_BYTES} 字节",
        )

    digest = hashlib.sha256(content).hexdigest()

    # 登记与版本号分配在同一串行写事务内完成，避免并发上传产生重号/错号
    with db.tx() as conn:
        same = conn.execute(
            "SELECT * FROM artifacts WHERE model_name=? AND sha256=?",
            (model_name, digest),
        ).fetchone()
        if same:
            log_event(
                conn, "ARTIFACT_DUP",
                f"模型 {model_name} 相同字节重复上传，回指既有版本 v{same['version']}（不生成新版本）",
            )
            raise HTTPException(
                409,
                f"相同内容已登记为 {model_name} 版本 v{same['version']}，不可改写",
            )

        row = conn.execute(
            "SELECT COALESCE(MAX(version),0) AS v FROM artifacts WHERE model_name=?",
            (model_name,),
        ).fetchone()
        version = row["v"] + 1
        artifact_id = "art-" + uuid.uuid4().hex[:12]
        path = _store_bytes(content, digest)
        conn.execute(
            "INSERT INTO artifacts(id,model_name,version,sha256,size_bytes,filename,"
            "storage_path,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (artifact_id, model_name, version, digest, len(content),
             filename, path, now()),
        )
        log_event(
            conn, "ARTIFACT_REGISTER",
            f"登记制品 {model_name} v{version}：sha256={digest[:12]}…，{len(content)} 字节",
        )

    return get_artifact(artifact_id)


def list_artifacts(model_name: str | None = None) -> list[dict]:
    if model_name:
        rows = db.query(
            "SELECT * FROM artifacts WHERE model_name=? ORDER BY model_name,version",
            (model_name.strip(),),
        )
    else:
        rows = db.query(
            "SELECT * FROM artifacts ORDER BY model_name,version,id"
        )
    return [_public(r) for r in rows]


def get_artifact(artifact_id: str) -> dict:
    row = db.query_one("SELECT * FROM artifacts WHERE id=?", (artifact_id,))
    if not row:
        raise HTTPException(404, "制品版本不存在")
    return _public(row)


def get_artifact_for_deploy(model_name: str, artifact_id: str | None,
                            version: int | None) -> dict:
    """部署选版：显式 artifact_id 优先；其次 model_name+version；都缺省时取最新版。"""
    if artifact_id:
        row = db.query_one("SELECT * FROM artifacts WHERE id=?", (artifact_id,))
        if not row:
            raise HTTPException(404, f"制品 {artifact_id} 不存在")
        if row["model_name"] != model_name:
            raise HTTPException(
                409,
                f"制品 {artifact_id} 属于模型 {row['model_name']}，与部署模型 {model_name} 不符",
            )
        return _public(row)
    if version is not None:
        row = db.query_one(
            "SELECT * FROM artifacts WHERE model_name=? AND version=?",
            (model_name, version),
        )
        if not row:
            raise HTTPException(404, f"模型 {model_name} 不存在版本 v{version}")
        return _public(row)
    row = db.query_one(
        "SELECT * FROM artifacts WHERE model_name=? ORDER BY version DESC LIMIT 1",
        (model_name,),
    )
    if not row:
        raise HTTPException(
            409,
            f"模型 {model_name} 尚未登记任何制品版本，请先在“模型制品登记”上传文件",
        )
    return _public(row)


def read_artifact_bytes(artifact_id: str) -> tuple[dict, bytes]:
    row = db.query_one("SELECT * FROM artifacts WHERE id=?", (artifact_id,))
    if not row:
        raise HTTPException(404, "制品版本不存在")
    if not os.path.exists(row["storage_path"]):
        raise HTTPException(410, f"制品文件丢失：{artifact_id}")
    with open(row["storage_path"], "rb") as f:
        data = f.read()
    # 出库前再次核对摘要，防止磁盘上的字节被外部改动后仍被当作正确版本分发
    if hashlib.sha256(data).hexdigest() != row["sha256"]:
        raise HTTPException(500, "平台侧制品字节与登记摘要不符，已停止分发")
    return _public(row), data


def _public(row) -> dict:
    d = dict(row)
    d.pop("storage_path", None)  # 服务端路径不外泄
    d["version_label"] = f"v{row['version']}"
    d["sha256_short"] = row["sha256"][:12]
    return d
