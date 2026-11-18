# 推理平台副本调度值班台

值班员用于在两台共享推理节点上安排模型副本，核心目标是**避免加载超时后重复占用显存**，
并通过**模型制品登记与字节级摘要核对**解决“同名模型更新后节点仍装着旧文件”的问题。
FastAPI + SQLite 实现，Docker Compose 一键启动 1 个调度平台 + 2 台可注入故障的本地节点。

## 一、快速启动

```bash
docker compose up --build
# 打开 http://localhost:8000
```

| 服务 | 端口 | 说明 |
| --- | --- | --- |
| platform | 8000 | 调度平台（网页 + REST API + 后台轮询/健康监测） |
| node-a | 9101 | 模拟节点 A，固定显存 8192MB |
| node-b | 9102 | 模拟节点 B，固定显存 8192MB |

不使用 Docker 时的本地运行方式：

```bash
pip install -r requirements.txt
# 终端 1/2：两个节点
NODE_ID=node-a NODE_NAME=模拟节点A CAPACITY_MB=8192 \
  uvicorn node.simulator:app --port 9101
NODE_ID=node-b NODE_NAME=模拟节点B CAPACITY_MB=8192 \
  uvicorn node.simulator:app --port 9102
# 终端 3：调度平台
PLATFORM_NODES="node-a|模拟节点A|http://127.0.0.1:9101|8192,node-b|模拟节点B|http://127.0.0.1:9102|8192" \
  uvicorn app.main:app --port 8000
```

## 二、网页操作

- **模型制品登记**：上传本地小文件（≤16MB），平台按字节计算 SHA-256，生成**不可改写**的
  单调版本（v1、v2…）；同模型重复上传相同字节返回 409 并回指既有版本，不产生新版本。
- **提交部署**：模型名、每副本显存（MB）、副本数（1～2），并在下拉框中**选择制品版本**；
  不指定时取该模型最新版本，但所选版本（id + 摘要）会固化到部署记录，之后同名再传新版本
  也不会改变在途或已有部署使用的版本（重试同样固定原版本）。
- **节点账本**：实时显示每台节点容量、已预留、剩余显存与在线状态。
- **部署列表**：每个部署的副本分配（节点、epoch、状态、错误原因）、**期望摘要与节点实际
  加载摘要及核对结果**，以及事件流水。
- **故障注入**：对每台节点一键切换 正常 / 延迟 / 失败(503) / 失联(挂起超时)，
  以及“下次加载失败”“迟到回执”“**模拟制品损坏**”“**模拟下载超时**”“重置节点”。
- **手动对账 / 重试**：部署行内按钮；页面 2 秒自动刷新。

## 三、核心调度规则（与需求逐条对应）

1. **副本分散**：同一部署的 2 个副本必须落在不同节点（按剩余显存降序选节点）。
2. **全部确认且摘要相符才可用**：所有副本都被节点确认 `loaded`，**且节点上报的文件摘要与
   部署所选版本的 SHA-256 完全一致**，部署才标记 `AVAILABLE`；摘要不符一律转待核并撤销，
   另有评估兜底（`DIGEST_GUARD`）与已加载副本周期审计（`poll_loaded_digests`）双保险。
3. **并发不超额预留**：预留检查与插入在同一个 SQLite 写事务内串行执行，
   显存账本只统计仍占用的副本（`PENDING/LOADING/LOADED/TIMEOUT/UNVERIFIED/RELEASING`），
   不足时整个部署返回 409，不会产生半分配。
4. **节点从平台取文件并核对字节**：加载时平台把制品下载地址与期望摘要下发给节点，节点回
   平台拉取原始字节并自行计算 SHA-256；**文件损坏（摘要不符）显式拒绝加载**（HTTP 422，
   确认未占用可生效副本）；**下载超时/连接异常时结果不明，节点保持 `loading`、平台保留
   显存待核**，绝不擅自释放。
5. **超时/失联保留占用待核**：
   - 传输层错误（超时、连接失败）→ 节点真实状态不可知 → 副本转 `TIMEOUT`，**显存继续占用**；
   - 显式 HTTP 失败（4xx/5xx，含损坏拒绝）→ 节点确认未占用 → 副本转 `LOAD_FAILED` 并释放预留。
6. **恢复对账**：节点恢复后由健康监测或业务调用成功联动触发自动对账，也可手动对账：
   - 节点无残留 → 副本确认 `RELEASED`/`LOAD_FAILED`，显存才可重新分配；
   - 节点其实已加载且摘要相符 → 同一 epoch 内补确认（合法，不算复活）；
   - 节点持有错误版本（摘要不符）→ 转待核并重新下发卸载，直到确认释放，**不得判可用**；
   - 节点仍持有旧加载 → 重新下发卸载，直到确认释放；
   - 对账时节点仍失联 → 保持待核并在原因/备注中说明，绝不擅自释放。
7. **迟到回执不复活旧副本、不洗白错误版本**：副本带 `epoch` 标识；重试开启新 epoch，
   节点侧与调度器侧都只接受当前 epoch 的状态，旧 epoch 回执被丢弃；任何途径（轮询、迟到
   回执、手动对账）发现摘要与所选版本不符，都不会把该副本/部署判为可用。
8. **版本交错安全**：同名新版本只影响之后新建的部署；在途与已有部署固定创建时选定的
   artifact id 与摘要，重试也沿用，不被新版本带偏。
9. **失败可重试**：仅 `FAILED` 部署允许重试，重试前校验旧副本全部不再占显存，
   旧副本记录保留为终态用于审计。

## 四、状态机

部署：`PENDING（调度中）→ AVAILABLE（可用）/ FAILED（失败可重试）/ UNVERIFIED（待核）`

副本：
`PENDING → LOADING → LOADED`；异常分支 `TIMEOUT/UNVERIFIED → RELEASING → RELEASED`，
显式失败为 `LOAD_FAILED`（终态，不占显存）。

## 五、REST API 摘要

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| POST | `/api/artifacts` | 上传制品（multipart：`model_name` + `file`），登记不可改写版本与 SHA-256 |
| GET | `/api/artifacts` / `/api/artifacts/{id}` | 制品版本列表/详情（可按 `model_name` 过滤） |
| GET | `/api/artifacts/{id}/content` | 节点拉取制品原始字节（响应头带 `X-Sha256`，出库前再次核对摘要） |
| POST | `/api/deployments` | 提交部署 `{model_name, mem_per_replica_mb, replica_count, artifact_id?/version?}` |
| GET | `/api/deployments` / `/api/deployments/{id}` | 部署列表/详情（含副本期望/实际摘要、核对结果与事件） |
| POST | `/api/deployments/{id}/retry` | 失败部署重试（新 epoch，固定原制品版本） |
| POST | `/api/deployments/{id}/reconcile` | 手动对账，返回逐条处置说明 |
| GET | `/api/nodes` | 节点显存账本与在线状态 |
| POST | `/api/nodes/{id}/control` | 故障注入（mode/延迟/load_fail/corrupt_download/download_timeout 等） |
| POST | `/api/nodes/{id}/reset` | 重置节点模拟器 |
| GET | `/api/events` | 最近事件流水 |

## 六、自测

```bash
pip install -r requirements.txt pytest pytest-asyncio
pytest -q
```

`tests/test_platform.py` 基于 ASGI 传输同时拉起调度器与两台独立节点（节点回平台的制品
下载也走同一传输），11 个用例覆盖：
完整部署流程与摘要核对、并发超额预留、显式失败释放与重试、失联超时保留占用、
恢复对账撤销旧加载并确认释放、迟到回执不复活旧 epoch、对账失败保持待核并说明原因，
以及**制品不可改写与版本号、同名新版本不改变在途/已有部署（版本交错）、损坏文件拒绝
加载、下载超时保留显存待核且确认无占用后才释放重试、错误摘要在迟到回执与手动对账下
都不会被判可用**。

另可用真实 uvicorn TCP 进程做端到端冒烟（仓库内 `.smoke/smoke_orchestrated.py`，
该目录已在 `.gitignore` 中忽略）。

## 七、目录结构

```
app/            调度平台
  config.py     环境配置（节点、超时、轮询间隔、制品仓库路径/大小上限/平台下载基址）
  db.py         SQLite 访问层（WAL + 写锁事务 + 轻量列迁移）
  models.py     状态常量与中文映射
  artifacts.py  制品登记：内容寻址存储、SHA-256、不可改写版本、出库复核
  node_client.py 节点 HTTP 客户端（区分传输失败与显式 HTTP 失败，携带制品下载信息）
  service.py    调度核心：预留/加载/摘要核对/超时/对账/重试/健康监测/已加载审计
  main.py       FastAPI 入口、后台任务、制品上传与下载、故障注入代理
  static/index.html 值班网页（制品登记、版本选择、摘要展示、损坏/超时注入）
node/
  simulator.py  节点模拟器（应用工厂，支持延迟/失败/失联/迟到回执/下载损坏/下载超时）
tests/          端到端自测
docker-compose.yml / Dockerfile
```

## 八、关键环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PLATFORM_NODES` | 两台 8192MB 节点 | `id|名称|url|容量MB`，逗号分隔 |
| `PLATFORM_DB` | `data/platform.db` | SQLite 路径 |
| `ARTIFACT_DIR` | `data/artifacts` | 内容寻址制品文件目录（按 SHA-256 落盘） |
| `ARTIFACT_MAX_BYTES` | `16777216`（16MB） | 单次上传制品字节上限 |
| `PLATFORM_BASE_URL` | `http://platform:8000` | 节点回平台下载制品的基址 |
| `NODE_CALL_TIMEOUT` | `1.5` 秒 | 单次节点 HTTP 调用超时 |
| `LOAD_TIMEOUT` | `30` 秒 | 等待加载确认上限，超时转待核 |
| `POLL_INTERVAL` / `HEALTH_INTERVAL` | `1` / `10` 秒 | 轮询与健康探测间隔 |
| `DOWNLOAD_TIMEOUT_SLEEP`（节点） | `3600` 秒 | 节点模拟制品下载超时的挂起时长 |
