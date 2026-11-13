# PostgreSQL 备份试恢复验证

给数仓运维员用的网页小工具：上传 `pg_dump` **自定义格式**（`-Fc`）备份，在本地
PostgreSQL 的**独立临时库**里实际恢复一次，验证夜间备份是否真的可用于灾后恢复。

- 恢复成功 → 显示**各表行数**（逐表 `count(*)` 实数）
- 恢复失败 → 显示 `pg_restore` 的**错误输出**
- 范围：仅**普通表及其索引、约束**（含 identity/serial 背后的序列与默认值）；
  扩展、视图、函数等其它对象会在恢复清单中跳过并在页面上列出
- **最多两批**同时试恢复，可查看、可取消
- 成功 / 失败 / 取消，都必须等本批恢复进程退出、数据库会话停止、临时库清理完，
  才结束并释放名额；另一批和网页操作不受影响
- 取消一经受理，不会发布迟到报告
- 服务重启后先清理遗留恢复活动与临时库（只动 `rc_*` 临时库，不碰其它数据库），再接新批

## 快速开始

```bash
docker compose up --build
```

然后打开 <http://localhost:8080>。

Compose 包含三个服务：

| 服务 | 说明 |
|---|---|
| `db` | PostgreSQL 16，试恢复的目标实例（宿主端口 5432，可改） |
| `seed` | 一次性容器，生成三个示例备份到共享卷后退出 |
| `app` | FastAPI 网页（宿主端口 8080） |

### 内置示例备份（网页一键提交）

| 示例 | 内容 | 预期表现 |
|---|---|---|
| `normal.dump` | 3 张普通表（主键/外键/唯一约束/CHECK/索引），约 10 万行 | 成功，显示各表行数 |
| `slow.dump` | 单表 200 万行 + 索引（可用 `SLOW_ROWS` 调大） | 恢复耗时几十秒，便于演示取消 |
| `corrupt.dump` | `normal.dump` 中段被清零 4KiB | 恢复报错，显示错误输出 |

## 演示流程

1. **并行试恢复**：依次点击 `slow.dump` 和 `normal.dump`，两批并行进行；
   此时再提交第三批（上传或示例）会被拒绝并提示名额已满（HTTP 409）。
2. **取消**：在 `slow.dump` 恢复中点「取消」→ 状态变为「取消中·清理中」，
   恢复进程被终止、临时库会话被断开、临时库被删除后，批次才变为「已取消」并释放名额；
   另一批照常恢复完成。
3. **损坏备份**：提交 `corrupt.dump`，页面显示 `pg_restore` 的错误详情。
4. **重启清理**：在 `slow.dump` 恢复中执行 `docker compose restart app`，
   服务重启后会先清掉遗留的 `rc_*` 临时库、把进行中的批次标记为「已中断(重启)」，
   然后才接收新批次。可用 `psql -h localhost -U postgres -l` 确认无 `rc_` 开头的库残留。

## 实现要点（与运维要求的对应）

- **名额**：`JobManager` 用信号量限制并发批次数（默认 2，`SLOTS` 可调），满员即拒。
- **实际恢复**：每批创建 `rc_<12位hex>` 临时库，`pg_restore -L` 按过滤后的 TOC 清单恢复。
- **对象范围**：`tocfilter.py` 只保留 TABLE / TABLE DATA / INDEX / CONSTRAINT /
  FK CONSTRAINT / SEQUENCE / SEQUENCE SET / SEQUENCE OWNED BY / DEFAULT / 非 public SCHEMA；
  扩展等其它对象注释掉并计数展示。若表结构依赖扩展类型，恢复会报错并如实展示。
- **结束才释放名额**：worker 的 `finally` 中依次 ① 终止并回收 `pg_restore` 进程
  ② `pg_terminate_backend` 断开临时库全部会话并确认 ③ `DROP DATABASE ... WITH (FORCE)`
  ④ 写入最终状态 ⑤ 释放信号量。
- **取消不迟到**：取消在锁内置位并落库为「取消中」；worker 发布结果前在同一把锁内检查，
  一旦受理取消，最终状态只能是「已取消」，行数/错误报告一律丢弃。
- **重启清理**：FastAPI lifespan 启动阶段（开始接收请求之前）等待数据库就绪、
  清理引用 `rc_*` 临时库的残留 `pg_restore` 进程、删除所有 `rc_*` 临时库、
  把未完成批次标记为「已中断」。清理严格按 `rc_[0-9a-f]{12}` 名称匹配，不影响其它数据库。
- **历史留存**：批次记录存于 SQLite（`/data/jobs.db`，卷持久化），重启后历史可见。

## 配置项（app 服务环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `PGHOST/PGPORT/PGUSER/PGPASSWORD/PGMAINTDB` | db/5432/postgres/postgres/postgres | 目标 PostgreSQL |
| `SLOTS` | 2 | 最大并行试恢复批数 |
| `MAX_UPLOAD_MB` | 1024 | 上传大小上限 |
| `JOB_TIMEOUT_S` | 1800 | 单批恢复超时，超时按失败处理并清理 |
| `DATA_DIR` / `SAMPLES_DIR` | /data / /samples | 数据与示例目录 |

seed 服务的 `SLOW_ROWS`（默认 2000000）控制慢速示例的行数。

## 目录结构

```
├── docker-compose.yml
├── app/
│   ├── Dockerfile            # postgres:16 基底（自带 pg_restore 16）+ Python
│   ├── main.py               # FastAPI 端点与启动清理
│   ├── jobs.py               # 并发名额、worker、取消与清理状态机
│   ├── tocfilter.py          # TOC 过滤：仅普通表/索引/约束，跳过扩展等
│   ├── pgadmin.py            # 建/删临时库、断会话、重启清理（只碰 rc_*）
│   ├── store.py              # SQLite 批次历史
│   └── static/index.html     # 网页
├── seed/generate_samples.sh  # 生成 normal / slow / corrupt 三个示例备份
└── tests/                    # 纯 Python 单测（无需 Docker/PostgreSQL）
```

## 本地测试

```bash
python3 tests/test_tocfilter.py   # TOC 过滤逻辑
python3 tests/test_jobs.py        # 名额/取消/清理状态机（用假 pg 与假子进程）
```

## 说明与限制

- 仅支持 `pg_dump -Fc` 自定义格式；SQL 文本备份请先用 `pg_dump -Fc` 重导。
- 单进程运行（uvicorn 单 worker），名额由进程内状态机保证，请勿扩多副本。
- 临时库统一使用 `rc_` 前缀命名；请避免在同实例上手工创建同名前缀的库。
