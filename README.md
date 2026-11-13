# 03-changeflow（变流）

Changeflow — CDC / 变更流落库：事件入账 → 序/缺口裁决 → 幂等应用到 sink → 游标提交与对账。

**不是** Feed 多源归并（Feedstitch），**不是** 作业依赖编排（Batchgate）。

## 启动

```bash
docker compose up --build
```

| 入口 | 地址 |
| --- | --- |
| 前端 | http://localhost:5300 |
| API | http://localhost:10300 |

本地开发（可选）：

```bash
# backend
cd backend && pip install -r requirements.txt
set DATA_DIR=..\data
uvicorn app.main:app --reload --port 10300

# frontend
cd frontend && npm install && npm run dev
```

## 主链

配置变更流与夹具 → 触发 Apply → 按序应用 / 缺口隔离 → Sink + 游标 + 运行报告对账

分页拉取链：登记本机分页源（manifest 清单）→ 绑定流与位点 → 按位点逐页拉取 → 整页入账 → 现有 Apply 裁决 → 页成功才推进位点 → 任务报告与账本 / Sink / 游标对账

## 种子说明

- **Orders CDC（clean）**：JSONL，seq 1→5 连续 upsert/delete，首跑应全部应用、游标推进到 5。
- **Inventory CDC（clean）**：JSONL，seq 1→3 连续 upsert，独立实体键，用于第二路流演示。
- **Orders CDC（分页拉取）**：绑定 `orders_pages/manifest.json`（3 页 p1/p2/p3，seq 1→9，含 delete），位点从 0 开始。
- **Inventory CDC（分页拉取）**：绑定 `inventory_pages/manifest.json`（2 页 p1/p2，seq 1→5）。

打开「应用台」选流跑一次，再看账本 / 隔离区 / Sink / 游标页计数是否与本次报告一致。

## 分页拉取（本机变更日志 offset pull）

「分页拉取」页把按页拆好的本机变更日志一页页拉进 Changeflow：

- **登记源**：分页源 = fixtures 下的一份 `manifest.json`（显式列出页顺序：`page_token` + 文件路径），不扫目录、不访问外网；路径越出 fixtures 根目录一律拒绝。位点（`pull_position` = 下一页下标）随流持久化，重启不丢。
- **拉取**：「拉取一页」按当前位点取一页，「连续拉完」逐页循环到源尽或失败。每页先整页入账（账本 `disposition=pulled`），再逐条走与现有 Apply 完全相同的裁决（连续序应用 / 缺口隔离 / 重复跳过），**不绕开隔离直写 Sink**。
- **成功边界（写明）**：页内全部事件入账且裁决均为 `applied` / `skipped_duplicate`（零隔离、零解析错误）→ 该页记 `pulled` 并推进位点；任一事件被隔离或页文件解析失败 → 该页记 `failed`，**位点停在失败页、不得跨过**，已应用部分落盘、未裁决事件留 `pulled`，可从该位点**续拉**（已应用事件幂等跳过）。
- **任务报告**：拉取页数、事件条数、入账条数、失败/续拉页、applied / quarantined / skipped，附页清单（`pull_task_pages`）与现场对账（账本行数、隔离区行数、applied_seqs、Sink、流位点/游标），逐项打勾。

演示（Orders CDC（分页拉取））：

1. 「连续拉完」→ 3 页全成功，位点 0→3，游标 0→9，报告计数与账本 / Sink / 游标一致。
2. 故意失败：位点改成 2（跨过 p2）→「拉取一页」→ p3 的 seq 7 缺口进隔离区，任务 failed，位点停在 2。
3. 续拉：位点改回 1 →「连续拉完」→ p2、p3 全部成功，位点到 3，游标到 9；失败任务仍可回看。
4. 重启服务后位点与任务历史仍在（SQLite 持久化）。

## 技术栈

Python 3.12 + FastAPI + SQLite；Vue 3 + Vite + Nginx。

## 0-1 切口（未实现，见题单）

当前仓库已实现「连续序幂等应用」主链。缺口等待/跳过、乱序缓冲、事务整批、双 sink 部分失败、毒丸选择性跳过等均在 `backend/app/modules/*` 留空包，按题单逐题换主链路实现。
