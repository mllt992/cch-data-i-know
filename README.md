# CCH Stats Dashboard

FastAPI + PostgreSQL 的数据统计网站，支持：
- 费用统计
- 模型统计
- 调用趋势统计
- 模型可用性统计
- 渠道（供应商）调用统计
- Token 使用统计

所有统计维度都带缓存 TTL，避免高频打库。数据库连接、刷新频率、时间窗口都在 `.env` 配置。

## 功能

- 自动识别日志表和常见字段（时间、模型、渠道、费用、Token、状态、错误等）。
- 多页面可视化：
- `/` 首页总览
- `/availability` 模型可用性
- `/channels` 渠道数据分析
- `/tokens` Token 使用分析
- `/models` 模型使用分析
- ECharts 现代图表（仪表盘、Treemap、散点、混合图、排行图）。
- 柱状图支持排序切换（默认从高到低，可切换从低到高）。
- 模型可用性页面含“实时可用性检测”方格板块（今天/近七天/近一个月/全部）。
- Docker 一键部署。

## 环境变量

见 `.env.example`。关键项：

- `DATABASE_DSN`
- `HISTORY_DAYS`
- `MAX_RANGE_DAYS`

- `REFRESH_COST_SECONDS`
- `REFRESH_MODEL_SECONDS`
- `REFRESH_CALL_SECONDS`
- `REFRESH_AVAILABILITY_SECONDS`
- `REFRESH_REALTIME_AVAILABILITY_SECONDS`
- `REFRESH_CHANNEL_SECONDS`
- `REFRESH_TOKEN_SECONDS`

- `REALTIME_AVAILABILITY_MODEL_LIMIT`
- `REALTIME_AVAILABILITY_EVENT_LIMIT`
- `REALTIME_AVAILABILITY_ALL_MAX_DAYS`

- `CHANNEL_NAME_COLUMN_OVERRIDE`
- `CHANNEL_ID_COLUMN_OVERRIDE`
- `CHANNEL_LOOKUP_TABLE_OVERRIDE`
- `CHANNEL_LOOKUP_ID_COLUMN_OVERRIDE`
- `CHANNEL_LOOKUP_NAME_COLUMN_OVERRIDE`

渠道映射逻辑：
- 优先直接使用渠道名称列。
- 若只有渠道 ID，优先尝试外键映射到字典表名称。
- 无外键时，会启发式匹配 `provider/channel/vendor/supplier` 字典表。
- 仍无法映射时归并为“未知渠道”，不会直接展示 ID。

## 本地运行

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

打开 `http://localhost:8000`。

## Docker 运行

```bash
docker compose up -d --build
```

打开 `http://localhost:8000`。

## API

- `GET /healthz`
- `GET /api/dashboard`
- `GET /api/stats/cost`
- `GET /api/stats/model`
- `GET /api/stats/call-trend`
- `GET /api/stats/availability`
- `GET /api/stats/channel`
- `GET /api/stats/token`
- `GET /api/stats/realtime-availability?window=today|7d|30d|all`

说明：
- `window=all` 时按“天”聚合，每个方格固定代表 1 天，最多查询最近 `REALTIME_AVAILABILITY_ALL_MAX_DAYS` 天（默认 120 天）。

通用时间参数（统计 API 支持）：
- `start_date=YYYY-MM-DD`
- `end_date=YYYY-MM-DD`
