# 用户数据分析功能 - 更新说明

## 🎯 功能概述

新增用户维度的数据分析功能,支持:

1. **用户树形结构**: 用户 → 密钥 → 渠道的层级关系可视化
2. **用户统计数据**: 按用户维度查看总调用数、成功率、费用、Token 使用等
3. **密钥详细分析**: 查看特定密钥的:
   - 可用模型统计
   - 渠道使用情况
   - Token 使用统计
   - 成功率分析
   - 最近使用记录(带分页)

## 📦 新增内容

### 后端 API

**1. GET /api/users/tree**
- 获取用户树形结构(用户→密钥→渠道)
- 自动从数据库获取 users, keys, providers 表数据
- 支持 provider_group 自动关联渠道

**2. GET /api/users/{user_id}/stats**
- 获取指定用户的统计数据
- 支持时间范围筛选 (start_date, end_date)
- 返回: 总调用数、成功率、费用、Token 等指标

**3. GET /api/users/{user_id}/keys/{key_id}/stats**
- 获取用户特定密钥的详细统计
- 包含模型使用分布、渠道使用分布、使用记录明细
- 支持 records_limit 参数控制返回记录数量(默认100,最大1000)

### 前端页面

**新页面: /users** (`app/static/users.html`)

特性:
- 左侧树形结构: 可折叠的用户→密钥→渠道层级
- 右侧详情面板: 动态显示选中项的详细数据
- ECharts 图表可视化: 模型使用柱状图、渠道使用柱状图
- 响应式布局,支持时间范围筛选
- 实时数据刷新

### 数据库适配

智能检测并适配不同的数据库结构:
- 自动识别 `status_code` (integer) 或 `is_success` (boolean)
- 优先使用 `status_code = 200` 判断成功
- 回退到 `is_success = TRUE` (如果表中没有 status_code)

## 🚀 使用方法

### 访问页面

```bash
# 启动服务后访问
http://localhost:8000/users
```

### API 调用示例

```bash
# 获取用户树
curl http://localhost:8000/api/users/tree

# 获取用户统计(近30天)
curl "http://localhost:8000/api/users/10/stats?start_date=2025-01-01&end_date=2025-01-31"

# 获取密钥统计
curl "http://localhost:8000/api/users/10/keys/16/stats?records_limit=50"
```

## 📊 数据结构

### 用户树响应示例

```json
[
  {
    "id": 10,
    "name": "测试用户",
    "role": "user",
    "provider_group": "Codex,GPT",
    "is_enabled": true,
    "created_at": "2025-01-15 10:30:00",
    "keys": [
      {
        "id": 16,
        "name": "CC密钥",
        "key_preview": "sk-d55d91fe2a039425...",
        "provider_group": "CC,Claude",
        "is_enabled": true,
        "channels": [
          {
            "id": 99,
            "name": "Claude Provider",
            "group_tag": "CC",
            "is_enabled": true
          }
        ]
      }
    ]
  }
]
```

### 密钥统计响应示例

```json
{
  "user_id": 10,
  "key_id": 16,
  "key_info": {
    "name": "CC密钥",
    "key_preview": "sk-d55d91fe2a039425...",
    "provider_group": "CC,Claude"
  },
  "summary": {
    "total_requests": 882,
    "success_rate": 96.26,
    "total_cost_usd": 45.32,
    "unique_models": 3,
    "total_prompt_tokens": 1234567,
    "total_completion_tokens": 654321
  },
  "model_stats": [
    {
      "model_name": "claude-sonnet-4-5",
      "request_count": 692,
      "total_cost": 38.45,
      "success_rate": 97.5
    }
  ],
  "channel_stats": [...],
  "recent_records": [...]
}
```

## 🔧 技术实现

### 核心原则

✅ **KISS**: 简洁的树形结构和清晰的API设计
✅ **DRY**: 复用现有的 StatsService 缓存机制和时间处理逻辑
✅ **SOLID**: 单一职责,每个API端点专注于特定数据维度
✅ **YAGNI**: 仅实现必需功能,无过度设计

### 数据关系

```
users (1) ─── (N) keys
              │
              └─ provider_group 字段关联 ─→ providers.group_tag
```

- `keys.user_id` 外键关联 `users.id`
- `keys.provider_group` 包含逗号分隔的渠道分组标签
- `providers.group_tag` 匹配 provider_group 中的值

## ⚠️ 注意事项

1. **性能**: 用户树查询会遍历所有用户和密钥,大量数据时可能较慢(考虑添加缓存)
2. **权限**: 当前未实现用户权限控制,所有用户数据均可访问
3. **分页**: 使用记录默认限制100条,可通过 `records_limit` 调整(最大1000)

## 📝 后续优化建议

- [ ] 添加用户级别的缓存机制(类似现有的统计缓存)
- [ ] 支持用户树的搜索和过滤功能
- [ ] 添加 IP 地址统计(需要日志表包含 IP 字段)
- [ ] 支持导出用户/密钥统计报表
- [ ] 添加用户权限控制(仅查看自己的数据)

---

**测试通过**: ✅ All tests passed!
**更新时间**: 2026-03-05
