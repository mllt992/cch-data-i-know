// ======= page-users.js =======
// 用户数据分析页面逻辑

let userTreeData = [];
let selectedUserId = null;
let selectedKeyId = null;
let currentKeyData = null;
let currentUserData = null;

const expandedUserIds = new Set();
const expandedKeyIds = new Set();

let treeSearchKeyword = "";
let treeStatusFilter = "all";
let keyDetailTab = "overview";

let recordKeyword = "";
let recordStatus = "all";

let recordsPage = 1;
let recordsPageSize = 10;

let keyAutoRefreshEnabled = false;
let keyAutoRefreshSeconds = 30;
let refreshTimer = null;
let refreshCountdownTimer = null;
let nextAutoRefreshAt = 0;
let lastRefreshAtText = "-";

const visualConfig = {
  refresh_seconds: 30,
  auto_refresh_enabled: false,
  records_default_page_size: 10,
  records_max_page_size: 100,
};

function clampInt(value, min, max) {
  const n = Number(value);
  if (!Number.isFinite(n)) return min;
  return Math.min(Math.max(Math.floor(n), min), max);
}

function fmtDateTime(value) {
  return CCH.formatDateTimeCN(value);
}

function normalizeRecordStatus(status) {
  const raw = String(status || "").toLowerCase();
  if (!raw) return "other";
  if (raw === "success" || raw === "ok" || raw === "200") return "success";
  if (raw === "failed" || raw === "fail" || raw === "error" || raw === "500") return "failed";
  return "other";
}

function toStatusTag(status) {
  const normalized = normalizeRecordStatus(status);
  if (normalized === "success") return '<span class="status-tag status-success">成功</span>';
  if (normalized === "failed") return '<span class="status-tag status-failed">失败</span>';
  return '<span class="status-tag status-other">其他</span>';
}

function toChannelCell(channel) {
  const text = String(channel || "unknown");
  const safeText = CCH.escapeHtml(text);
  const cls = text.toLowerCase() === "unknown" ? "channel-tag is-unknown" : "channel-tag";
  return `<span class="${cls}" title="${safeText}">${safeText}</span>`;
}

function toErrorCell(errorMessage) {
  const text = (errorMessage || "").trim();
  if (!text) return '<span class="error-empty">-</span>';
  const safeText = CCH.escapeHtml(text);
  return `<span class="error-text" title="${safeText}">${safeText}</span>`;
}

function statusBadgeHtml(isEnabled) {
  if (isEnabled) return '<span class="tree-status-badge tree-status-ok">启用</span>';
  return '<span class="tree-status-badge tree-status-off">禁用</span>';
}

function formatChannelMetric(value) {
  if (value === null || value === undefined || value === "") return "-";
  const n = Number(value);
  if (!Number.isFinite(n)) return String(value);
  if (Number.isInteger(n)) return String(n);
  return n.toFixed(2).replace(/\.0+$/, "").replace(/(\.\d*?)0+$/, "$1");
}

function toDateInputValue(dateObj) {
  const y = dateObj.getFullYear();
  const m = String(dateObj.getMonth() + 1).padStart(2, "0");
  const d = String(dateObj.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function highlightText(text, keyword) {
  const source = String(text || "");
  const safe = CCH.escapeHtml(source);
  if (!keyword) return safe;
  const escapedKeyword = keyword.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const matcher = new RegExp(`(${escapedKeyword})`, "ig");
  return safe.replace(matcher, "<mark>$1</mark>");
}

function matchesEnabledFilter(isEnabled, filter) {
  if (filter === "enabled") return Boolean(isEnabled);
  if (filter === "disabled") return !Boolean(isEnabled);
  return true;
}

function setActiveContext(text) {
  CCH.setText("activeContextText", text || "未选择");
}

function switchKeyTab(tab) {
  keyDetailTab = tab;
  document.querySelectorAll("#keyViewTabs .key-tab-btn").forEach((btn) => {
    btn.classList.toggle("is-active", btn.dataset.tab === tab);
  });
  document.querySelectorAll(".key-tab-panel").forEach((panel) => {
    panel.hidden = panel.dataset.tabPanel !== tab;
  });
  window.dispatchEvent(new Event("resize"));
}

function showPanel(which) {
  const empty = document.getElementById("detailEmpty");
  const userSec = document.getElementById("userStatSection");
  const keySec = document.getElementById("keyStatSection");
  if (empty) empty.hidden = which !== "empty";
  if (userSec) userSec.hidden = which !== "user";
  if (keySec) keySec.hidden = which !== "key";
}

function buildFilteredTreeData() {
  const keyword = treeSearchKeyword.trim().toLowerCase();
  let visibleUsers = 0;
  let visibleKeys = 0;

  const rows = userTreeData
    .map((user) => {
      const userText = `${user.name || ""} ${user.role || ""} ${user.provider_group || ""}`.toLowerCase();
      const userMatch = !keyword || userText.includes(keyword);
      const userEnabledMatch = matchesEnabledFilter(user.is_enabled, treeStatusFilter);

      const keys = (user.keys || [])
        .map((key) => {
          const keyText = `${key.name || ""} ${key.provider_group || ""}`.toLowerCase();
          const keyMatch = !keyword || keyText.includes(keyword);
          const keyEnabledMatch = matchesEnabledFilter(key.is_enabled, treeStatusFilter);

          const channels = (key.channels || []).filter((ch) => {
            const channelText = `${ch.name || ""} ${ch.group_tag || ""} ${ch.priority ?? ""} ${ch.weight ?? ""}`.toLowerCase();
            const channelKeywordMatch = !keyword || channelText.includes(keyword);
            const channelEnabledMatch = matchesEnabledFilter(ch.is_enabled, treeStatusFilter);
            return channelKeywordMatch && channelEnabledMatch;
          });

          const keepByKeyword = userMatch || keyMatch || channels.length > 0;
          const keepByStatus = keyEnabledMatch || channels.length > 0;
          if (!keepByKeyword || !keepByStatus) return null;

          return {
            ...key,
            channels,
          };
        })
        .filter(Boolean);

      const keepByKeyword = userMatch || keys.length > 0;
      const keepByStatus = userEnabledMatch || keys.length > 0;
      if (!keepByKeyword || !keepByStatus) return null;

      visibleUsers += 1;
      visibleKeys += keys.length;
      return {
        ...user,
        keys,
      };
    })
    .filter(Boolean);

  return { rows, visibleUsers, visibleKeys };
}

function renderTree() {
  const container = document.getElementById("userTree");
  if (!container) return;

  const { rows, visibleUsers, visibleKeys } = buildFilteredTreeData();
  CCH.setText("treeMetaText", `显示 ${visibleUsers} 用户 / ${visibleKeys} 密钥 · 总用户 ${userTreeData.length}`);

  if (!rows.length) {
    container.innerHTML = '<div class="meta">未找到匹配的用户/密钥</div>';
    return;
  }

  container.innerHTML = "";
  const keyword = treeSearchKeyword.trim();
  const autoExpandMatched = Boolean(keyword);

  rows.forEach((user) => {
    const userWrap = document.createElement("div");
    userWrap.className = "tree-user-wrap";

    const keysHtml = (user.keys || [])
      .map((key) => {
        const channelsHtml = (key.channels || []).length
          ? key.channels
              .map((ch) => {
                const priorityText = CCH.escapeHtml(formatChannelMetric(ch.priority));
                const weightText = CCH.escapeHtml(formatChannelMetric(ch.weight));
                return `<div class="tree-channel"><span class="tree-channel-name">${highlightText(ch.name, keyword)}${statusBadgeHtml(ch.is_enabled)}</span><span class="tree-channel-meta">优先级:${priorityText} | 权重:${weightText}</span></div>`;
              })
              .join("")
          : '<div class="tree-channel">无可用渠道</div>';

        const expanded = autoExpandMatched || expandedKeyIds.has(key.id) ? "is-expanded" : "";
        const active = selectedKeyId === key.id ? "is-active" : "";

        return `
          <div class="tree-key ${expanded} ${active}" data-user-id="${user.id}" data-key-id="${key.id}">
            <div class="tree-key-label">
              <span class="tree-expand-icon">▶</span>
              ${highlightText(key.name, keyword)}
              ${statusBadgeHtml(key.is_enabled)}
            </div>
            <div class="tree-key-preview">Key ID: ${CCH.escapeHtml(String(key.id || "-"))}</div>
            <div class="tree-channels">${channelsHtml}</div>
          </div>`;
      })
      .join("");

    const noKeysHtml = (user.keys || []).length === 0 ? '<div class="meta" style="font-size:11px;padding:4px 0">无密钥</div>' : "";
    const userExpanded = autoExpandMatched || expandedUserIds.has(user.id) ? "is-expanded" : "";
    const userActive = selectedUserId === user.id && !selectedKeyId ? "is-active" : "";

    userWrap.innerHTML = `
      <div class="tree-user ${userExpanded} ${userActive}" data-user-id="${user.id}">
        <div class="tree-user-header">
          <div>
            <span class="tree-expand-icon">▶</span>
            <span class="tree-user-name">${highlightText(user.name, keyword)}</span>
            ${statusBadgeHtml(user.is_enabled)}
          </div>
          <span class="tree-role-badge">${CCH.escapeHtml(user.role || "-")}</span>
        </div>
        <div class="tree-keys">${keysHtml}${noKeysHtml}</div>
      </div>`;

    container.appendChild(userWrap);

    const userEl = userWrap.querySelector(".tree-user");

    userEl.addEventListener("click", (e) => {
      if (e.target.closest(".tree-key")) return;
      if (expandedUserIds.has(user.id)) expandedUserIds.delete(user.id);
      else expandedUserIds.add(user.id);
      userEl.classList.toggle("is-expanded");
      loadUserStats(user.id);
    });

    userWrap.querySelectorAll(".tree-key").forEach((keyEl) => {
      keyEl.addEventListener("click", (e) => {
        e.stopPropagation();
        const uid = Number(keyEl.dataset.userId || 0);
        const kid = Number(keyEl.dataset.keyId || 0);
        if (expandedKeyIds.has(kid)) expandedKeyIds.delete(kid);
        else expandedKeyIds.add(kid);
        keyEl.classList.toggle("is-expanded");
        loadKeyStats(uid, kid);
      });
    });
  });
}

function expandAllTree() {
  expandedUserIds.clear();
  expandedKeyIds.clear();
  userTreeData.forEach((user) => {
    expandedUserIds.add(user.id);
    (user.keys || []).forEach((key) => expandedKeyIds.add(key.id));
  });
  renderTree();
}

function collapseAllTree() {
  expandedUserIds.clear();
  expandedKeyIds.clear();
  renderTree();
}

async function fetchUserTree() {
  try {
    const data = await fetch("/api/users/tree");
    if (!data.ok) throw new Error(`HTTP ${data.status}`);
    userTreeData = await data.json();
    renderTree();
  } catch (e) {
    const container = document.getElementById("userTree");
    if (container) container.innerHTML = `<div class="meta">加载失败: ${CCH.escapeHtml(e.message)}</div>`;
  }
}

function drawUserCharts(data) {
  const byModel = (data.by_model || []).slice(0, 12);
  const trend = data.trend || [];

  CCH.renderChart("chartUserModels", {
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "shadow" },
      formatter: (params) => {
        const item = params?.[0];
        if (!item) return "";
        const row = byModel[item.dataIndex] || {};
        return `${item.name}<br/>调用数: ${CCH.fmtNumber(row.calls || 0)}<br/>成功率: ${CCH.fmtPercent(row.success_rate || 0)}`;
      },
    },
    grid: { left: 150, right: 16, top: 20, bottom: 16 },
    xAxis: { type: "value", name: "调用数" },
    yAxis: {
      type: "category",
      inverse: true,
      data: byModel.map((x) => x.model || "unknown"),
      axisLabel: { width: 140, overflow: "truncate" },
    },
    series: [{ type: "bar", data: byModel.map((x) => Number(x.calls || 0)), itemStyle: { color: "rgba(46,131,255,0.82)", borderRadius: 8 } }],
  });

  CCH.renderChart("chartUserTrend", {
    tooltip: { trigger: "axis" },
    legend: { data: ["调用数", "成功率"] },
    grid: { left: 48, right: 20, top: 30, bottom: 24 },
    xAxis: { type: "category", data: trend.map((x) => x.day), axisLabel: { rotate: trend.length > 10 ? 30 : 0 } },
    yAxis: [{ type: "value", name: "调用数" }, { type: "value", name: "成功率", min: 0, max: 100, splitLine: { show: false } }],
    series: [
      { name: "调用数", type: "bar", yAxisIndex: 0, data: trend.map((x) => Number(x.calls || 0)), itemStyle: { color: "rgba(26,168,153,0.75)", borderRadius: 6 } },
      { name: "成功率", type: "line", smooth: true, yAxisIndex: 1, data: trend.map((x) => Number(x.success_rate || 0)) },
    ],
  });
}

function renderUserModelTable(data) {
  const rows = data.by_model || [];
  const tbody = document.getElementById("userModelTbody");
  if (!tbody) return;

  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="7" class="meta">当前时间范围暂无模型调用数据</td></tr>';
    return;
  }

  tbody.innerHTML = rows
    .map(
      (x) => `<tr>
      <td>${CCH.escapeHtml(x.model || "unknown")}</td>
      <td>${CCH.fmtNumber(x.calls || 0)}</td>
      <td>${CCH.fmtPercent(x.success_rate || 0)}</td>
      <td>$${CCH.fmtMoney(x.cost || 0)}</td>
      <td>${CCH.fmtTokenM(x.tokens || 0)}</td>
      <td>${CCH.fmtTokenM(x.prompt_tokens || 0)}</td>
      <td>${CCH.fmtTokenM(x.completion_tokens || 0)}</td>
    </tr>`
    )
    .join("");
}

async function loadUserStats(userId) {
  document.querySelectorAll(".tree-key.is-active").forEach((el) => el.classList.remove("is-active"));
  document.querySelectorAll(".tree-user.is-active").forEach((el) => el.classList.remove("is-active"));
  const userEl = document.querySelector(`.tree-user[data-user-id="${userId}"]`);
  if (userEl) userEl.classList.add("is-active");

  selectedUserId = userId;
  selectedKeyId = null;
  currentKeyData = null;
  stopAutoRefresh();

  CCH.setText("metaText", "正在加载用户统计数据...");
  CCH.setText("recordFilterMeta", "用户视图");
  setActiveContext(`用户 #${userId}`);
  showPanel("user");

  const range = CCH.getRange();
  const url = `/api/users/${userId}/stats?start_date=${range.start}&end_date=${range.end}`;

  try {
    const resp = await fetch(url);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    currentUserData = data;
    renderUserDetail(data);
    CCH.setText("metaText", `用户: ${data.user_info?.name || "-"} | 统计范围: ${CCH.formatDateCN(data.time_range?.start)} 至 ${CCH.formatDateCN(data.time_range?.end)}`);
  } catch (e) {
    CCH.setText("metaText", `加载用户数据失败: ${e.message}`);
  }
}

function renderUserDetail(data) {
  const stats = data.stats || {};
  const userInfo = data.user_info || {};

  CCH.setText("userStatTitle", `用户: ${userInfo.name || "-"}`);
  CCH.setText("userStatSubtitle", `角色: ${userInfo.role || "-"} | Provider Group: ${userInfo.provider_group || "N/A"}`);

  CCH.setText("uKpiCalls", CCH.fmtNumber(stats.total_requests || 0));
  CCH.setText("uKpiSuccess", CCH.fmtPercent(stats.success_rate || 0));
  CCH.setText("uKpiCost", `$${CCH.fmtMoney(stats.total_cost_usd || 0)}`);
  CCH.setText("uKpiTokens", CCH.fmtTokenM(Number(stats.total_tokens || 0)));
  CCH.setText("uKpiModels", CCH.fmtNumber(stats.unique_models || 0));
  CCH.setText("uKpiFails", CCH.fmtNumber(stats.failure_count || 0));
  CCH.setText("uKpiEnabledKeys", `${CCH.fmtNumber(stats.enabled_keys || 0)} / ${CCH.fmtNumber(stats.total_keys || 0)}`);
  CCH.setText("uKpiAvgCost", `$${CCH.fmtMoney(stats.avg_cost_per_request || 0)}`);

  drawUserCharts(data);
  renderUserModelTable(data);
  setActiveContext(`用户：${userInfo.name || userInfo.id || "-"}`);
  showPanel("user");
}

async function loadKeyStats(userId, keyId, forceRefresh = false) {
  document.querySelectorAll(".tree-user.is-active").forEach((el) => el.classList.remove("is-active"));
  document.querySelectorAll(".tree-key.is-active").forEach((el) => el.classList.remove("is-active"));
  const keyEl = document.querySelector(`.tree-key[data-user-id="${userId}"][data-key-id="${keyId}"]`);
  if (keyEl) keyEl.classList.add("is-active");

  selectedUserId = userId;
  selectedKeyId = keyId;

  CCH.setText("metaText", "正在加载密钥统计数据...");
  CCH.setText("recordFilterMeta", "密钥视图");
  setActiveContext(`密钥 #${keyId}`);
  showPanel("key");

  const range = CCH.getRange();
  const url =
    `/api/users/${userId}/keys/${keyId}/stats` +
    `?start_date=${range.start}&end_date=${range.end}` +
    `&records_page=${recordsPage}&records_page_size=${recordsPageSize}` +
    (forceRefresh ? "&force_refresh=true" : "");

  try {
    const resp = await fetch(url);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    currentKeyData = data;
    recordsPage = Number(data?.records_pagination?.page || recordsPage);

    fillKeyKpi(data);
    drawKeyCharts(data);
    renderKeyModelTable(data);
    renderKeyRecordTable(data);

    const keyInfo = data.key_info || {};
    CCH.setText("keyStatTitle", `密钥: ${keyInfo.name || "-"}`);
    CCH.setText("keyStatSubtitle", `Key ID: ${keyInfo.id || "-"} | Provider Group: ${keyInfo.provider_group || "N/A"}`);
    CCH.setText(
      "metaText",
      `密钥: ${keyInfo.name || "-"} | 统计范围: ${CCH.formatDateCN(data.time_range?.start)} 至 ${CCH.formatDateCN(data.time_range?.end)} | 更新时间: ${CCH.formatDateTimeCN(data.generated_at)}`
    );

    setActiveContext(`密钥：${keyInfo.name || keyInfo.id || "-"}`);
    lastRefreshAtText = CCH.formatDateTimeCN(new Date());
    renderRefreshStatus();
  } catch (e) {
    CCH.setText("metaText", `加载密钥数据失败: ${e.message}`);
    renderRefreshStatus();
  }
}

async function safeLoadKeyStats(forceRefresh = false) {
  if (!selectedUserId || !selectedKeyId) return;
  try {
    await loadKeyStats(selectedUserId, selectedKeyId, forceRefresh);
  } catch (e) {
    CCH.setText("metaText", `数据加载失败: ${e.message}`);
    renderRefreshStatus();
  }
}

function fillKeyKpi(data) {
  const summary = data.summary || {};
  const totalCalls = Number(summary.total_calls || 0);
  const totalCost = Number(summary.total_cost || 0);
  const avgCost = totalCalls > 0 ? totalCost / totalCalls : 0;

  CCH.setText("kKpiCalls", CCH.fmtNumber(totalCalls));
  CCH.setText("kKpiSuccess", CCH.fmtPercent(summary.success_rate || 0));
  CCH.setText("kKpiCost", `$${CCH.fmtMoney(totalCost)}`);
  CCH.setText("kKpiTokens", CCH.fmtTokenM(summary.total_tokens || 0));
  CCH.setText("kKpiAvgTokens", CCH.fmtNumber(summary.avg_tokens_per_call || 0));
  CCH.setText("kKpiAvgCost", `$${CCH.fmtMoney(avgCost)}`);
}

function drawKeyCharts(data) {
  const summary = data.summary || {};
  const trend = data.trend || [];
  const sortOrder = document.getElementById("keyModelSort")?.value || "desc";
  const byModel = CCH.sortRows(data.by_model || [], "tokens", sortOrder).slice(0, 12);

  CCH.renderChart("chartKeyTrend", {
    tooltip: { trigger: "axis" },
    legend: { data: ["调用数", "Token(M)", "费用"] },
    grid: { left: 46, right: 50, top: 30, bottom: 24 },
    xAxis: { type: "category", data: trend.map((x) => x.day) },
    yAxis: [{ type: "value", name: "调用数" }, { type: "value", name: "Token(M)/费用", splitLine: { show: false } }],
    series: [
      { name: "调用数", type: "bar", yAxisIndex: 0, itemStyle: { color: "rgba(46,131,255,0.72)", borderRadius: 6 }, data: trend.map((x) => Number(x.calls || 0)) },
      { name: "Token(M)", type: "line", yAxisIndex: 1, smooth: true, data: trend.map((x) => CCH.toTokenM(x.tokens || 0)) },
      { name: "费用", type: "line", yAxisIndex: 1, smooth: true, data: trend.map((x) => Number(x.cost || 0)) },
    ],
  });

  CCH.renderChart("chartKeyByModel", {
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "shadow" },
      formatter: (params) => {
        const item = params?.[0];
        if (!item) return "";
        return `${item.name}<br/>Token: ${Number(item.value || 0).toFixed(2)} M`;
      },
    },
    grid: { left: 140, right: 20, top: 22, bottom: 18 },
    xAxis: { type: "value", name: "Token(M)" },
    yAxis: { type: "category", inverse: true, data: byModel.map((x) => x.model), axisLabel: { width: 130, overflow: "truncate" } },
    series: [{ name: "Token", type: "bar", data: byModel.map((x) => CCH.toTokenM(x.tokens)), itemStyle: { color: "rgba(26,168,153,0.82)", borderRadius: 8 } }],
  });

  CCH.renderChart("chartKeyTokenSplit", {
    tooltip: { trigger: "item", formatter: (p) => `${p.name}<br/>${Number(p.value || 0).toFixed(2)} M (${p.percent}%)` },
    legend: { bottom: 0 },
    series: [
      {
        type: "pie",
        radius: ["38%", "70%"],
        center: ["50%", "45%"],
        itemStyle: { borderColor: "#fff", borderWidth: 2 },
        label: { formatter: "{b}\n{d}%" },
        data: [
          { name: "输入Token", value: CCH.toTokenM(summary.prompt_tokens || 0) },
          { name: "输出Token", value: CCH.toTokenM(summary.completion_tokens || 0) },
          { name: "缓存Token", value: CCH.toTokenM(summary.cache_tokens || 0) },
        ],
      },
    ],
  });
}

function renderKeyModelTable(data) {
  const sortOrder = document.getElementById("keyModelSort")?.value || "desc";
  const rows = CCH.sortRows(data.by_model || [], "tokens", sortOrder);
  const tbody = document.getElementById("keyModelTbody");
  if (!tbody) return;

  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="8" class="meta">当前时间范围暂无模型数据</td></tr>';
    return;
  }

  tbody.innerHTML = rows
    .slice(0, 30)
    .map(
      (x) => `<tr>
      <td>${CCH.escapeHtml(x.model)}</td>
      <td>${CCH.fmtNumber(x.calls)}</td>
      <td>${CCH.fmtPercent(x.success_rate)}</td>
      <td>$${CCH.fmtMoney(x.cost)}</td>
      <td>${CCH.fmtTokenM(x.tokens)}</td>
      <td>${CCH.fmtTokenM(x.prompt_tokens)}</td>
      <td>${CCH.fmtTokenM(x.completion_tokens)}</td>
      <td>${CCH.fmtTokenM(x.cache_tokens)}</td>
    </tr>`
    )
    .join("");
}

function filterRecordRows(rows) {
  const keyword = recordKeyword.trim().toLowerCase();
  return (rows || []).filter((row) => {
    const statusMatched = recordStatus === "all" || normalizeRecordStatus(row.status) === recordStatus;
    if (!statusMatched) return false;
    if (!keyword) return true;

    const text = `${row.model || ""} ${row.channel || ""} ${row.error_message || ""} ${row.key_name || row.key_value || ""}`.toLowerCase();
    return text.includes(keyword);
  });
}

function renderKeyRecordTable(data) {
  const rows = data.records || [];
  const filteredRows = filterRecordRows(rows);

  const tbody = document.getElementById("keyRecordTbody");
  if (!tbody) return;

  if (!filteredRows.length) {
    tbody.innerHTML = '<tr><td colspan="9" class="meta">当前筛选条件下无记录</td></tr>';
  } else {
    tbody.innerHTML = filteredRows
      .map(
        (x) => `<tr>
      <td>${CCH.escapeHtml(x.key_name || x.key_value || "unknown")}</td>
      <td>${CCH.escapeHtml(fmtDateTime(x.called_at))}</td>
      <td>${CCH.escapeHtml(x.model)}</td>
      <td>${toChannelCell(x.channel)}</td>
      <td>${toStatusTag(x.status)}</td>
      <td>$${CCH.fmtMoney(x.cost)}</td>
      <td>${CCH.fmtTokenM(x.total_tokens)}</td>
      <td>${CCH.fmtNumber(x.latency_ms)}</td>
      <td>${toErrorCell(x.error_message)}</td>
    </tr>`
      )
      .join("");
  }

  CCH.setText("recordFilterMeta", `当前页记录 ${filteredRows.length} / ${rows.length}`);
  renderRecordPager(data.records_pagination || {});
}

function renderRecordPager(pagination) {
  const page = Number(pagination?.page || 1);
  const totalPages = Number(pagination?.total_pages || 1);
  const totalRecords = Number(pagination?.total_records || 0);

  CCH.setText("recordPageInfo", `第 ${page} / ${totalPages} 页，共 ${CCH.fmtNumber(totalRecords)} 条`);

  const jumpInput = document.getElementById("recordJumpInput");
  if (jumpInput) {
    jumpInput.min = "1";
    jumpInput.max = String(totalPages);
    jumpInput.value = String(page);
  }

  const pagesEl = document.getElementById("recordPageButtons");
  if (pagesEl) {
    const html = buildPageNumberButtons(page, totalPages)
      .map((item) => {
        if (item === "...") return '<span class="pager-ellipsis">...</span>';
        const active = item === page ? "active" : "";
        return `<button class="pager-btn ${active}" data-page="${item}">${item}</button>`;
      })
      .join("");
    pagesEl.innerHTML = html;
  }

  const prevBtn = document.getElementById("recordPrevBtn");
  const nextBtn = document.getElementById("recordNextBtn");
  if (prevBtn) prevBtn.disabled = page <= 1;
  if (nextBtn) nextBtn.disabled = page >= totalPages;
}

function buildPageNumberButtons(currentPage, totalPages) {
  if (totalPages <= 9) {
    return Array.from({ length: totalPages }, (_, i) => i + 1);
  }

  const pages = new Set([1, totalPages]);
  for (let i = currentPage - 2; i <= currentPage + 2; i += 1) {
    if (i >= 1 && i <= totalPages) pages.add(i);
  }
  if (currentPage <= 4) {
    for (let i = 1; i <= 6; i += 1) pages.add(i);
  }
  if (currentPage >= totalPages - 3) {
    for (let i = totalPages - 5; i <= totalPages; i += 1) {
      if (i >= 1) pages.add(i);
    }
  }

  const sorted = Array.from(pages).sort((a, b) => a - b);
  const withEllipsis = [];
  for (let i = 0; i < sorted.length; i += 1) {
    const p = sorted[i];
    const prev = sorted[i - 1];
    if (prev && p - prev > 1) withEllipsis.push("...");
    withEllipsis.push(p);
  }
  return withEllipsis;
}

function renderRefreshStatus() {
  const statusText = keyAutoRefreshEnabled
    ? `自动刷新：已开启（${keyAutoRefreshSeconds} 秒） | 最近刷新：${lastRefreshAtText}`
    : `自动刷新：已关闭 | 最近刷新：${lastRefreshAtText}`;
  CCH.setText("recordRefreshStatus", statusText);

  if (!keyAutoRefreshEnabled || !nextAutoRefreshAt) {
    CCH.setText("recordRefreshCountdown", "下次刷新：-");
    return;
  }

  const remainSeconds = Math.max(0, Math.ceil((nextAutoRefreshAt - Date.now()) / 1000));
  CCH.setText("recordRefreshCountdown", `下次刷新：${remainSeconds} 秒后`);
}

function stopRefreshCountdownTicker() {
  if (!refreshCountdownTimer) return;
  clearInterval(refreshCountdownTimer);
  refreshCountdownTimer = null;
}

function startRefreshCountdownTicker() {
  stopRefreshCountdownTicker();
  if (!keyAutoRefreshEnabled || !nextAutoRefreshAt) {
    renderRefreshStatus();
    return;
  }
  renderRefreshStatus();
  refreshCountdownTimer = setInterval(renderRefreshStatus, 1000);
}

function stopAutoRefresh() {
  if (refreshTimer) {
    clearInterval(refreshTimer);
    refreshTimer = null;
  }
  stopRefreshCountdownTicker();
  nextAutoRefreshAt = 0;
  renderRefreshStatus();
}

function resetAutoRefreshTimer() {
  if (refreshTimer) {
    clearInterval(refreshTimer);
    refreshTimer = null;
  }
  stopRefreshCountdownTicker();

  if (!keyAutoRefreshEnabled || !selectedKeyId) {
    nextAutoRefreshAt = 0;
    renderRefreshStatus();
    return;
  }

  safeLoadKeyStats(true);
  nextAutoRefreshAt = Date.now() + keyAutoRefreshSeconds * 1000;
  startRefreshCountdownTicker();

  refreshTimer = setInterval(() => {
    safeLoadKeyStats(true);
    nextAutoRefreshAt = Date.now() + keyAutoRefreshSeconds * 1000;
    renderRefreshStatus();
  }, keyAutoRefreshSeconds * 1000);
}

function buildPageSizeOptions(maxPageSize) {
  const base = [10, 20, 30, 50, 100, 200];
  const filtered = base.filter((item) => item <= maxPageSize);
  if (!filtered.includes(maxPageSize)) filtered.push(maxPageSize);
  return Array.from(new Set(filtered)).sort((a, b) => a - b);
}

function renderVisualConfigControls() {
  const pageSizeSelect = document.getElementById("recordsPageSizeSelect");
  if (!pageSizeSelect) return;

  const options = buildPageSizeOptions(visualConfig.records_max_page_size);
  pageSizeSelect.innerHTML = options.map((size) => `<option value="${size}">${size} 条/页</option>`).join("");
  pageSizeSelect.value = String(recordsPageSize);

  const autoInput = document.getElementById("autoRefreshSecondsInput");
  if (autoInput) autoInput.value = String(keyAutoRefreshSeconds);

  const autoEnabledInput = document.getElementById("autoRefreshEnabledInput");
  if (autoEnabledInput) autoEnabledInput.checked = Boolean(keyAutoRefreshEnabled);

  renderRefreshStatus();
}

function applyVisualConfigFromUI(triggerRefresh = false) {
  const pageSizeSelect = document.getElementById("recordsPageSizeSelect");
  const autoInput = document.getElementById("autoRefreshSecondsInput");
  const autoEnabledInput = document.getElementById("autoRefreshEnabledInput");

  const selectedPageSize = clampInt(pageSizeSelect?.value, 1, visualConfig.records_max_page_size);
  const selectedRefreshSeconds = clampInt(autoInput?.value, 30, 86400);
  const selectedAutoEnabled = Boolean(autoEnabledInput?.checked);
  const pageSizeChanged = selectedPageSize !== recordsPageSize;

  recordsPageSize = selectedPageSize;
  keyAutoRefreshSeconds = selectedRefreshSeconds;
  keyAutoRefreshEnabled = selectedAutoEnabled;
  if (pageSizeChanged) recordsPage = 1;

  localStorage.setItem("cch_user_records_page_size", String(recordsPageSize));
  localStorage.setItem("cch_user_auto_refresh_seconds", String(keyAutoRefreshSeconds));
  localStorage.setItem("cch_user_auto_refresh_enabled", keyAutoRefreshEnabled ? "1" : "0");

  renderVisualConfigControls();
  resetAutoRefreshTimer();
  if (triggerRefresh && !keyAutoRefreshEnabled) {
    safeLoadKeyStats(true);
  }
}

async function initVisualConfig() {
  const config = await CCH.fetchKeyVisualizationConfig();
  visualConfig.refresh_seconds = clampInt(config?.refresh_seconds, 30, 86400);
  visualConfig.auto_refresh_enabled = Boolean(config?.auto_refresh_enabled);
  visualConfig.records_max_page_size = clampInt(config?.records_max_page_size, 1, 1000);
  visualConfig.records_default_page_size = clampInt(
    config?.records_default_page_size,
    1,
    visualConfig.records_max_page_size
  );

  const savedPageSize = Number(localStorage.getItem("cch_user_records_page_size") || 0);
  recordsPageSize = clampInt(
    savedPageSize || visualConfig.records_default_page_size,
    1,
    visualConfig.records_max_page_size
  );

  const savedRefreshSeconds = Number(localStorage.getItem("cch_user_auto_refresh_seconds") || 0);
  keyAutoRefreshSeconds = clampInt(savedRefreshSeconds || visualConfig.refresh_seconds, 30, 86400);

  const savedAutoEnabled = localStorage.getItem("cch_user_auto_refresh_enabled");
  keyAutoRefreshEnabled =
    savedAutoEnabled === null ? visualConfig.auto_refresh_enabled : savedAutoEnabled === "1";

  renderVisualConfigControls();
}

function triggerDownloadJson(filename, payload) {
  const blob = new Blob([JSON.stringify(payload, null, 2)], {
    type: "application/json;charset=utf-8",
  });
  const link = document.createElement("a");
  link.href = URL.createObjectURL(blob);
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(link.href);
}

function exportCurrentView() {
  const stamp = toDateInputValue(new Date());

  if (selectedKeyId && currentKeyData) {
    triggerDownloadJson(`user-key-${selectedKeyId}-${stamp}.json`, {
      exported_at: new Date().toISOString(),
      type: "key",
      user_id: selectedUserId,
      key_id: selectedKeyId,
      range: CCH.getRange(),
      payload: currentKeyData,
    });
    return;
  }

  if (selectedUserId && currentUserData) {
    triggerDownloadJson(`user-${selectedUserId}-${stamp}.json`, {
      exported_at: new Date().toISOString(),
      type: "user",
      user_id: selectedUserId,
      range: CCH.getRange(),
      payload: currentUserData,
    });
    return;
  }

  triggerDownloadJson(`users-tree-${stamp}.json`, {
    exported_at: new Date().toISOString(),
    type: "tree",
    payload: userTreeData,
  });
}

function bindRecordPagerActions() {
  document.getElementById("recordPrevBtn")?.addEventListener("click", () => {
    recordsPage = Math.max(recordsPage - 1, 1);
    safeLoadKeyStats();
  });

  document.getElementById("recordNextBtn")?.addEventListener("click", () => {
    const totalPages = Number(currentKeyData?.records_pagination?.total_pages || 1);
    recordsPage = Math.min(recordsPage + 1, totalPages);
    safeLoadKeyStats();
  });

  document.getElementById("recordPageButtons")?.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) return;
    const page = Number(target.dataset.page || 0);
    if (!Number.isFinite(page) || page <= 0) return;
    recordsPage = page;
    safeLoadKeyStats();
  });

  document.getElementById("recordJumpBtn")?.addEventListener("click", () => {
    const totalPages = Number(currentKeyData?.records_pagination?.total_pages || 1);
    const raw = document.getElementById("recordJumpInput")?.value;
    recordsPage = clampInt(raw, 1, totalPages);
    safeLoadKeyStats();
  });

  document.getElementById("recordJumpInput")?.addEventListener("keydown", (event) => {
    if (event.key !== "Enter") return;
    event.preventDefault();
    const totalPages = Number(currentKeyData?.records_pagination?.total_pages || 1);
    recordsPage = clampInt(event.target?.value, 1, totalPages);
    safeLoadKeyStats();
  });
}

function bindRecordFilterActions() {
  const applyFilter = () => {
    recordKeyword = String(document.getElementById("recordKeywordInput")?.value || "").trim();
    recordStatus = document.getElementById("recordStatusFilter")?.value || "all";
    if (currentKeyData) renderKeyRecordTable(currentKeyData);
  };

  document.getElementById("applyRecordFilterBtn")?.addEventListener("click", applyFilter);
  document.getElementById("recordKeywordInput")?.addEventListener("keydown", (event) => {
    if (event.key !== "Enter") return;
    event.preventDefault();
    applyFilter();
  });
  document.getElementById("recordStatusFilter")?.addEventListener("change", applyFilter);

  document.getElementById("resetRecordFilterBtn")?.addEventListener("click", () => {
    const keywordInput = document.getElementById("recordKeywordInput");
    const statusSelect = document.getElementById("recordStatusFilter");
    if (keywordInput) keywordInput.value = "";
    if (statusSelect) statusSelect.value = "all";
    recordKeyword = "";
    recordStatus = "all";
    if (currentKeyData) renderKeyRecordTable(currentKeyData);
  });
}

function bindVisualConfigActions() {
  document.getElementById("applyVisualConfigBtn")?.addEventListener("click", () => {
    applyVisualConfigFromUI(true);
  });
  document.getElementById("autoRefreshEnabledInput")?.addEventListener("change", () => {
    applyVisualConfigFromUI();
  });
  document.getElementById("autoRefreshSecondsInput")?.addEventListener("change", () => {
    applyVisualConfigFromUI();
  });
}

function bindTreeActions() {
  const treeSearchInput = document.getElementById("treeSearchInput");
  if (treeSearchInput) {
    treeSearchInput.addEventListener("input", () => {
      treeSearchKeyword = String(treeSearchInput.value || "").trim();
      renderTree();
    });
  }

  const statusFilter = document.getElementById("treeStatusFilter");
  if (statusFilter) {
    statusFilter.addEventListener("change", () => {
      treeStatusFilter = statusFilter.value || "all";
      renderTree();
    });
  }

  document.getElementById("treeExpandAllBtn")?.addEventListener("click", expandAllTree);
  document.getElementById("treeCollapseAllBtn")?.addEventListener("click", collapseAllTree);
}

function bindKeyTabActions() {
  document.getElementById("keyViewTabs")?.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) return;
    const button = target.closest(".key-tab-btn");
    if (!(button instanceof HTMLElement)) return;
    const tab = button.dataset.tab;
    if (!tab) return;
    switchKeyTab(tab);
  });
}

function applyQuickRange(days) {
  const end = new Date();
  const start = new Date(end);
  start.setDate(end.getDate() - (days - 1));

  const startInput = document.getElementById("startDate");
  const endInput = document.getElementById("endDate");
  if (!startInput || !endInput) return;

  startInput.value = toDateInputValue(start);
  endInput.value = toDateInputValue(end);
  document.getElementById("applyRangeBtn")?.click();
}

function bindQuickRangeActions() {
  document.getElementById("quickRange7Btn")?.addEventListener("click", () => applyQuickRange(7));
  document.getElementById("quickRange30Btn")?.addEventListener("click", () => applyQuickRange(30));
  document.getElementById("quickRange90Btn")?.addEventListener("click", () => applyQuickRange(90));
}

document.addEventListener("DOMContentLoaded", async () => {
  CCH.markActiveMenu();
  switchKeyTab("overview");
  setActiveContext("未选择");

  await initVisualConfig();
  renderVisualConfigControls();

  CCH.initRangeControls(() => {
    recordsPage = 1;
    if (selectedKeyId) {
      safeLoadKeyStats();
    } else if (selectedUserId) {
      loadUserStats(selectedUserId);
    }
  });

  CCH.bindRefresh(async (forceRefresh) => {
    await fetchUserTree();
    if (selectedKeyId) {
      await safeLoadKeyStats(forceRefresh);
    } else if (selectedUserId) {
      await loadUserStats(selectedUserId);
    }
  });

  document.getElementById("keyModelSort")?.addEventListener("change", () => {
    if (!currentKeyData) return;
    drawKeyCharts(currentKeyData);
    renderKeyModelTable(currentKeyData);
  });

  document.getElementById("exportCurrentBtn")?.addEventListener("click", exportCurrentView);

  bindTreeActions();
  bindKeyTabActions();
  bindQuickRangeActions();
  bindRecordPagerActions();
  bindRecordFilterActions();
  bindVisualConfigActions();

  await fetchUserTree();
});



