let keyData = null;
let availableKeys = [];
let selectedKeySlugs = [];
let refreshTimer = null;

const visualConfig = {
  refresh_seconds: 30,
  auto_refresh_enabled: false,
  records_default_page_size: 10,
  records_max_page_size: 100,
};

let recordsPage = 1;
let recordsPageSize = 10;
let keyAutoRefreshSeconds = 30;
let keyAutoRefreshEnabled = false;

function clampInt(value, min, max) {
  const n = Number(value);
  if (!Number.isFinite(n)) return min;
  return Math.min(Math.max(Math.floor(n), min), max);
}

function getPathKeySlug() {
  const parts = window.location.pathname.split("/").filter(Boolean);
  if (parts.length < 2 || parts[0] !== "keys") return "";
  return decodeURIComponent(parts[1] || "").toLowerCase();
}

function fmtDateTime(value) {
  return CCH.formatDateTimeCN(value);
}

function toStatusTag(status) {
  const raw = String(status || "").toLowerCase();
  if (raw === "success") return '<span class="status-tag status-success">成功</span>';
  if (raw === "failed") return '<span class="status-tag status-failed">失败</span>';
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

function getSelectedFromUI() {
  return Array.from(document.querySelectorAll('input[name="keySelectItem"]:checked')).map((item) =>
    String(item.value || "").toLowerCase()
  );
}

function renderKeySelector() {
  const holder = document.getElementById("keySelectorList");
  if (!holder) return;

  holder.innerHTML = availableKeys
    .map((item) => {
      const checked = selectedKeySlugs.includes(String(item.slug || "").toLowerCase()) ? "checked" : "";
      return `<label class="key-option">
        <input type="checkbox" name="keySelectItem" value="${CCH.escapeHtml(item.slug)}" ${checked} />
        <span>${CCH.escapeHtml(item.name)}</span>
      </label>`;
    })
    .join("");
}

function setSelectedAll() {
  selectedKeySlugs = availableKeys.map((item) => String(item.slug || "").toLowerCase());
  renderKeySelector();
}

function clearSelectedAll() {
  selectedKeySlugs = [];
  renderKeySelector();
}

function applySelectedFromUI() {
  selectedKeySlugs = getSelectedFromUI();
}

function updateTitleWithSelection() {
  const selected = availableKeys.filter((item) =>
    selectedKeySlugs.includes(String(item.slug || "").toLowerCase())
  );
  if (!selected.length) {
    CCH.setText("keyTitle", "Key 聚合分析");
    CCH.setText("keySubtitle", "请选择至少一个 key 后查看聚合统计");
    return;
  }
  if (selected.length === 1) {
    CCH.setText("keyTitle", `Key 使用分析 - ${selected[0].name}`);
    CCH.setText("keySubtitle", "按指定 key 查看调用、费用、Token 与明细记录");
    return;
  }
  const names = selected.slice(0, 3).map((item) => item.name).join("、");
  const suffix = selected.length > 3 ? ` 等 ${selected.length} 个` : ` 共 ${selected.length} 个`;
  CCH.setText("keyTitle", "Key 聚合分析");
  CCH.setText("keySubtitle", `${names}${suffix} key 的聚合调用、费用、Token 与明细记录`);
}

function fillKeyKpi(data) {
  const summary = data.summary || {};
  const totalCalls = Number(summary.total_calls || 0);
  const totalCost = Number(summary.total_cost || 0);
  const avgCost = totalCalls > 0 ? totalCost / totalCalls : 0;

  CCH.setText("kpiKeyTotalCalls", CCH.fmtNumber(totalCalls));
  CCH.setText("kpiKeySuccessRate", CCH.fmtPercent(summary.success_rate || 0));
  CCH.setText("kpiKeyTotalCost", `$${CCH.fmtMoney(totalCost)}`);
  CCH.setText("kpiKeyTotalTokens", CCH.fmtTokenM(summary.total_tokens || 0));
  CCH.setText("kpiKeyAvgTokens", CCH.fmtNumber(summary.avg_tokens_per_call || 0));
  CCH.setText("kpiKeyAvgCost", `$${CCH.fmtMoney(avgCost)}`);
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
    yAxis: [
      { type: "value", name: "调用数" },
      { type: "value", name: "Token(M)/费用", splitLine: { show: false } },
    ],
    series: [
      {
        name: "调用数",
        type: "bar",
        yAxisIndex: 0,
        itemStyle: { color: "rgba(46,131,255,0.72)", borderRadius: 6 },
        data: trend.map((x) => Number(x.calls || 0)),
      },
      {
        name: "Token(M)",
        type: "line",
        yAxisIndex: 1,
        smooth: true,
        data: trend.map((x) => CCH.toTokenM(x.tokens || 0)),
      },
      {
        name: "费用",
        type: "line",
        yAxisIndex: 1,
        smooth: true,
        data: trend.map((x) => Number(x.cost || 0)),
      },
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
    yAxis: {
      type: "category",
      inverse: true,
      data: byModel.map((x) => x.model),
      axisLabel: { width: 130, overflow: "truncate" },
    },
    series: [
      {
        name: "Token",
        type: "bar",
        data: byModel.map((x) => CCH.toTokenM(x.tokens)),
        itemStyle: { color: "rgba(26,168,153,0.82)", borderRadius: 8 },
      },
    ],
  });

  CCH.renderChart("chartKeyTokenSplit", {
    tooltip: {
      trigger: "item",
      formatter: (p) => `${p.name}<br/>${Number(p.value || 0).toFixed(2)} M (${p.percent}%)`,
    },
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
  const html = rows
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
  document.getElementById("keyModelTbody").innerHTML = html;
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
        if (item === "...") {
          return '<span class="pager-ellipsis">...</span>';
        }
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

function renderKeyRecordTable(data) {
  const rows = data.records || [];
  const html = rows
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
  document.getElementById("keyRecordTbody").innerHTML = html;
  renderRecordPager(data.records_pagination || {});
}

function setKeyMeta(data) {
  const source = data?.source || {};
  const selectedKeys = (data?.keys || []).map((item) => item.name).join("、") || "-";
  CCH.setText(
    "metaText",
    `Key: ${selectedKeys} | 数据源: ${source.table || "-"} | Key字段: ${source.request_key_col || "未识别"} | ` +
      `统计范围: ${CCH.formatDateCN(data.time_range?.start)} 至 ${CCH.formatDateCN(data.time_range?.end)} | 更新时间: ${CCH.formatDateTimeCN(data.generated_at)}`
  );
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
  pageSizeSelect.innerHTML = options
    .map((size) => `<option value="${size}">${size} 条/页</option>`)
    .join("");
  pageSizeSelect.value = String(recordsPageSize);
  const autoInput = document.getElementById("autoRefreshSecondsInput");
  if (autoInput) autoInput.value = String(keyAutoRefreshSeconds);
  const autoEnabledInput = document.getElementById("autoRefreshEnabledInput");
  if (autoEnabledInput) autoEnabledInput.checked = Boolean(keyAutoRefreshEnabled);
}

function resetAutoRefreshTimer() {
  if (refreshTimer) {
    clearInterval(refreshTimer);
    refreshTimer = null;
  }
  if (!keyAutoRefreshEnabled) return;
  refreshTimer = setInterval(() => {
    safeLoadKeyDetailPage();
  }, keyAutoRefreshSeconds * 1000);
}

function applyVisualConfigFromUI() {
  const pageSizeSelect = document.getElementById("recordsPageSizeSelect");
  const autoInput = document.getElementById("autoRefreshSecondsInput");
  const autoEnabledInput = document.getElementById("autoRefreshEnabledInput");
  const selectedPageSize = clampInt(
    pageSizeSelect?.value,
    1,
    visualConfig.records_max_page_size
  );
  const selectedRefreshSeconds = clampInt(autoInput?.value, 30, 86400);
  const selectedAutoEnabled = Boolean(autoEnabledInput?.checked);

  recordsPageSize = selectedPageSize;
  keyAutoRefreshSeconds = selectedRefreshSeconds;
  keyAutoRefreshEnabled = selectedAutoEnabled;
  recordsPage = 1;

  localStorage.setItem("cch_key_records_page_size", String(recordsPageSize));
  localStorage.setItem("cch_key_auto_refresh_seconds", String(keyAutoRefreshSeconds));
  localStorage.setItem("cch_key_auto_refresh_enabled", keyAutoRefreshEnabled ? "1" : "0");

  renderVisualConfigControls();
  resetAutoRefreshTimer();
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

  const savedPageSize = Number(localStorage.getItem("cch_key_records_page_size") || 0);
  recordsPageSize = clampInt(
    savedPageSize || visualConfig.records_default_page_size,
    1,
    visualConfig.records_max_page_size
  );

  const savedRefreshSeconds = Number(localStorage.getItem("cch_key_auto_refresh_seconds") || 0);
  keyAutoRefreshSeconds = clampInt(savedRefreshSeconds || visualConfig.refresh_seconds, 30, 86400);
  const savedAutoEnabled = localStorage.getItem("cch_key_auto_refresh_enabled");
  keyAutoRefreshEnabled =
    savedAutoEnabled === null ? visualConfig.auto_refresh_enabled : savedAutoEnabled === "1";

  renderVisualConfigControls();
  resetAutoRefreshTimer();
}

async function loadKeyDetailPage() {
  updateTitleWithSelection();
  if (!selectedKeySlugs.length) {
    CCH.setText("metaText", "请先选择至少一个 key。");
    document.getElementById("keyRecordTbody").innerHTML = "";
    renderRecordPager({
      page: 1,
      total_pages: 1,
      total_records: 0,
    });
    return;
  }

  CCH.setText("metaText", "正在加载 Key 聚合数据...");
  const range = CCH.getRange();
  const slugParam = selectedKeySlugs.join(",");
  const data = await CCH.fetchJson(
    `/api/stats/keys?slugs=${encodeURIComponent(slugParam)}&records_page=${recordsPage}&records_page_size=${recordsPageSize}`,
    range
  );
  keyData = data || {};
  recordsPage = Number(keyData?.records_pagination?.page || recordsPage);
  fillKeyKpi(keyData);
  drawKeyCharts(keyData);
  renderKeyModelTable(keyData);
  renderKeyRecordTable(keyData);
  setKeyMeta(keyData);
}

async function safeLoadKeyDetailPage() {
  try {
    await loadKeyDetailPage();
  } catch (e) {
    CCH.setText("metaText", `数据加载失败: ${e.message}`);
  }
}

async function initKeySelection() {
  availableKeys = await CCH.fetchConfiguredKeys();
  const pathSlug = getPathKeySlug();
  const allSlugs = availableKeys.map((item) => String(item.slug || "").toLowerCase());
  if (pathSlug && allSlugs.includes(pathSlug)) {
    selectedKeySlugs = [pathSlug];
  } else {
    selectedKeySlugs = [...allSlugs];
  }
  renderKeySelector();
  updateTitleWithSelection();
}

function bindKeySelectionActions() {
  document.getElementById("selectAllKeysBtn")?.addEventListener("click", () => {
    setSelectedAll();
    updateTitleWithSelection();
  });
  document.getElementById("clearKeysBtn")?.addEventListener("click", () => {
    clearSelectedAll();
    updateTitleWithSelection();
  });
  document.getElementById("applyKeysBtn")?.addEventListener("click", () => {
    applySelectedFromUI();
    recordsPage = 1;
    updateTitleWithSelection();
    safeLoadKeyDetailPage();
  });
}

function bindRecordPagerActions() {
  document.getElementById("recordPrevBtn")?.addEventListener("click", () => {
    recordsPage = Math.max(recordsPage - 1, 1);
    safeLoadKeyDetailPage();
  });
  document.getElementById("recordNextBtn")?.addEventListener("click", () => {
    const totalPages = Number(keyData?.records_pagination?.total_pages || 1);
    recordsPage = Math.min(recordsPage + 1, totalPages);
    safeLoadKeyDetailPage();
  });
  document.getElementById("recordPageButtons")?.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) return;
    const page = Number(target.dataset.page || 0);
    if (!Number.isFinite(page) || page <= 0) return;
    recordsPage = page;
    safeLoadKeyDetailPage();
  });
  document.getElementById("recordJumpBtn")?.addEventListener("click", () => {
    const totalPages = Number(keyData?.records_pagination?.total_pages || 1);
    const raw = document.getElementById("recordJumpInput")?.value;
    const toPage = clampInt(raw, 1, totalPages);
    recordsPage = toPage;
    safeLoadKeyDetailPage();
  });
  document.getElementById("recordJumpInput")?.addEventListener("keydown", (event) => {
    if (event.key !== "Enter") return;
    event.preventDefault();
    const totalPages = Number(keyData?.records_pagination?.total_pages || 1);
    const toPage = clampInt(event.target?.value, 1, totalPages);
    recordsPage = toPage;
    safeLoadKeyDetailPage();
  });
}

function bindVisualConfigActions() {
  document.getElementById("applyVisualConfigBtn")?.addEventListener("click", () => {
    applyVisualConfigFromUI();
    safeLoadKeyDetailPage();
  });
}

document.addEventListener("DOMContentLoaded", async () => {
  CCH.markActiveMenu();
  await initKeySelection();
  await initVisualConfig();
  bindKeySelectionActions();
  bindRecordPagerActions();
  bindVisualConfigActions();
  CCH.initRangeControls(() => {
    recordsPage = 1;
    safeLoadKeyDetailPage();
  });
  CCH.bindRefresh(safeLoadKeyDetailPage);
  document.getElementById("keyModelSort")?.addEventListener("change", () => {
    if (!keyData) return;
    drawKeyCharts(keyData);
    renderKeyModelTable(keyData);
  });
  safeLoadKeyDetailPage();
});
