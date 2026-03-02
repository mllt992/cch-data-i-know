const chartRegistry = new Map();

function fmtNumber(value) {
  return Number(value || 0).toLocaleString();
}

function fmtMoney(value) {
  return Number(value || 0).toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 4,
  });
}

function fmtPercent(value) {
  return `${Number(value || 0).toFixed(2)}%`;
}

function toTokenM(value) {
  return Number(value || 0) / 1_000_000;
}

function fmtTokenM(value) {
  return `${toTokenM(value).toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })} M`;
}

function setText(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = value;
}

function toDateInputValue(dateObj) {
  const y = dateObj.getFullYear();
  const m = String(dateObj.getMonth() + 1).padStart(2, "0");
  const d = String(dateObj.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function initDefaultRange(defaultDays = 30) {
  const end = new Date();
  const start = new Date(end);
  start.setDate(end.getDate() - (defaultDays - 1));
  return {
    start: toDateInputValue(start),
    end: toDateInputValue(end),
  };
}

function saveRange(start, end) {
  localStorage.setItem("cch_range_start", start);
  localStorage.setItem("cch_range_end", end);
}

function getSavedRange() {
  const start = localStorage.getItem("cch_range_start");
  const end = localStorage.getItem("cch_range_end");
  return { start, end };
}

function getRange() {
  return {
    start: document.getElementById("startDate").value,
    end: document.getElementById("endDate").value,
  };
}

function validateRange(range) {
  if (!range.start || !range.end) return false;
  return range.start <= range.end;
}

function rangeToQuery(range) {
  const params = new URLSearchParams();
  if (range.start) params.set("start_date", range.start);
  if (range.end) params.set("end_date", range.end);
  return params.toString();
}

async function fetchJson(path, range) {
  const query = rangeToQuery(range);
  const url = query ? `${path}?${query}` : path;
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}`);
  }
  return res.json();
}

function renderChart(elId, option) {
  const el = document.getElementById(elId);
  if (!el) return;
  const old = chartRegistry.get(elId);
  if (old) old.dispose();
  const chart = echarts.init(el, null, { renderer: "canvas" });
  chart.setOption(option);
  chartRegistry.set(elId, chart);
}

function resizeCharts() {
  chartRegistry.forEach((chart) => chart.resize());
}

function sortRows(rows, key, order = "desc") {
  const sorted = [...(rows || [])];
  sorted.sort((a, b) => {
    const av = Number(a?.[key] || 0);
    const bv = Number(b?.[key] || 0);
    return order === "asc" ? av - bv : bv - av;
  });
  return sorted;
}

function markActiveMenu() {
  const page = document.body.getAttribute("data-page");
  document.querySelectorAll(".menu a").forEach((a) => {
    a.classList.toggle("active", a.dataset.page === page);
  });
}

function initRangeControls(onApply) {
  const saved = getSavedRange();
  const fallback = initDefaultRange(30);
  const startInput = document.getElementById("startDate");
  const endInput = document.getElementById("endDate");

  startInput.value = saved.start || fallback.start;
  endInput.value = saved.end || fallback.end;

  document.getElementById("applyRangeBtn").addEventListener("click", () => {
    const range = getRange();
    if (!validateRange(range)) {
      alert("\u65f6\u95f4\u8303\u56f4\u4e0d\u5408\u6cd5\uff1a\u5f00\u59cb\u65e5\u671f\u9700\u65e9\u4e8e\u6216\u7b49\u4e8e\u7ed3\u675f\u65e5\u671f\u3002");
      return;
    }
    saveRange(range.start, range.end);
    onApply();
  });

  document.getElementById("resetRangeBtn").addEventListener("click", () => {
    const reset = initDefaultRange(30);
    startInput.value = reset.start;
    endInput.value = reset.end;
    saveRange(reset.start, reset.end);
    onApply();
  });
}

function bindRefresh(onRefresh) {
  const btn = document.getElementById("refreshBtn");
  if (!btn) return;
  btn.addEventListener("click", () => onRefresh());
}

function setMetaFromDashboard(data) {
  const source = data.source || {};
  const channelSource =
    source.channel_name_col ||
    source.channel_lookup_name_col ||
    source.channel_col ||
    "\u672a\u8bc6\u522b";
  setText(
    "metaText",
    `\u6570\u636e\u6e90: ${source.table || "-"} | \u6e20\u9053\u540d\u79f0\u5217: ${channelSource} | ` +
      `\u7edf\u8ba1\u8303\u56f4: ${data.time_range?.start || "-"} \u81f3 ${data.time_range?.end || "-"} | ` +
      `\u66f4\u65b0\u65f6\u95f4: ${data.generated_at || "-"}`
  );
}

window.addEventListener("resize", resizeCharts);

window.CCH = {
  bindRefresh,
  fetchJson,
  fmtMoney,
  fmtNumber,
  fmtPercent,
  fmtTokenM,
  getRange,
  initRangeControls,
  markActiveMenu,
  renderChart,
  sortRows,
  setMetaFromDashboard,
  setText,
  toTokenM,
};
