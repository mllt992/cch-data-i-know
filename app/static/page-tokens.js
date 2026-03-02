let tokenData = null;

function fillTokenKpi(tokenUsage) {
  const summary = tokenUsage.summary || {};
  CCH.setText("kpiTotalTokens", CCH.fmtTokenM(summary.total_tokens));
  CCH.setText("kpiPromptTokens", CCH.fmtTokenM(summary.prompt_tokens));
  CCH.setText("kpiCompletionTokens", CCH.fmtTokenM(summary.completion_tokens));
  CCH.setText("kpiCacheTokens", CCH.fmtTokenM(summary.cache_tokens));
  CCH.setText("kpiCacheCreationTokens", CCH.fmtTokenM(summary.cache_creation_tokens));
  CCH.setText("kpiCacheReadTokens", CCH.fmtTokenM(summary.cache_read_tokens));
}

function drawTokenCharts(tokenUsage) {
  const trend = tokenUsage.trend || [];
  const sortOrder = document.getElementById("tokenModelSort")?.value || "desc";
  const byModel = CCH.sortRows(tokenUsage.by_model || [], "tokens", sortOrder).slice(0, 12);
  const summary = tokenUsage.summary || {};

  CCH.renderChart("chartTokenTrend", {
    tooltip: { trigger: "axis" },
    legend: { data: ["输入Token", "输出Token", "缓存Token"] },
    grid: { left: 40, right: 20, top: 30, bottom: 24 },
    xAxis: { type: "category", data: trend.map((x) => x.day) },
    yAxis: { type: "value", name: "Token(M)" },
    series: [
      {
        name: "输入Token",
        type: "line",
        stack: "token",
        areaStyle: { opacity: 0.22 },
        smooth: true,
        data: trend.map((x) => CCH.toTokenM(x.prompt_tokens)),
      },
      {
        name: "输出Token",
        type: "line",
        stack: "token",
        areaStyle: { opacity: 0.22 },
        smooth: true,
        data: trend.map((x) => CCH.toTokenM(x.completion_tokens)),
      },
      {
        name: "缓存Token",
        type: "line",
        stack: "token",
        areaStyle: { opacity: 0.22 },
        smooth: true,
        data: trend.map((x) => CCH.toTokenM(x.cache_tokens)),
      },
    ],
  });

  CCH.renderChart("chartTokenByModel", {
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

  CCH.renderChart("chartTokenSplit", {
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

function renderTokenTable(tokenUsage) {
  const sortOrder = document.getElementById("tokenModelSort")?.value || "desc";
  const rows = CCH.sortRows(tokenUsage.by_model || [], "tokens", sortOrder);
  const html = rows
    .slice(0, 30)
    .map(
      (x) => `<tr>
      <td>${x.model}</td>
      <td>${CCH.fmtTokenM(x.tokens)}</td>
      <td>${CCH.fmtTokenM(x.prompt_tokens)}</td>
      <td>${CCH.fmtTokenM(x.completion_tokens)}</td>
      <td>${CCH.fmtTokenM(x.cache_tokens)}</td>
      <td>${CCH.fmtTokenM(x.cache_creation_tokens)}</td>
      <td>${CCH.fmtTokenM(x.cache_read_tokens)}</td>
    </tr>`
    )
    .join("");
  document.getElementById("tokenTbody").innerHTML = html;
}

async function loadTokenPage() {
  const range = CCH.getRange();
  CCH.setText("metaText", "正在加载 Token 分析数据...");
  const data = await CCH.fetchJson("/api/dashboard", range);
  tokenData = data.token_usage || {};
  fillTokenKpi(tokenData);
  drawTokenCharts(tokenData);
  renderTokenTable(tokenData);
  CCH.setMetaFromDashboard(data);
}

async function safeLoadTokens() {
  try {
    await loadTokenPage();
  } catch (e) {
    CCH.setText("metaText", `数据加载失败: ${e.message}`);
  }
}

document.addEventListener("DOMContentLoaded", () => {
  CCH.markActiveMenu();
  CCH.initRangeControls(safeLoadTokens);
  CCH.bindRefresh(safeLoadTokens);
  document.getElementById("tokenModelSort")?.addEventListener("change", () => {
    if (!tokenData) return;
    drawTokenCharts(tokenData);
    renderTokenTable(tokenData);
  });
  safeLoadTokens();
  setInterval(safeLoadTokens, 90000);
});
