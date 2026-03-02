let overviewData = null;

function normalizeChannelName(value) {
  const v = String(value || "").trim();
  if (!v || v.toLowerCase() === "unknown") return "\u672a\u77e5\u6e20\u9053";
  return v;
}

function drawOverviewCharts(data) {
  const trend = data.call_trend || [];
  const sortOrder = document.getElementById("topModelSort")?.value || "desc";
  const modelUsage = CCH.sortRows(data.model_usage || [], "calls", sortOrder).slice(0, 10);
  const channelUsage = (data.channel_usage || []).map((x) => ({
    ...x,
    channel: normalizeChannelName(x.channel),
  })).slice(0, 10);

  CCH.renderChart("chartCallCost", {
    tooltip: { trigger: "axis" },
    legend: { data: ["\u8c03\u7528\u6b21\u6570", "\u8d39\u7528"] },
    grid: { left: 36, right: 44, top: 34, bottom: 26 },
    xAxis: { type: "category", data: trend.map((x) => x.day) },
    yAxis: [
      { type: "value", name: "\u8c03\u7528\u6b21\u6570" },
      { type: "value", name: "\u8d39\u7528" },
    ],
    series: [
      {
        name: "\u8c03\u7528\u6b21\u6570",
        type: "line",
        smooth: true,
        symbol: "circle",
        symbolSize: 6,
        areaStyle: { color: "rgba(46,131,255,0.16)" },
        lineStyle: { width: 3, color: "#2e83ff" },
        data: trend.map((x) => x.calls),
      },
      {
        name: "\u8d39\u7528",
        type: "bar",
        yAxisIndex: 1,
        itemStyle: { color: "rgba(26,168,153,0.75)", borderRadius: [6, 6, 0, 0] },
        data: trend.map((x) => x.cost),
      },
    ],
  });

  CCH.renderChart("chartTopModels", {
    tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
    grid: { left: 140, right: 22, top: 20, bottom: 20 },
    xAxis: { type: "value" },
    yAxis: {
      type: "category",
      data: modelUsage.map((x) => x.model),
      axisLabel: { width: 130, overflow: "truncate" },
    },
    series: [
      {
        type: "bar",
        data: modelUsage.map((x) => x.calls),
        itemStyle: {
          color: new echarts.graphic.LinearGradient(1, 0, 0, 0, [
            { offset: 0, color: "#2e83ff" },
            { offset: 1, color: "#73c1ff" },
          ]),
          borderRadius: 8,
        },
      },
    ],
  });

  CCH.renderChart("chartChannels", {
    tooltip: { trigger: "item" },
    legend: { bottom: 0 },
    series: [
      {
        name: "\u6e20\u9053\u8c03\u7528",
        type: "pie",
        radius: ["42%", "72%"],
        center: ["50%", "45%"],
        itemStyle: { borderRadius: 10, borderColor: "#fff", borderWidth: 2 },
        label: { formatter: "{b}\n{d}%" },
        data: channelUsage.map((x) => ({ name: x.channel, value: x.calls })),
      },
    ],
  });
}

function fillOverviewKpi(data) {
  const cost = data.cost_overview || {};
  CCH.setText("kpiCalls", `${CCH.fmtNumber(cost.total_calls)} \u6b21`);
  CCH.setText("kpiCost", `$${CCH.fmtMoney(cost.total_cost)}`);
  CCH.setText("kpiTokens", CCH.fmtTokenM(cost.total_tokens));
  CCH.setText("kpiAvailability", CCH.fmtPercent(cost.success_rate));
}

async function loadOverview() {
  const range = CCH.getRange();
  CCH.setText("metaText", "\u6b63\u5728\u52a0\u8f7d\u603b\u89c8\u6570\u636e...");
  overviewData = await CCH.fetchJson("/api/dashboard", range);
  fillOverviewKpi(overviewData);
  drawOverviewCharts(overviewData);
  CCH.setMetaFromDashboard(overviewData);
}

async function safeLoadOverview() {
  try {
    await loadOverview();
  } catch (e) {
    CCH.setText("metaText", `\u6570\u636e\u52a0\u8f7d\u5931\u8d25: ${e.message}`);
  }
}

document.addEventListener("DOMContentLoaded", () => {
  CCH.markActiveMenu();
  CCH.initRangeControls(safeLoadOverview);
  CCH.bindRefresh(safeLoadOverview);
  document.getElementById("topModelSort")?.addEventListener("change", () => {
    if (overviewData) drawOverviewCharts(overviewData);
  });
  safeLoadOverview();
  setInterval(safeLoadOverview, 90000);
});
