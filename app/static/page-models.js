let modelRows = [];

function fillModelKpi(rows) {
  const modelCount = rows.length;
  const totalCalls = rows.reduce((s, x) => s + Number(x.calls || 0), 0);
  const totalCost = rows.reduce((s, x) => s + Number(x.cost || 0), 0);
  const avgSuccess =
    rows.length > 0
      ? rows.reduce((s, x) => s + Number(x.success_rate || 0), 0) / rows.length
      : 0;
  CCH.setText("kpiModelCount", `${CCH.fmtNumber(modelCount)} \u4e2a`);
  CCH.setText("kpiModelCalls", `${CCH.fmtNumber(totalCalls)} \u6b21`);
  CCH.setText("kpiModelCost", `$${CCH.fmtMoney(totalCost)}`);
  CCH.setText("kpiModelSuccess", CCH.fmtPercent(avgSuccess));
}

function drawModelCharts(rows) {
  const sortOrder = document.getElementById("modelSort")?.value || "desc";
  const topRows = CCH.sortRows(rows, "calls", sortOrder).slice(0, 12);

  CCH.renderChart("chartModelCombo", {
    tooltip: { trigger: "axis" },
    legend: { data: ["\u8c03\u7528\u6b21\u6570", "\u6210\u529f\u7387"] },
    grid: { left: 50, right: 48, top: 26, bottom: 34 },
    xAxis: { type: "category", data: topRows.map((x) => x.model), axisLabel: { rotate: 28 } },
    yAxis: [
      { type: "value", name: "\u8c03\u7528\u6b21\u6570" },
      { type: "value", name: "\u6210\u529f\u7387", min: 0, max: 100 },
    ],
    series: [
      {
        name: "\u8c03\u7528\u6b21\u6570",
        type: "bar",
        itemStyle: { color: "rgba(46,131,255,0.78)", borderRadius: [8, 8, 0, 0] },
        data: topRows.map((x) => x.calls),
      },
      {
        name: "\u6210\u529f\u7387",
        type: "line",
        yAxisIndex: 1,
        smooth: true,
        lineStyle: { width: 3, color: "#1aa899" },
        data: topRows.map((x) => Number(x.success_rate || 0)),
      },
    ],
  });

  CCH.renderChart("chartModelScatter", {
    tooltip: {
      formatter: (p) =>
        `${p.data[3]}<br/>Token: ${Number(p.data[0] || 0).toFixed(2)} M<br/>\u8d39\u7528: $${CCH.fmtMoney(
          p.data[1]
        )}<br/>\u8c03\u7528: ${CCH.fmtNumber(p.data[2])}`,
    },
    grid: { left: 46, right: 24, top: 24, bottom: 30 },
    xAxis: { type: "value", name: "Token(M)" },
    yAxis: { type: "value", name: "\u8d39\u7528" },
    series: [
      {
        type: "scatter",
        data: topRows.map((x) => [CCH.toTokenM(x.tokens), x.cost, x.calls, x.model]),
        symbolSize: (val) => Math.max(10, Math.sqrt(val[2]) * 1.5),
        itemStyle: { color: "rgba(93,168,255,0.78)" },
      },
    ],
  });

  CCH.renderChart("chartModelCost", {
    tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
    grid: { left: 144, right: 22, top: 20, bottom: 20 },
    xAxis: { type: "value", name: "\u8d39\u7528" },
    yAxis: {
      type: "category",
      inverse: true,
      data: topRows.map((x) => x.model),
      axisLabel: { width: 132, overflow: "truncate" },
    },
    series: [
      {
        type: "bar",
        data: topRows.map((x) => x.cost),
        itemStyle: { color: "rgba(26,168,153,0.8)", borderRadius: 8 },
      },
    ],
  });
}

function renderModelTable(rows) {
  const sortOrder = document.getElementById("modelSort")?.value || "desc";
  const sortedRows = CCH.sortRows(rows, "calls", sortOrder);
  const html = sortedRows
    .slice(0, 30)
    .map(
      (x) => `<tr>
      <td>${x.model}</td>
      <td>${CCH.fmtNumber(x.calls)} \u6b21</td>
      <td>$${CCH.fmtMoney(x.cost)}</td>
      <td>${CCH.fmtTokenM(x.tokens)}</td>
      <td>${CCH.fmtPercent(x.success_rate)}</td>
    </tr>`
    )
    .join("");
  document.getElementById("modelTbody").innerHTML = html;
}

async function loadModelPage(forceRefresh = false) {
  const range = CCH.getRange();
  CCH.setText("metaText", "\u6b63\u5728\u52a0\u8f7d\u6a21\u578b\u4f7f\u7528\u5206\u6790\u6570\u636e...");
  const data = await CCH.fetchJson("/api/dashboard", range, { forceRefresh });
  modelRows = data.model_usage || [];
  fillModelKpi(modelRows);
  drawModelCharts(modelRows);
  renderModelTable(modelRows);
  CCH.setMetaFromDashboard(data);
}

async function safeLoadModels(forceRefresh = false) {
  try {
    await loadModelPage(forceRefresh);
  } catch (e) {
    CCH.setText("metaText", `\u6570\u636e\u52a0\u8f7d\u5931\u8d25: ${e.message}`);
  }
}

document.addEventListener("DOMContentLoaded", () => {
  CCH.markActiveMenu();
  CCH.initRangeControls(safeLoadModels);
  CCH.bindRefresh(safeLoadModels);
  document.getElementById("modelSort")?.addEventListener("change", () => {
    if (!modelRows.length) return;
    drawModelCharts(modelRows);
    renderModelTable(modelRows);
  });
  safeLoadModels();
  setInterval(safeLoadModels, 90000);
});
