let channelRows = [];

function normalizeChannelName(value) {
  const v = String(value || "").trim();
  if (!v || v.toLowerCase() === "unknown") return "\u672a\u77e5\u6e20\u9053";
  if (/^\d+$/.test(v)) return "\u672a\u77e5\u6e20\u9053";
  if (/^[0-9a-f-]{24,}$/i.test(v)) return "\u672a\u77e5\u6e20\u9053";
  return v;
}

function fillChannelKpi(rows) {
  const channelCount = rows.length;
  const totalCalls = rows.reduce((s, x) => s + Number(x.calls || 0), 0);
  const totalCost = rows.reduce((s, x) => s + Number(x.cost || 0), 0);
  const totalTokens = rows.reduce((s, x) => s + Number(x.tokens || 0), 0);
  CCH.setText("kpiChannelCount", `${CCH.fmtNumber(channelCount)} \u4e2a`);
  CCH.setText("kpiChannelCalls", `${CCH.fmtNumber(totalCalls)} \u6b21`);
  CCH.setText("kpiChannelCost", `$${CCH.fmtMoney(totalCost)}`);
  CCH.setText("kpiChannelTokens", CCH.fmtTokenM(totalTokens));
}

function drawChannelCharts(rows) {
  const sortOrder = document.getElementById("channelSort")?.value || "desc";
  const sorted = CCH.sortRows(rows, "calls", sortOrder);
  const topRows = sorted.slice(0, 15);

  CCH.renderChart("chartChannelBars", {
    tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
    grid: { left: 150, right: 18, top: 26, bottom: 22 },
    xAxis: { type: "value" },
    yAxis: {
      type: "category",
      inverse: true,
      data: topRows.map((x) => normalizeChannelName(x.channel)),
      axisLabel: { width: 140, overflow: "truncate" },
    },
    series: [
      {
        name: "\u8c03\u7528\u6b21\u6570",
        type: "bar",
        itemStyle: { color: "rgba(46,131,255,0.82)", borderRadius: 8 },
        data: topRows.map((x) => x.calls),
      },
    ],
  });

  CCH.renderChart("chartChannelTreemap", {
    tooltip: { formatter: (p) => `${p.name}<br/>Token: ${Number(p.value || 0).toFixed(2)} M` },
    series: [
      {
        type: "treemap",
        roam: false,
        breadcrumb: { show: false },
        nodeClick: false,
        data: topRows.map((x) => ({
          name: normalizeChannelName(x.channel),
          value: CCH.toTokenM(x.tokens),
        })),
      },
    ],
  });

  CCH.renderChart("chartChannelScatter", {
    tooltip: {
      formatter: (p) =>
        `${p.data[3]}<br/>Token: ${p.data[0].toFixed(2)} M<br/>\u8d39\u7528: ${CCH.fmtMoney(
          p.data[1]
        )}<br/>\u8c03\u7528: ${CCH.fmtNumber(p.data[2])}`,
    },
    grid: { left: 44, right: 22, top: 24, bottom: 28 },
    xAxis: { type: "value", name: "Token(M)" },
    yAxis: { type: "value", name: "\u8d39\u7528" },
    series: [
      {
        type: "scatter",
        symbolSize: (val) => Math.max(10, Math.sqrt(val[2]) * 1.5),
        itemStyle: { color: "rgba(26,168,153,0.78)" },
        data: topRows.map((x) => [
          CCH.toTokenM(x.tokens),
          x.cost,
          x.calls,
          normalizeChannelName(x.channel),
        ]),
      },
    ],
  });
}

function renderChannelTable(rows) {
  const sorted = CCH.sortRows(rows, "calls", document.getElementById("channelSort")?.value || "desc");
  const html = sorted
    .slice(0, 30)
    .map(
      (x) => `<tr>
      <td>${normalizeChannelName(x.channel)}</td>
      <td>${CCH.fmtNumber(x.calls)} \u6b21</td>
      <td>$${CCH.fmtMoney(x.cost)}</td>
      <td>${CCH.fmtTokenM(x.tokens)}</td>
    </tr>`
    )
    .join("");
  document.getElementById("channelTbody").innerHTML = html;
}

async function loadChannelPage() {
  const range = CCH.getRange();
  CCH.setText("metaText", "\u6b63\u5728\u52a0\u8f7d\u6e20\u9053\u5206\u6790\u6570\u636e...");
  const data = await CCH.fetchJson("/api/dashboard", range);
  channelRows = (data.channel_usage || []).map((x) => ({
    ...x,
    channel: normalizeChannelName(x.channel),
  }));
  fillChannelKpi(channelRows);
  drawChannelCharts(channelRows);
  renderChannelTable(channelRows);
  CCH.setMetaFromDashboard(data);
}

async function safeLoadChannels() {
  try {
    await loadChannelPage();
  } catch (e) {
    CCH.setText("metaText", `\u6570\u636e\u52a0\u8f7d\u5931\u8d25: ${e.message}`);
  }
}

document.addEventListener("DOMContentLoaded", () => {
  CCH.markActiveMenu();
  CCH.initRangeControls(safeLoadChannels);
  CCH.bindRefresh(safeLoadChannels);
  document.getElementById("channelSort")?.addEventListener("change", () => {
    if (!channelRows.length) return;
    drawChannelCharts(channelRows);
    renderChannelTable(channelRows);
  });
  safeLoadChannels();
  setInterval(safeLoadChannels, 90000);
});
