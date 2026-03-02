let availabilityRows = [];
let realtimeWindow = "today";

function windowLabel(window) {
  if (window === "today") return "今天";
  if (window === "7d") return "近七天";
  if (window === "30d") return "近一个月";
  return "全部";
}

function calcOverallAvailability(rows) {
  const totalCalls = rows.reduce((sum, r) => sum + Number(r.total_calls || 0), 0);
  const successCalls = rows.reduce((sum, r) => sum + Number(r.success_calls || 0), 0);
  if (!totalCalls) return 0;
  return (successCalls / totalCalls) * 100;
}

function fillAvailabilityKpi(rows) {
  const totalModels = rows.length;
  const overall = calcOverallAvailability(rows);
  const unstable = rows.filter((x) => Number(x.availability_pct || 0) < 95).length;
  CCH.setText("kpiModelCount", `${CCH.fmtNumber(totalModels)} 个`);
  CCH.setText("kpiOverall", CCH.fmtPercent(overall));
  CCH.setText("kpiUnstable", `${CCH.fmtNumber(unstable)} 个`);
  CCH.setText("kpiTopModel", rows[0]?.model || "-");
}

function drawAvailabilityCharts(rows) {
  const sortOrder = document.getElementById("availabilitySort")?.value || "desc";
  const sorted = CCH.sortRows(rows, "total_calls", sortOrder);
  const topRows = sorted.slice(0, 12);
  const overall = calcOverallAvailability(rows);

  CCH.renderChart("chartGauge", {
    series: [
      {
        type: "gauge",
        radius: "88%",
        progress: { show: true, width: 15 },
        axisLine: { lineStyle: { width: 15 } },
        splitLine: { show: false },
        axisTick: { show: false },
        axisLabel: { show: false },
        detail: { valueAnimation: true, formatter: "{value}%", fontSize: 28 },
        data: [{ value: Number(overall.toFixed(2)), name: "整体可用率" }],
      },
    ],
  });

  CCH.renderChart("chartAvailabilityStack", {
    tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
    legend: { data: ["成功调用", "失败调用"] },
    grid: { left: 150, right: 20, top: 26, bottom: 18 },
    xAxis: { type: "value", name: "调用次数(次)" },
    yAxis: {
      type: "category",
      inverse: true,
      data: topRows.map((x) => x.model),
      axisLabel: { width: 138, overflow: "truncate" },
    },
    series: [
      {
        name: "成功调用",
        type: "bar",
        stack: "total",
        itemStyle: { color: "rgba(26,168,153,0.85)" },
        data: topRows.map((x) => x.success_calls),
      },
      {
        name: "失败调用",
        type: "bar",
        stack: "total",
        itemStyle: { color: "rgba(240,100,100,0.82)" },
        data: topRows.map((x) => x.failed_calls),
      },
    ],
  });

  CCH.renderChart("chartAvailabilityRate", {
    tooltip: { trigger: "axis" },
    grid: { left: 58, right: 24, top: 24, bottom: 30 },
    xAxis: { type: "category", data: topRows.map((x) => x.model), axisLabel: { rotate: 30 } },
    yAxis: { type: "value", min: 0, max: 100, name: "可用率(%)" },
    visualMap: {
      show: false,
      min: 0,
      max: 100,
      inRange: { color: ["#f26f6f", "#f8cf72", "#2e83ff", "#1aa899"] },
    },
    series: [
      {
        type: "line",
        smooth: true,
        symbolSize: 8,
        lineStyle: { width: 3 },
        data: topRows.map((x) => Number(x.availability_pct || 0)),
      },
    ],
  });
}

function renderAvailabilityTable(rows) {
  const html = rows
    .slice(0, 30)
    .map(
      (x) => `<tr>
      <td>${x.model}</td>
      <td>${CCH.fmtNumber(x.total_calls)} 次</td>
      <td>${CCH.fmtNumber(x.success_calls)} 次</td>
      <td>${CCH.fmtNumber(x.failed_calls)} 次</td>
      <td>${CCH.fmtPercent(x.availability_pct)}</td>
      <td>${x.last_call_at || "-"}</td>
    </tr>`
    )
    .join("");
  document.getElementById("availabilityTbody").innerHTML = html;
}

function statusClass(status) {
  if (status === "success") return "rt-cell rt-ok";
  if (status === "failed") return "rt-cell rt-fail";
  return "rt-cell rt-other";
}

function renderRealtimeGrid(models) {
  const container = document.getElementById("realtimeGrid");
  if (!models.length) {
    container.innerHTML = '<div class="meta">当前时间范围内无调用记录。</div>';
    return;
  }

  const rowsHtml = models
    .map((model) => {
      const cells = (model.statuses || [])
        .map((status) => `<i class="${statusClass(status)}"></i>`)
        .join("");
      return `
        <div class="rt-row">
          <div class="rt-model">
            <div class="rt-model-name" title="${model.model}">${model.model}</div>
            <div class="rt-model-meta">${CCH.fmtNumber(model.total_calls)} 次调用</div>
          </div>
          <div class="rt-cells">${cells}</div>
        </div>
      `;
    })
    .join("");

  container.innerHTML = rowsHtml;
}

async function loadRealtimeAvailability() {
  const hintEl = document.getElementById("realtimeHint");
  hintEl.textContent = "正在加载实时可用性...";
  const res = await fetch(`/api/stats/realtime-availability?window=${encodeURIComponent(realtimeWindow)}`);
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}`);
  }
  const data = await res.json();
  renderRealtimeGrid(data.models || []);
  hintEl.textContent = `时间窗口: ${windowLabel(data.window)} | 每模型最多展示 ${data.event_limit} 次调用`;
}

async function loadAvailability() {
  const range = CCH.getRange();
  CCH.setText("metaText", "正在加载模型可用性数据...");
  const data = await CCH.fetchJson("/api/dashboard", range);
  availabilityRows = data.model_availability || [];
  fillAvailabilityKpi(availabilityRows);
  drawAvailabilityCharts(availabilityRows);
  renderAvailabilityTable(availabilityRows);
  CCH.setMetaFromDashboard(data);
}

async function safeLoadAvailability() {
  try {
    await Promise.all([loadAvailability(), loadRealtimeAvailability()]);
  } catch (e) {
    CCH.setText("metaText", `数据加载失败: ${e.message}`);
    document.getElementById("realtimeHint").textContent = `实时可用性加载失败: ${e.message}`;
  }
}

function bindRealtimeWindowButtons() {
  document.querySelectorAll(".rt-window-btn").forEach((btn) => {
    btn.addEventListener("click", async () => {
      realtimeWindow = btn.dataset.window || "today";
      document.querySelectorAll(".rt-window-btn").forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");
      try {
        await loadRealtimeAvailability();
      } catch (e) {
        document.getElementById("realtimeHint").textContent = `实时可用性加载失败: ${e.message}`;
      }
    });
  });
}

document.addEventListener("DOMContentLoaded", () => {
  CCH.markActiveMenu();
  CCH.initRangeControls(safeLoadAvailability);
  CCH.bindRefresh(safeLoadAvailability);
  document.getElementById("availabilitySort")?.addEventListener("change", () => {
    if (availabilityRows.length) drawAvailabilityCharts(availabilityRows);
  });
  bindRealtimeWindowButtons();
  safeLoadAvailability();
  setInterval(safeLoadAvailability, 90000);
});
