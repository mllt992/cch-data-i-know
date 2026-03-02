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

function getSortedAvailabilityRows(rows) {
  const sortOrder = document.getElementById("availabilitySort")?.value || "desc";
  return CCH.sortRows(rows, "total_calls", sortOrder);
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
  const sorted = getSortedAvailabilityRows(rows);
  const stackVisibleCount = 12;
  const rateVisibleCount = 16;
  const stackZoomEnd =
    sorted.length > stackVisibleCount
      ? Number(((stackVisibleCount / sorted.length) * 100).toFixed(2))
      : 100;
  const rateZoomEnd =
    sorted.length > rateVisibleCount
      ? Number(((rateVisibleCount / sorted.length) * 100).toFixed(2))
      : 100;
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
    grid: { left: 150, right: 34, top: 26, bottom: 22 },
    xAxis: { type: "value", name: "调用次数(次)" },
    yAxis: {
      type: "category",
      inverse: true,
      data: sorted.map((x) => x.model),
      axisLabel: { width: 138, overflow: "truncate" },
    },
    dataZoom:
      sorted.length > stackVisibleCount
        ? [
            {
              type: "inside",
              yAxisIndex: 0,
              start: 0,
              end: stackZoomEnd,
            },
            {
              type: "slider",
              yAxisIndex: 0,
              start: 0,
              end: stackZoomEnd,
              width: 10,
              right: 10,
            },
          ]
        : [],
    series: [
      {
        name: "成功调用",
        type: "bar",
        stack: "total",
        itemStyle: { color: "rgba(26,168,153,0.85)" },
        data: sorted.map((x) => x.success_calls),
      },
      {
        name: "失败调用",
        type: "bar",
        stack: "total",
        itemStyle: { color: "rgba(240,100,100,0.82)" },
        data: sorted.map((x) => x.failed_calls),
      },
    ],
  });

  CCH.renderChart("chartAvailabilityRate", {
    tooltip: { trigger: "axis" },
    grid: { left: 58, right: 24, top: 24, bottom: 54 },
    xAxis: { type: "category", data: sorted.map((x) => x.model), axisLabel: { rotate: 24 } },
    yAxis: { type: "value", min: 0, max: 100, name: "可用率(%)" },
    dataZoom:
      sorted.length > rateVisibleCount
        ? [
            {
              type: "inside",
              xAxisIndex: 0,
              start: 0,
              end: rateZoomEnd,
            },
            {
              type: "slider",
              xAxisIndex: 0,
              start: 0,
              end: rateZoomEnd,
              bottom: 8,
              height: 14,
            },
          ]
        : [],
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
        data: sorted.map((x) => Number(x.availability_pct || 0)),
      },
    ],
  });
}

function renderAvailabilityTable(rows) {
  const sorted = getSortedAvailabilityRows(rows);
  const html = sorted
    .map(
      (x) => `<tr>
      <td>${x.model}</td>
      <td>${CCH.fmtNumber(x.total_calls)} 次</td>
      <td>${CCH.fmtNumber(x.success_calls)} 次</td>
      <td>${CCH.fmtNumber(x.failed_calls)} 次</td>
      <td>${CCH.fmtPercent(x.availability_pct)}</td>
      <td>${CCH.formatDateTimeCN(x.last_call_at)}</td>
    </tr>`
    )
    .join("");
  document.getElementById("availabilityTbody").innerHTML = html;
}

function formatSlotDuration(seconds) {
  const sec = Number(seconds || 0);
  if (!sec) return "-";
  if (sec < 60) return `${sec}秒`;
  if (sec < 3600) return `${Math.round(sec / 60)}分钟`;
  if (sec < 86400) return `${Math.round(sec / 3600)}小时`;
  return `${Math.round(sec / 86400)}天`;
}

function normalizeSlot(slot) {
  if (slot && typeof slot === "object" && !Array.isArray(slot)) {
    const successCalls = Number(slot.success_calls || 0);
    const failedCalls = Number(slot.failed_calls || 0);
    const otherCalls = Number(slot.other_calls || 0);
    const totalCalls = Math.max(
      Number(slot.total_calls || 0),
      successCalls + failedCalls + otherCalls
    );
    return { successCalls, failedCalls, otherCalls, totalCalls };
  }
  const status = String(slot || "empty");
  if (status === "success") return { successCalls: 1, failedCalls: 0, otherCalls: 0, totalCalls: 1 };
  if (status === "failed") return { successCalls: 0, failedCalls: 1, otherCalls: 0, totalCalls: 1 };
  if (status === "other") return { successCalls: 0, failedCalls: 0, otherCalls: 1, totalCalls: 1 };
  return { successCalls: 0, failedCalls: 0, otherCalls: 0, totalCalls: 0 };
}

function calcCellVisual(slot, maxSlotCalls) {
  const total = Number(slot.totalCalls || 0);
  if (!total) {
    return {
      className: "rt-cell rt-empty",
      style: "",
      title: "无调用",
    };
  }

  const parts = [
    { key: "success", value: Number(slot.successCalls || 0), color: "#1aa899" },
    { key: "failed", value: Number(slot.failedCalls || 0), color: "#f06464" },
    { key: "other", value: Number(slot.otherCalls || 0), color: "#f2bf53" },
  ].filter((x) => x.value > 0);

  let background = "#1aa899";
  if (parts.length === 1) {
    background = parts[0].color;
  } else {
    let start = 0;
    const segments = parts.map((x, idx) => {
      const end = idx === parts.length - 1 ? 360 : start + (x.value / total) * 360;
      const seg = `${x.color} ${start.toFixed(2)}deg ${end.toFixed(2)}deg`;
      start = end;
      return seg;
    });
    background = `conic-gradient(${segments.join(",")})`;
  }

  const ratio = maxSlotCalls > 0 ? Math.min(total / maxSlotCalls, 1) : 1;
  const opacity = 0.28 + Math.pow(ratio, 0.65) * 0.72;
  const borderAlpha = 0.18 + Math.pow(ratio, 0.7) * 0.32;
  const style = `background:${background};opacity:${opacity.toFixed(3)};border-color:rgba(20,84,152,${borderAlpha.toFixed(3)});`;
  const title = `成功 ${slot.successCalls} | 失败 ${slot.failedCalls} | 其他 ${slot.otherCalls} | 总计 ${slot.totalCalls}`;
  return { className: "rt-cell", style, title };
}

function renderRealtimeGrid(models) {
  const container = document.getElementById("realtimeGrid");
  if (!models.length) {
    container.innerHTML = '<div class="meta">当前时间范围内无调用记录。</div>';
    return;
  }

  const normalizedModels = models.map((model) => {
    const rawSlots = Array.isArray(model.slots) && model.slots.length ? model.slots : model.statuses || [];
    const slots = rawSlots.map(normalizeSlot);
    const maxSlotCalls = slots.reduce((m, s) => Math.max(m, Number(s.totalCalls || 0)), 0);
    return { ...model, slots, maxSlotCalls };
  });
  const globalMaxSlotCalls = normalizedModels.reduce((m, x) => Math.max(m, x.maxSlotCalls || 0), 0);

  const rowsHtml = normalizedModels
    .map((model) => {
      const cells = (model.slots || [])
        .map((slot) => {
          const visual = calcCellVisual(slot, globalMaxSlotCalls);
          return `<i class="${visual.className}" style="${visual.style}" title="${visual.title}"></i>`;
        })
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

async function loadRealtimeAvailability(forceRefresh = false) {
  const hintEl = document.getElementById("realtimeHint");
  hintEl.textContent = "正在加载实时可用性...";
  const params = new URLSearchParams();
  params.set("window", realtimeWindow);
  if (forceRefresh) params.set("force_refresh", "1");
  const res = await fetch(`/api/stats/realtime-availability?${params.toString()}`);
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}`);
  }
  const data = await res.json();
  renderRealtimeGrid(data.models || []);
  hintEl.textContent =
    `时间窗口: ${windowLabel(data.window)} | ` +
    `每模型 ${data.slot_count || data.event_limit} 个时间格 | ` +
    `每格约 ${formatSlotDuration(data.slot_seconds)} | ` +
    `色块占比=状态构成，深浅=调用密度`;
}

async function loadAvailability(forceRefresh = false) {
  const range = CCH.getRange();
  CCH.setText("metaText", "正在加载模型可用性数据...");
  const data = await CCH.fetchJson("/api/dashboard", range, { forceRefresh });
  availabilityRows = data.model_availability || [];
  fillAvailabilityKpi(availabilityRows);
  drawAvailabilityCharts(availabilityRows);
  renderAvailabilityTable(availabilityRows);
  CCH.setMetaFromDashboard(data);
}

async function safeLoadAvailability(forceRefresh = false) {
  try {
    await Promise.all([loadAvailability(forceRefresh), loadRealtimeAvailability(forceRefresh)]);
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
    if (!availabilityRows.length) return;
    drawAvailabilityCharts(availabilityRows);
    renderAvailabilityTable(availabilityRows);
  });
  bindRealtimeWindowButtons();
  safeLoadAvailability();
  setInterval(safeLoadAvailability, 90000);
});
