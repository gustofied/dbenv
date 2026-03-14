# type: ignore
"""Lab viewer — combined metrics dashboard + rollout viewer.

Polls doltgres Prometheus metrics and reads run results from results/.
Orchestrator spawns this automatically if not already running.

Usage:
    uv run uvicorn lab_viewer:app --port 8090
"""

import json
from pathlib import Path
import httpx
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, PlainTextResponse, JSONResponse

app = FastAPI()

DIR = Path(__file__).parent
RESULTS_DIR = DIR / "results"
METRICS_URL = "http://localhost:11228/metrics"


@app.get("/metrics")
async def metrics():
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(METRICS_URL, timeout=2)
            return PlainTextResponse(r.text)
    except Exception:
        return PlainTextResponse("", status_code=502)


@app.get("/api/runs")
async def list_runs():
    if not RESULTS_DIR.exists():
        return JSONResponse([])
    files = sorted(RESULTS_DIR.glob("run_*.json"), reverse=True)
    return JSONResponse([f.name for f in files])


@app.get("/api/run/{filename}")
async def get_run(filename: str):
    path = RESULTS_DIR / filename
    if not path.exists():
        return JSONResponse(None, status_code=404)
    return JSONResponse(json.loads(path.read_text()))


@app.get("/api/latest")
async def latest_run():
    if not RESULTS_DIR.exists():
        return JSONResponse(None)
    files = sorted(RESULTS_DIR.glob("run_*.json"), reverse=True)
    if not files:
        return JSONResponse(None)
    return JSONResponse(json.loads(files[0].read_text()))


@app.get("/")
async def index():
    return HTMLResponse(PAGE_HTML)


PAGE_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>lab viewer</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: monospace; background: #111; color: #ccc; padding: 24px; max-width: 1200px; margin: 0 auto; }

  h1 { font-size: 14px; color: #666; margin-bottom: 4px; }
  .status { font-size: 12px; color: #444; margin-bottom: 20px; }
  .status.live { color: #4a4; }
  .status.dead { color: #a44; }

  /* key metrics row */
  .key-metrics { display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: 12px; margin-bottom: 24px; }
  .km { background: #1a1a1a; border-left: 3px solid #333; padding: 14px 16px; border-radius: 0 4px 4px 0; }
  .km .label { font-size: 9px; color: #555; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 6px; }
  .km .value { font-size: 28px; font-weight: bold; }
  .km .sub { font-size: 10px; color: #444; margin-top: 2px; }
  .km-green { border-left-color: #4a4; }
  .km-green .value { color: #6c6; }
  .km-blue { border-left-color: #48f; }
  .km-blue .value { color: #6af; }
  .km-amber { border-left-color: #a84; }
  .km-amber .value { color: #da6; }
  .km-purple { border-left-color: #84f; }
  .km-purple .value { color: #a6f; }
  .km-red { border-left-color: #a44; }
  .km-red .value { color: #e66; }

  /* live metrics cards */
  .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(160px, 1fr)); gap: 10px; margin-bottom: 24px; }
  .card { background: #1a1a1a; border: 1px solid #222; padding: 12px; border-radius: 4px; }
  .card .label { font-size: 9px; color: #555; margin-bottom: 4px; text-transform: uppercase; letter-spacing: 0.5px; }
  .card .value { font-size: 20px; color: #eee; }
  .card .unit { font-size: 10px; color: #444; }

  /* charts */
  .charts { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 24px; }
  .chart-box { background: #1a1a1a; border: 1px solid #222; padding: 16px; border-radius: 4px; }
  .chart-box h2 { font-size: 10px; color: #555; margin-bottom: 8px; text-transform: uppercase; letter-spacing: 1px; }
  canvas { display: block; }

  /* section headers */
  .section { margin: 28px 0 12px; border-top: 1px solid #222; padding-top: 16px; }
  .section h2 { font-size: 12px; color: #555; text-transform: uppercase; letter-spacing: 1px; }

  /* run selector */
  .run-bar { display: flex; align-items: center; gap: 12px; margin-bottom: 16px; }
  .run-bar select {
    font-family: monospace; font-size: 12px;
    background: #1a1a1a; color: #ccc; border: 1px solid #333;
    padding: 4px 8px; border-radius: 2px;
  }
  .run-bar .run-info { font-size: 11px; color: #555; }

  /* summary stats */
  .summary { font-size: 12px; color: #888; margin: 8px 0 16px; }
  .summary span { color: #ccc; }

  /* epoch table */
  table.epoch { width: 100%; border-collapse: collapse; font-size: 12px; margin-bottom: 16px; }
  table.epoch th { text-align: left; color: #555; padding: 4px 10px; border-bottom: 1px solid #333; font-size: 10px; text-transform: uppercase; letter-spacing: 0.5px; }
  table.epoch td { padding: 4px 10px; border-bottom: 1px solid #1a1a1a; }
  table.epoch tr:hover { background: #1a1a1a; }
  .ok { color: #4a4; }
  .fail { color: #a44; }

  /* collapsible steps */
  .step-group { margin-bottom: 2px; }
  .step-header {
    display: flex; align-items: center; gap: 10px;
    padding: 6px 0; cursor: pointer; user-select: none;
    border-bottom: 1px solid #1a1a1a;
  }
  .step-header:hover { background: #161616; }
  .step-title { font-size: 11px; color: #888; min-width: 56px; }
  .badge { display: inline-block; font-size: 10px; padding: 1px 6px; border-radius: 2px; background: #1a1a1a; color: #666; border: 1px solid #282828; }
  .badge-ok { color: #4a4; border-color: #2a3a2a; }
  .badge-fail { color: #a44; border-color: #3a2a2a; }
  .badge-time { color: #48f; border-color: #2a2a3a; }
  .badge-user { color: #c084fc; border-color: #2a2a3a; }

  .collapse-icon { margin-left: auto; color: #333; font-size: 8px; transition: transform 0.15s; }
  .collapse-icon::after { content: '\\25BC'; }
  .collapsed > .step-header .collapse-icon,
  .collapsed > .rollout-header .collapse-icon { transform: rotate(-90deg); }
  .collapsed > .step-body,
  .collapsed > .rollout-body { display: none; }
  .step-body { padding-left: 16px; border-left: 1px solid #222; margin-left: 8px; }

  /* rollout cards */
  .rollout-card { margin: 0; }
  .rollout-header {
    display: flex; align-items: center; gap: 8px;
    padding: 4px 0; cursor: pointer; user-select: none;
  }
  .rollout-header:hover { background: #161616; }
  .rollout-idx { font-size: 10px; color: #666; min-width: 80px; }
  .rollout-hash { font-size: 9px; color: #333; }
  .rollout-body { padding: 8px 16px; border-left: 1px solid #222; margin-left: 6px; margin-bottom: 4px; }

  .detail-table { width: 100%; border-collapse: collapse; font-size: 11px; margin: 4px 0; }
  .detail-table td { padding: 2px 8px; }
  .detail-table td:first-child { color: #555; width: 100px; }

  /* controls */
  .controls { display: flex; gap: 8px; margin-bottom: 12px; }
  .controls button {
    font-family: monospace; font-size: 10px; padding: 4px 12px;
    color: #666; background: #1a1a1a; border: 1px solid #333;
    border-radius: 2px; cursor: pointer;
  }
  .controls button:hover { color: #ccc; border-color: #555; }

  /* history table */
  .history { margin-top: 24px; margin-bottom: 24px; }
  .history h2 { font-size: 11px; color: #555; margin-bottom: 8px; text-transform: uppercase; letter-spacing: 1px; }
  .history-scroll { max-height: 240px; overflow-y: auto; }
  .history-scroll::-webkit-scrollbar { width: 4px; }
  .history-scroll::-webkit-scrollbar-thumb { background: #333; border-radius: 2px; }
  table.hist { width: 100%; border-collapse: collapse; font-size: 11px; }
  table.hist th { text-align: left; color: #444; padding: 4px 8px; border-bottom: 1px solid #222; font-size: 10px; text-transform: uppercase; letter-spacing: 0.5px; position: sticky; top: 0; background: #111; }
  table.hist td { padding: 3px 8px; border-bottom: 1px solid #1a1a1a; }
  .bar { display: inline-block; height: 8px; background: #4a4; border-radius: 2px; min-width: 2px; }

  /* live charts */
  .live-charts { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 24px; }
  .live-charts .chart-box { background: #1a1a1a; border: 1px solid #222; padding: 16px; border-radius: 4px; }
  .live-charts .chart-box h2 { font-size: 10px; color: #555; margin-bottom: 8px; text-transform: uppercase; letter-spacing: 1px; }

  @media (max-width: 700px) { .charts { grid-template-columns: 1fr; } }
</style>
</head>
<body>

<h1>lab viewer</h1>
<div class="status" id="status">connecting...</div>

<!-- key metrics -->
<div class="key-metrics" id="key-metrics"></div>

<!-- live metrics -->
<div class="grid" id="cards"></div>

<!-- live rolling charts -->
<div class="live-charts" id="live-charts" style="display:none">
  <div class="chart-box">
    <h2>memory (MB)</h2>
    <canvas id="liveMemChart"></canvas>
  </div>
  <div class="chart-box">
    <h2>connections &amp; queries</h2>
    <canvas id="liveConnChart"></canvas>
  </div>
</div>

<!-- metrics history -->
<div class="history" id="history-section" style="display:none">
  <h2>history (last 60 samples)</h2>
  <div class="history-scroll">
    <table class="hist">
      <thead><tr><th>time</th><th>conn</th><th>queries</th><th>alloc MB</th><th>sys MB</th><th>gc</th><th>mem</th></tr></thead>
      <tbody id="history"></tbody>
    </table>
  </div>
</div>

<!-- run timing charts -->
<div class="charts" id="charts-container" style="display:none">
  <div class="chart-box">
    <h2>step time (s)</h2>
    <canvas id="timeChart"></canvas>
  </div>
  <div class="chart-box">
    <h2>breakdown (s)</h2>
    <canvas id="breakdownChart"></canvas>
  </div>
  <div class="chart-box">
    <h2>rollout duration (ms)</h2>
    <canvas id="rolloutChart"></canvas>
  </div>
  <div class="chart-box">
    <h2>create vs delete (s)</h2>
    <canvas id="cdChart"></canvas>
  </div>
</div>

<!-- run results -->
<div class="section" id="run-section" style="display:none">
  <h2>run results</h2>
</div>

<div class="run-bar" id="run-bar" style="display:none">
  <select id="run-select"></select>
  <span class="run-info" id="run-info"></span>
</div>

<div id="run-summary" class="summary"></div>

<div class="controls" id="run-controls" style="display:none">
  <button onclick="toggleAll(true)">expand all</button>
  <button onclick="toggleAll(false)">collapse all</button>
</div>

<div id="run-body"></div>

<script>
// ── metrics ──

const watched = {
  'dss_concurrent_connections': { label: 'connections', unit: '' },
  'dss_concurrent_queries': { label: 'queries', unit: '' },
  'go_memstats_alloc_bytes': { label: 'memory allocated', unit: 'MB', transform: v => (v / 1024 / 1024).toFixed(1) },
  'go_memstats_sys_bytes': { label: 'memory from OS', unit: 'MB', transform: v => (v / 1024 / 1024).toFixed(1) },
  'go_goroutines': { label: 'goroutines', unit: '' },
  'go_gc_duration_seconds_count': { label: 'gc cycles', unit: '' },
};

function parseMetrics(text) {
  const result = {};
  for (const line of text.split('\\n')) {
    if (line.startsWith('#') || !line.trim()) continue;
    const m = line.match(/^([a-z_]+?)(?:\\{[^}]*\\})?\\s+([\\d.e+-]+)/);
    if (m && m[1] in watched) result[m[1]] = parseFloat(m[2]);
  }
  return result;
}

function renderCards(data) {
  const el = document.getElementById('cards');
  el.innerHTML = '';
  for (const [key, cfg] of Object.entries(watched)) {
    const raw = data[key];
    if (raw === undefined) continue;
    const val = cfg.transform ? cfg.transform(raw) : raw;
    el.innerHTML += `<div class="card"><div class="label">${cfg.label}</div><div class="value">${val} <span class="unit">${cfg.unit}</span></div></div>`;
  }
}

const metricsHistory = [];
const MAX_HISTORY = 60;
let maxMem = 1;

function renderHistory() {
  const el = document.getElementById('history');
  let html = '';
  for (let i = metricsHistory.length - 1; i >= 0; i--) {
    const h = metricsHistory[i];
    const mem = h.alloc || 0;
    const pct = Math.max(2, (mem / maxMem) * 120);
    html += `<tr>
      <td>${h.time}</td>
      <td>${h.conn ?? '-'}</td>
      <td>${h.queries ?? '-'}</td>
      <td>${h.alloc ? h.alloc.toFixed(1) : '-'}</td>
      <td>${h.sys ? h.sys.toFixed(1) : '-'}</td>
      <td>${h.gc ?? '-'}</td>
      <td><span class="bar" style="width:${pct}px"></span></td>
    </tr>`;
  }
  el.innerHTML = html;
}

async function pollMetrics() {
  try {
    const res = await fetch('/metrics');
    if (res.status === 502) throw new Error('no metrics');
    const text = await res.text();
    const data = parseMetrics(text);
    document.getElementById('status').className = 'status live';
    document.getElementById('status').textContent = 'live — polling every 5s';
    renderCards(data);

    // track history
    const alloc = data['go_memstats_alloc_bytes'] ? data['go_memstats_alloc_bytes'] / 1024 / 1024 : 0;
    const sys = data['go_memstats_sys_bytes'] ? data['go_memstats_sys_bytes'] / 1024 / 1024 : 0;
    if (alloc > maxMem) maxMem = alloc;

    metricsHistory.push({
      time: new Date().toLocaleTimeString(),
      conn: data['dss_concurrent_connections'],
      queries: data['dss_concurrent_queries'],
      alloc: alloc,
      sys: sys,
      gc: data['go_gc_duration_seconds_count'],
    });
    if (metricsHistory.length > MAX_HISTORY) metricsHistory.shift();

    document.getElementById('history-section').style.display = '';
    renderHistory();

    // live rolling charts
    document.getElementById('live-charts').style.display = '';
    const histLabels = metricsHistory.map(h => h.time.replace(/.*?(\\d+:\\d+).*/, '$1'));
    drawChart('liveMemChart', [
      { label: 'alloc', values: metricsHistory.map(h => h.alloc) },
      { label: 'sys', values: metricsHistory.map(h => h.sys) },
    ], histLabels);
    drawChart('liveConnChart', [
      { label: 'conn', values: metricsHistory.map(h => h.conn ?? 0) },
      { label: 'queries', values: metricsHistory.map(h => h.queries ?? 0) },
    ], histLabels);
  } catch (e) {
    document.getElementById('status').className = 'status dead';
    document.getElementById('status').textContent = 'server not reachable';
    document.getElementById('cards').innerHTML = '';
  }
}

// ── charts ──

function drawChart(canvasId, datasets, labels) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;

  // get actual pixel size from parent, not from hidden element
  const parent = canvas.parentElement;
  const W = parent.clientWidth - 32; // minus padding
  const H = 180;
  if (W <= 0) return;

  const dpr = window.devicePixelRatio || 1;
  canvas.width = W * dpr;
  canvas.height = H * dpr;
  canvas.style.width = W + 'px';
  canvas.style.height = H + 'px';

  const ctx = canvas.getContext('2d');
  ctx.scale(dpr, dpr);

  const pad = { t: 12, r: 12, b: 24, l: 48 };
  const pW = W - pad.l - pad.r, pH = H - pad.t - pad.b;

  let allVals = datasets.flatMap(d => d.values.filter(v => v !== null && v !== undefined));
  if (!allVals.length) return;
  let minV = Math.min(...allVals), maxV = Math.max(...allVals);
  // pad range by 5% so lines don't sit on edges
  const margin = (maxV - minV) * 0.05 || 1;
  minV -= margin; maxV += margin;
  const rangeV = maxV - minV;
  const n = datasets[0].values.length;
  if (n === 0) return;

  ctx.clearRect(0, 0, W, H);

  // grid lines
  ctx.strokeStyle = '#252525'; ctx.lineWidth = 0.5;
  for (let i = 0; i <= 4; i++) {
    const y = pad.t + pH - (i / 4) * pH;
    ctx.beginPath(); ctx.moveTo(pad.l, y); ctx.lineTo(W - pad.r, y); ctx.stroke();
    ctx.fillStyle = '#555'; ctx.font = '9px monospace'; ctx.textAlign = 'right';
    ctx.fillText((minV + (i / 4) * rangeV).toFixed(2), pad.l - 6, y + 3);
  }

  // x labels
  ctx.fillStyle = '#555'; ctx.textAlign = 'center';
  const xStep = Math.max(1, Math.floor(n / 8));
  for (let i = 0; i < n; i += xStep) {
    const x = pad.l + (n === 1 ? pW / 2 : (i / (n - 1)) * pW);
    ctx.fillText(labels[i], x, H - 6);
  }

  // data lines
  const colors = ['#4a4', '#48f', '#f84', '#c84f', '#a6f'];
  datasets.forEach((ds, di) => {
    ctx.strokeStyle = colors[di % colors.length];
    ctx.lineWidth = 1.5;
    ctx.beginPath();
    let started = false;
    ds.values.forEach((v, i) => {
      if (v === null || v === undefined) return;
      const x = pad.l + (n === 1 ? pW / 2 : (i / (n - 1)) * pW);
      const y = pad.t + pH - ((v - minV) / rangeV) * pH;
      if (!started) { ctx.moveTo(x, y); started = true; } else ctx.lineTo(x, y);
    });
    ctx.stroke();

    // dots for small datasets
    if (n <= 20) {
      ctx.fillStyle = colors[di % colors.length];
      ds.values.forEach((v, i) => {
        if (v === null || v === undefined) return;
        const x = pad.l + (n === 1 ? pW / 2 : (i / (n - 1)) * pW);
        const y = pad.t + pH - ((v - minV) / rangeV) * pH;
        ctx.beginPath(); ctx.arc(x, y, 2, 0, Math.PI * 2); ctx.fill();
      });
    }
  });

  // legend
  if (datasets.length > 1) {
    ctx.font = '9px monospace';
    let lx = pad.l + 8;
    datasets.forEach((ds, di) => {
      ctx.fillStyle = colors[di % colors.length];
      ctx.fillRect(lx, pad.t + 2, 8, 2);
      ctx.fillText(ds.label, lx + 12, pad.t + 7);
      lx += ctx.measureText(ds.label).width + 24;
    });
  }
}

// ── run results ──

let currentRun = null;
let currentFile = null;
let knownRuns = [];
let chartsDrawn = false;

async function pollRuns() {
  try {
    const res = await fetch('/api/runs');
    const runs = await res.json();
    if (!runs.length) return;

    const sel = document.getElementById('run-select');
    const newLatest = runs[0];

    // update dropdown if run list changed
    if (JSON.stringify(runs) !== JSON.stringify(knownRuns)) {
      knownRuns = runs;
      const prevValue = sel.value;
      sel.innerHTML = runs.map(r => `<option value="${r}">${r.replace('run_', '').replace('.json', '')}</option>`).join('');

      // restore previous selection if it still exists, otherwise pick latest
      if (runs.includes(prevValue)) {
        sel.value = prevValue;
      }

      document.getElementById('run-section').style.display = '';
      document.getElementById('run-bar').style.display = '';
      document.getElementById('run-controls').style.display = '';
    }

    // auto-load if: no run loaded yet, or a new latest appeared while viewing the old latest
    if (!currentFile || (currentFile === knownRuns[1] && newLatest !== currentFile)) {
      sel.value = newLatest;
      loadRun(newLatest);
    }
  } catch (e) {}
}

async function loadRun(filename) {
  try {
    const res = await fetch(`/api/run/${filename}`);
    const data = await res.json();
    if (!data) return;
    currentRun = data;
    currentFile = filename;
    renderRun(data);
  } catch (e) {}
}

function renderKeyMetrics(run) {
  const el = document.getElementById('key-metrics');
  if (!run || !run.steps || !run.steps.length) { el.innerHTML = ''; return; }

  const steps = run.steps;
  const cfg = run.config || {};
  const gc = run.gc || {};
  const totals = steps.map(s => s.timing.total);
  const avg = totals.reduce((a, b) => a + b, 0) / totals.length;
  const allOk = steps.every(s => s.ok !== false);
  const totalRollouts = steps.reduce((a, s) => a + s.rollouts.length, 0);
  const allDurations = steps.flatMap(s => s.rollouts.map(r => r.duration_ms));
  const avgRollout = allDurations.length ? allDurations.reduce((a, b) => a + b, 0) / allDurations.length : 0;

  let drift = 1.0;
  if (totals.length >= 10) {
    const first5 = totals.slice(0, 5).reduce((a, b) => a + b) / 5;
    const last5 = totals.slice(-5).reduce((a, b) => a + b) / 5;
    drift = last5 / first5;
  }
  const driftClass = drift < 1.5 ? 'km-green' : drift < 2 ? 'km-amber' : 'km-red';

  el.innerHTML = `
    <div class="km ${allOk ? 'km-green' : 'km-red'}">
      <div class="label">verdict</div>
      <div class="value">${allOk ? 'pass' : 'FAIL'}</div>
      <div class="sub">${steps.length} steps verified</div>
    </div>
    <div class="km km-blue">
      <div class="label">avg step</div>
      <div class="value">${avg.toFixed(3)}s</div>
      <div class="sub">${totalRollouts} rollouts total</div>
    </div>
    <div class="km km-purple">
      <div class="label">avg rollout</div>
      <div class="value">${avgRollout.toFixed(0)}ms</div>
      <div class="sub">${cfg.num_rows || '?'} rows each</div>
    </div>
    <div class="km ${driftClass}">
      <div class="label">drift</div>
      <div class="value">${drift.toFixed(2)}x</div>
      <div class="sub">${drift < 1.5 ? 'stable' : 'degrading'}</div>
    </div>
    ${gc.gc_time !== undefined ? `
    <div class="km km-amber">
      <div class="label">gc</div>
      <div class="value">${gc.gc_time}s</div>
      <div class="sub">${gc.disk_before} → ${gc.disk_after} MB</div>
    </div>` : ''}
  `;
}

function renderRun(run) {
  if (!run || !run.steps || !run.steps.length) return;

  const steps = run.steps;
  const cfg = run.config || {};

  renderKeyMetrics(run);

  // info
  document.getElementById('run-info').textContent =
    `${cfg.num_steps || '?'} steps × ${cfg.rollouts_per_world || '?'} rollouts × ${cfg.num_rows || '?'} rows`;

  document.getElementById('run-summary').innerHTML = '';

  // show charts container BEFORE drawing so canvas has dimensions
  document.getElementById('charts-container').style.display = '';

  // defer drawing to next frame so layout is computed
  requestAnimationFrame(() => {
    const labels = steps.map(s => String(s.step));

    drawChart('timeChart', [{ label: 'total', values: steps.map(s => s.timing.total) }], labels);
    drawChart('breakdownChart', [
      { label: 'create', values: steps.map(s => s.timing.create) },
      { label: 'work', values: steps.map(s => s.timing.work) },
      { label: 'delete', values: steps.map(s => s.timing.delete) },
    ], labels);

    const avgDurations = steps.map(s => {
      const ds = s.rollouts.map(r => r.duration_ms);
      return ds.length ? ds.reduce((a, b) => a + b, 0) / ds.length : 0;
    });
    drawChart('rolloutChart', [{ label: 'avg ms', values: avgDurations }], labels);

    drawChart('cdChart', [
      { label: 'create', values: steps.map(s => s.timing.create) },
      { label: 'delete', values: steps.map(s => s.timing.delete) },
    ], labels);
  });

  // step groups with rollout cards
  let html = '';
  for (const step of steps) {
    const t = step.timing;
    const okClass = step.ok !== false ? 'badge-ok' : 'badge-fail';
    const okText = step.ok !== false ? 'ok' : 'fail';

    let rolloutCards = '';
    for (const r of step.rollouts) {
      rolloutCards += `
        <div class="rollout-card collapsed">
          <div class="rollout-header" onclick="this.parentElement.classList.toggle('collapsed')">
            <span class="rollout-idx">${esc(r.branch)}</span>
            <span class="badge badge-user">${esc(r.user)}</span>
            <span class="badge">${r.rows} rows</span>
            <span class="badge badge-time">${r.duration_ms}ms</span>
            <span class="rollout-hash">${esc(r.commit_hash.slice(0, 8))}</span>
            <span class="collapse-icon"></span>
          </div>
          <div class="rollout-body">
            <table class="detail-table">
              <tr><td>world</td><td>${esc(r.world)}</td></tr>
              <tr><td>world hash</td><td>${esc(r.world_hash)}</td></tr>
              <tr><td>commit</td><td>${esc(r.commit_hash)}</td></tr>
              <tr><td>branch</td><td>${esc(r.branch)}</td></tr>
              <tr><td>duration</td><td>${r.duration_ms}ms</td></tr>
            </table>
          </div>
        </div>`;
    }

    html += `
      <div class="step-group collapsed">
        <div class="step-header" onclick="this.parentElement.classList.toggle('collapsed')">
          <span class="step-title">step ${step.step}</span>
          <span class="badge ${okClass}">${okText}</span>
          <span class="badge badge-time">${t.total.toFixed(3)}s</span>
          <span class="badge">${step.rollouts.length} rollouts</span>
          <span class="collapse-icon"></span>
        </div>
        <div class="step-body">${rolloutCards}</div>
      </div>`;
  }
  document.getElementById('run-body').innerHTML = html;
}

function esc(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }

function toggleAll(expand) {
  document.querySelectorAll('.step-group, .rollout-card').forEach(c => {
    c.classList.toggle('collapsed', !expand);
  });
}

// ── init ──

document.getElementById('run-select').addEventListener('change', function() {
  loadRun(this.value);
});

pollMetrics();
setInterval(pollMetrics, 5000);
pollRuns();
setInterval(pollRuns, 5000);
window.addEventListener('resize', () => { if (currentRun) renderRun(currentRun); });
</script>
</body>
</html>"""
