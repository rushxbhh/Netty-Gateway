/* ── State ─────────────────────────────────────────────────────────────── */
let ws = null;
let currentType = 0;
let nextTxnId = (Date.now() % 10000000) + 1000;
let submitTs = 0;

let chartLatency = null;
let chartThroughput = null;
let chartScaling = null;
let benchSuites = [];
let selectedSuite = 0;

document.getElementById('txnId').value = nextTxnId;

/* ── Clock ─────────────────────────────────────────────────────────────── */
function updateClock() {
  const d = new Date();
  document.getElementById('clock').textContent =
    d.getUTCHours().toString().padStart(2, '0') + ':' +
    d.getUTCMinutes().toString().padStart(2, '0') + ':' +
    d.getUTCSeconds().toString().padStart(2, '0');
}
setInterval(updateClock, 1000);
updateClock();

/* ── View Switching ────────────────────────────────────────────────────── */
function switchView(name) {
  ['live', 'arch', 'bench'].forEach(v => {
    const cap = v.charAt(0).toUpperCase() + v.slice(1);
    document.getElementById('view' + cap).classList.toggle('active', v === name);
    document.getElementById('nav' + cap).classList.toggle('active', v === name);
  });
  if (name === 'bench') fetchBenchmarkData();
}

/* ── WebSocket ─────────────────────────────────────────────────────────── */
function connectWS() {
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  const url = `${proto}//${location.host || 'localhost:8080'}/ws`;
  logCon(`Connecting to ${url}…`, 'log-info');
  ws = new WebSocket(url);
  ws.binaryType = 'arraybuffer';

  ws.onopen = () => {
    setWsStatus('green', 'CONNECTED');
    logCon('WebSocket handshake established', 'log-info');
  };
  ws.onmessage = e => {
    const msg = typeof e.data === 'string' ? e.data : '(binary frame)';
    const cls = msg.startsWith('ACK') ? 'log-ack' : msg.startsWith('ACCEPT') ? 'log-accept' : 'log-reject';
    logCon(msg, cls);
    handleMessage(msg);
  };
  ws.onerror = () => logCon('Connection error', 'log-err');
  ws.onclose = () => {
    setWsStatus('red', 'DISCONNECTED');
    logCon('Disconnected — reconnecting in 3 s…', 'log-err');
    setTimeout(connectWS, 3000);
  };
}
connectWS();

function setWsStatus(color, text) {
  document.getElementById('wsDot').className = `dot ${color}`;
  const val = document.getElementById('wsVal');
  val.className = `status-val ${color}`;
  val.textContent = text;
}

/* ── Type Selector ─────────────────────────────────────────────────────── */
function selectType(t) {
  currentType = t;
  ['tabT', 'tabD', 'tabW'].forEach((id, i) => {
    document.getElementById(id).className = 'type-tab' +
      (i === t ? (i === 0 ? ' active-transfer' : i === 1 ? ' active-deposit' : ' active-withdraw') : '');
  });
  const from = document.getElementById('fromAccount');
  const to   = document.getElementById('toAccount');
  if (t === 0) { from.value = 101; from.disabled = false; to.value = 202; to.disabled = false; }
  if (t === 1) { from.value = 0;   from.disabled = true;  to.value = 202; to.disabled = false; }
  if (t === 2) { from.value = 101; from.disabled = false; to.value = 0;   to.disabled = true; }
}

function applyPreset(from, to, amount, type) {
  selectType(type);
  document.getElementById('fromAccount').value = from;
  document.getElementById('toAccount').value   = to;
  document.getElementById('amount').value      = amount;
  document.getElementById('txnId').value       = ++nextTxnId;
}

/* ── FlatBuffer Binary Encoder (60 bytes, schema NIGT) ─────────────────── */
function buildFlatBuffer(txnId, amount, fromAcct, toAcct, type) {
  const buf  = new ArrayBuffer(60);
  const view = new DataView(buf);
  view.setUint32(0, 16, true);
  view.setUint8(4, 0x4E); view.setUint8(5, 0x49); view.setUint8(6, 0x47); view.setUint8(7, 0x54);
  view.setUint16(8, 6, true); view.setUint16(10, 44, true); view.setUint16(12, 4, true);
  view.setInt32(16, 8, true);
  view.setBigUint64(20, BigInt(txnId),    true);
  view.setBigInt64(28,  BigInt(amount),   true);
  view.setBigUint64(36, BigInt(fromAcct), true);
  view.setBigUint64(44, BigInt(toAcct),   true);
  view.setUint8(52, type);
  return buf;
}

/* ── Form Submit ───────────────────────────────────────────────────────── */
function submitForm(e) {
  e.preventDefault();
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    logCon('Cannot submit: WebSocket not connected', 'log-err');
    return;
  }
  const txnId  = parseInt(document.getElementById('txnId').value, 10);
  const amount = parseInt(document.getElementById('amount').value, 10);
  const from   = parseInt(document.getElementById('fromAccount').value, 10);
  const to     = parseInt(document.getElementById('toAccount').value, 10);

  resetLifecycle();
  submitTs = performance.now();

  setStep(1, 'active', '60 B frame sent');
  setBadge('PROCESSING', 'amber');

  ws.send(buildFlatBuffer(txnId, amount, from, to, currentType));
  logCon(`→ TX #${txnId} sent (60 B, ${['TRANSFER', 'DEPOSIT', 'WITHDRAWAL'][currentType]})`, 'log-out');
  document.getElementById('txnId').value = ++nextTxnId;

  document.getElementById('submitSpinner').style.display = 'block';
  document.getElementById('submitText').textContent = 'Awaiting ACK…';
  document.getElementById('submitBtn').disabled = true;
}

function restoreSubmitBtn() {
  document.getElementById('submitSpinner').style.display = 'none';
  document.getElementById('submitText').textContent = 'Send Binary Transaction';
  document.getElementById('submitBtn').disabled = false;
}

/* ── WebSocket Message Handler ─────────────────────────────────────────── */
function handleMessage(msg) {
  const ms = (performance.now() - submitTs).toFixed(0);

  if (msg.startsWith('ACCEPT|stage=INGRESS')) {
    const inbSeq = msg.match(/inboundSequence=(\d+)/)?.[1] ?? '—';
    setStep(1, 'done',   'Delivered');
    setStep(2, 'done',   `inbSeq=${inbSeq} · ${ms} ms`);
    setTimeout(() => setStep(3, 'done', 'CRC32C + seqId (<1 ms)'), 15);
    setTimeout(() => setStep(4, 'active', 'Batch committing…'), 20);
  } else if (msg.startsWith('ACK|stage=AUDITED')) {
    const jSeq = msg.match(/journalSequence=(\d+)/)?.[1] ?? '—';
    setStep(3, 'done', `seq=${jSeq}`);
    setStep(4, 'done', 'Committed');
    setStep(5, 'done', `E2E ${ms} ms`);
    setBadge(`PERSISTED · ${ms} ms`, 'green');
    restoreSubmitBtn();
    setTimeout(fetchAudit, 250);
  } else if (msg.startsWith('REJECT')) {
    setStep(1, 'done', 'Delivered');
    setStep(2, 'error', msg.replace('REJECT|', ''));
    setBadge('REJECTED', 'red');
    restoreSubmitBtn();
  }
}

/* ── Lifecycle Stepper Helpers ─────────────────────────────────────────── */
function setStep(n, state, detail) {
  document.getElementById('step' + n).className = 'step-card ' + state;
  if (detail !== null && detail !== undefined) {
    document.getElementById('st' + n).textContent = detail;
  }
}

function resetLifecycle() {
  for (let i = 1; i <= 5; i++) {
    document.getElementById('step' + i).className = 'step-card';
    document.getElementById('st' + i).textContent = '—';
  }
}

function setBadge(text, color) {
  const b = document.getElementById('lifecycleBadge');
  b.className = color ? `state-pill ${color}` : 'state-pill';
  b.textContent = text;
}

/* ── Ring & DB Telemetry ───────────────────────────────────────────────── */
async function fetchTelemetry() {
  try {
    const r = await fetch('/api/telemetry');
    if (!r.ok) return;
    const d = await r.json();

    const setSvc = (dotId, valId, ok, okText, failText) => {
      document.getElementById(dotId).className = ok ? 'dot green' : 'dot red';
      const el = document.getElementById(valId);
      el.className = ok ? 'status-val green' : 'status-val red';
      el.textContent = ok ? okText : failText;
    };

    const svc = d.services || {};
    const seqOk = svc.sequencer === 'ONLINE';
    const audOk = svc.audit === 'ONLINE';
    const dbOk  = !!d.dbConnected;

    setSvc('seqDot', 'seqVal', seqOk, 'ONLINE', 'OFFLINE');
    setSvc('audDot', 'audVal', audOk, 'ONLINE', 'OFFLINE');
    setSvc('dbDot',  'dbVal',  dbOk,  'CONNECTED', 'OFFLINE');

    document.getElementById('inboundDot').className = seqOk ? 'dot green' : 'dot amber';
    document.getElementById('journalDot').className = (seqOk && audOk) ? 'dot green' : 'dot amber';
    document.getElementById('ackDot').className     = audOk ? 'dot green' : 'dot amber';

    const fmt = n => Number(n).toLocaleString();
    const pct = (depth, cap) => Math.min(100, Math.max(0, (depth / cap) * 100)).toFixed(1) + '%';

    document.getElementById('inboundProducer').textContent = fmt(d.inbound.producerSeq);
    document.getElementById('inboundConsumer').textContent = fmt(d.inbound.consumerSeq);
    document.getElementById('inboundDepth').textContent    = fmt(d.inbound.depth);
    document.getElementById('inboundBar').style.width      = pct(d.inbound.depth, d.inbound.capacity);

    document.getElementById('journalProducer').textContent = fmt(d.journal.producerSeq);
    document.getElementById('journalConsumer').textContent = fmt(d.journal.consumerSeq);
    document.getElementById('journalDepth').textContent    = fmt(d.journal.depth);
    document.getElementById('journalBar').style.width      = pct(d.journal.depth, d.journal.capacity);

    document.getElementById('ackProducer').textContent = fmt(d.ack.producerSeq);
    document.getElementById('ackConsumer').textContent = fmt(d.ack.consumerSeq);
    document.getElementById('ackDepth').textContent    = fmt(d.ack.depth);
    document.getElementById('ackBar').style.width      = pct(d.ack.depth, d.ack.capacity);
  } catch (_) {}
}
setInterval(fetchTelemetry, 1000);
fetchTelemetry();

/* ── Audit Ledger Table ────────────────────────────────────────────────── */
async function fetchAudit() {
  try {
    const r = await fetch('/api/audit');
    if (!r.ok) return;
    const d = await r.json();
    const tbody = document.getElementById('auditBody');
    if (!d.records || d.records.length === 0) {
      document.getElementById('auditBadge').textContent = '0 records';
      tbody.innerHTML = '<tr><td colspan="9" class="tbl-empty">No audit records yet — submit a transaction above.</td></tr>';
      return;
    }
    document.getElementById('auditBadge').textContent = d.records.length + ' records';
    const typeNames = ['TRANSFER', 'DEPOSIT', 'WITHDRAW'];
    const typeCls   = ['transfer', 'deposit', 'withdraw'];
    tbody.innerHTML = d.records.map(row => {
      const typeName = typeNames[row.type] || 'UNKNOWN';
      const typeCl   = typeCls[row.type]   || 'transfer';
      const dollars  = '$' + (row.amount / 100).toFixed(2);
      return `<tr>
        <td class="mono font-bold">${row.sequenceId}</td>
        <td class="mono" style="color:var(--text-3)">${row.clientTxnId}</td>
        <td><span class="badge ${typeCl}">${typeName}</span></td>
        <td class="mono" style="color:var(--text-3)">${row.fromAccount}</td>
        <td class="mono" style="color:var(--text-3)">${row.toAccount}</td>
        <td class="mono font-bold" style="text-align:right">${dollars}</td>
        <td><span class="badge persisted">PERSISTED</span></td>
        <td class="mono" style="color:var(--text-4);font-size:11px">${row.checksum}</td>
        <td class="mono" style="text-align:right;color:var(--text-3);font-size:11px">${row.auditedAt.slice(0, 19)}</td>
      </tr>`;
    }).join('');
  } catch (_) {}
}
setInterval(fetchAudit, 2500);
fetchAudit();

/* ── Console ───────────────────────────────────────────────────────────── */
function logCon(text, cls) {
  const c = document.getElementById('console');
  const d = document.createElement('div');
  d.className = cls || '';
  const ts = new Date().toISOString().slice(11, 23);
  d.textContent = `[${ts}] ${text}`;
  c.appendChild(d);
  if (c.children.length > 100) c.removeChild(c.firstChild);
  c.scrollTop = c.scrollHeight;
}
function clearConsole() { document.getElementById('console').innerHTML = ''; }

/* ── Benchmark Data & Chart.js ─────────────────────────────────────────── */
const DEFAULT_BENCH = {
  environment: { os: 'Windows 11 (amd64)', jvm: 'Oracle JDK 21.0.4', processors: 12, database: 'Neon PostgreSQL 16 (Singapore)' },
  suites: [
    { suite: 'Sequential-50',  concurrency: 1,  count: 50,  success: 50,  duration_sec: 10.00, throughput: 5.00,
      ingress:  { min: 0.801, p50: 1.207, p90: 1.573, p95: 1.673, p99: 2.320, max: 2.783, mean: 1.275, stddev: 0.303 },
      db_audit: { min: 170.71, p50: 197.01, p90: 202.46, p95: 215.99, p99: 341.19, max: 460.36, mean: 198.36, stddev: 38.78 },
      e2e:      { min: 172.11, p50: 198.47, p90: 203.50, p95: 216.92, p99: 342.55, max: 461.75, mean: 199.63, stddev: 38.80 } },
    { suite: 'Sequential-100', concurrency: 1,  count: 100, success: 100, duration_sec: 19.28, throughput: 5.19,
      ingress:  { min: 0.601, p50: 0.892, p90: 1.212, p95: 1.346, p99: 1.728, max: 1.977, mean: 0.935, stddev: 0.236 },
      db_audit: { min: 181.64, p50: 186.42, p90: 201.79, p95: 202.84, p99: 215.48, max: 219.19, mean: 191.83, stddev: 8.72 },
      e2e:      { min: 182.82, p50: 187.29, p90: 202.69, p95: 203.80, p99: 216.44, max: 220.25, mean: 192.76, stddev: 8.73 } },
    { suite: 'Concurrent-5',   concurrency: 5,  count: 50,  success: 50,  duration_sec: 2.05,  throughput: 24.41,
      ingress:  { min: 0.362, p50: 0.710, p90: 1.095, p95: 1.282, p99: 1.349, max: 1.355, mean: 0.754, stddev: 0.241 },
      db_audit: { min: 181.56, p50: 185.08, p90: 213.60, p95: 322.25, p99: 322.61, max: 322.71, mean: 202.81, stddev: 40.56 },
      e2e:      { min: 182.23, p50: 185.80, p90: 214.22, p95: 323.56, p99: 323.94, max: 324.05, mean: 203.56, stddev: 40.73 } },
    { suite: 'Concurrent-10',  concurrency: 10, count: 100, success: 100, duration_sec: 1.97,  throughput: 50.66,
      ingress:  { min: 0.392, p50: 0.735, p90: 1.128, p95: 1.287, p99: 1.384, max: 1.455, mean: 0.788, stddev: 0.251 },
      db_audit: { min: 177.87, p50: 199.15, p90: 201.38, p95: 201.59, p99: 201.73, max: 201.79, mean: 193.00, stddev: 8.97 },
      e2e:      { min: 179.16, p50: 200.03, p90: 202.07, p95: 202.37, p99: 202.51, max: 202.56, mean: 193.79, stddev: 8.94 } }
  ]
};

async function fetchBenchmarkData() {
  let data = DEFAULT_BENCH;
  try {
    const r = await fetch('/api/benchmark');
    if (r.ok) {
      const j = await r.json();
      if (j && j.suites && j.suites.length > 0) data = j;
    }
  } catch (_) {}

  benchSuites = data.suites;
  const env = data.environment || {};
  if (env.os)  document.getElementById('envOs').textContent  = `${env.os} · ${env.processors || 12} cores`;
  if (env.jvm) document.getElementById('envJvm').textContent = env.jvm;

  renderSuiteTabs();
  renderSuiteDetails(selectedSuite);
  renderCharts();
}

function renderSuiteTabs() {
  document.getElementById('suiteTabs').innerHTML = benchSuites.map((s, i) =>
    `<button class="suite-tab${i === selectedSuite ? ' active' : ''}" onclick="selectSuiteTab(${i})">${s.suite}</button>`
  ).join('');
}

function selectSuiteTab(i) {
  selectedSuite = i;
  renderSuiteTabs();
  renderSuiteDetails(i);
}

function renderSuiteDetails(i) {
  const s = benchSuites[i];
  if (!s) return;

  document.getElementById('heroIngressP50').textContent = s.ingress.p50.toFixed(2);
  document.getElementById('heroIngressP99').textContent = s.ingress.p99.toFixed(2) + ' ms';
  document.getElementById('heroDbP50').textContent      = s.db_audit.p50.toFixed(0);
  document.getElementById('heroTput').textContent       = s.throughput.toFixed(1);
  document.getElementById('activeSuiteLabel').textContent = `${s.suite} (conc=${s.concurrency})`;
  document.getElementById('barTotal').textContent       = s.e2e.p50.toFixed(2) + ' ms';

  const inPct = Math.max(0.5, (s.ingress.p50 / s.e2e.p50) * 100);
  const dbPct = 100 - inPct;
  document.getElementById('barIngress').style.width     = inPct.toFixed(1) + '%';
  document.getElementById('barDb').style.width          = dbPct.toFixed(1) + '%';
  document.getElementById('barIngressLbl').textContent  = s.ingress.p50.toFixed(2) + ' ms';
  document.getElementById('barDbLbl').textContent       = s.db_audit.p50.toFixed(2) + ' ms';
  document.getElementById('barIngressPct').textContent  = inPct.toFixed(1) + '%';
  document.getElementById('barDbPct').textContent       = dbPct.toFixed(1) + '%';

  const rows = [
    { label: 'Gateway Ingress', cls: 'text-blue',   d: s.ingress },
    { label: 'DB Audit Commit', cls: 'text-green',  d: s.db_audit },
    { label: 'End-to-End',      cls: 'text-purple', d: s.e2e }
  ];
  document.getElementById('perfTableBody').innerHTML = rows.map(row => `
    <tr>
      <td class="${row.cls}">${row.label}</td>
      <td>${row.d.min.toFixed(2)}</td>
      <td class="col-p50">${row.d.p50.toFixed(2)}</td>
      <td>${row.d.p90.toFixed(2)}</td>
      <td>${row.d.p95.toFixed(2)}</td>
      <td class="col-p99">${row.d.p99.toFixed(2)}</td>
      <td>${row.d.max.toFixed(2)}</td>
      <td>${row.d.mean.toFixed(2)}</td>
      <td>${row.d.stddev.toFixed(2)}</td>
    </tr>`).join('');
}

const C_BLUE   = '#1D4ED8';
const C_GREEN  = '#047857';
const C_AMBER  = '#B45309';
const C_PURPLE = '#6D28D9';

function renderCharts() {
  if (typeof Chart === 'undefined') return;
  const labels = benchSuites.map(s => s.suite.replace('Sequential', 'Seq').replace('Concurrent', 'Conc'));

  const latCtx = document.getElementById('chartLatency').getContext('2d');
  if (chartLatency) chartLatency.destroy();
  chartLatency = new Chart(latCtx, {
    type: 'bar',
    data: {
      labels,
      datasets: [
        { label: 'Ingress p50 (ms)',  data: benchSuites.map(s => s.ingress.p50),  backgroundColor: C_BLUE + '26',   borderColor: C_BLUE,   borderWidth: 1.5, borderRadius: 3 },
        { label: 'DB Audit p50 (ms)', data: benchSuites.map(s => s.db_audit.p50), backgroundColor: C_GREEN + '26',  borderColor: C_GREEN,  borderWidth: 1.5, borderRadius: 3 },
        { label: 'E2E p50 (ms)',      data: benchSuites.map(s => s.e2e.p50),      backgroundColor: C_PURPLE + '22', borderColor: C_PURPLE, borderWidth: 1.5, borderRadius: 3 }
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { position: 'top', labels: { font: { size: 11 }, boxWidth: 11, padding: 12 } } },
      scales: {
        x: { grid: { color: '#E2E8F0' }, ticks: { font: { size: 10.5 } } },
        y: { grid: { color: '#E2E8F0' }, ticks: { font: { size: 10.5 } }, title: { display: true, text: 'ms', font: { size: 10 }, color: '#94A3B8' } }
      }
    }
  });

  const tputColors = [C_BLUE, C_GREEN, C_AMBER, C_PURPLE];
  const maxTput = Math.max(...benchSuites.map(s => s.throughput)) * 1.15;
  document.getElementById('tputBars').innerHTML = benchSuites.map((s, i) => `
    <div class="tput-row">
      <div class="tput-name">${s.suite}</div>
      <div class="tput-track">
        <div class="tput-fill" style="width:${((s.throughput / maxTput) * 100).toFixed(1)}%;background:${tputColors[i] || C_BLUE}"></div>
      </div>
      <div class="tput-val">${s.throughput.toFixed(2)} tx/s</div>
    </div>`).join('');

  const tputCtx = document.getElementById('chartThroughput').getContext('2d');
  if (chartThroughput) chartThroughput.destroy();
  chartThroughput = new Chart(tputCtx, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'Throughput (tx/sec)',
        data: benchSuites.map(s => s.throughput),
        backgroundColor: benchSuites.map((_, i) => tputColors[i] + '33'),
        borderColor: benchSuites.map((_, i) => tputColors[i]),
        borderWidth: 1.5, borderRadius: 3
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { grid: { color: '#E2E8F0' }, ticks: { font: { size: 10.5 } } },
        y: { grid: { color: '#E2E8F0' }, ticks: { font: { size: 10.5 } }, title: { display: true, text: 'tx/sec', font: { size: 10 }, color: '#94A3B8' } }
      }
    }
  });

  const scaleCtx = document.getElementById('chartScaling').getContext('2d');
  if (chartScaling) chartScaling.destroy();
  chartScaling = new Chart(scaleCtx, {
    type: 'line',
    data: {
      labels,
      datasets: [
        { label: 'Ingress p50 (ms)', data: benchSuites.map(s => s.ingress.p50), borderColor: C_BLUE, backgroundColor: C_BLUE + '1A', borderWidth: 2, pointRadius: 4, tension: 0.25, fill: true, yAxisID: 'y' },
        { label: 'E2E p50 (ms)',     data: benchSuites.map(s => s.e2e.p50),     borderColor: C_PURPLE, backgroundColor: 'transparent', borderWidth: 2, pointRadius: 4, tension: 0.25, fill: false, yAxisID: 'y2' }
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { position: 'top', labels: { font: { size: 11 }, boxWidth: 11, padding: 12 } } },
      scales: {
        x:  { grid: { color: '#E2E8F0' }, ticks: { font: { size: 10.5 } } },
        y:  { type: 'linear', display: true, position: 'left',  grid: { color: '#E2E8F0' }, ticks: { font: { size: 10.5 } }, title: { display: true, text: 'Ingress (ms)', font: { size: 10 }, color: C_BLUE } },
        y2: { type: 'linear', display: true, position: 'right', grid: { drawOnChartArea: false }, ticks: { font: { size: 10.5 } }, title: { display: true, text: 'E2E (ms)', font: { size: 10 }, color: C_PURPLE } }
      }
    }
  });
}

fetchBenchmarkData();
