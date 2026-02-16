"""Dashboard Blueprint for the Trading Strategy Engine."""

from flask import Blueprint, jsonify, request, render_template_string

dashboard_bp = Blueprint("dashboard", __name__)

_engine = None


def init_dashboard(engine):
    """Wire the strategy engine into the dashboard routes."""
    global _engine
    _engine = engine


# ─── Dashboard HTML ─────────────────────────────────────────────────────────

DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>NIFTY Trading Dashboard</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: #f0f2f5; color: #1a1a2e; line-height: 1.5;
  }
  .container { max-width: 960px; margin: 0 auto; padding: 20px; }

  /* ── Header ── */
  header {
    display: flex; align-items: center; gap: 10px;
    margin-bottom: 4px; flex-wrap: wrap;
  }
  header h1 { font-size: 20px; font-weight: 700; }
  .badge {
    display: inline-block; padding: 3px 10px; border-radius: 12px;
    font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px;
  }
  .badge.stopped       { background: #e0e0e0; color: #555; }
  .badge.waiting       { background: #fff3cd; color: #856404; }
  .badge.active        { background: #d4edda; color: #155724; }
  .badge.market_closed { background: #cce5ff; color: #004085; }
  #symbol { font-size: 14px; color: #555; font-weight: 500; }
  .header-spacer { flex: 1; }
  header .btn { padding: 6px 16px; font-size: 13px; min-height: auto; }
  .auth-link {
    display: inline-flex; align-items: center; gap: 6px;
    font-size: 13px; color: #555; text-decoration: none;
  }
  .auth-link:hover { text-decoration: underline; }
  .dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }
  .dot.green { background: #28a745; }
  .dot.red   { background: #dc3545; }

  /* ── Info line (status-msg + toast) ── */
  .info-line {
    display: flex; justify-content: space-between; align-items: center;
    min-height: 20px; margin-bottom: 12px;
  }
  #status-msg { font-size: 13px; color: #888; }
  .toast {
    font-size: 13px; font-weight: 500; padding: 4px 0;
    transition: opacity 0.4s;
  }
  .toast.success { color: #155724; }
  .toast.error   { color: #dc3545; }

  /* ── Cards ── */
  .card {
    background: #fff; border-radius: 10px; padding: 20px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.08); margin-bottom: 16px;
  }
  .card h3 {
    font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px;
    color: #888; margin-bottom: 12px;
  }

  /* ── Grid ── */
  .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 16px; }

  /* ── P&L Card ── */
  .pnl-value { font-size: 36px; font-weight: 700; }
  .pnl-value.profit { color: #28a745; }
  .pnl-value.loss   { color: #dc3545; }
  .pnl-row { display: flex; justify-content: space-between; font-size: 14px; margin-top: 6px; color: #666; }

  /* ── Position Card ── */
  .pos-dir { font-size: 28px; font-weight: 700; margin-bottom: 8px; }
  .pos-dir.sell { color: #dc3545; }
  .pos-dir.buy  { color: #28a745; }
  .pos-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 4px 16px; font-size: 14px; }
  .pos-grid .label { color: #888; }
  .pos-grid .value { font-weight: 600; text-align: right; }
  .no-position { color: #aaa; font-style: italic; padding: 20px 0; }

  /* ── Buttons ── */
  .btn {
    padding: 10px 20px; border: none; border-radius: 6px;
    font-size: 14px; font-weight: 600; cursor: pointer; transition: opacity 0.15s;
    min-height: 44px;
  }
  .btn:hover { opacity: 0.85; }
  .btn:disabled { opacity: 0.5; cursor: not-allowed; }
  .btn-save  { background: #387ed1; color: #fff; }
  .btn-start { background: #28a745; color: #fff; }
  .btn-stop  { background: #dc3545; color: #fff; }
  .btn-sm    { padding: 6px 14px; font-size: 12px; min-height: auto; }

  /* ── Settings ── */
  .settings-header {
    display: flex; align-items: center; justify-content: space-between;
    margin-bottom: 12px;
  }
  .settings-header h3 { margin-bottom: 0; }
  .settings-grid {
    display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px;
  }
  .form-group label { display: block; font-size: 12px; color: #888; margin-bottom: 4px; }
  .form-group input, .form-group select {
    width: 100%; padding: 8px 10px; border: 1px solid #ddd; border-radius: 6px;
    font-size: 14px; background: #fafafa;
  }
  .form-group input:focus, .form-group select:focus {
    outline: none; border-color: #387ed1; background: #fff;
  }

  /* ── Tables ── */
  .table-wrap { overflow-x: auto; -webkit-overflow-scrolling: touch; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th { text-align: left; padding: 8px 10px; border-bottom: 2px solid #eee; color: #888; font-weight: 600; white-space: nowrap; }
  td { padding: 8px 10px; border-bottom: 1px solid #f0f0f0; white-space: nowrap; }
  tr:last-child td { border-bottom: none; }
  .dir-sell { color: #dc3545; font-weight: 600; }
  .dir-buy  { color: #28a745; font-weight: 600; }
  .pnl-pos  { color: #28a745; font-weight: 600; }
  .pnl-neg  { color: #dc3545; font-weight: 600; }
  .empty-log { color: #aaa; font-style: italic; padding: 16px 0; text-align: center; }

  /* ── Collapsible ── */
  .collapsible-toggle {
    cursor: pointer; user-select: none;
  }
  .collapsible-toggle::before {
    content: '\\25B6'; margin-right: 6px; font-size: 10px;
    display: inline-block; transition: transform 0.2s;
  }
  .collapsible-toggle.open::before { transform: rotate(90deg); }

  /* ── Log Viewer ── */
  .log-toggle { cursor: pointer; user-select: none; }
  .log-toggle::before { content: '\\25B6'; margin-right: 6px; font-size: 10px; display: inline-block; transition: transform 0.2s; }
  .log-toggle.open::before { transform: rotate(90deg); }
  .log-pre {
    background: #1a1a2e; color: #d4d4d4; padding: 12px; border-radius: 6px;
    font-family: 'Menlo', 'Consolas', monospace; font-size: 12px; line-height: 1.4;
    max-height: 400px; overflow-y: auto; white-space: pre-wrap; word-break: break-all;
    margin-top: 10px;
  }

  /* ── Mobile ── */
  @media (max-width: 640px) {
    .container { padding: 12px; }
    .grid-2 { grid-template-columns: 1fr; }
    .settings-grid { grid-template-columns: 1fr 1fr; }
    header h1 { font-size: 18px; }
    .pnl-value { font-size: 28px; }
    .pos-dir { font-size: 24px; }
  }
</style>
</head>
<body>
<div class="container">

  <!-- Header -->
  <header>
    <h1 id="title">Trading</h1>
    <span id="status" class="badge stopped">STOPPED</span>
    <span id="symbol"></span>
    <span class="header-spacer"></span>
    <button class="btn btn-start" id="toggle-btn" onclick="toggleEngine()">Start Engine</button>
    <a href="/login" id="auth-link" class="auth-link">Login</a>
  </header>
  <div class="info-line">
    <span id="status-msg"></span>
    <span class="toast" id="toast"></span>
  </div>

  <!-- P&L + Position -->
  <div class="grid-2">
    <div class="card" style="margin-bottom:0">
      <h3>P&amp;L</h3>
      <div id="pnl-total" class="pnl-value">&#8377;0.00</div>
      <div class="pnl-row"><span>Realized</span><span id="pnl-realized">&#8377;0.00</span></div>
      <div class="pnl-row"><span>Unrealized</span><span id="pnl-unrealized">&#8377;0.00</span></div>
    </div>
    <div class="card" style="margin-bottom:0">
      <h3>Active Position</h3>
      <div id="position"><div class="no-position">No active position</div></div>
    </div>
  </div>

  <!-- Server Logs — Collapsible -->
  <div class="card">
    <h3 class="log-toggle" id="log-toggle" onclick="toggleLogs()">Server Logs</h3>
    <div id="log-viewer" style="display:none;">
      <div style="display:flex; gap:8px; margin-bottom:8px; align-items:center;">
        <span style="flex:1"></span>
        <select id="log-level" onchange="loadLogs()" style="padding:6px 8px; border:1px solid #ddd; border-radius:6px; font-size:12px;">
          <option value="info">Info</option>
          <option value="debug">Debug</option>
        </select>
        <select id="log-lines" onchange="loadLogs()" style="padding:6px 8px; border:1px solid #ddd; border-radius:6px; font-size:12px;">
          <option value="50">50 lines</option>
          <option value="100">100 lines</option>
          <option value="200">200 lines</option>
        </select>
        <button class="btn btn-save btn-sm" onclick="loadLogs()">Refresh</button>
      </div>
      <pre class="log-pre" id="log-content">Click Refresh to load logs...</pre>
    </div>
  </div>

  <!-- Settings -->
  <div class="card">
    <div class="settings-header">
      <h3>Settings</h3>
      <button class="btn btn-save btn-sm" id="save-btn" onclick="saveSettings()">Save Settings</button>
    </div>
    <div class="settings-grid">
      <div class="form-group">
        <label>Strategy</label>
        <select id="s-strategy"></select>
      </div>
      <div class="form-group">
        <label>Start Time</label>
        <input type="time" id="s-start" value="10:00">
      </div>
      <div class="form-group">
        <label>Stop Time</label>
        <input type="time" id="s-stop" value="15:15">
      </div>
      <div class="form-group">
        <label>SL Points</label>
        <input type="number" id="s-sl" value="10" step="0.5" min="1">
      </div>
      <div class="form-group">
        <label>Target Points</label>
        <input type="number" id="s-tgt" value="10" step="0.5" min="1">
      </div>
      <div class="form-group">
        <label>Target Premium</label>
        <input type="number" id="s-premium" value="1000" step="50" min="100">
      </div>
      <div class="form-group">
        <label>Lots (<span id="s-qty-label">lot size: --</span>)</label>
        <input type="number" id="s-qty" value="0" min="1" step="1">
      </div>
      <div class="form-group">
        <label>Product</label>
        <select id="s-product">
          <option value="MIS">MIS (Intraday)</option>
          <option value="NRML">NRML (Overnight)</option>
        </select>
      </div>
    </div>
  </div>

  <!-- Trade Log (Today) -->
  <div class="card">
    <h3>Trade Log (Today)</h3>
    <div class="table-wrap" id="trades"><div class="empty-log">No trades yet today</div></div>
  </div>

  <!-- Trade History (All Time) — Collapsible -->
  <div class="card">
    <div style="display:flex; align-items:center; justify-content:space-between;">
      <h3 class="collapsible-toggle" onclick="toggleHistory()" style="margin-bottom:0">Trade History (All Time)</h3>
      <button class="btn btn-save btn-sm" onclick="loadHistory(); var s=document.getElementById('history-section'); if(s.style.display==='none') toggleHistory();">Load</button>
    </div>
    <div id="history-section" style="display:none; margin-top:12px;">
      <div class="table-wrap" id="history"><div class="empty-log">Click Load to view all-time trade history</div></div>
    </div>
  </div>

</div>

<script>
let settingsLoaded = false;
const STRATEGY_LABELS = {sar: 'Buy/Sell CE Alternate', buy_alternate: 'Buy CE/PE Alternate', buy_scale_out: 'Buy CE/PE Scale Out Alternate'};

// Populate strategy dropdown on load
fetch('/api/strategies')
  .then(r => r.json())
  .then(function(names) {
    var sel = document.getElementById('s-strategy');
    sel.innerHTML = '';
    names.forEach(function(n) {
      var opt = document.createElement('option');
      opt.value = n;
      opt.textContent = STRATEGY_LABELS[n] || n;
      sel.appendChild(opt);
    });
  });

// Check auth status and update header link
fetch('/status')
  .then(r => r.json())
  .then(function(data) {
    var el = document.getElementById('auth-link');
    if (data.authenticated) {
      el.innerHTML = '<span class="dot green"></span>' + (data.user_id || '');
    } else {
      el.innerHTML = '<span class="dot red"></span>Login';
    }
  })
  .catch(function() {});

function refresh() {
  fetch('/api/dashboard')
    .then(r => r.json())
    .then(updateUI)
    .catch(e => console.error('Refresh error:', e));
}

function updateUI(d) {
  // Status badge
  const s = document.getElementById('status');
  s.textContent = d.engine_status.replace('_', ' ');
  s.className = 'badge ' + d.engine_status.toLowerCase();

  // Symbol
  document.getElementById('symbol').textContent = d.trading_symbol || '';

  // Position
  const posEl = document.getElementById('position');
  const pos = d.active_position;
  if (pos && pos.direction) {
    const dirClass = pos.direction.toLowerCase();
    const ltp = d.current_ltp || 0;
    const qty = (pos.remaining_lots && d.lot_size) ? pos.remaining_lots * d.lot_size : ((d.settings.quantity || 1) * (d.lot_size || 1));
    let unrealized = 0;
    if (pos.direction === 'SELL') unrealized = (pos.entry_price - ltp) * qty;
    else unrealized = (ltp - pos.entry_price) * qty;

    posEl.innerHTML =
      '<div class="pos-dir ' + dirClass + '">' + pos.direction + '</div>' +
      '<div class="pos-grid">' +
        '<span class="label">Entry</span><span class="value">' + pos.entry_price.toFixed(2) + '</span>' +
        '<span class="label">LTP</span><span class="value">' + ltp.toFixed(2) + '</span>' +
        '<span class="label">SL</span><span class="value">' + pos.sl_price.toFixed(2) + '</span>' +
        '<span class="label">Target</span><span class="value">' + pos.target_price.toFixed(2) + '</span>' +
        '<span class="label">Lots</span><span class="value">' + (pos.remaining_lots || '-') + '</span>' +
        '<span class="label">Unrealized</span><span class="value ' + (unrealized >= 0 ? 'pnl-pos' : 'pnl-neg') + '">' +
          (unrealized >= 0 ? '+' : '') + unrealized.toFixed(2) + '</span>' +
        '<span class="label">Since</span><span class="value">' + (pos.entry_time || '-') + '</span>' +
      '</div>';

    // Unrealized P&L
    document.getElementById('pnl-unrealized').textContent =
      (unrealized >= 0 ? '+' : '') + '\u20B9' + unrealized.toFixed(2);
  } else {
    posEl.innerHTML = '<div class="no-position">No active position</div>';
    document.getElementById('pnl-unrealized').textContent = '\u20B90.00';
  }

  // P&L
  const realized = d.total_pnl || 0;
  document.getElementById('pnl-realized').textContent =
    (realized >= 0 ? '+' : '') + '\u20B9' + realized.toFixed(2);

  let unrealizedVal = 0;
  if (pos && pos.direction && d.current_ltp) {
    const uQty = (pos.remaining_lots && d.lot_size) ? pos.remaining_lots * d.lot_size : ((d.settings.quantity || 1) * (d.lot_size || 1));
    if (pos.direction === 'SELL') unrealizedVal = (pos.entry_price - d.current_ltp) * uQty;
    else unrealizedVal = (d.current_ltp - pos.entry_price) * uQty;
  }
  const total = realized + unrealizedVal;
  const totalEl = document.getElementById('pnl-total');
  totalEl.textContent = (total >= 0 ? '+' : '') + '\u20B9' + total.toFixed(2);
  totalEl.className = 'pnl-value ' + (total >= 0 ? 'profit' : 'loss');

  // Settings (first load only)
  if (!settingsLoaded) {
    document.getElementById('s-start').value = d.settings.start_time || '10:00';
    document.getElementById('s-stop').value = d.settings.stop_time || '15:15';
    document.getElementById('s-sl').value = d.settings.sl_points || 10;
    document.getElementById('s-tgt').value = d.settings.target_points || 10;
    document.getElementById('s-premium').value = d.settings.target_premium || 1000;
    document.getElementById('s-qty').value = d.settings.quantity || 0;
    document.getElementById('s-product').value = d.settings.product || 'MIS';
    document.getElementById('s-strategy').value = d.strategy_name || 'sar';
    settingsLoaded = true;
  }

  // Strategy dropdown: disable while running, update title
  var stratSel = document.getElementById('s-strategy');
  var running = d.engine_status !== 'STOPPED' && d.engine_status !== 'MARKET_CLOSED';
  stratSel.disabled = running;
  var stratName = STRATEGY_LABELS[d.strategy_name] || d.strategy_name || 'SAR';
  document.getElementById('title').textContent = stratName;

  // Update lot size label and input constraints dynamically
  var lot = d.lot_size || 0;
  var lotMult = (d.strategy_name === 'buy_scale_out') ? 3 : 1;
  var qtyInput = document.getElementById('s-qty');
  var qtyLabel = document.getElementById('s-qty-label');
  if (lot > 0) {
    qtyLabel.textContent = 'lot size: ' + lot + (lotMult > 1 ? ' (min ' + lotMult + ' lots)' : '');
    qtyInput.min = lotMult;
    qtyInput.step = lotMult;
  } else {
    qtyLabel.textContent = 'lot size: --';
  }

  // Status message — only show when it adds info beyond the badge
  var msg = d.status_message || '';
  var badgeText = d.engine_status.replace(/_/g, ' ');
  var msgEl = document.getElementById('status-msg');
  if (msg && msg.toLowerCase() !== badgeText.toLowerCase()) {
    msgEl.textContent = msg;
  } else {
    msgEl.textContent = '';
  }

  // Toggle button (only update if not disabled — avoid overwriting "Starting..."/"Stopping...")
  const btn = document.getElementById('toggle-btn');
  if (!btn.disabled) {
    if (d.engine_status === 'STOPPED' || d.engine_status === 'MARKET_CLOSED') {
      btn.textContent = 'Start Engine';
      btn.className = 'btn btn-start';
    } else {
      btn.textContent = 'Stop Engine';
      btn.className = 'btn btn-stop';
    }
  }

  // Trade log
  const trades = d.trades_today || [];
  const tEl = document.getElementById('trades');
  if (trades.length === 0) {
    tEl.innerHTML = '<div class="empty-log">No trades yet today</div>';
  } else {
    let html = '<table><thead><tr>' +
      '<th>#</th><th>Dir</th><th>Entry</th><th>Exit</th>' +
      '<th>Entry Time</th><th>Exit Time</th><th>P&amp;L</th>' +
      '</tr></thead><tbody>';
    trades.forEach(function(t, i) {
      const dirClass = t.direction === 'SELL' ? 'dir-sell' : 'dir-buy';
      const pnlClass = t.pnl >= 0 ? 'pnl-pos' : 'pnl-neg';
      html += '<tr>' +
        '<td>' + (i + 1) + '</td>' +
        '<td class="' + dirClass + '">' + t.direction + '</td>' +
        '<td>' + (t.entry_price || 0).toFixed(2) + '</td>' +
        '<td>' + (t.exit_price || 0).toFixed(2) + '</td>' +
        '<td>' + (t.entry_time || '-') + '</td>' +
        '<td>' + (t.exit_time || '-') + '</td>' +
        '<td class="' + pnlClass + '">' + (t.pnl >= 0 ? '+' : '') + t.pnl.toFixed(2) + '</td>' +
        '</tr>';
    });
    html += '</tbody></table>';
    tEl.innerHTML = html;
  }
}

function showToast(msg, type) {
  var el = document.getElementById('toast');
  el.textContent = msg;
  el.className = 'toast ' + (type || 'success');
  el.style.opacity = '1';
  clearTimeout(el._timer);
  el._timer = setTimeout(function() { el.style.opacity = '0'; }, 3000);
}

function saveSettings() {
  var newStrategy = document.getElementById('s-strategy').value;
  var saveBtn = document.getElementById('save-btn');

  // Enforce lots is valid multiple of lot_multiplier
  var lotMult = (newStrategy === 'buy_scale_out') ? 3 : 1;
  var lots = parseInt(document.getElementById('s-qty').value, 10) || 0;
  if (lots > 0 && (lots < lotMult || lots % lotMult !== 0)) {
    var corrected = Math.max(Math.round(lots / lotMult), 1) * lotMult;
    document.getElementById('s-qty').value = corrected;
    showToast('Lots corrected to ' + corrected, 'error');
    return;
  }

  saveBtn.disabled = true;
  saveBtn.textContent = 'Saving...';

  var chain = Promise.resolve();

  // Switch strategy first if changed (only allowed when stopped)
  chain = chain.then(function() {
    return fetch('/api/strategy', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({strategy_name: newStrategy}),
    }).then(function(r) {
      if (!r.ok) return r.json().then(function(j) { throw new Error(j.error || 'Strategy switch failed'); });
      return r.json();
    });
  });

  // Then save other settings
  chain = chain.then(function() {
    const data = {
      start_time: document.getElementById('s-start').value,
      stop_time:  document.getElementById('s-stop').value,
      sl_points:  parseFloat(document.getElementById('s-sl').value),
      target_points: parseFloat(document.getElementById('s-tgt').value),
      target_premium: parseFloat(document.getElementById('s-premium').value),
      quantity:   parseInt(document.getElementById('s-qty').value, 10),
      product:    document.getElementById('s-product').value,
    };
    return fetch('/api/settings', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(data),
    }).then(r => r.json());
  });

  chain.then(function() {
    settingsLoaded = false;
    refresh();
    saveBtn.disabled = false;
    saveBtn.textContent = 'Save Settings';
    showToast('Settings saved', 'success');
  }).catch(function(e) {
    saveBtn.disabled = false;
    saveBtn.textContent = 'Save Settings';
    showToast('Save failed: ' + e, 'error');
  });
}

function toggleEngine() {
  var btn = document.getElementById('toggle-btn');
  var action = btn.textContent.startsWith('Start') ? 'start' : 'stop';

  // Disable button and show transitional text
  btn.disabled = true;
  btn.textContent = action === 'start' ? 'Starting...' : 'Stopping...';

  fetch('/api/engine/toggle', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({action: action}),
  }).then(function(r) { return r.json(); }).then(function() {
    settingsLoaded = false;
    // Rapid poll: 500ms intervals, 10 times
    var polls = 0;
    var rapidPoll = setInterval(function() {
      polls++;
      fetch('/api/dashboard').then(function(r) { return r.json(); }).then(function(d) {
        updateUI(d);
        btn.disabled = false;
        // If we started but immediately stopped, show status_message as toast
        if (action === 'start' && d.engine_status === 'STOPPED' && d.status_message) {
          clearInterval(rapidPoll);
          showToast('Engine stopped: ' + d.status_message, 'error');
        }
        if (polls >= 10) clearInterval(rapidPoll);
      });
    }, 500);
  }).catch(function(e) {
    btn.disabled = false;
    showToast('Toggle failed: ' + e, 'error');
  });
}

function toggleHistory() {
  var section = document.getElementById('history-section');
  var header = section.previousElementSibling.querySelector('.collapsible-toggle');
  if (section.style.display === 'none') {
    section.style.display = 'block';
    header.classList.add('open');
  } else {
    section.style.display = 'none';
    header.classList.remove('open');
  }
}

function toggleLogs() {
  var toggle = document.getElementById('log-toggle');
  var viewer = document.getElementById('log-viewer');
  if (viewer.style.display === 'none') {
    viewer.style.display = 'block';
    toggle.classList.add('open');
    loadLogs();
  } else {
    viewer.style.display = 'none';
    toggle.classList.remove('open');
  }
}

function loadLogs() {
  var lines = document.getElementById('log-lines').value;
  var level = document.getElementById('log-level').value;
  fetch('/api/logs?lines=' + lines + '&level=' + level)
    .then(function(r) { return r.json(); })
    .then(function(data) {
      var el = document.getElementById('log-content');
      el.textContent = (data.lines || []).join('\\n') || 'No logs available';
      el.scrollTop = el.scrollHeight;
    })
    .catch(function(e) {
      document.getElementById('log-content').textContent = 'Failed to load logs: ' + e;
    });
}

function loadHistory() {
  fetch('/api/trades/history')
    .then(r => r.json())
    .then(function(trades) {
      var el = document.getElementById('history');
      if (!trades || trades.length === 0) {
        el.innerHTML = '<div class="empty-log">No trade history found</div>';
        return;
      }
      // Show newest first
      trades.reverse();
      var html = '<table><thead><tr>' +
        '<th>Date</th><th>Symbol</th><th>Dir</th><th>Entry</th><th>Exit</th>' +
        '<th>Entry Time</th><th>Exit Time</th><th>Lots</th><th>P&amp;L</th>' +
        '</tr></thead><tbody>';
      var totalPnl = 0;
      trades.forEach(function(t) {
        var dirClass = t.direction === 'SELL' ? 'dir-sell' : 'dir-buy';
        var pnlClass = t.pnl >= 0 ? 'pnl-pos' : 'pnl-neg';
        totalPnl += t.pnl || 0;
        html += '<tr>' +
          '<td>' + (t.date || '-') + '</td>' +
          '<td style="font-size:11px">' + (t.symbol || '-') + '</td>' +
          '<td class="' + dirClass + '">' + t.direction + '</td>' +
          '<td>' + (t.entry_price || 0).toFixed(2) + '</td>' +
          '<td>' + (t.exit_price || 0).toFixed(2) + '</td>' +
          '<td>' + (t.entry_time || '-') + '</td>' +
          '<td>' + (t.exit_time || '-') + '</td>' +
          '<td>' + (t.quantity || 0) + '</td>' +
          '<td class="' + pnlClass + '">' + (t.pnl >= 0 ? '+' : '') + t.pnl.toFixed(2) + '</td>' +
          '</tr>';
      });
      html += '</tbody></table>';
      var sumClass = totalPnl >= 0 ? 'pnl-pos' : 'pnl-neg';
      html += '<div style="text-align:right;margin-top:8px;font-weight:700;">' +
        'Total: <span class="' + sumClass + '">' +
        (totalPnl >= 0 ? '+' : '') + '\u20B9' + totalPnl.toFixed(2) +
        '</span> (' + trades.length + ' trades)</div>';
      el.innerHTML = html;
    })
    .catch(e => alert('Failed to load history: ' + e));
}

setInterval(refresh, 5000);
refresh();
</script>
</body>
</html>
"""


# ─── Routes ─────────────────────────────────────────────────────────────────


@dashboard_bp.route("/dashboard")
def dashboard():
    return render_template_string(DASHBOARD_HTML)


@dashboard_bp.route("/api/dashboard")
def api_dashboard():
    if _engine is None:
        return jsonify({"error": "Engine not initialised"}), 503
    return jsonify(_engine.get_snapshot())


@dashboard_bp.route("/api/settings", methods=["POST"])
def api_settings():
    if _engine is None:
        return jsonify({"error": "Engine not initialised"}), 503

    data = request.get_json(silent=True) or {}

    # Type coercion
    if "sl_points" in data:
        data["sl_points"] = float(data["sl_points"])
    if "target_points" in data:
        data["target_points"] = float(data["target_points"])
    if "target_premium" in data:
        data["target_premium"] = float(data["target_premium"])
    if "quantity" in data:
        data["quantity"] = int(data["quantity"])

    _engine.update_settings(**data)
    return jsonify({"ok": True})


@dashboard_bp.route("/api/engine/toggle", methods=["POST"])
def api_toggle():
    if _engine is None:
        return jsonify({"error": "Engine not initialised"}), 503

    data = request.get_json(silent=True) or {}
    action = data.get("action")

    if action == "start":
        _engine.start()
    elif action == "stop":
        _engine.stop()
    else:
        return jsonify({"error": "action must be 'start' or 'stop'"}), 400

    return jsonify({"ok": True, "engine_status": _engine.state.engine_status})


@dashboard_bp.route("/api/trades/history")
def api_trade_history():
    from .strategy import StrategyEngine
    trades = StrategyEngine.load_trade_history()
    return jsonify(trades)


@dashboard_bp.route("/api/strategies")
def api_strategies():
    from .base_strategy import STRATEGY_REGISTRY
    return jsonify(list(STRATEGY_REGISTRY.keys()))


@dashboard_bp.route("/api/strategy", methods=["POST"])
def api_switch_strategy():
    if _engine is None:
        return jsonify({"error": "Engine not initialised"}), 503

    data = request.get_json(silent=True) or {}
    name = data.get("strategy_name", "")
    if not name:
        return jsonify({"error": "strategy_name required"}), 400

    try:
        _engine.switch_strategy(name)
    except RuntimeError as e:
        return jsonify({"error": str(e)}), 409
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    return jsonify({"ok": True, "strategy_name": name})
