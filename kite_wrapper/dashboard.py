"""Dashboard Blueprint for the NIFTY SAR Trading Strategy."""

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
<title>NIFTY SAR Dashboard</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: #f0f2f5; color: #1a1a2e; line-height: 1.5;
  }
  .container { max-width: 960px; margin: 0 auto; padding: 20px; }

  /* ── Header ── */
  header {
    display: flex; align-items: center; gap: 12px;
    margin-bottom: 24px; flex-wrap: wrap;
  }
  header h1 { font-size: 22px; font-weight: 700; }
  header a { font-size: 13px; color: #666; text-decoration: none; }
  header a:hover { text-decoration: underline; }
  .badge {
    display: inline-block; padding: 4px 12px; border-radius: 12px;
    font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px;
  }
  .badge.stopped    { background: #e0e0e0; color: #555; }
  .badge.waiting     { background: #fff3cd; color: #856404; }
  .badge.active      { background: #d4edda; color: #155724; }
  .badge.market_closed { background: #cce5ff; color: #004085; }
  #symbol { font-size: 14px; color: #555; font-weight: 500; }

  /* ── Cards ── */
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 16px; }
  @media (max-width: 640px) { .grid { grid-template-columns: 1fr; } }
  .card {
    background: #fff; border-radius: 10px; padding: 20px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.08);
  }
  .card h3 { font-size: 13px; text-transform: uppercase; letter-spacing: 0.5px; color: #888; margin-bottom: 12px; }

  /* ── Position Card ── */
  .pos-dir { font-size: 28px; font-weight: 700; margin-bottom: 8px; }
  .pos-dir.sell { color: #dc3545; }
  .pos-dir.buy  { color: #28a745; }
  .pos-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 6px 16px; font-size: 14px; }
  .pos-grid .label { color: #888; }
  .pos-grid .value { font-weight: 600; text-align: right; }
  .no-position { color: #aaa; font-style: italic; padding: 20px 0; }

  /* ── P&L Card ── */
  .pnl-value { font-size: 32px; font-weight: 700; }
  .pnl-value.profit { color: #28a745; }
  .pnl-value.loss   { color: #dc3545; }
  .pnl-row { display: flex; justify-content: space-between; font-size: 14px; margin-top: 8px; color: #666; }

  /* ── Settings Card ── */
  .settings-grid {
    display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 12px;
    margin-bottom: 16px;
  }
  @media (max-width: 640px) { .settings-grid { grid-template-columns: 1fr 1fr; } }
  .form-group label { display: block; font-size: 12px; color: #888; margin-bottom: 4px; }
  .form-group input, .form-group select {
    width: 100%; padding: 8px 10px; border: 1px solid #ddd; border-radius: 6px;
    font-size: 14px; background: #fafafa;
  }
  .form-group input:focus, .form-group select:focus {
    outline: none; border-color: #387ed1; background: #fff;
  }
  .btn-row { display: flex; gap: 10px; margin-top: 4px; }
  .btn {
    padding: 10px 20px; border: none; border-radius: 6px;
    font-size: 14px; font-weight: 600; cursor: pointer; transition: opacity 0.15s;
  }
  .btn:hover { opacity: 0.85; }
  .btn-save  { background: #387ed1; color: #fff; }
  .btn-start { background: #28a745; color: #fff; }
  .btn-stop  { background: #dc3545; color: #fff; }

  /* ── Trade Log ── */
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th { text-align: left; padding: 8px 10px; border-bottom: 2px solid #eee; color: #888; font-weight: 600; }
  td { padding: 8px 10px; border-bottom: 1px solid #f0f0f0; }
  tr:last-child td { border-bottom: none; }
  .dir-sell { color: #dc3545; font-weight: 600; }
  .dir-buy  { color: #28a745; font-weight: 600; }
  .pnl-pos  { color: #28a745; font-weight: 600; }
  .pnl-neg  { color: #dc3545; font-weight: 600; }
  .empty-log { color: #aaa; font-style: italic; padding: 16px 0; text-align: center; }
</style>
</head>
<body>
<div class="container">

  <!-- Header -->
  <header>
    <h1>NIFTY SAR Trading</h1>
    <span id="status" class="badge stopped">STOPPED</span>
    <span id="symbol"></span>
    <span style="flex:1"></span>
    <a href="/">Auth Status</a>
  </header>

  <!-- Position + P&L -->
  <div class="grid">
    <div class="card">
      <h3>Active Position</h3>
      <div id="position"><div class="no-position">No active position</div></div>
    </div>
    <div class="card">
      <h3>P&amp;L</h3>
      <div id="pnl-total" class="pnl-value">&#8377;0.00</div>
      <div class="pnl-row"><span>Realized</span><span id="pnl-realized">&#8377;0.00</span></div>
      <div class="pnl-row"><span>Unrealized</span><span id="pnl-unrealized">&#8377;0.00</span></div>
    </div>
  </div>

  <!-- Settings -->
  <div class="card" style="margin-bottom:16px">
    <h3>Settings</h3>
    <div class="settings-grid">
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
        <label>Quantity (<span id="s-qty-label">lot size: --</span>)</label>
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
    <div class="btn-row">
      <button class="btn btn-save" onclick="saveSettings()">Save Settings</button>
    </div>
  </div>

  <!-- Engine Toggle -->
  <div style="text-align:center; margin-bottom:16px;">
    <button class="btn btn-start" id="toggle-btn" onclick="toggleEngine()" style="padding:14px 40px; font-size:16px;">Start Engine</button>
  </div>

  <!-- Trade Log -->
  <div class="card">
    <h3>Trade Log (Today)</h3>
    <div id="trades"><div class="empty-log">No trades yet today</div></div>
  </div>

  <!-- All-Time History -->
  <div class="card">
    <h3 style="display:flex; align-items:center; justify-content:space-between;">
      Trade History (All Time)
      <button class="btn btn-save" onclick="loadHistory()" style="padding:6px 14px; font-size:12px;">Load</button>
    </h3>
    <div id="history"><div class="empty-log">Click Load to view all-time trade history</div></div>
  </div>

</div>

<script>
let settingsLoaded = false;

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
    const qty = d.settings.quantity || 25;
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
    const qty = d.settings.quantity || 25;
    if (pos.direction === 'SELL') unrealizedVal = (pos.entry_price - d.current_ltp) * qty;
    else unrealizedVal = (d.current_ltp - pos.entry_price) * qty;
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
    settingsLoaded = true;
  }

  // Update lot size label and input constraints dynamically
  var lot = d.lot_size || 0;
  var qtyInput = document.getElementById('s-qty');
  var qtyLabel = document.getElementById('s-qty-label');
  if (lot > 0) {
    qtyLabel.textContent = 'lot size: ' + lot;
    qtyInput.min = lot;
    qtyInput.step = lot;
  } else {
    qtyLabel.textContent = 'lot size: --';
  }

  // Toggle button
  const btn = document.getElementById('toggle-btn');
  if (d.engine_status === 'STOPPED' || d.engine_status === 'MARKET_CLOSED') {
    btn.textContent = 'Start Engine';
    btn.className = 'btn btn-start';
  } else {
    btn.textContent = 'Stop Engine';
    btn.className = 'btn btn-stop';
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

function saveSettings() {
  const data = {
    start_time: document.getElementById('s-start').value,
    stop_time:  document.getElementById('s-stop').value,
    sl_points:  parseFloat(document.getElementById('s-sl').value),
    target_points: parseFloat(document.getElementById('s-tgt').value),
    target_premium: parseFloat(document.getElementById('s-premium').value),
    quantity:   parseInt(document.getElementById('s-qty').value, 10),
    product:    document.getElementById('s-product').value,
  };
  fetch('/api/settings', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(data),
  }).then(r => r.json()).then(function() {
    alert('Settings saved');
  }).catch(e => alert('Save failed: ' + e));
}

function toggleEngine() {
  const btn = document.getElementById('toggle-btn');
  const action = btn.textContent.startsWith('Start') ? 'start' : 'stop';
  fetch('/api/engine/toggle', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({action: action}),
  }).then(r => r.json()).then(function() {
    settingsLoaded = false;
    refresh();
  }).catch(e => alert('Toggle failed: ' + e));
}

// Poll every 2 seconds
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
      // Group by date for readability
      var html = '<table><thead><tr>' +
        '<th>Date</th><th>Symbol</th><th>Dir</th><th>Entry</th><th>Exit</th>' +
        '<th>Entry Time</th><th>Exit Time</th><th>Qty</th><th>P&amp;L</th>' +
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
