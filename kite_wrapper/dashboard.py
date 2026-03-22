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

  /* ── Signal Levels ── */
  .signal-row {
    display: grid; grid-template-columns: 1fr 1fr; gap: 4px 12px;
    font-size: 12px; margin-top: 6px; padding-top: 6px;
    border-top: 1px dashed #e0e0e0;
  }
  .signal-row .sig-label { color: #999; }
  .signal-row .sig-value { font-weight: 600; text-align: right; }
  .sig-up { color: #28a745; }
  .sig-down { color: #dc3545; }
  .sig-wait { color: #856404; }

  /* ── P&L Chart ── */
  .chart-card { padding: 16px 16px 12px; position: relative; }
  .chart-card h3 { margin-bottom: 8px; }
  #pnl-chart { width: 100%; height: 180px; display: block; cursor: crosshair; }
  .chart-tooltip {
    position: absolute; pointer-events: none; display: none;
    background: rgba(26,26,46,0.9); color: #fff; padding: 6px 10px;
    border-radius: 6px; font-size: 12px; white-space: nowrap;
    line-height: 1.4; z-index: 10;
  }

  /* ── Mobile ── */
  @media (max-width: 640px) {
    .container { padding: 12px; }
    .grid-2 { grid-template-columns: 1fr; }
    .settings-grid { grid-template-columns: 1fr 1fr; }
    .strat-info .info-grid { grid-template-columns: 120px 1fr; }
    header h1 { font-size: 18px; }
    .pnl-value { font-size: 28px; }
    .pos-dir { font-size: 24px; }
    #pnl-chart { height: 140px; }
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
    <a href="/backtest" class="auth-link" style="margin-right:4px">Backtest</a>
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

  <!-- P&L Chart -->
  <div class="card chart-card">
    <h3>P&amp;L Today</h3>
    <canvas id="pnl-chart"></canvas>
    <div class="chart-tooltip" id="chart-tooltip"></div>
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
      <div class="form-group">
        <label>Market Bias</label>
        <select id="s-bias">
          <option value="BULLISH">Bullish</option>
          <option value="BEARISH">Bearish</option>
        </select>
      </div>
      <div class="form-group">
        <label>Min Premium</label>
        <input type="number" id="s-minprem" value="100" step="10" min="0">
      </div>
      <div class="form-group">
        <label>Expiry Type</label>
        <select id="s-expiry">
          <option value="weekly">Weekly</option>
          <option value="monthly">Monthly</option>
        </select>
      </div>
      <div class="form-group">
        <label style="display:flex;align-items:center;gap:5px;">
          <input type="checkbox" id="s-cutoff"> Daily Cutoff
        </label>
      </div>
      <div class="form-group">
        <label>Profit %</label>
        <input type="number" id="s-profit-pct" value="25" step="1" min="0">
        <span id="s-profit-val" style="font-size:11px;color:#888;"></span>
      </div>
      <div class="form-group">
        <label>Loss %</label>
        <input type="number" id="s-loss-pct" value="25" step="1" min="0">
        <span id="s-loss-val" style="font-size:11px;color:#888;"></span>
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
const STRATEGY_LABELS = {sar: 'Buy/Sell CE Alternate', buy_ce: 'Buy CE', buy_pe: 'Buy PE', buy_ce_pe_alternate_candle_close: 'Buy CE/PE Alternate (Candle Close)', buy_ce_pe_scale_out_candle_close: 'Buy CE/PE Scale Out (Candle Close)', supertrend_candle_close: 'Buy CE/PE Supertrend (Candle Close)'};

var _dashLotSize = 0;
function updateCutoffValues() {
  var qty = parseInt(document.getElementById('s-qty').value, 10) || 1;
  var prem = parseFloat(document.getElementById('s-minprem').value) || 100;
  var notional = qty * _dashLotSize * prem;
  var pp = parseFloat(document.getElementById('s-profit-pct').value) || 0;
  var lp = parseFloat(document.getElementById('s-loss-pct').value) || 0;
  document.getElementById('s-profit-val').textContent = _dashLotSize ? '= \\u20B9' + Math.round(notional * pp / 100) : '';
  document.getElementById('s-loss-val').textContent = _dashLotSize ? '= \\u20B9' + Math.round(notional * lp / 100) : '';
}
document.getElementById('s-profit-pct').addEventListener('input', updateCutoffValues);
document.getElementById('s-loss-pct').addEventListener('input', updateCutoffValues);
document.getElementById('s-qty').addEventListener('input', updateCutoffValues);
document.getElementById('s-minprem').addEventListener('input', updateCutoffValues);

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

  // Dual-position slots (signal-based strategies)
  const slots = d.position_slots || {};
  const hasSlots = Object.keys(slots).length > 0;

  // Symbol
  var symText = d.trading_symbol || '';
  if (hasSlots && !symText) {
    var symParts = [];
    ['CE', 'PE'].forEach(function(opt) {
      if (slots[opt] && slots[opt].trading_symbol) symParts.push(slots[opt].trading_symbol);
    });
    symText = symParts.join(' | ');
  }
  document.getElementById('symbol').textContent = symText;

  // Position
  const posEl = document.getElementById('position');
  const pos = d.active_position;

  if (hasSlots) {
    // Dual-position display for signal-based strategies
    var slotHtml = '';
    var totalUnrealized = 0;
    var sd = d.strategy_data || {};
    var barLabel = '5min';
    ['CE', 'PE'].forEach(function(opt) {
      var slot = slots[opt];
      if (!slot) return;
      var sp = slot.active_position;
      var slotLtp = slot.current_ltp || 0;

      // Live signal levels from strategy_data
      var stVal = sd['_st_value_' + opt];
      var stClose = sd['_st_close_' + opt];
      var stTrend = sd['_st_trend_' + opt];
      var stReentry = sd['_st_reentry_' + opt];
      var isWaiting = sd['_waiting_' + opt];
      var inBuyTrend = sd['_in_buy_trend_' + opt];

      if (sp && sp.direction) {
        var sQty = (sp.remaining_lots && slot.lot_size) ? sp.remaining_lots * slot.lot_size : ((d.settings.quantity || 1) * (slot.lot_size || 1));
        var sUnreal = (sp.direction === 'BUY') ? (slotLtp - sp.entry_price) * sQty : (sp.entry_price - slotLtp) * sQty;
        totalUnrealized += sUnreal;
        slotHtml +=
          '<div style="margin-bottom:8px;padding:6px;background:#f8f9fa;border-radius:6px;">' +
          '<div style="font-weight:700;font-size:14px;color:#28a745;">' + opt + ' ' + sp.direction + ' <span style="font-size:11px;color:#888;">' + (slot.trading_symbol || '') + '</span></div>' +
          '<div class="pos-grid" style="font-size:13px;">' +
            '<span class="label">Entry</span><span class="value">' + sp.entry_price.toFixed(2) + '</span>' +
            '<span class="label">LTP</span><span class="value">' + slotLtp.toFixed(2) + '</span>' +
            '<span class="label">SL</span><span class="value">' + sp.sl_price.toFixed(2) + '</span>' +
            '<span class="label">Target</span><span class="value">' + sp.target_price.toFixed(2) + '</span>' +
            '<span class="label">Lots</span><span class="value">' + (sp.remaining_lots || '-') + '</span>' +
            '<span class="label">P&L</span><span class="value ' + (sUnreal >= 0 ? 'pnl-pos' : 'pnl-neg') + '">' + (sUnreal >= 0 ? '+' : '') + sUnreal.toFixed(2) + '</span>' +
          '</div>';
        // Show signal levels below active position
        if (stVal != null) {
          slotHtml += '<div class="signal-row">' +
            '<span class="sig-label">Supertrend</span><span class="sig-value">' + stVal.toFixed(2) + '</span>' +
            '<span class="sig-label">Trend</span><span class="sig-value ' + (stTrend === 'UP' ? 'sig-up' : 'sig-down') + '">' + (stTrend || '-') + '</span>' +
          '</div>';
        }
        slotHtml += '</div>';
      } else {
        // No active position — show symbol + plain-text entry condition
        var sym = slot.trading_symbol || '-';
        var condText = '';

        if (stVal != null) {
          var price = stClose != null ? stClose.toFixed(2) : (slotLtp > 0 ? slotLtp.toFixed(2) : '-');
          if (stTrend === 'DOWN') {
            condText = 'Price: ' + price + '. Will enter if ' + barLabel + ' close crosses above ' + stVal.toFixed(2);
          } else if (stReentry > 0 && stClose < stReentry) {
            condText = 'Price: ' + price + '. Will enter if ' + barLabel + ' close crosses above ' + stReentry.toFixed(2);
          } else {
            condText = 'Price: ' + price + '. Entry signal pending';
          }
        } else {
          condText = 'Waiting for supertrend data...';
        }

        slotHtml += '<div style="margin-bottom:8px;padding:8px;background:#f8f9fa;border-radius:6px;">' +
          '<div style="font-weight:700;font-size:14px;color:#387ed1;">' + sym + ' <span style="font-size:12px;color:#888;">(' + opt + ')</span></div>' +
          '<div style="font-size:13px;color:#555;margin-top:4px;">' + condText + '</div>' +
          '</div>';
      }
    });
    posEl.innerHTML = slotHtml || '<div class="no-position">Initializing...</div>';
    document.getElementById('pnl-unrealized').textContent =
      (totalUnrealized >= 0 ? '+' : '') + '\u20B9' + totalUnrealized.toFixed(2);
  } else if (pos && pos.direction) {
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
  if (hasSlots) {
    ['CE', 'PE'].forEach(function(opt) {
      var slot = slots[opt];
      if (!slot) return;
      var sp = slot.active_position;
      var slotLtp = slot.current_ltp || 0;
      if (sp && sp.direction && slotLtp) {
        var sQty = (sp.remaining_lots && slot.lot_size) ? sp.remaining_lots * slot.lot_size : ((d.settings.quantity || 1) * (slot.lot_size || 1));
        if (sp.direction === 'BUY') unrealizedVal += (slotLtp - sp.entry_price) * sQty;
        else unrealizedVal += (sp.entry_price - slotLtp) * sQty;
      }
    });
  } else if (pos && pos.direction && d.current_ltp) {
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
    document.getElementById('s-qty').value = d.settings.quantity || 0;
    document.getElementById('s-product').value = d.settings.product || 'MIS';
    document.getElementById('s-bias').value = d.settings.market_bias || 'AUTO';
    document.getElementById('s-minprem').value = d.settings.min_premium || 100;
    document.getElementById('s-expiry').value = d.settings.expiry_type || 'weekly';
    document.getElementById('s-cutoff').checked = !!d.settings.daily_cutoff;
    document.getElementById('s-profit-pct').value = d.settings.daily_profit_pct || 25;
    document.getElementById('s-loss-pct').value = d.settings.daily_loss_pct || 25;
    document.getElementById('s-strategy').value = d.strategy_name || 'sar';
    settingsLoaded = true;
  }

  // Strategy + bias dropdown: disable while running, update title
  var stratSel = document.getElementById('s-strategy');
  var running = d.engine_status !== 'STOPPED' && d.engine_status !== 'MARKET_CLOSED';
  stratSel.disabled = running;
  document.getElementById('s-bias').disabled = running;
  var stratName = STRATEGY_LABELS[d.strategy_name] || d.strategy_name || 'SAR';
  document.getElementById('title').textContent = stratName;

  // Update lot size label and input constraints dynamically
  var lot = d.lot_size || 0;
  _dashLotSize = lot;
  var lotMult = (d.strategy_name === 'buy_ce_pe_scale_out_candle_close') ? 3 : 1;
  var qtyInput = document.getElementById('s-qty');
  var qtyLabel = document.getElementById('s-qty-label');
  if (lot > 0) {
    qtyLabel.textContent = 'lot size: ' + lot + (lotMult > 1 ? ' (min ' + lotMult + ' lots)' : '');
    qtyInput.min = lotMult;
    qtyInput.step = lotMult;
  } else {
    qtyLabel.textContent = 'lot size: --';
  }
  updateCutoffValues();

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

  // P&L chart
  drawPnlChart(trades);
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
  var lotMult = (newStrategy === 'buy_ce_pe_scale_out_candle_close') ? 3 : 1;
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
      quantity:   parseInt(document.getElementById('s-qty').value, 10),
      product:    document.getElementById('s-product').value,
      market_bias: document.getElementById('s-bias').value,
      min_premium: parseFloat(document.getElementById('s-minprem').value),
      expiry_type: document.getElementById('s-expiry').value,
      daily_cutoff: document.getElementById('s-cutoff').checked,
      daily_profit_pct: parseFloat(document.getElementById('s-profit-pct').value) || 25,
      daily_loss_pct: parseFloat(document.getElementById('s-loss-pct').value) || 25,
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

var _chartDots = []; // [{x, y, pnl, tradePnl, time, symbol}]

function drawPnlChart(trades) {
  var canvas = document.getElementById('pnl-chart');
  var ctx = canvas.getContext('2d');
  var dpr = window.devicePixelRatio || 1;
  var rect = canvas.getBoundingClientRect();
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  ctx.scale(dpr, dpr);
  var W = rect.width, H = rect.height;

  ctx.clearRect(0, 0, W, H);
  _chartDots = [];

  // Compute cumulative P&L points: start at 0, then after each trade
  var points = [{time: '', pnl: 0, tradePnl: 0, symbol: ''}];
  var cumPnl = 0;
  if (trades && trades.length > 0) {
    trades.forEach(function(t) {
      cumPnl += (t.pnl || 0);
      points.push({time: t.exit_time || '', pnl: cumPnl, tradePnl: t.pnl || 0, symbol: t.symbol || ''});
    });
  }

  if (points.length < 2) {
    ctx.fillStyle = '#aaa';
    ctx.font = '13px -apple-system, sans-serif';
    ctx.textAlign = 'center';
    ctx.fillText('No trades yet', W / 2, H / 2);
    return;
  }

  // Chart area with padding
  var padL = 60, padR = 16, padT = 16, padB = 28;
  var cW = W - padL - padR;
  var cH = H - padT - padB;

  var minP = 0, maxP = 0;
  points.forEach(function(p) {
    if (p.pnl < minP) minP = p.pnl;
    if (p.pnl > maxP) maxP = p.pnl;
  });
  // Add 10% padding to Y range, ensure non-zero range
  var range = maxP - minP;
  if (range < 100) range = 100;
  minP = minP - range * 0.1;
  maxP = maxP + range * 0.1;
  range = maxP - minP;

  function xPos(i) { return padL + (i / (points.length - 1)) * cW; }
  function yPos(v) { return padT + (1 - (v - minP) / range) * cH; }

  // Background
  ctx.fillStyle = '#fafbfc';
  ctx.fillRect(padL, padT, cW, cH);

  // Grid lines
  ctx.strokeStyle = '#e8e8e8';
  ctx.lineWidth = 0.5;
  var gridLines = 4;
  for (var g = 0; g <= gridLines; g++) {
    var gVal = minP + (range * g / gridLines);
    var gy = yPos(gVal);
    ctx.beginPath(); ctx.moveTo(padL, gy); ctx.lineTo(W - padR, gy); ctx.stroke();
    // Y labels
    ctx.fillStyle = '#999';
    ctx.font = '10px -apple-system, sans-serif';
    ctx.textAlign = 'right';
    ctx.fillText((gVal >= 0 ? '+' : '') + Math.round(gVal).toLocaleString(), padL - 6, gy + 3);
  }

  // Zero line
  if (minP < 0 && maxP > 0) {
    var zy = yPos(0);
    ctx.strokeStyle = '#ccc';
    ctx.lineWidth = 1;
    ctx.setLineDash([4, 3]);
    ctx.beginPath(); ctx.moveTo(padL, zy); ctx.lineTo(W - padR, zy); ctx.stroke();
    ctx.setLineDash([]);
  }

  // Fill area under/above zero
  ctx.beginPath();
  ctx.moveTo(xPos(0), yPos(0));
  for (var i = 0; i < points.length; i++) {
    ctx.lineTo(xPos(i), yPos(points[i].pnl));
  }
  ctx.lineTo(xPos(points.length - 1), yPos(0));
  ctx.closePath();
  var lastPnl = points[points.length - 1].pnl;
  ctx.fillStyle = lastPnl >= 0 ? 'rgba(40,167,69,0.1)' : 'rgba(220,53,69,0.1)';
  ctx.fill();

  // Line
  ctx.strokeStyle = lastPnl >= 0 ? '#28a745' : '#dc3545';
  ctx.lineWidth = 2;
  ctx.lineJoin = 'round';
  ctx.beginPath();
  for (var i = 0; i < points.length; i++) {
    var px = xPos(i), py = yPos(points[i].pnl);
    if (i === 0) ctx.moveTo(px, py); else ctx.lineTo(px, py);
  }
  ctx.stroke();

  // Dots + store positions for hover
  for (var i = 1; i < points.length; i++) {
    var px = xPos(i), py = yPos(points[i].pnl);
    ctx.beginPath();
    ctx.arc(px, py, 3, 0, Math.PI * 2);
    ctx.fillStyle = points[i].pnl >= 0 ? '#28a745' : '#dc3545';
    ctx.fill();
    _chartDots.push({x: px, y: py, pnl: points[i].pnl, tradePnl: points[i].tradePnl, time: points[i].time, symbol: points[i].symbol});
  }

  // X labels (show a few time labels)
  ctx.fillStyle = '#999';
  ctx.font = '10px -apple-system, sans-serif';
  ctx.textAlign = 'center';
  var step = Math.max(1, Math.floor(points.length / 6));
  for (var i = 1; i < points.length; i += step) {
    var label = points[i].time || '';
    if (label.length > 5) label = label.substring(0, 5);
    ctx.fillText(label, xPos(i), H - 6);
  }
  // Always show last label
  if ((points.length - 1) % step !== 0 && points.length > 2) {
    var ll = points[points.length - 1].time || '';
    if (ll.length > 5) ll = ll.substring(0, 5);
    ctx.fillText(ll, xPos(points.length - 1), H - 6);
  }

  // Current value label at end of line
  var endX = xPos(points.length - 1), endY = yPos(lastPnl);
  ctx.fillStyle = lastPnl >= 0 ? '#28a745' : '#dc3545';
  ctx.font = 'bold 11px -apple-system, sans-serif';
  ctx.textAlign = 'left';
  var valText = (lastPnl >= 0 ? '+' : '') + Math.round(lastPnl).toLocaleString();
  if (endX + ctx.measureText(valText).width + 8 > W - padR) {
    ctx.textAlign = 'right';
    ctx.fillText(valText, endX - 8, endY - 8);
  } else {
    ctx.fillText(valText, endX + 8, endY - 8);
  }
}

// Chart hover tooltip
(function() {
  var canvas = document.getElementById('pnl-chart');
  var tooltip = document.getElementById('chart-tooltip');

  canvas.addEventListener('mousemove', function(e) {
    if (!_chartDots.length) { tooltip.style.display = 'none'; return; }
    var rect = canvas.getBoundingClientRect();
    var mx = e.clientX - rect.left;
    var my = e.clientY - rect.top;

    // Find nearest dot within 15px
    var best = null, bestDist = 225; // 15^2
    for (var i = 0; i < _chartDots.length; i++) {
      var d = _chartDots[i];
      var dist = (mx - d.x) * (mx - d.x) + (my - d.y) * (my - d.y);
      if (dist < bestDist) { bestDist = dist; best = d; }
    }

    if (!best) { tooltip.style.display = 'none'; return; }

    var sign = best.tradePnl >= 0 ? '+' : '';
    var cumSign = best.pnl >= 0 ? '+' : '';
    var html = '<div style="font-weight:600;">' + (best.time || '-') + '</div>';
    if (best.symbol) html += '<div style="color:#aaa;font-size:11px;">' + best.symbol + '</div>';
    html += '<div>Trade: <span style="color:' + (best.tradePnl >= 0 ? '#6fcf97' : '#ff6b6b') + '">' + sign + best.tradePnl.toFixed(2) + '</span></div>';
    html += '<div>Total: <span style="color:' + (best.pnl >= 0 ? '#6fcf97' : '#ff6b6b') + '">' + cumSign + best.pnl.toFixed(2) + '</span></div>';
    tooltip.innerHTML = html;
    tooltip.style.display = 'block';

    // Position tooltip near cursor, keep within card
    var cardRect = canvas.parentElement.getBoundingClientRect();
    var tx = e.clientX - cardRect.left + 12;
    var ty = e.clientY - cardRect.top - 10;
    if (tx + tooltip.offsetWidth > cardRect.width - 8) tx = tx - tooltip.offsetWidth - 24;
    if (ty < 0) ty = 0;
    tooltip.style.left = tx + 'px';
    tooltip.style.top = ty + 'px';
  });

  canvas.addEventListener('mouseleave', function() {
    tooltip.style.display = 'none';
  });
})();

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
    if "quantity" in data:
        data["quantity"] = int(data["quantity"])
    if "min_premium" in data:
        data["min_premium"] = float(data["min_premium"])
    if "daily_profit_pct" in data:
        data["daily_profit_pct"] = float(data["daily_profit_pct"])
    if "daily_loss_pct" in data:
        data["daily_loss_pct"] = float(data["daily_loss_pct"])
    if "daily_cutoff" in data:
        data["daily_cutoff"] = bool(data["daily_cutoff"])

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
