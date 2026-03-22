"""Backtest Dashboard Blueprint — run strategy simulations on historical data."""

from calendar import monthrange

from flask import Blueprint, jsonify, request, render_template_string

from .backtest import BacktestEngine
from .base_strategy import STRATEGY_REGISTRY
from .candle_store import (
    has_nifty_day, has_nifty_minute, has_option_minute,
    save_nifty_day, save_nifty_minute, save_option_minute,
    load_nifty_day, list_cached_dates,
)

backtest_bp = Blueprint("backtest", __name__)

_client = None


def init_backtest(client):
    """Wire the KiteClient into the backtest routes."""
    global _client
    _client = client


# ─── HTML ──────────────────────────────────────────────────────────────────────

BACKTEST_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Backtest</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: #f0f2f5; color: #1a1a2e; line-height: 1.5;
  }
  .container { max-width: 960px; margin: 0 auto; padding: 20px; }

  /* Header */
  header {
    display: flex; align-items: center; gap: 10px;
    margin-bottom: 16px; flex-wrap: wrap;
  }
  header h1 { font-size: 20px; font-weight: 700; }
  .header-spacer { flex: 1; }
  .nav-link {
    font-size: 13px; color: #387ed1; text-decoration: none; font-weight: 500;
  }
  .nav-link:hover { text-decoration: underline; }

  /* Cards */
  .card {
    background: #fff; border-radius: 10px; padding: 20px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.08); margin-bottom: 16px;
  }
  .card h3 {
    font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px;
    color: #888; margin-bottom: 12px;
  }

  /* Form */
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

  /* Buttons */
  .btn {
    padding: 10px 20px; border: none; border-radius: 6px;
    font-size: 14px; font-weight: 600; cursor: pointer; transition: opacity 0.15s;
    min-height: 44px;
  }
  .btn:hover { opacity: 0.85; }
  .btn:disabled { opacity: 0.5; cursor: not-allowed; }
  .btn-primary { background: #387ed1; color: #fff; }
  .btn-row { display: flex; gap: 10px; margin-top: 16px; align-items: center; }

  /* Summary */
  .summary-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; }
  .stat { text-align: center; }
  .stat .value { font-size: 28px; font-weight: 700; }
  .stat .label { font-size: 12px; color: #888; text-transform: uppercase; }
  .profit { color: #28a745; }
  .loss { color: #dc3545; }

  /* Tables */
  .table-wrap { overflow-x: auto; -webkit-overflow-scrolling: touch; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th { text-align: left; padding: 8px 10px; border-bottom: 2px solid #eee; color: #888; font-weight: 600; white-space: nowrap; }
  td { padding: 8px 10px; border-bottom: 1px solid #f0f0f0; white-space: nowrap; }
  tr:last-child td { border-bottom: none; }
  .dir-sell { color: #dc3545; font-weight: 600; }
  .dir-buy  { color: #28a745; font-weight: 600; }
  .pnl-pos  { color: #28a745; font-weight: 600; }
  .pnl-neg  { color: #dc3545; font-weight: 600; }
  .empty-msg { color: #aaa; font-style: italic; padding: 16px 0; text-align: center; }

  /* Error */
  .error-box {
    background: #f8d7da; color: #721c24; padding: 12px 16px;
    border-radius: 8px; font-size: 14px; margin-bottom: 16px;
  }

  /* Spinner */
  .spinner { display: none; }
  .spinner.show { display: inline-block; }
  .spinner::after {
    content: ''; display: inline-block; width: 14px; height: 14px;
    border: 2px solid #fff; border-top-color: transparent;
    border-radius: 50%; animation: spin 0.6s linear infinite;
    vertical-align: middle; margin-left: 6px;
  }
  @keyframes spin { to { transform: rotate(360deg); } }

  /* Compare table */
  .compare-table { width: 100%; border-collapse: collapse; font-size: 14px; }
  .compare-table th { text-align: left; padding: 10px 12px; border-bottom: 2px solid #eee; color: #888; font-weight: 600; }
  .compare-table td { padding: 10px 12px; border-bottom: 1px solid #f0f0f0; }
  .compare-table tr:last-child td { border-bottom: none; }
  .compare-table .best { background: #d4edda; }
  .compare-table .strat-name { font-weight: 600; }
  .btn-outline {
    background: #fff; color: #387ed1; border: 2px solid #387ed1;
  }

  /* Collapsible toggle */
  .coll-toggle {
    cursor: pointer; user-select: none; font-size: 12px; text-transform: uppercase;
    letter-spacing: 0.5px; color: #888; font-weight: 600;
  }
  .coll-toggle::before {
    content: '\\25B6'; margin-right: 6px; font-size: 10px;
    display: inline-block; transition: transform 0.2s;
  }
  .coll-toggle.open::before { transform: rotate(90deg); }

  /* Mobile */
  @media (max-width: 640px) {
    .container { padding: 12px; }
    .settings-grid { grid-template-columns: 1fr 1fr; }
    .summary-grid { grid-template-columns: 1fr 1fr; }
    .stat .value { font-size: 22px; }
  }
</style>
</head>
<body>
<div class="container">

  <header>
    <h1>Backtest</h1>
    <span class="header-spacer"></span>
    <a href="/dashboard" class="nav-link">Dashboard</a>
  </header>

  <!-- Input Card -->
  <div class="card">
    <h3>Configuration</h3>
    <div class="settings-grid">
      <div class="form-group">
        <label>Date</label>
        <input type="date" id="f-date">
      </div>
      <div class="form-group">
        <label>Strategy</label>
        <select id="f-strategy">
          <option value="sar">Buy/Sell CE Alternate</option>
          <option value="buy_ce_pe_alternate_candle_close">Buy CE/PE Alternate (Candle Close)</option>
          <option value="buy_ce_pe_scale_out_candle_close">Buy CE/PE Scale Out (Candle Close)</option>
          <option value="supertrend_candle_close">Buy CE/PE Supertrend (Candle Close)</option>
        </select>
      </div>
      <div class="form-group">
        <label>SL Points</label>
        <input type="number" id="f-sl" value="10" step="0.5" min="0.5">
      </div>
      <div class="form-group">
        <label>Target Points</label>
        <input type="number" id="f-tgt" value="10" step="0.5" min="0.5">
      </div>
      <div class="form-group">
        <label>Lots</label>
        <input type="number" id="f-qty" value="1" min="1" step="1">
        <span id="f-qty-hint" style="font-size:11px;color:#888;"></span>
      </div>
      <div class="form-group">
        <label>Market Bias</label>
        <select id="f-bias">
          <option value="BULLISH">Bullish</option>
          <option value="BEARISH">Bearish</option>
        </select>
      </div>
      <div class="form-group">
        <label>Min Premium</label>
        <input type="number" id="f-minprem" value="100" step="10" min="0">
      </div>
      <div class="form-group">
        <label>Expiry Type</label>
        <select id="f-expiry">
          <option value="weekly">Weekly</option>
          <option value="monthly">Monthly</option>
        </select>
      </div>
      <div class="form-group">
        <label>Start Time</label>
        <input type="time" id="f-start" value="09:20">
      </div>
      <div class="form-group">
        <label>Stop Time</label>
        <input type="time" id="f-stop" value="15:15">
      </div>
      <div class="form-group">
        <label style="display:flex;align-items:center;gap:5px;">
          <input type="checkbox" id="f-cutoff-enabled"> Daily Cutoff
        </label>
        <span id="f-lot-size-info" style="font-size:11px;color:#888;font-weight:600;"></span>
      </div>
      <div class="form-group">
        <label>Profit %</label>
        <input type="number" id="f-profit-pct" value="25" step="1" min="0">
        <span id="f-profit-val" style="font-size:11px;color:#888;"></span>
      </div>
      <div class="form-group">
        <label>Loss %</label>
        <input type="number" id="f-loss-pct" value="25" step="1" min="0">
        <span id="f-loss-val" style="font-size:11px;color:#888;"></span>
      </div>
    </div>
    <div class="btn-row">
      <button class="btn btn-primary" id="run-btn" onclick="runBacktest()">
        Run Backtest<span class="spinner" id="spinner"></span>
      </button>
      <button class="btn btn-outline" id="compare-btn" onclick="runCompareAll()">
        Compare All<span class="spinner" id="compare-spinner"></span>
      </button>
      <span id="status-msg" style="font-size:13px; color:#888;"></span>
    </div>
    <div id="compare-strategies" style="margin-top:10px; display:flex; flex-wrap:wrap; gap:8px 16px; font-size:13px;">
      <label><input type="checkbox" value="sar" checked> SAR (Buy/Sell CE)</label>
      <label><input type="checkbox" value="buy_ce_pe_alternate_candle_close" checked> Buy CE/PE Alternate (Candle Close)</label>
      <label><input type="checkbox" value="buy_ce_pe_scale_out_candle_close" checked> Buy CE/PE Scale Out (Candle Close)</label>
      <label><input type="checkbox" value="supertrend_candle_close" checked> Supertrend (Candle Close)</label>
    </div>
  </div>

  <!-- Historical Data Download -->
  <div class="card">
    <h3>Historical Data</h3>
    <div class="settings-grid">
      <div class="form-group">
        <label>Start Date</label>
        <input type="date" id="dl-start">
      </div>
      <div class="form-group">
        <label>End Date</label>
        <input type="date" id="dl-end">
      </div>
      <div class="form-group">
        <label>Strikes Each Side</label>
        <input type="number" id="dl-strikes" value="20" min="1" max="40" step="1">
      </div>
      <div class="form-group">
        <label>Expiry Type</label>
        <select id="dl-expiry">
          <option value="weekly">Weekly</option>
          <option value="monthly">Monthly</option>
        </select>
      </div>
    </div>
    <div class="btn-row">
      <button class="btn btn-primary" id="dl-btn" onclick="startDownload()">
        Download Data<span class="spinner" id="dl-spinner"></span>
      </button>
      <span id="dl-status" style="font-size:13px; color:#888;"></span>
    </div>
    <div id="dl-summary" style="margin-top:10px; font-size:13px; color:#888;"></div>
  </div>

  <!-- Error -->
  <div class="error-box" id="error-box" style="display:none;"></div>

  <!-- Summary -->
  <div class="card" id="summary-card" style="display:none;">
    <h3>Summary</h3>
    <div class="summary-grid">
      <div class="stat">
        <div class="value" id="s-pnl">0.00</div>
        <div class="label">Total P&amp;L</div>
      </div>
      <div class="stat">
        <div class="value" id="s-trades">0</div>
        <div class="label">Trades</div>
      </div>
      <div class="stat">
        <div class="value profit" id="s-wins">0</div>
        <div class="label">Wins</div>
      </div>
      <div class="stat">
        <div class="value loss" id="s-losses">0</div>
        <div class="label">Losses</div>
      </div>
    </div>
  </div>

  <!-- Trades Table -->
  <div class="card" id="trades-card" style="display:none;">
    <h3>Trades</h3>
    <div class="table-wrap" id="trades-table"></div>
  </div>

  <!-- Compare All: Summary -->
  <div class="card" id="compare-card" style="display:none;">
    <h3>Strategy Comparison</h3>
    <div class="table-wrap" id="compare-table"></div>
  </div>

  <!-- Compare All: Per-strategy trades -->
  <div id="compare-details"></div>

</div>

<script>
// Default date to today
var todayStr = new Date().toISOString().slice(0, 10);
document.getElementById('f-date').value = todayStr;
document.getElementById('dl-start').value = todayStr.slice(0, 8) + '01';
document.getElementById('dl-end').value = todayStr;

// Load cached data summary on page load
(function loadCachedSummary() {
  fetch('/api/backtest/download/list')
    .then(function(r) { return r.json(); })
    .then(function(data) {
      var dates = data.dates || [];
      var el = document.getElementById('dl-summary');
      if (dates.length === 0) {
        el.textContent = 'No cached data found.';
        return;
      }
      var total_opts = 0;
      for (var d in (data.detail || {})) {
        total_opts += (data.detail[d].options || 0);
      }
      el.textContent = 'Cached: ' + dates.length + ' days (' +
        dates[0] + ' to ' + dates[dates.length - 1] + '), ' +
        total_opts + ' option files';
    })
    .catch(function() {});
})();

function startDownload() {
  var btn = document.getElementById('dl-btn');
  var spinner = document.getElementById('dl-spinner');
  var statusEl = document.getElementById('dl-status');

  btn.disabled = true;
  spinner.classList.add('show');
  statusEl.textContent = 'Starting...';

  var payload = {
    start_date: document.getElementById('dl-start').value,
    end_date: document.getElementById('dl-end').value,
    strikes_each_side: parseInt(document.getElementById('dl-strikes').value, 10) || 20,
    expiry_type: document.getElementById('dl-expiry').value,
  };

  fetch('/api/backtest/download', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload),
  })
  .then(function(r) { return r.json(); })
  .then(function(data) {
    if (data.error) {
      btn.disabled = false;
      spinner.classList.remove('show');
      statusEl.textContent = 'Error: ' + data.error;
      return;
    }
    pollDownload(data.job_id);
  })
  .catch(function(e) {
    btn.disabled = false;
    spinner.classList.remove('show');
    statusEl.textContent = 'Failed: ' + e;
  });
}

function pollDownload(jobId) {
  var btn = document.getElementById('dl-btn');
  var spinner = document.getElementById('dl-spinner');
  var statusEl = document.getElementById('dl-status');

  var poll = setInterval(function() {
    fetch('/api/backtest/download/status?job_id=' + jobId)
      .then(function(r) { return r.json(); })
      .then(function(data) {
        if (data.status === 'running') {
          statusEl.textContent = data.progress || 'Downloading...';
          return;
        }
        clearInterval(poll);
        btn.disabled = false;
        spinner.classList.remove('show');

        if (data.status === 'error') {
          statusEl.textContent = 'Error: ' + (data.error || 'Unknown');
          return;
        }
        statusEl.textContent = 'Done! ' + (data.summary || '');
        // Refresh cached summary
        loadCachedSummary();
      })
      .catch(function(e) {
        clearInterval(poll);
        btn.disabled = false;
        spinner.classList.remove('show');
        statusEl.textContent = 'Poll failed: ' + e;
      });
  }, 2000);
}

// Expose for re-use after download completes
function loadCachedSummary() {
  fetch('/api/backtest/download/list')
    .then(function(r) { return r.json(); })
    .then(function(data) {
      var dates = data.dates || [];
      var el = document.getElementById('dl-summary');
      if (dates.length === 0) {
        el.textContent = 'No cached data found.';
        return;
      }
      var total_opts = 0;
      for (var d in (data.detail || {})) {
        total_opts += (data.detail[d].options || 0);
      }
      el.textContent = 'Cached: ' + dates.length + ' days (' +
        dates[0] + ' to ' + dates[dates.length - 1] + '), ' +
        total_opts + ' option files';
    })
    .catch(function() {});
}

var LOT_MULTIPLIERS = {sar: 1, buy_ce_pe_alternate_candle_close: 1, buy_ce_pe_scale_out_candle_close: 3, supertrend_candle_close: 1};
var NIFTY_LOT_SIZE = 75;

// Default backtest fields from live dashboard settings
fetch('/api/dashboard').then(function(r){return r.json();}).then(function(d){
  if (d.lot_size) {
    NIFTY_LOT_SIZE = d.lot_size;
    document.getElementById('f-lot-size-info').textContent = 'Lot size: ' + NIFTY_LOT_SIZE;
  }
  if (d.strategy_name) document.getElementById('f-strategy').value = d.strategy_name;
  if (d.settings) {
    var s = d.settings;
    if (s.sl_points) document.getElementById('f-sl').value = s.sl_points;
    if (s.target_points) document.getElementById('f-tgt').value = s.target_points;
    if (s.quantity) document.getElementById('f-qty').value = s.quantity;
    if (s.market_bias) document.getElementById('f-bias').value = s.market_bias;
    if (s.min_premium) document.getElementById('f-minprem').value = s.min_premium;
    if (s.start_time) document.getElementById('f-start').value = s.start_time;
    if (s.stop_time) document.getElementById('f-stop').value = s.stop_time;
    if (s.expiry_type) document.getElementById('f-expiry').value = s.expiry_type;
    document.getElementById('f-cutoff-enabled').checked = !!s.daily_cutoff;
    if (s.daily_profit_pct) document.getElementById('f-profit-pct').value = s.daily_profit_pct;
    if (s.daily_loss_pct) document.getElementById('f-loss-pct').value = s.daily_loss_pct;
    enforceLots();
  }
  updateCutoffValues();
}).catch(function(){
  // Fallback: try dedicated endpoint
  fetch('/api/backtest/lot-size').then(function(r){return r.json();}).then(function(d){
    NIFTY_LOT_SIZE = d.lot_size || 75;
    document.getElementById('f-lot-size-info').textContent = 'Lot size: ' + NIFTY_LOT_SIZE;
    updateCutoffValues();
  }).catch(function(){});
});

function enforceLots() {
  var strat = document.getElementById('f-strategy').value;
  var mult = LOT_MULTIPLIERS[strat] || 1;
  var qtyInput = document.getElementById('f-qty');
  var hint = document.getElementById('f-qty-hint');
  qtyInput.min = mult;
  qtyInput.step = mult;
  var val = parseInt(qtyInput.value, 10) || 1;
  if (val < mult || val % mult !== 0) {
    qtyInput.value = Math.max(Math.round(val / mult), 1) * mult;
  }
  hint.textContent = mult > 1 ? '(min ' + mult + ', step ' + mult + ')' : '';
}

document.getElementById('f-strategy').addEventListener('change', enforceLots);
enforceLots();

function updateCutoffValues() {
  var qty = parseInt(document.getElementById('f-qty').value, 10) || 1;
  var prem = parseFloat(document.getElementById('f-minprem').value) || 100;
  var notional = qty * NIFTY_LOT_SIZE * prem;
  var pp = parseFloat(document.getElementById('f-profit-pct').value) || 0;
  var lp = parseFloat(document.getElementById('f-loss-pct').value) || 0;
  document.getElementById('f-profit-val').textContent = '= \\u20B9' + Math.round(notional * pp / 100);
  document.getElementById('f-loss-val').textContent = '= \\u20B9' + Math.round(notional * lp / 100);
}
document.getElementById('f-cutoff-enabled').addEventListener('change', function() {
  var on = this.checked;
  document.getElementById('f-profit-pct').disabled = !on;
  document.getElementById('f-loss-pct').disabled = !on;
  if (on) updateCutoffValues();
});
document.getElementById('f-profit-pct').addEventListener('input', updateCutoffValues);
document.getElementById('f-loss-pct').addEventListener('input', updateCutoffValues);
document.getElementById('f-qty').addEventListener('input', updateCutoffValues);
document.getElementById('f-minprem').addEventListener('input', updateCutoffValues);
updateCutoffValues();

function runBacktest() {
  var btn = document.getElementById('run-btn');
  var spinner = document.getElementById('spinner');
  var statusMsg = document.getElementById('status-msg');
  var errorBox = document.getElementById('error-box');

  btn.disabled = true;
  spinner.classList.add('show');
  statusMsg.textContent = 'Running...';
  errorBox.style.display = 'none';
  document.getElementById('summary-card').style.display = 'none';
  document.getElementById('trades-card').style.display = 'none';
  document.getElementById('compare-card').style.display = 'none';
  document.getElementById('compare-details').innerHTML = '';

  // Enforce lots before sending
  enforceLots();

  var payload = {
    date: document.getElementById('f-date').value,
    strategy_name: document.getElementById('f-strategy').value,
    sl_points: parseFloat(document.getElementById('f-sl').value),
    target_points: parseFloat(document.getElementById('f-tgt').value),
    quantity: parseInt(document.getElementById('f-qty').value, 10),
    min_premium: parseFloat(document.getElementById('f-minprem').value),
    start_time: document.getElementById('f-start').value,
    stop_time: document.getElementById('f-stop').value,
    market_bias: document.getElementById('f-bias').value,
    expiry_type: document.getElementById('f-expiry').value,
    daily_cutoff: document.getElementById('f-cutoff-enabled').checked,
    daily_profit_pct: parseFloat(document.getElementById('f-profit-pct').value) || 0,
    daily_loss_pct: parseFloat(document.getElementById('f-loss-pct').value) || 0,
  };

  fetch('/api/backtest/run', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload),
  })
  .then(function(r) { return r.json(); })
  .then(function(data) {
    btn.disabled = false;
    spinner.classList.remove('show');

    if (data.error) {
      errorBox.textContent = data.error;
      errorBox.style.display = 'block';
      statusMsg.textContent = 'Error';
      return;
    }

    var info = data.summary.total_trades + ' trades';
    var instruments = data.summary.instruments_used || [];
    if (instruments.length > 0) {
      info += ' | ' + instruments.join(', ');
    } else if (data.summary.instrument) {
      info += ' | ' + data.summary.instrument;
    }
    statusMsg.textContent = 'Done (' + info + ')';
    showResults(data);
  })
  .catch(function(e) {
    btn.disabled = false;
    spinner.classList.remove('show');
    errorBox.textContent = 'Request failed: ' + e;
    errorBox.style.display = 'block';
    statusMsg.textContent = 'Failed';
  });
}

function showResults(data) {
  var s = data.summary;

  // Summary
  var pnlEl = document.getElementById('s-pnl');
  pnlEl.textContent = (s.total_pnl >= 0 ? '+' : '') + '\\u20B9' + s.total_pnl.toFixed(2);
  pnlEl.className = 'value ' + (s.total_pnl >= 0 ? 'profit' : 'loss');
  document.getElementById('s-trades').textContent = s.total_trades;
  document.getElementById('s-wins').textContent = s.wins;
  document.getElementById('s-losses').textContent = s.losses;
  document.getElementById('summary-card').style.display = 'block';

  // Trades table
  var trades = data.trades || [];
  var el = document.getElementById('trades-table');

  if (trades.length === 0) {
    el.innerHTML = '<div class="empty-msg">No trades generated</div>';
  } else {
    var html = '<table><thead><tr>' +
      '<th>#</th><th>Dir</th><th>Symbol</th><th>Entry</th><th>Exit</th>' +
      '<th>Entry Time</th><th>Exit Time</th><th>Lots</th><th>Reason</th><th>P&amp;L</th>' +
      '</tr></thead><tbody>';
    trades.forEach(function(t, i) {
      var dirClass = t.direction === 'SELL' ? 'dir-sell' : 'dir-buy';
      var pnlClass = t.pnl >= 0 ? 'pnl-pos' : 'pnl-neg';
      html += '<tr>' +
        '<td>' + (i + 1) + '</td>' +
        '<td class="' + dirClass + '">' + t.direction + '</td>' +
        '<td style="font-size:11px">' + (t.symbol || '-') + '</td>' +
        '<td>' + t.entry_price.toFixed(2) + '</td>' +
        '<td>' + t.exit_price.toFixed(2) + '</td>' +
        '<td>' + (t.entry_time || '-') + '</td>' +
        '<td>' + (t.exit_time || '-') + '</td>' +
        '<td>' + t.lots + '</td>' +
        '<td>' + (t.exit_reason || '-') + '</td>' +
        '<td class="' + pnlClass + '">' + (t.pnl >= 0 ? '+' : '') + t.pnl.toFixed(2) + '</td>' +
        '</tr>';
    });
    html += '</tbody></table>';
    el.innerHTML = html;
  }

  document.getElementById('trades-card').style.display = 'block';
}

var STRATEGY_LABELS = {
  sar: 'SAR (Buy/Sell CE)',
  buy_ce_pe_alternate_candle_close: 'Buy CE/PE Alternate (Candle Close)',
  buy_ce_pe_scale_out_candle_close: 'Buy CE/PE Scale Out (Candle Close)',
  supertrend_candle_close: 'Buy CE/PE Supertrend (Candle Close)'
};

function runCompareAll() {
  var btn = document.getElementById('compare-btn');
  var spinner = document.getElementById('compare-spinner');
  var statusMsg = document.getElementById('status-msg');
  var errorBox = document.getElementById('error-box');

  btn.disabled = true;
  document.getElementById('run-btn').disabled = true;
  spinner.classList.add('show');
  errorBox.style.display = 'none';

  // Hide single-strategy results
  document.getElementById('summary-card').style.display = 'none';
  document.getElementById('trades-card').style.display = 'none';
  document.getElementById('compare-card').style.display = 'none';
  document.getElementById('compare-details').innerHTML = '';

  // Collect selected strategies
  var checks = document.querySelectorAll('#compare-strategies input:checked');
  var selectedStrategies = [];
  checks.forEach(function(cb) { selectedStrategies.push(cb.value); });
  if (selectedStrategies.length === 0) {
    btn.disabled = false;
    document.getElementById('run-btn').disabled = false;
    spinner.classList.remove('show');
    errorBox.textContent = 'Please select at least one strategy to compare.';
    errorBox.style.display = 'block';
    statusMsg.textContent = '';
    return;
  }

  // Derive max lot multiplier from selected strategies
  var maxMult = 1;
  selectedStrategies.forEach(function(s) {
    var m = LOT_MULTIPLIERS[s] || 1;
    if (m > maxMult) maxMult = m;
  });
  var qtyInput = document.getElementById('f-qty');
  var lots = parseInt(qtyInput.value, 10) || maxMult;
  if (lots < maxMult || lots % maxMult !== 0) {
    lots = Math.max(Math.round(lots / maxMult), 1) * maxMult;
    qtyInput.value = lots;
  }

  // Derive month from selected date
  var selDate = document.getElementById('f-date').value;
  var monthLabel = selDate.slice(0, 7);  // "2026-02"
  statusMsg.textContent = 'Running all strategies for ' + monthLabel + '...';

  var payload = {
    date: selDate,
    sl_points: parseFloat(document.getElementById('f-sl').value),
    target_points: parseFloat(document.getElementById('f-tgt').value),
    quantity: lots,
    min_premium: parseFloat(document.getElementById('f-minprem').value),
    start_time: document.getElementById('f-start').value,
    stop_time: document.getElementById('f-stop').value,
    market_bias: document.getElementById('f-bias').value,
    expiry_type: document.getElementById('f-expiry').value,
    strategies: selectedStrategies,
    daily_cutoff: document.getElementById('f-cutoff-enabled').checked,
    daily_profit_pct: parseFloat(document.getElementById('f-profit-pct').value) || 0,
    daily_loss_pct: parseFloat(document.getElementById('f-loss-pct').value) || 0,
  };

  // Start job — returns immediately with job_id
  fetch('/api/backtest/run-all', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload),
  })
  .then(function(r) { return r.json(); })
  .then(function(data) {
    if (data.error) {
      throw new Error(data.error);
    }
    // Poll for results
    pollCompareJob(data.job_id);
  })
  .catch(function(e) {
    btn.disabled = false;
    document.getElementById('run-btn').disabled = false;
    spinner.classList.remove('show');
    errorBox.textContent = 'Request failed: ' + e;
    errorBox.style.display = 'block';
    statusMsg.textContent = 'Failed';
  });
}

function pollCompareJob(jobId) {
  var btn = document.getElementById('compare-btn');
  var spinner = document.getElementById('compare-spinner');
  var statusMsg = document.getElementById('status-msg');
  var errorBox = document.getElementById('error-box');

  var poll = setInterval(function() {
    fetch('/api/backtest/run-all/status?job_id=' + jobId)
      .then(function(r) { return r.json(); })
      .then(function(data) {
        if (data.status === 'running') {
          statusMsg.textContent = data.progress || 'Running...';
          return;
        }

        // Done or error — stop polling
        clearInterval(poll);
        btn.disabled = false;
        document.getElementById('run-btn').disabled = false;
        spinner.classList.remove('show');

        if (data.status === 'error') {
          errorBox.textContent = data.error || 'Unknown error';
          errorBox.style.display = 'block';
          statusMsg.textContent = 'Error';
          return;
        }

        var resp = data.result;
        var nDays = (resp.days || []).length;
        statusMsg.textContent = 'Done (' + resp.month + ' | ' + nDays + ' days)';
        showCompareResults(resp);
      })
      .catch(function(e) {
        clearInterval(poll);
        btn.disabled = false;
        document.getElementById('run-btn').disabled = false;
        spinner.classList.remove('show');
        errorBox.textContent = 'Poll failed: ' + e;
        errorBox.style.display = 'block';
        statusMsg.textContent = 'Failed';
      });
  }, 2000);
}

function showCompareResults(resp) {
  var strategies = resp.strategies;
  var days = resp.days || [];

  // Build rows sorted by total P&L descending
  var rows = [];
  for (var name in strategies) {
    var s = strategies[name];
    rows.push({
      name: name,
      pnl: s.total_pnl,
      trades: s.total_trades,
      wins: s.wins,
      losses: s.losses,
      days_traded: s.days_traded,
      profitable_days: s.profitable_days,
      daily: s.daily || {},
    });
  }
  rows.sort(function(a, b) { return b.pnl - a.pnl; });

  var bestPnl = rows.length > 0 ? rows[0].pnl : null;

  // ── Aggregated comparison table ──
  var html = '<table class="compare-table"><thead><tr>' +
    '<th>Strategy</th><th>Total P&L</th><th>Trades</th><th>Wins</th>' +
    '<th>Losses</th><th>Win Rate</th><th>Days</th><th>Profitable Days</th>' +
    '</tr></thead><tbody>';

  rows.forEach(function(r) {
    var isBest = (r.pnl === bestPnl && bestPnl !== null);
    var rowClass = isBest ? ' class="best"' : '';
    var pnlClass = r.pnl >= 0 ? 'pnl-pos' : 'pnl-neg';
    var winRate = r.trades > 0 ? Math.round((r.wins / r.trades) * 100) : 0;
    html += '<tr' + rowClass + '>' +
      '<td class="strat-name">' + (STRATEGY_LABELS[r.name] || r.name) + (isBest ? ' &#9733;' : '') + '</td>' +
      '<td class="' + pnlClass + '">' + (r.pnl >= 0 ? '+' : '') + '\\u20B9' + r.pnl.toFixed(2) + '</td>' +
      '<td>' + r.trades + '</td>' +
      '<td class="pnl-pos">' + r.wins + '</td>' +
      '<td class="pnl-neg">' + r.losses + '</td>' +
      '<td>' + winRate + '%</td>' +
      '<td>' + r.days_traded + '</td>' +
      '<td>' + r.profitable_days + '/' + r.days_traded + '</td>' +
      '</tr>';
  });
  html += '</tbody></table>';

  document.getElementById('compare-table').innerHTML = html;
  document.getElementById('compare-card').style.display = 'block';

  // ── Per-strategy daily breakdown (collapsible) ──
  var detailsHtml = '';
  rows.forEach(function(r, idx) {
    if (r.days_traded === 0) return;
    var label = STRATEGY_LABELS[r.name] || r.name;
    var pnlClass = r.pnl >= 0 ? 'pnl-pos' : 'pnl-neg';
    var pnlStr = (r.pnl >= 0 ? '+' : '') + '\\u20B9' + r.pnl.toFixed(2);

    // Daily P&L table
    var dayHtml = '<table><thead><tr>' +
      '<th>Date</th><th>P&L</th><th>Trades</th><th>Wins</th><th>Losses</th><th>Instruments</th>' +
      '</tr></thead><tbody>';

    var cumPnl = 0;
    days.forEach(function(day) {
      var dd = r.daily[day];
      if (!dd) return;
      if (dd.error) {
        dayHtml += '<tr><td>' + day + '</td><td colspan="5" style="color:#888;font-style:italic;">No data</td></tr>';
        return;
      }
      cumPnl += dd.pnl;
      var dpClass = dd.pnl >= 0 ? 'pnl-pos' : 'pnl-neg';
      dayHtml += '<tr>' +
        '<td>' + day + '</td>' +
        '<td class="' + dpClass + '">' + (dd.pnl >= 0 ? '+' : '') + dd.pnl.toFixed(2) + '</td>' +
        '<td>' + dd.trades + '</td>' +
        '<td class="pnl-pos">' + (dd.wins || 0) + '</td>' +
        '<td class="pnl-neg">' + (dd.losses || 0) + '</td>' +
        '<td style="font-size:11px">' + ((dd.instruments || []).join(', ') || '-') + '</td>' +
        '</tr>';
    });
    dayHtml += '</tbody></table>';

    detailsHtml += '<div class="card">' +
      '<h3 class="coll-toggle" onclick="toggleCompareDetail(' + idx + ')" id="coll-hdr-' + idx + '">' +
        label + ' &mdash; <span class="' + pnlClass + '">' + pnlStr + '</span>' +
        ' (' + r.profitable_days + '/' + r.days_traded + ' profitable days)' +
      '</h3>' +
      '<div id="coll-body-' + idx + '" style="display:none;">' +
      '<div class="table-wrap">' + dayHtml + '</div>' +
      '</div></div>';
  });
  document.getElementById('compare-details').innerHTML = detailsHtml;
}

function toggleCompareDetail(idx) {
  var body = document.getElementById('coll-body-' + idx);
  var hdr = document.getElementById('coll-hdr-' + idx);
  if (body.style.display === 'none') {
    body.style.display = 'block';
    hdr.classList.add('open');
  } else {
    body.style.display = 'none';
    hdr.classList.remove('open');
  }
}

function buildTradeTable(trades) {
  var html = '<table><thead><tr>' +
    '<th>#</th><th>Dir</th><th>Symbol</th><th>Entry</th><th>Exit</th>' +
    '<th>Entry Time</th><th>Exit Time</th><th>Lots</th><th>Reason</th><th>P&amp;L</th>' +
    '</tr></thead><tbody>';
  trades.forEach(function(t, i) {
    var dirClass = t.direction === 'SELL' ? 'dir-sell' : 'dir-buy';
    var pnlClass = t.pnl >= 0 ? 'pnl-pos' : 'pnl-neg';
    html += '<tr>' +
      '<td>' + (i + 1) + '</td>' +
      '<td class="' + dirClass + '">' + t.direction + '</td>' +
      '<td style="font-size:11px">' + (t.symbol || '-') + '</td>' +
      '<td>' + t.entry_price.toFixed(2) + '</td>' +
      '<td>' + t.exit_price.toFixed(2) + '</td>' +
      '<td>' + (t.entry_time || '-') + '</td>' +
      '<td>' + (t.exit_time || '-') + '</td>' +
      '<td>' + t.lots + '</td>' +
      '<td>' + (t.exit_reason || '-') + '</td>' +
      '<td class="' + pnlClass + '">' + (t.pnl >= 0 ? '+' : '') + t.pnl.toFixed(2) + '</td>' +
      '</tr>';
  });
  html += '</tbody></table>';
  return html;
}
</script>
</body>
</html>
"""


# ─── Routes ────────────────────────────────────────────────────────────────────


@backtest_bp.route("/backtest")
def backtest_page():
    return render_template_string(BACKTEST_HTML)


@backtest_bp.route("/api/backtest/run", methods=["POST"])
def api_backtest_run():
    if _client is None:
        return jsonify({"error": "Client not initialised"}), 503

    data = request.get_json(silent=True) or {}

    required = ["date", "strategy_name",
                 "sl_points", "target_points", "quantity",
                 "start_time", "stop_time"]
    missing = [k for k in required if k not in data]
    if missing:
        return jsonify({"error": f"Missing fields: {', '.join(missing)}"}), 400

    try:
        engine = BacktestEngine(_client)
        result = engine.run(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    if "error" in result:
        return jsonify(result), 400

    return jsonify(result)


# ─── Compare-All: Async Job ─────────────────────────────────────────────────

import threading
import uuid

_compare_jobs = {}  # job_id -> {"status": "running"/"done"/"error", "progress": "...", "result": {...}}


@backtest_bp.route("/api/backtest/run-all", methods=["POST"])
def api_backtest_run_all():
    """Start compare-all job in background, return job_id immediately."""
    if _client is None:
        return jsonify({"error": "Client not initialised"}), 503

    data = request.get_json(silent=True) or {}

    required = ["date", "sl_points", "target_points", "quantity",
                 "start_time", "stop_time"]
    missing = [k for k in required if k not in data]
    if missing:
        return jsonify({"error": f"Missing fields: {', '.join(missing)}"}), 400

    job_id = uuid.uuid4().hex[:8]
    _compare_jobs[job_id] = {"status": "running", "progress": "Starting...", "result": None}

    thread = threading.Thread(
        target=_run_all_worker, args=(job_id, dict(data)), daemon=True,
    )
    thread.start()

    return jsonify({"job_id": job_id})


@backtest_bp.route("/api/backtest/run-all/status")
def api_backtest_run_all_status():
    """Poll for compare-all job result."""
    job_id = request.args.get("job_id", "")
    job = _compare_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Unknown job_id"}), 404

    if job["status"] == "running":
        return jsonify({"status": "running", "progress": job["progress"]})

    # Done or error — return result and clean up
    result = job["result"]
    _compare_jobs.pop(job_id, None)
    if job["status"] == "error":
        return jsonify({"status": "error", "error": result}), 500
    return jsonify({"status": "done", "result": result})


def _run_all_worker(job_id, data):
    """Background worker that runs all strategies across a month."""
    from datetime import date as _date

    job = _compare_jobs[job_id]

    try:
        sel = _date.fromisoformat(data["date"])
    except (ValueError, TypeError):
        job["status"] = "error"
        job["result"] = "Invalid date format"
        return

    sel_year, sel_month = sel.year, sel.month
    _, last_day = monthrange(sel_year, sel_month)
    today = _date.today()
    weekdays = []
    for day_num in range(1, last_day + 1):
        dt = _date(sel_year, sel_month, day_num)
        if dt > today:
            break
        if dt.weekday() < 5:
            weekdays.append(dt.isoformat())

    if not weekdays:
        job["status"] = "error"
        job["result"] = "No trading days in selected month"
        return

    requested = data.get("strategies")
    if requested:
        strategy_names = [n for n in requested if n in STRATEGY_REGISTRY]
    else:
        strategy_names = list(STRATEGY_REGISTRY.keys())

    job["progress"] = "Fetching instruments..."
    try:
        instruments = _client.kite.instruments("NFO")
    except Exception as exc:
        job["status"] = "error"
        job["result"] = f"Failed to fetch instruments: {exc}"
        return

    shared_cache = {}
    results = {}
    for name in strategy_names:
        results[name] = {
            "total_pnl": 0.0, "total_trades": 0, "wins": 0, "losses": 0,
            "days_traded": 0, "profitable_days": 0, "daily": {},
        }

    engine = BacktestEngine(_client)
    total_days = len(weekdays)

    for day_idx, day_str in enumerate(weekdays):
        job["progress"] = f"Day {day_idx + 1}/{total_days}: {day_str}"
        shared_cache.clear()

        for name in strategy_names:
            config = dict(data, date=day_str, strategy_name=name,
                          _instruments=instruments, _shared_cache=shared_cache)
            try:
                result = engine.run(config)
            except Exception as exc:
                result = {"error": str(exc)}

            agg = results[name]
            if "error" in result:
                agg["daily"][day_str] = {"pnl": 0, "trades": 0, "error": result["error"]}
                continue

            s = result["summary"]
            day_pnl = s["total_pnl"]
            agg["total_pnl"] = round(agg["total_pnl"] + day_pnl, 2)
            agg["total_trades"] += s["total_trades"]
            agg["wins"] += s["wins"]
            agg["losses"] += s["losses"]
            agg["days_traded"] += 1
            if day_pnl > 0:
                agg["profitable_days"] += 1

            agg["daily"][day_str] = {
                "pnl": round(day_pnl, 2),
                "trades": s["total_trades"],
                "wins": s["wins"],
                "losses": s["losses"],
                "instruments": s.get("instruments_used", []),
            }

    job["status"] = "done"
    job["result"] = {
        "month": f"{sel_year}-{sel_month:02d}",
        "days": weekdays,
        "strategies": results,
    }


# ─── Historical Data Download ──────────────────────────────────────────────

import time
import logging

_download_jobs = {}  # job_id -> {"status", "progress", "result"}
_download_lock = threading.Lock()

_dl_logger = logging.getLogger(__name__ + ".download")

NIFTY_INDEX_TOKEN = 256265  # NSE:NIFTY 50 instrument token


@backtest_bp.route("/api/backtest/download", methods=["POST"])
def api_download_start():
    """Start async historical data download job."""
    if _client is None:
        return jsonify({"error": "Client not initialised"}), 503

    # Reject if a download is already running
    with _download_lock:
        for jid, j in _download_jobs.items():
            if j["status"] == "running":
                return jsonify({"error": "A download is already running",
                                "job_id": jid}), 409

    data = request.get_json(silent=True) or {}
    start_date = data.get("start_date", "")
    end_date = data.get("end_date", "")
    if not start_date or not end_date:
        return jsonify({"error": "start_date and end_date required"}), 400

    job_id = uuid.uuid4().hex[:8]
    _download_jobs[job_id] = {
        "status": "running", "progress": "Starting...", "result": None,
    }

    thread = threading.Thread(
        target=_download_worker,
        args=(job_id, start_date, end_date,
              int(data.get("strikes_each_side", 20)),
              data.get("expiry_type", "weekly")),
        daemon=True,
    )
    thread.start()

    return jsonify({"job_id": job_id})


@backtest_bp.route("/api/backtest/download/status")
def api_download_status():
    """Poll download job progress."""
    job_id = request.args.get("job_id", "")
    job = _download_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Unknown job_id"}), 404

    if job["status"] == "running":
        return jsonify({"status": "running", "progress": job["progress"]})

    result = job["result"]
    _download_jobs.pop(job_id, None)
    if job["status"] == "error":
        return jsonify({"status": "error", "error": result}), 500
    return jsonify({"status": "done", "summary": result})


@backtest_bp.route("/api/backtest/lot-size")
def api_lot_size():
    """Return NIFTY lot size from instruments."""
    if _client is None:
        return jsonify({"lot_size": 75})
    try:
        instruments = _client.kite.instruments("NFO")
        for inst in instruments:
            if inst["name"] == "NIFTY" and inst.get("lot_size"):
                return jsonify({"lot_size": int(inst["lot_size"])})
    except Exception:
        pass
    return jsonify({"lot_size": 75})


@backtest_bp.route("/api/backtest/download/list")
def api_download_list():
    """Return cached dates summary."""
    return jsonify(list_cached_dates())


def _download_worker(job_id, start_date, end_date, strikes_each_side, expiry_type):
    """Background worker: download NIFTY + option candles for date range."""
    from datetime import date as _date, timedelta

    job = _download_jobs[job_id]

    try:
        d_start = _date.fromisoformat(start_date)
        d_end = _date.fromisoformat(end_date)
    except (ValueError, TypeError):
        job["status"] = "error"
        job["result"] = "Invalid date format"
        return

    if d_start > d_end:
        job["status"] = "error"
        job["result"] = "start_date must be <= end_date"
        return

    # Build list of weekdays in range
    weekdays = []
    d = d_start
    while d <= d_end:
        if d.weekday() < 5:
            weekdays.append(d.isoformat())
        d += timedelta(days=1)

    if not weekdays:
        job["status"] = "error"
        job["result"] = "No weekdays in date range"
        return

    total_days = len(weekdays)
    files_saved = 0
    files_skipped = 0
    days_skipped = 0  # holidays

    # Fetch instruments once
    job["progress"] = "Fetching instruments..."
    try:
        instruments = _client.kite.instruments("NFO")
    except Exception as exc:
        job["status"] = "error"
        job["result"] = f"Failed to fetch instruments: {exc}"
        return

    for day_idx, day_str in enumerate(weekdays):
        job["progress"] = (
            f"Day {day_idx + 1}/{total_days}: {day_str} "
            f"({files_saved} saved, {files_skipped} skipped)"
        )

        target_date = _date.fromisoformat(day_str)

        # ── 1. NIFTY daily candle ──
        if has_nifty_day(day_str):
            nifty_day = load_nifty_day(day_str)
            files_skipped += 1
        else:
            try:
                nifty_day = _client.kite.historical_data(
                    NIFTY_INDEX_TOKEN, target_date, target_date, "day",
                )
                time.sleep(0.35)
            except Exception as exc:
                _dl_logger.warning(f"NIFTY day fetch failed for {day_str}: {exc}")
                nifty_day = []

            if not nifty_day:
                # Likely a holiday — save empty file as marker, skip rest
                save_nifty_day(day_str, [])
                days_skipped += 1
                files_saved += 1
                continue

            save_nifty_day(day_str, nifty_day)
            files_saved += 1

        # Check if saved day data was empty (holiday marker)
        if not nifty_day:
            days_skipped += 1
            continue

        spot_open = nifty_day[0]["open"]
        atm_strike = round(float(spot_open) / 50) * 50

        # ── 2. NIFTY minute candles ──
        if has_nifty_minute(day_str):
            files_skipped += 1
        else:
            try:
                nifty_min = _client.kite.historical_data(
                    NIFTY_INDEX_TOKEN, target_date, target_date, "minute",
                )
                save_nifty_minute(day_str, nifty_min)
                files_saved += 1
                time.sleep(0.35)
            except Exception as exc:
                _dl_logger.warning(f"NIFTY minute fetch failed for {day_str}: {exc}")

        # ── 3. Find option instruments ──
        strike_range = strikes_each_side * 50
        low_strike = atm_strike - strike_range
        high_strike = atm_strike + strike_range

        # Filter NIFTY options for this date
        all_opts = [
            i for i in instruments
            if i["name"] == "NIFTY"
            and i["instrument_type"] in ("CE", "PE")
            and i["expiry"] >= target_date
            and low_strike <= i["strike"] <= high_strike
        ]

        # Select expiry
        if expiry_type == "monthly":
            from .base_strategy import _pick_monthly_expiry
            all_expiries = sorted({i["expiry"] for i in all_opts})
            target_expiry = _pick_monthly_expiry(all_expiries, target_date)
        else:
            valid_expiries = [i["expiry"] for i in all_opts]
            target_expiry = min(valid_expiries) if valid_expiries else None

        if target_expiry is None:
            _dl_logger.warning(f"No {expiry_type} expiry found for {day_str}")
            continue

        target_opts = [i for i in all_opts if i["expiry"] == target_expiry]
        _dl_logger.info(
            f"{day_str}: ATM={atm_strike}, range={low_strike}-{high_strike}, "
            f"expiry={target_expiry}, {len(target_opts)} instruments"
        )

        # ── 4. Download each option's minute candles ──
        for opt in target_opts:
            symbol = opt["tradingsymbol"]
            if has_option_minute(day_str, symbol):
                files_skipped += 1
                continue
            try:
                candles = _client.kite.historical_data(
                    opt["instrument_token"], target_date, target_date, "minute",
                )
                save_option_minute(day_str, symbol, candles)
                files_saved += 1
            except Exception as exc:
                _dl_logger.warning(f"Option fetch failed {symbol} {day_str}: {exc}")
            time.sleep(0.35)

        job["progress"] = (
            f"Day {day_idx + 1}/{total_days}: {day_str} done "
            f"({files_saved} saved, {files_skipped} skipped)"
        )

    job["status"] = "done"
    job["result"] = (
        f"{total_days} days processed, {files_saved} files saved, "
        f"{files_skipped} already cached, {days_skipped} holidays skipped"
    )
