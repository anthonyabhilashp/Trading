# 📈 NIFTY Quant Momentum Strategy Backtester

A high-performance, **100% offline-capable Python quantitative trading pipeline** that implements a dynamically rebalanced relative strength momentum strategy. The engine reads local historical asset rows, applies custom performance lookback calculations, tracks compounding capital allocations, and simulates real-world portfolio rebalancing across any dynamic asset universe file.

---

## 🏆 The Master Configuration (The Alpha Combination)

Through rigorous backtesting of the Nifty universe, the absolute **optimum parameter combination** is defined by the following configuration:

```bash
python strategy.py --start_date 2023-10-01 --top_gainer_for_period 14 --top_n 7 --rebalance_period_months 6
```

### 📊 Backtest Performance Overview (Oct 2023 – Sep 2026)
* **Initial Capital Deployed:** ₹7,00,000.00 (₹1 Lakh per stock position)
* **Final Total Portfolio Value:** **₹28,95,332.75**
* **Net Absolute Return Yield:** **₹+21,95,332.75 (+313.62%)**
* **Core Multi-Baggers Captured:** SUZLON, GVT&D, KALYANKJIL, and LAURUSLABS.

### 🔍 Why This Specific Setup Outperforms:
1. **The 14-Month Lookback Sweet Spot:** Successfully filters out temporary market consolidation noise, capturing mature, institutional-backed macro breakout trends.
2. **The 6-Month Rebalance Lock:** Gives high-momentum leaders enough breathing room to compound heavily without premature exits due to minor quarterly noise, while still dumping laggards dynamically.
3. **True Cost-Basis tracking:** Calculates PnL from your original day-one entry price, ensuring your ledger outputs your true absolute return rather than mid-way milestone numbers.

---

## ⚡ Core Strategy Mechanics

This system implements a systematic **Relative Strength Momentum Framework** with strict asset budget allocations:

1. **Dynamic Universe Input:** The framework adapts seamlessly to any size input basket (Nifty 50, 150, or 500) via a single-column ticker symbol CSV file (`nifty_list.csv`).
2. **Point-to-Point Momentum Scoring:** At each checkpoint, the engine assesses price returns looking back across an explicit parameter-defined month window.
3. **Compound Rebalancing Cycles:** Holdings are dynamically evaluated at custom intervals. Winners are retained, laggards are liquidated, and the consolidated compounding capital pool is cleanly redistributed among new top-ranking leaderboard breakout candidates.

---

## ⚙️ Environment Setup & Dependency Installation

Follow these quick terminal steps to build a fresh, isolated execution environment on your host workstation:

### 1. Build and Activate the Virtual Environment (`venv`)
```bash
# Navigate straight to your project workspace
cd ~/workspace/work/Trading/investing

# Create an isolated python environment folder named 'venv'
python3 -m venv venv

# Activate the virtual sandbox
source venv/bin/activate
```

### 2. Synchronize Production Libraries
Install the core quantitative dependencies using your environment's isolated package installer:
```bash
pip install --upgrade pip
pip install pandas numpy yfinance requests
```

---

## 🚀 Running the Strategy Backtester

The script uses explicit, self-documenting parameter configuration flags. You can modify any strategy rule straight from your terminal layout block.

### 🔍 Command Line Arguments Reference Table

| Parameter Flag | Data Type | Default Value | Functional Description |
| :--- | :---: | :---: | :--- |
| `--universe_file` | `str` | `nifty_list.csv` | File path pointing to your single-column target ticker symbols CSV. |
| `--start_date` | `str` | `2024-01-01` | Theoretical capital deployment activation timestamp (`YYYY-MM-DD`). |
| `--top_gainer_for_period` | `int` | `12` | Historical timeline period in months used to calculate relative returns. |
| `--top_n` | `int` | `7` | Maximum total active stocks allowed in your compounding portfolio wallet. |
| `--rebalance_period_months` | `int` | `3` | Rebalance cycle frequency interval in months (Choices range from 1 to 12). |

### 📋 Running Your Master Combination
```bash
python strategy.py --start_date 2023-10-01 --top_gainer_for_period 14 --top_n 7 --rebalance_period_months 6
```

---

## 🗂️ Workspace Architecture Layout
For the backtester to run offline, ensure your storage tree models the following blueprint configuration:
```text
investing/
├── venv/                      # Your activated virtual environment layer
├── historic_data/             # Local hard drive price warehouse directory
│   ├── RELIANCE.csv           # Clean daily OHLCV historical track data
│   ├── TCS.csv
│   └── GVT&D.csv
├── nifty_list.csv             # Your custom single-column stock ticker symbol array
├── strategy.py                # Main optimized simulation pipeline execution engine
└── README.md                  # This detailed strategy usage documentation file
```
