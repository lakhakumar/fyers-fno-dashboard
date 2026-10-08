# FYERS F&O Intraday Rank Dashboard + Strategy-1 Virtual Trading & Backtesting

State-of-the-art live NSE F&O equity rank dashboard with **Strategy-1 virtual forward testing**, **End-of-Day Backtesting (post 15:40 IST)**, real-time momentum snapshots, 2-week interactive candlestick charts with Institutional Order Blocks, and automated **Telegram** trade reports. Built for **FYERS API v3**.

---

## 🌟 Key Features

| Feature | Description |
|---|---|
| **Universe** | 212 active NSE F&O equities pre-mapped across 12 sectors with real-time tracking |
| **Anti-Freeze Quotes** | Multi-tier quote recovery: WebSocket real-time feed with auto-reconnection + background REST polling watchdog |
| **75 5-Min Intervals** | Full intraday momentum timeline (09:20–15:30 IST) with color-coded rank jumps across rows |
| **Rebuild Timeline Button** | `🔄 Rebuild Timeline` button in the topbar gap-fills and recalculates all 5m rank intervals in the background |
| **Strategy-1 Virtual Trading** | Automated paper execution: 1/3 available capital per trade (max 3 trades/day), 1% Stop Loss, 1:2 & 1:3 profit targets, breakeven trailing, and 15:15 IST flat square-off |
| **Compounding & Taxes** | Realized net PnL (adjusted for STT, GST, SEBI, stamp duty, and brokerage) compounds into available capital for future trades and subsequent days |
| **EOD Day Backtest** | `⚡ Backtest Day (Post 15:40)` executes Strategy-1 against today's 5m candles based on 09:20 Market Breadth and dispatches a comprehensive report to Telegram |
| **2-Week IST Charts** | Lightweight Charts v4 with ~1,000 5m candles, Demand/Supply Institutional Order Blocks, native Indian Standard Time (`Asia/Kolkata`), and no external redirect buttons |
| **Telegram Integration** | Automated instant alerts for Entry, Partial Profit, Breakeven Trailing, Stop Loss, Target 2, and Day Backtest reports |

---

## 📐 Strategy-1 Trading Rules

1. **Market Breadth Filter (09:20 IST)**:
   - If F&O Advancing $\ge$ Declining $\to$ **BUY Only** from Top 15 Gainers.
   - If F&O Declining > Advancing $\to$ **SHORT Only** from Top 15 Losers.
2. **Quality Filtering (09:50–11:00 IST)**:
   - Rank $\le$ 15, Rank acceleration $\ge$ +10 positions from baseline.
   - Sector average $> 0.3\%$ alignment.
   - Volume surge pace $> 50,000$ shares.
   - EMA 9 > EMA 21 > EMA 50 + VWAP alignment.
   - RSI in the 52–72 sweet spot.
3. **Strict Sizing & Capital Allocation**:
   - Max 3 trades per day.
   - Per-trade allocated capital: $\text{Capital} / 3$.
   - Quantity: $\lfloor (\text{Capital} / 3) / \text{EntryPrice} \rfloor$.
   - Stop Loss: Strictly 1% distance ($\text{Entry} \times 0.99$ for BUY, $\text{Entry} \times 1.01$ for SELL). Max risk capped at 1% of allocated capital.
4. **Execution & Trailing**:
   - Target 1 (+2%): Book 50% partial profit and trail Stop Loss to breakeven ($\text{EntryPrice}$).
   - Target 2 (+3%): Exit remaining 50% quantity.
   - Time-based Exit: Flat all open trades by 15:15 IST.
5. **Capital Compounding**:
   - Net PnL accounts for all statutory charges and brokerage.
   - Realized net profit or loss updates available capital in PostgreSQL and memory, sizing subsequent trades and the next trading day.

---

## 🚀 Deployment on Render

### 1. Repository Files
Ensure the repository contains:
- `server.js` — Node.js backend with FYERS API v3 client, WebSocket engine, PostgreSQL persistence, and S1 backtester.
- `index.html` — Responsive dashboard UI with Lightweight Charts v4.
- `package.json` — Dependencies (`axios`, `fyers-api-v3`, `pg`, `ws`).
- `render.yaml` — Blueprint deployment configuration for web service and PostgreSQL database.
- `seed_snapshots.json` — 75 baseline timeline intervals.
- `backtest_candles_cache.json` — 2-week 5m candle cache.

### 2. Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `DATABASE_URL` | Yes | Attached from Render DB | PostgreSQL connection string |
| `FYERS_APP_ID` | Optional | Browser Session | Your FYERS App ID (e.g. `XF00000-100`) |
| `FYERS_ACCESS_TOKEN` | Optional | Browser Session | Active daily JWT token from `myapi.fyers.in` |
| `TELEGRAM_BOT_TOKEN` | Optional | — | Telegram Bot token from `@BotFather` |
| `TELEGRAM_CHAT_ID` | Optional | — | Telegram chat or channel ID for instant alerts |
| `VIRTUAL_CAPITAL` | Optional | `300000` | Initial virtual paper trading capital (₹) |

---

## 📡 API Endpoints

```text
POST /api/login                   - Authenticate FYERS App ID and Access Token
POST /api/logout                  - Clear session and disconnect socket
GET  /api/dashboard               - Get full live snapshot (ranks, breadth, sectors, virtual)
POST /api/rebuild                 - Trigger background timeline rebuild from 09:20 IST
GET  /api/chart?key=STOCK         - Retrieve 2-week 5m candles + Institutional Order Blocks
GET  /api/virtual/status          - Current virtual account state, positions, and signals
POST /api/virtual/scan            - Force immediate Strategy-1 quality scan
POST /api/virtual/backtest-day    - Run EOD backtest (post 15:40 IST) and dispatch to Telegram
POST /api/virtual/test-telegram   - Test Telegram connectivity
GET  /api/virtual/report          - Historical performance report across date ranges
GET  /api/health                  - Health check endpoint
```
