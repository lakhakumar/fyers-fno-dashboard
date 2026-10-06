# F&O Intraday Rank Dashboard + Strategy-1 Virtual Trading

Live NSE F&O equity rank dashboard (Gainers / Losers / Sectors / Scanner) with **Strategy-1 virtual forward testing**, trade log, report viewer, and optional **Telegram** alerts.

## Features

| Area | Detail |
|------|--------|
| Universe | ~210 NSE F&O equities from FYERS FO master (`exInstType=13`) |
| Ranks | 5-minute snapshots 09:20–15:30 IST, stored in Postgres |
| Live | FYERS market-data WebSocket |
| Scanner | D-1 / OR / VWAP / volume filters |
| **Virtual Trade** | Strategy-1 paper trades (09:50–11:00, max 3/day) |
| **Report** | Date-range PnL / win rate from DB |
| **Telegram** | Entry, partial, exit messages |

## Strategy 1 (summary)

1. **09:20–09:50** — watch leaderboard only (no trades)  
2. **09:50–11:00** — candidates must pass 7 checks (top rank, rank acceleration, sector, volume, EMA/VWAP, RSI, market breadth)  
3. Risk **1%** of virtual capital, SL ~ATR (0.8–1.2%), target **1:2**, book 60% then BE + trail to **1:3**  
4. Force flat by **15:15 IST**  

## Deploy on Render (beginner steps)

### 1. GitHub files (repo root)

Upload / replace:

```text
server.js
index.html
package.json
.github/workflows/keep-alive.yml   (optional keep-alive)
README.md
```

Commit and push to `main`.

### 2. Render web service

- **Build:** `npm install`  
- **Start:** `npm start` (or `node server.js`)  
- **Root Directory:** leave **empty** (files at repo root)  

### 3. Environment variables (Render → Environment)

| Variable | Required | Purpose |
|----------|----------|---------|
| `DATABASE_URL` | Yes (for ranks + trade log) | Render Postgres connection string |
| `TELEGRAM_BOT_TOKEN` | Optional | Bot token from @BotFather |
| `TELEGRAM_CHAT_ID` | Optional | Your chat / group id |
| `VIRTUAL_CAPITAL` | Optional | Default `300000` (₹) |

### 4. Telegram setup (optional)

1. Message **@BotFather** → `/newbot` → copy token → `TELEGRAM_BOT_TOKEN`  
2. Message your bot, then open  
   `https://api.telegram.org/bot<TOKEN>/getUpdates`  
   and copy `chat.id` → `TELEGRAM_CHAT_ID`  
3. Redeploy  

### 5. Keep-alive (free tier)

**Option A — GitHub Actions**

1. Add secret `RENDER_URL` = `https://your-service.onrender.com`  
2. Enable Actions; run workflow once manually  

**Option B — UptimeRobot (recommended)**

- Monitor: `https://your-service.onrender.com/api/health`  
- Interval: **5 minutes**  

### 6. Login

Open the Render URL → enter **FYERS App ID** + **access token** → dashboard loads.

## UI tabs

1. **Gainers** / **Losers** — top 30 + historical rank columns  
2. **Sector Trends** — sector bars  
3. **Scanner** — checkbox filters  
4. **Virtual Trade** — open positions, signals, closed trades, S1 toggle  
5. Top bar **S1 Report** — date-range forward-test report  

Click a **stock name** for the 5m interactive chart overlay.

## API endpoints

```text
POST /api/login
POST /api/logout
GET  /api/dashboard
GET  /api/health
GET  /api/chart?key=RELIANCE&days=14
GET  /api/virtual/status
GET  /api/virtual/report?from=YYYY-MM-DD&to=YYYY-MM-DD
POST /api/virtual/toggle
POST /api/virtual/scan
```

## Local run

```bash
npm install
export DATABASE_URL=postgres://...
export TELEGRAM_BOT_TOKEN=...   # optional
export TELEGRAM_CHAT_ID=...     # optional
npm start
```

Open `http://localhost:10000`.

## Notes

- Virtual trades are **paper only** (no real orders).  
- Strategy runs only while the server has a **valid FYERS session** and market is open.  
- Free Render sleep pauses collection until wake + login; use keep-alive during NSE hours.  
- Never commit access tokens or Telegram secrets to GitHub.  
