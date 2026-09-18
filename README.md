F&O Intraday Rank Dashboard

Web dashboard that tracks NSE equity Futures & Options (F&O) stocks and ranks them by intraday percentage change.

It shows:





Top gainers and losers (top 30)



Rank change from the 09:20 IST baseline



Historical ranks every 5 minutes from 09:20 → 15:30 IST



LTP and % change



Sector trends (average % change, clickable drill-down)



NIFTY 50, BANK NIFTY, SENSEX



F&O market breadth (advances / declines / unchanged)



Live updates via WebSocket + REST fallback



Persistent history in PostgreSQL

Built with Node.js (native HTTP server), FYERS API, PostgreSQL, WebSocket, and a single-page HTML frontend.



Features







Feature



Description





F&O universe



Built from FYERS FO master (exInstType = 13 / FUTSTK) matched to NSE cash EQ symbols





Gainers / Losers



Top 30 by intraday % change





Baseline



First ranking snapshot at 09:20 IST





Rank delta



baselineRank − currentRank (positive = moved up)





History



5-minute snapshots 09:20 … 15:30 (NSE continuous session end)





Day-end



Explicit 15:30 close ranking so end-of-day ranks are complete





Sectors



Server-side sector map; bar chart + stock list on click





Breadth



Advances / declines / unchanged over the F&O equity universe





Live data



FYERS market-data WebSocket; REST quotes for seed / fallback





Persistence



Rank snapshots stored in PostgreSQL (survives restart)





Rate limits



Global FYERS REST limiter (~180 req/min, ≥350 ms gap) + 429 retry





Mobile



Short NSE tickers, sticky stock + rank-change columns



Project structure

.
├── server.js       # Node HTTP API, FYERS integration, ranking, Postgres, WS
├── index.html      # Dashboard UI (HTML + CSS + browser JS)
├── package.json    # Dependencies and start script
└── README.md       # This file

No Express app — the server uses Node’s built-in http module plus ws.



Requirements





Node.js 18 or newer



FYERS App ID and access token (FYERS API)



PostgreSQL (optional but recommended for history persistence)



Deploy target: any Node host (e.g. Render.com)



Environment variables







Variable



Required



Description





DATABASE_URL



Recommended



PostgreSQL connection string. If missing, history is rebuilt from FYERS after each restart.





PORT



Optional



Listen port (default 10000). Render sets this automatically.

Example:

DATABASE_URL=postgresql://user:password@host:5432/dbname
PORT=10000



Local setup

# 1. Clone
git clone <your-repo-url>
cd <repo-folder>

# 2. Install dependencies
npm install

# 3. (Optional) set database
export DATABASE_URL="postgresql://..."

# 4. Start
npm start

Open: http://localhost:10000

Log in with:





FYERS App ID



Access token

Credentials are kept in the browser session and sent only to this app’s API for FYERS calls. Tokens are not written to logs.



Scripts

npm start          # node server.js
npm run check      # node --check server.js (syntax only)



API endpoints







Method



Path



Description





POST



/api/login



Validate FYERS credentials, load universe, history, start sockets





POST



/api/logout



Clear server-side FYERS session





GET



/api/dashboard



Full dashboard payload (headers: x-fyers-app-id, x-fyers-access-token)





GET



/api/logs



Recent system logs





GET



/api/health



Health / universe size / history snapshot count





WS



/ws



Live dashboard + log push to the browser

Static UI: GET / → index.html



How ranking works

Session (IST / Asia/Kolkata)







Period



Behaviour





Before 09:15



Show last completed trading session





09:15 – 15:30



Live session; collect ranks from 09:20 every 5 minutes through 15:30





After 15:30



Show today’s completed session; ensure 15:30 day-end snapshot exists

Rank change

rankDelta = rank_at_09:20 − current_rank





Positive → moved up the board



Negative → moved down



Significant moves (≥ 10 ranks) are highlighted in the UI

F&O universe





Load NSE_FO_sym_master.json



Keep instruments with exInstType = 13 (stock futures / FUTSTK)



Take unique underSym (skip index underlyings)



Match cash symbols in NSE_CM_sym_master.json as NSE:<TICKER>-EQ



Result: ~200 unique F&O equities (e.g. NSE:RELIANCE-EQ, NSE:PREMIERENE-EQ)

History rebuild

If Postgres has no (or sparse) history for the day:





Bulk quotes → previous close map



Rate-limited 5-minute history per stock



Bucket by IST close time (09:20 … 15:30)



Rank gainers / losers and save to Postgres

First full rebuild of ~210 names may take 1–3 minutes because of FYERS rate limits. Progress is written to the API log panel.



PostgreSQL schema

Created automatically on startup when DATABASE_URL is set:

CREATE TABLE IF NOT EXISTS rank_snapshots (
  trading_date date NOT NULL,
  candle_time  time NOT NULL,
  side         varchar(10) NOT NULL,  -- 'gainers' | 'losers'
  rank         int NOT NULL,
  symbol       varchar(80) NOT NULL,
  name         varchar(120) NOT NULL,
  sector       varchar(80) NOT NULL,
  pct          numeric NOT NULL,
  close        numeric,
  PRIMARY KEY (trading_date, candle_time, side, rank)
);

CREATE INDEX IF NOT EXISTS rank_snapshots_lookup
  ON rank_snapshots (trading_date, side, candle_time);



Deploy on Render.com

1. GitHub





Create a GitHub repository.



Upload / push these files to the root:





server.js



index.html



package.json



README.md



Commit and push to main (or the branch Render watches).

2. PostgreSQL on Render





New → PostgreSQL



Name e.g. fno-ranking-db



Region e.g. Singapore



After create, copy the Internal Database URL (or External if needed).

3. Web service on Render





New → Web Service → connect the GitHub repo



Settings:





Runtime: Node



Build command: npm install



Start command: npm start



Region: same as the database when possible



Environment:





DATABASE_URL = Postgres connection string from step 2



Deploy and open the service URL.

4. Verify

In Logs you should see roughly:

Postgres persistence ready.
F&O universe build: FUTSTK=629, unique equities=210, ...
Validation: PREMIERENE -> NSE:PREMIERENE-EQ (OK)
F&O equity universe ready: 210 stocks.
Dashboard server listening on 0.0.0.0:<PORT>

Then open the URL → log in with FYERS App ID + access token.



FYERS notes





Data host used: https://api-t1.fyers.in/data



Symbol masters:





https://public.fyers.in/sym_details/NSE_FO_sym_master.json



https://public.fyers.in/sym_details/NSE_CM_sym_master.json



Access tokens expire; generate a fresh token from the FYERS API dashboard when login fails.



Do not commit App secrets or tokens to GitHub.



Troubleshooting







Symptom



What to check





Login fails



Valid App ID + current access token; Render outbound network





Empty historical ranks



Logs for sparse history rebuild; wait for “Historical ranking rebuild complete”





HTTP 429 from FYERS



Expected under load; limiter + retries are built in — rebuild is slower, not broken





No LTP / flat ranks after close



Expect 15:30 day-end snapshot; check log for Saved 15:30 day-end ranking snapshot





No Postgres persistence



DATABASE_URL set on the web service; DB reachable from the service region





Universe count 0



FO/CM master fetch failed — check logs for symbol master HTTP errors

Useful log lines:

09:20 gainers in DB: N; sample keys: ...
Historical timeline available: 09:20, 09:25, ...
Saved 15:30 day-end ranking snapshot ...



Security





FYERS token is stored in browser sessionStorage and in server memory only while logged in.



Server does not log the access token.



Prefer HTTPS (Render provides this).



Rotate tokens if they may have been exposed.



License

Private project — adjust this section if you publish under an open-source license.



Changelog (high level)





Correct F&O universe: exInstType = 13, underSym, NSE EQ match



Global FYERS REST rate limiter + 429 handling



Sparse history detection and full rebuild



Rank timeline through 15:30 market close + day-end snapshot



UI: LTP, % change, F&O breadth, sticky columns, mobile tickers



Postgres persistence for 5-minute rank snapshots
