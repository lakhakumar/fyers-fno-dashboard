/**
 * F&O Intraday Rank Dashboard + Strategy-1 Virtual Forward Testing & EOD Backtesting
 * Broker API: FYERS API v3 (REST + WebSocket DataSocket)
 * Node.js + pg + ws + axios + fyers-api-v3
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const { URL } = require('url');
const { Pool } = require('pg');
const axios = require('axios');
const WebSocket = require('ws');
let fyersDataSocket = null;
try {
  fyersDataSocket = require('fyers-api-v3').fyersDataSocket;
} catch (e) {
  console.warn('[WARN] fyers-api-v3 not loaded, socket fallback will be used:', e.message);
}

const PORT = Number(process.env.PORT || 10000);
const HOST = '0.0.0.0';
const DATA_HOST = 'https://api-t1.fyers.in/data';
const API_HOST = 'https://api-t1.fyers.in/api/v3';
const MASTER_BASE = 'https://public.fyers.in/sym_details/';

const INDEX_CONFIG = [
  { name: 'NIFTY 50', symbol: 'NSE:NIFTY50-INDEX', yahoo: '%5ENSEI' },
  { name: 'BANK NIFTY', symbol: 'NSE:NIFTYBANK-INDEX', yahoo: '%5ENSEBANK' },
  { name: 'SENSEX', symbol: 'BSE:SENSEX-INDEX', yahoo: '%5EBSESN' }
];

const NIFTY50 = new Set([
  'RELIANCE','TCS','HDFCBANK','INFY','ICICIBANK','HINDUNILVR','ITC','SBIN','BHARTIARTL',
  'KOTAKBANK','LT','AXISBANK','ASIANPAINT','MARUTI','SUNPHARMA','TITAN','BAJFINANCE',
  'NESTLEIND','ULTRACEMCO','WIPRO','HCLTECH','POWERGRID','NTPC','TATASTEEL','M&M',
  'ADANIENT','JSWSTEEL','INDUSINDBK','TECHM','GRASIM','HINDALCO','ONGC','CIPLA','DRREDDY',
  'COALINDIA','BPCL','SBILIFE','BAJAJFINSV','EICHERMOT','DIVISLAB','BRITANNIA','HEROMOTOCO',
  'APOLLOHOSP','HDFCLIFE','TATACONSUM','ADANIPORTS','BAJAJ-AUTO','UPL','TATAMOTORS'
]);

const SECTOR_MAP = {
  "360ONE": "Financial Services", "ABB": "Industrials & Defence", "ABCAPITAL": "Financial Services",
  "ADANIENSOL": "Power & Green Energy", "ADANIENT": "Services & Hospitality", "ADANIGREEN": "Power & Green Energy",
  "ADANIPORTS": "Real Estate & Infra", "ADANIPOWER": "Power & Green Energy", "ALKEM": "Healthcare",
  "AMBER": "Consumer Durables", "AMBUJACEM": "Chemicals & Materials", "ANANDRATHI": "Financial Services",
  "ANGELONE": "Financial Services", "APLAPOLLO": "Chemicals & Materials", "APOLLOHOSP": "Healthcare",
  "ASHOKLEY": "Automobile", "ASIANPAINT": "Consumer Durables", "ASTRAL": "Chemicals & Materials",
  "ATHERENERG": "Automobile", "AUBANK": "Banking", "AUROPHARMA": "Healthcare", "AXISBANK": "Banking",
  "BAJAJ-AUTO": "Automobile", "BAJAJFINSV": "Financial Services", "BAJAJHLDNG": "Financial Services",
  "BAJFINANCE": "Financial Services", "BANDHANBNK": "Banking", "BANKBARODA": "Banking", "BANKINDIA": "Banking",
  "BDL": "Industrials & Defence", "BEL": "Industrials & Defence", "BHARATFORG": "Automobile",
  "BHARTIARTL": "Telecommunication", "BHEL": "Industrials & Defence", "BIOCON": "Healthcare",
  "BOSCHLTD": "Automobile", "BPCL": "Energy, Oil & Gas", "BRITANNIA": "FMCG", "BSE": "Financial Services",
  "CANBK": "Banking", "CANFINHOME": "Financial Services", "CDSL": "Financial Services",
  "CGPOWER": "Industrials & Defence", "CHAMBLFERT": "Chemicals & Materials", "CHOLAFIN": "Financial Services",
  "CIPLA": "Healthcare", "COALINDIA": "Metals & Mining", "COFORGE": "Information Technology",
  "COLPAL": "FMCG", "CONCOR": "Services & Hospitality", "CROMPTON": "Consumer Durables",
  "CUMMINSIND": "Industrials & Defence", "DABUR": "FMCG", "DALBHARAT": "Chemicals & Materials",
  "DEEPAKNTR": "Chemicals & Materials", "DELHIVERY": "Services & Hospitality", "DIVISLAB": "Healthcare",
  "DIXON": "Consumer Durables", "DLF": "Real Estate & Infra", "DMART": "Services & Hospitality",
  "DRREDDY": "Healthcare", "EICHERMOT": "Automobile", "EXIDEIND": "Automobile", "FEDERALBNK": "Banking",
  "FORTIS": "Healthcare", "GAIL": "Energy, Oil & Gas", "GLENMARK": "Healthcare", "GMRAIRPORT": "Services & Hospitality",
  "GODREJCP": "FMCG", "GODREJPROP": "Real Estate & Infra", "GRANULES": "Healthcare", "GRASIM": "Chemicals & Materials",
  "HAL": "Industrials & Defence", "HAVELLS": "Consumer Durables", "HCLTECH": "Information Technology",
  "HDFCAMC": "Financial Services", "HDFCBANK": "Banking", "HDFCLIFE": "Financial Services",
  "HEROMOTOCO": "Automobile", "HINDALCO": "Metals & Mining", "HINDPETRO": "Energy, Oil & Gas",
  "HINDUNILVR": "FMCG", "HINDZINC": "Metals & Mining", "HUDCO": "Financial Services",
  "ICICIBANK": "Banking", "ICICIGI": "Financial Services", "ICICIPRULI": "Financial Services",
  "IDFCFIRSTB": "Banking", "IEX": "Financial Services", "IGL": "Energy, Oil & Gas",
  "INDHOTEL": "Services & Hospitality", "INDIAMART": "Services & Hospitality", "INDIANB": "Banking",
  "INDIGO": "Services & Hospitality", "INDUSINDBK": "Banking", "INDUSTOWER": "Telecommunication",
  "INFY": "Information Technology", "INOXWIND": "Power & Green Energy", "IOC": "Energy, Oil & Gas",
  "IPCALAB": "Healthcare", "IRB": "Real Estate & Infra", "IRCTC": "Services & Hospitality",
  "IREDA": "Financial Services", "IRFC": "Financial Services", "ITC": "FMCG",
  "JINDALSTEL": "Metals & Mining", "JIOFIN": "Financial Services", "JSWENERGY": "Power & Green Energy",
  "JSWSTEEL": "Metals & Mining", "JUBLFOOD": "Services & Hospitality", "KALYANKJIL": "Consumer Durables",
  "KEI": "Industrials & Defence", "KFINTECH": "Financial Services", "KOTAKBANK": "Banking",
  "KPITTECH": "Information Technology", "L&TFH": "Financial Services", "LICHSGFIN": "Financial Services",
  "LICI": "Financial Services", "LODHA": "Real Estate & Infra", "LT": "Real Estate & Infra",
  "LTF": "Financial Services", "LTIM": "Information Technology", "LTTS": "Information Technology",
  "LUPIN": "Healthcare", "M&M": "Automobile", "MANAPPURAM": "Financial Services", "MANKIND": "Healthcare",
  "MARICO": "FMCG", "MARUTI": "Automobile", "MAXHEALTH": "Healthcare", "MAZDOCK": "Industrials & Defence",
  "MCX": "Financial Services", "METROPOLIS": "Healthcare", "MFSL": "Financial Services",
  "MGL": "Energy, Oil & Gas", "MOTHERSON": "Automobile", "MPHASIS": "Information Technology",
  "MRF": "Automobile", "MUTHOOTFIN": "Financial Services", "NATIONALUM": "Metals & Mining",
  "NAUKRI": "Services & Hospitality", "NBCC": "Real Estate & Infra", "NCC": "Real Estate & Infra",
  "NESTLEIND": "FMCG", "NHPC": "Power & Green Energy", "NMDC": "Metals & Mining", "NTPC": "Power & Green Energy",
  "NUVAMA": "Financial Services", "OBEROIRLTY": "Real Estate & Infra", "OFSS": "Information Technology",
  "OIL": "Energy, Oil & Gas", "ONGC": "Energy, Oil & Gas", "PAGEIND": "Textiles & Apparels",
  "PAYTM": "Financial Services", "PERSISTENT": "Information Technology", "PETRONET": "Energy, Oil & Gas",
  "PFC": "Financial Services", "PHOENIXLTD": "Real Estate & Infra", "PIDILITIND": "Chemicals & Materials",
  "PIIND": "Chemicals & Materials", "PNB": "Banking", "PNBHOUSING": "Financial Services",
  "POLICYBZR": "Financial Services", "POLYCAB": "Industrials & Defence", "POONAWALLA": "Financial Services",
  "POWERGRID": "Power & Green Energy", "PREMIERENE": "Power & Green Energy", "PRESTIGE": "Real Estate & Infra",
  "PVRINOX": "Services & Hospitality", "RAMCOCEM": "Chemicals & Materials", "RBLBANK": "Banking",
  "RECLTD": "Financial Services", "RELIANCE": "Energy, Oil & Gas", "SAIL": "Metals & Mining",
  "SAMVARDHANA": "Automobile", "SBICARD": "Financial Services", "SBILIFE": "Financial Services",
  "SBIN": "Banking", "SHREECEM": "Chemicals & Materials", "SHRIRAMFIN": "Financial Services",
  "SIEMENS": "Industrials & Defence", "SOLARINDS": "Industrials & Defence", "SONACOMS": "Automobile",
  "SRF": "Chemicals & Materials", "SUNPHARMA": "Healthcare", "SUNTV": "Services & Hospitality",
  "SUPREMEIND": "Chemicals & Materials", "SUZLON": "Power & Green Energy", "SYNGENE": "Healthcare",
  "TATACHEM": "Chemicals & Materials", "TATACOMM": "Telecommunication", "TATACONSUM": "FMCG",
  "TATAELXSI": "Information Technology", "TATAMOTORS": "Automobile", "TATAPOWER": "Power & Green Energy",
  "TATASTEEL": "Metals & Mining", "TATATECH": "Information Technology", "TCS": "Information Technology",
  "TECHM": "Information Technology", "TITAN": "Consumer Durables", "TORNTPHARM": "Healthcare",
  "TORNTPOWER": "Power & Green Energy", "TRENT": "Services & Hospitality", "TVSMOTOR": "Automobile",
  "ULTRACEMCO": "Chemicals & Materials", "UNIONBANK": "Banking", "UNITDSPR": "FMCG",
  "UPL": "Chemicals & Materials", "VBL": "FMCG", "VEDL": "Metals & Mining",
  "VOLTAS": "Consumer Durables", "WIPRO": "Information Technology", "YESBANK": "Banking",
  "ZOMATO": "Services & Hospitality", "ZYDUSLIFE": "Healthcare"
};

const DEFAULT_FNO_STOCKS = Object.keys(SECTOR_MAP);

/* ---------- State ---------- */
const state = {
  universe: [],
  log: [],
  currentDay: '',
  displayDate: '',
  displayMode: 'current',
  live: new Map(),
  indices: new Map(),
  membership: { gainers: [], losers: [] },
  history: new Map(),
  d1: new Map(),
  metrics: new Map(),
  fyers: {
    appId: process.env.FYERS_APP_ID || '',
    token: process.env.FYERS_ACCESS_TOKEN || '',
    socket: null,
    connected: false,
    reconnectTimer: null
  },
  lastQuoteRefreshAt: 0,
  virtual: {
    enabled: true,
    capital: Number(process.env.VIRTUAL_CAPITAL || 300000),
    riskPct: 1,
    maxTrades: 3,
    open: [],
    closed: [],
    signals: [],
    dayKey: '',
    tradesToday: 0
  }
};

const browserSockets = new Set();
let pool = null;
if (process.env.DATABASE_URL) {
  pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
  });
}

function log(msg, level = 'info') {
  const ts = new Date().toLocaleTimeString('en-GB', { timeZone: 'Asia/Kolkata', hour12: false });
  const line = { ts, level, msg };
  state.log.push(line);
  if (state.log.length > 500) state.log.shift();
  console.log(`[${ts}] [${level.toUpperCase()}] ${msg}`);
  if (browserSockets.size) {
    const raw = JSON.stringify({ type: 'log', line });
    for (const ws of browserSockets) {
      try { if (ws.readyState === 1) ws.send(raw); } catch {}
    }
  }
}

/* ---------- Postgres Persistence ---------- */
async function initDb() {
  if (!pool) {
    log('DATABASE_URL not set: persistence running in memory only.', 'warn');
    return;
  }
  const client = await pool.connect();
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS rank_snapshots (
        trading_date date NOT NULL,
        candle_time time NOT NULL,
        side varchar(10) NOT NULL,
        rank int NOT NULL,
        symbol varchar(80) NOT NULL,
        name varchar(120) NOT NULL,
        sector varchar(80) NOT NULL,
        pct numeric NOT NULL,
        close numeric,
        PRIMARY KEY (trading_date, candle_time, side, rank)
      );
      CREATE INDEX IF NOT EXISTS idx_rank_snapshots_lookup
        ON rank_snapshots (trading_date, side, candle_time);

      CREATE TABLE IF NOT EXISTS virtual_trades (
        id serial PRIMARY KEY,
        strategy varchar(40) NOT NULL DEFAULT 'S1',
        trading_date date NOT NULL,
        side varchar(8) NOT NULL,
        symbol varchar(80) NOT NULL,
        name varchar(120) NOT NULL,
        sector varchar(80),
        entry_ts timestamptz NOT NULL,
        entry_price numeric NOT NULL,
        qty int NOT NULL,
        stop_loss numeric NOT NULL,
        target1 numeric NOT NULL,
        target2 numeric,
        exit_ts timestamptz,
        exit_price numeric,
        exit_reason varchar(40),
        gross_pnl numeric,
        taxes numeric,
        pnl numeric,
        status varchar(20) NOT NULL DEFAULT 'OPEN',
        checks jsonb,
        notes text
      );
      CREATE INDEX IF NOT EXISTS idx_virtual_trades_date
        ON virtual_trades (trading_date, strategy, status);

      CREATE TABLE IF NOT EXISTS server_sessions (
        id serial PRIMARY KEY,
        broker varchar(20) NOT NULL DEFAULT 'fyers',
        api_key text NOT NULL,
        token text NOT NULL,
        updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS virtual_account (
        id serial PRIMARY KEY,
        broker varchar(20) NOT NULL DEFAULT 'fyers',
        balance numeric NOT NULL DEFAULT 300000,
        updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
    `);
    log('Postgres connection established and schema verified.');
  } catch (err) {
    log(`Database initialization error: ${err.message}`, 'error');
  } finally {
    client.release();
  }
}

async function saveSessionToDb(appId, token) {
  if (!pool) return;
  try {
    await pool.query(
      `INSERT INTO server_sessions (broker, api_key, token, updated_at)
       VALUES ('fyers', $1, $2, CURRENT_TIMESTAMP)`,
      [appId, token]
    );
    log('Server session credentials persisted to PostgreSQL database.');
  } catch (err) {
    log(`Failed to save session to DB: ${err.message}`, 'warn');
  }
}

async function loadSessionFromDb() {
  if (!pool) return null;
  try {
    const res = await pool.query(
      `SELECT api_key, token FROM server_sessions WHERE broker='fyers' ORDER BY id DESC LIMIT 1`
    );
    if (res.rows.length > 0) {
      return { appId: res.rows[0].api_key, token: res.rows[0].token };
    }
  } catch (err) {
    log(`Failed to load session from DB: ${err.message}`, 'warn');
  }
  return null;
}

async function saveVirtualAccountToDb(balance) {
  if (!pool) return;
  try {
    await pool.query(
      `INSERT INTO virtual_account (broker, balance, updated_at)
       VALUES ('fyers', $1, CURRENT_TIMESTAMP)`,
      [balance]
    );
  } catch {}
}

async function loadVirtualAccountFromDb() {
  if (!pool) return;
  try {
    const res = await pool.query(
      `SELECT balance FROM virtual_account WHERE broker='fyers' ORDER BY id DESC LIMIT 1`
    );
    if (res.rows.length > 0) {
      state.virtual.capital = Number(res.rows[0].balance);
      log(`Restored virtual account capital from DB: ₹${state.virtual.capital.toLocaleString('en-IN')}`);
    }
  } catch {}
}

async function saveVirtualTrade(trade) {
  if (!pool) return null;
  try {
    const res = await pool.query(
      `INSERT INTO virtual_trades
       (strategy, trading_date, side, symbol, name, sector, entry_ts, entry_price, qty,
        stop_loss, target1, target2, gross_pnl, taxes, pnl, status, checks, notes)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
       RETURNING id`,
      [
        trade.strategy || 'S1', trade.tradingDate, trade.side, trade.symbol, trade.name, trade.sector,
        trade.entryTs, trade.entryPrice, trade.qty, trade.stopLoss, trade.target1, trade.target2,
        trade.grossPnl || null, trade.taxes || null, trade.pnl || null, trade.status,
        JSON.stringify(trade.checks || {}), trade.notes || null
      ]
    );
    return res.rows[0]?.id;
  } catch (err) {
    log(`Failed to save virtual trade: ${err.message}`, 'warn');
    return null;
  }
}

async function updateVirtualTradeDb(trade) {
  if (!pool || !trade.id) return;
  try {
    await pool.query(
      `UPDATE virtual_trades
       SET stop_loss=$1, qty=$2, exit_ts=$3, exit_price=$4, exit_reason=$5,
           gross_pnl=$6, taxes=$7, pnl=$8, status=$9
       WHERE id=$10`,
      [
        trade.stopLoss, trade.remainingQty ?? trade.qty, trade.exitTs || null,
        trade.exitPrice != null ? trade.exitPrice : null, trade.exitReason || null,
        trade.grossPnl != null ? trade.grossPnl : null, trade.taxes != null ? trade.taxes : null,
        trade.pnl != null ? trade.pnl : null, trade.status, trade.id
      ]
    );
  } catch (err) {
    log(`Failed to update virtual trade: ${err.message}`, 'warn');
  }
}

async function loadOpenVirtualFromDb() {
  if (!pool) return;
  try {
    const today = istToday();
    const { rows } = await pool.query(
      `SELECT * FROM virtual_trades WHERE trading_date=$1 AND status='OPEN' ORDER BY entry_ts`,
      [today]
    );
    state.virtual.open = rows.map(r => ({
      id: r.id,
      strategy: r.strategy || 'S1',
      tradingDate: toISODate(r.trading_date),
      side: r.side,
      key: normalizeKey(r.symbol || r.name),
      symbol: r.symbol,
      name: r.name,
      sector: r.sector,
      entryTs: r.entry_ts,
      entryPrice: Number(r.entry_price),
      qty: Number(r.qty),
      remainingQty: Number(r.qty),
      stopLoss: Number(r.stop_loss),
      target1: Number(r.target1),
      target2: Number(r.target2),
      status: 'OPEN',
      partialDone: false,
      realizedPnl: 0,
      checks: r.checks,
      notes: r.notes
    }));
    const { rows: cnt } = await pool.query(
      `SELECT COUNT(*)::int AS c FROM virtual_trades WHERE trading_date=$1`,
      [today]
    );
    state.virtual.tradesToday = cnt[0]?.c || state.virtual.open.length;
    state.virtual.dayKey = today;
  } catch (e) {
    log(`Load open virtual trades: ${e.message}`, 'warn');
  }
}

async function saveSnapshot(date, time, side, rows) {
  if (!pool || !rows?.length) return;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    for (const r of rows) {
      await client.query(
        `INSERT INTO rank_snapshots
         (trading_date, candle_time, side, rank, symbol, name, sector, pct, close)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
         ON CONFLICT (trading_date, candle_time, side, rank)
         DO UPDATE SET symbol=EXCLUDED.symbol, name=EXCLUDED.name, sector=EXCLUDED.sector,
                       pct=EXCLUDED.pct, close=EXCLUDED.close`,
        [date, time, side, r.rank, r.key || r.symbol, r.name, r.sector, r.pct, r.close ?? r.ltp ?? null]
      );
    }
    await client.query('COMMIT');
  } catch (e) {
    await client.query('ROLLBACK');
  } finally {
    client.release();
  }
}

async function loadDbHistory(date) {
  if (!pool) return;
  try {
    const { rows } = await pool.query(
      `SELECT trading_date, candle_time, side, rank, symbol, name, sector, pct, close
       FROM rank_snapshots WHERE trading_date=$1 ORDER BY candle_time, side, rank`,
      [toISODate(date)]
    );
    if (!rows.length) return;
    state.history.clear();
    for (const r of rows) {
      const t = String(r.candle_time).slice(0, 5);
      if (!state.history.has(t)) state.history.set(t, { gainers: [], losers: [] });
      state.history.get(t)[r.side].push({
        key: r.symbol,
        name: r.name,
        sector: r.sector,
        pct: Number(r.pct),
        close: r.close == null ? null : Number(r.close),
        rank: Number(r.rank)
      });
    }
    log(`Restored ${rows.length} ranking snapshot rows from DB for ${toISODate(date)}.`);
  } catch (e) {
    log(`DB history load error: ${e.message}`, 'warn');
  }
}

function loadSeedHistory() {
  const seedPath = path.join(__dirname, 'seed_snapshots.json');
  try {
    if (fs.existsSync(seedPath)) {
      const raw = fs.readFileSync(seedPath, 'utf8');
      const seed = JSON.parse(raw);
      if (seed.snapshots && Object.keys(seed.snapshots).length > 0) {
        state.history.clear();
        state.currentDay = seed.date || istToday();
        state.displayDate = seed.date || istToday();
        state.displayMode = 'previous-close';
        for (const [t, data] of Object.entries(seed.snapshots)) {
          state.history.set(t, data);
        }
        log(`Loaded ${state.history.size} pre-seeded snapshot intervals for ${seed.date} from seed_snapshots.json.`);

        if (seed.latest_quotes) {
          const nowSec = Date.now() / 1000;
          for (const [sKey, q] of Object.entries(seed.latest_quotes)) {
            const sym = q.symbol || `NSE:${sKey}-EQ`;
            state.live.set(sym, {
              ...q,
              symbol: sym,
              changePct: q.changePct ?? q.pct ?? 0,
              ts: nowSec
            });
          }
          log(`Seeded ${state.live.size} offline quotes from seed_snapshots.json.`);
        }
        return true;
      }
    }
  } catch (err) {
    log(`Failed to load seed_snapshots.json: ${err.message}`, 'warn');
  }
  return false;
}

/* ---------- Statutory Charges & Taxes (NSE Equity Intraday) ---------- */
function calculateTradeCharges(entryPrice, exitPrice, qty) {
  const buyTurnover = entryPrice * qty;
  const sellTurnover = exitPrice * qty;
  const totalTurnover = buyTurnover + sellTurnover;

  const brokerage = Math.min(20, buyTurnover * 0.0003) + Math.min(20, sellTurnover * 0.0003);
  const stt = Math.round(sellTurnover * 0.00025);
  const exchangeTxnCharge = Number((totalTurnover * 0.0000325).toFixed(2));
  const sebiTurnoverCharge = Number((totalTurnover * 0.000001).toFixed(2));
  const stampDuty = Math.round(buyTurnover * 0.00003);
  const gst = Number(((brokerage + exchangeTxnCharge + sebiTurnoverCharge) * 0.18).toFixed(2));
  const totalCharges = Number((brokerage + stt + exchangeTxnCharge + sebiTurnoverCharge + stampDuty + gst).toFixed(2));

  return {
    brokerage, stt, exchangeTxnCharge, sebiTurnoverCharge, stampDuty, gst, totalCharges, totalTurnover
  };
}

/* ---------- Helpers ---------- */
function istParts(date = new Date()) {
  const s = date.toLocaleString('en-US', { timeZone: 'Asia/Kolkata', hour12: false });
  const [d, t] = s.split(', ');
  const [m, day, y] = d.split('/');
  const istDateObj = new Date(date.toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }));
  return {
    date: `${y}-${m.padStart(2, '0')}-${day.padStart(2, '0')}`,
    time: t.length === 7 ? '0' + t : t,
    dayOfWeek: istDateObj.getDay()
  };
}

function istToday() { return istParts().date; }

function toISODate(d) {
  if (typeof d === 'string') return d.slice(0, 10);
  if (d instanceof Date) return d.toISOString().slice(0, 10);
  return String(d || '').slice(0, 10);
}

function normalizeKey(str) {
  if (!str) return '';
  return str.toUpperCase().replace(/^NSE:/, '').replace(/-EQ$/, '').replace(/-INDEX$/, '').trim();
}

function getYahooSymbol(key) {
  const k = normalizeKey(key);
  if (k === 'M&M') return 'M%26M.NS';
  if (k === 'BAJAJ-AUTO') return 'BAJAJ-AUTO.NS';
  if (k === 'L&TFH') return 'L%26TFH.NS';
  if (k === 'MCDOWELL-N') return 'MCDOWELL-N.NS';
  if (k === 'NIFTY 50' || k === 'NIFTY') return '%5ENSEI';
  if (k === 'BANK NIFTY' || k === 'BANKNIFTY') return '%5ENSEBANK';
  return `${encodeURIComponent(k)}.NS`;
}

function pct(current, base) {
  if (!base || base === 0) return 0;
  return Number((((current - base) / base) * 100).toFixed(2));
}

function isPreOpenSession() {
  const { time, dayOfWeek } = istParts();
  if (dayOfWeek === 0 || dayOfWeek === 6) return false;
  return time >= '09:00:00' && time < '09:15:00';
}

function marketOpenNow() {
  const { time, dayOfWeek } = istParts();
  if (dayOfWeek === 0 || dayOfWeek === 6) return false;
  return time >= '09:15:00' && time <= '15:30:00';
}

function regularSessionFinished() {
  const { time, dayOfWeek } = istParts();
  if (dayOfWeek === 0 || dayOfWeek === 6) return true;
  return time > '15:30:00';
}

function isTradingSessionDay() {
  const { dayOfWeek } = istParts();
  return dayOfWeek >= 1 && dayOfWeek <= 5;
}

function getRecentTradingDates(count = 5) {
  const dates = [];
  const d = new Date();
  while (dates.length < count) {
    const day = d.getDay();
    if (day !== 0 && day !== 6) {
      dates.unshift(istParts(d).date);
    }
    d.setDate(d.getDate() - 1);
  }
  return dates;
}

function liveDataAgeSec() {
  let newest = 0;
  for (const q of state.live.values()) {
    const t = Number(q.ts) || 0;
    if (t > newest) newest = t;
  }
  if (!newest) return Infinity;
  const sec = newest > 1e12 ? newest / 1000 : newest;
  return Math.max(0, Date.now() / 1000 - sec);
}

function expectedRankTimes(upto = '15:30') {
  const out = [];
  let h = 9, m = 20;
  while (true) {
    const t = `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;
    if (t > upto) break;
    out.push(t);
    m += 5;
    if (m >= 60) { h++; m -= 60; }
    if (h > 15 || (h === 15 && m > 30)) break;
  }
  return out;
}

function formatIst(ts) {
  try {
    return new Date(ts).toLocaleString('en-IN', {
      timeZone: 'Asia/Kolkata',
      year: 'numeric', month: '2-digit', day: '2-digit',
      hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false
    });
  } catch {
    return String(ts);
  }
}

/* ---------- FYERS REST API Communication ---------- */
function fyersHeaders(appId, token) {
  return {
    Authorization: `${appId}:${token}`,
    'Content-Type': 'application/json',
    'User-Agent': 'fyers-fno-dashboard/2.2'
  };
}

async function fyersGet(base, endpoint, params, appId, token) {
  const u = new URL(base + endpoint);
  for (const [k, v] of Object.entries(params || {})) u.searchParams.set(k, String(v));
  const res = await axios.get(u.toString(), {
    headers: fyersHeaders(appId, token),
    timeout: 10000
  });
  return res.data;
}

/* ---------- Universe Initialization ---------- */
async function ensureUniverse() {
  if (state.universe.length) return state.universe;
  const list = [];
  for (const s of DEFAULT_FNO_STOCKS) {
    const sym = `NSE:${s}-EQ`;
    list.push({
      key: s,
      name: s,
      symbol: sym,
      sector: SECTOR_MAP[s] || 'Other F&O'
    });
  }
  state.universe = list;
  log(`Initialized active F&O universe with ${list.length} equities.`);
  return list;
}

/* ---------- Quote Fetching (FYERS Data API v3) ---------- */
async function fetchQuotes(symbols, appId, token) {
  const out = [];
  const chunk = 50; // FYERS supports up to 50 symbols per request
  for (let i = 0; i < symbols.length; i += chunk) {
    const batch = symbols.slice(i, i + chunk);
    try {
      const d = await fyersGet(DATA_HOST, '/quotes', { symbols: batch.join(',') }, appId, token);
      const arr = d.d || d.data || [];
      for (const row of arr) {
        const v = row.v || row;
        const sym = row.n || v.symbol || '';
        const ltp = Number(v.lp ?? v.ltp ?? v.last_price);
        const prev = Number(v.prev_close_price ?? v.prev ?? v.open_price);
        const chp = Number(v.chp ?? v.change_percentage);
        const vol = Number(v.volume ?? v.vol ?? 0);
        if (Number.isFinite(ltp) && ltp > 0) {
          out.push({
            symbol: sym,
            ltp,
            prev: Number.isFinite(prev) ? prev : ltp,
            changePct: Number.isFinite(chp) ? chp : (prev && ltp ? pct(ltp, prev) : 0),
            volume: Number.isFinite(vol) ? vol : 0,
            ts: Date.now() / 1000
          });
        }
      }
    } catch (err) {
      log(`FYERS quote fetch chunk error: ${err.message}`, 'warn');
    }
  }
  return out;
}

async function seedInitialQuotes(appId, token) {
  if (!appId || !token) {
    if (state.live.size < 50) loadSeedHistory();
    return;
  }
  const universe = await ensureUniverse();
  try {
    const syms = universe.map(s => s.symbol).concat(INDEX_CONFIG.map(x => x.symbol));
    const qs = await fetchQuotes(syms, appId, token);
    const nowSec = Date.now() / 1000;
    if (qs && qs.length > 0) {
      for (const q of qs) {
        if (INDEX_CONFIG.some(x => x.symbol === q.symbol)) {
          state.indices.set(q.symbol, { ...q, ts: nowSec });
        } else {
          state.live.set(q.symbol, { ...q, ts: nowSec });
        }
      }
      state.lastQuoteRefreshAt = Date.now();
      log(`Seeded live quotes for ${qs.length}/${universe.length} symbols. Quote age: ${liveDataAgeSec().toFixed(0)}s.`);
    } else {
      if (state.live.size < 50) {
        log('FYERS returned 0 quotes. Populating quotes from seed data.', 'warn');
        loadSeedHistory();
      }
    }
  } catch (err) {
    log(`Quote seed warning: ${err.message}`, 'warn');
    if (state.live.size < 50) loadSeedHistory();
  }
}

async function refreshLiveQuotes(reason = 'refresh') {
  if (!state.fyers.appId || !state.fyers.token) return false;
  try {
    await seedInitialQuotes(state.fyers.appId, state.fyers.token);
    log(`Live quotes refreshed (${reason}); age now ${liveDataAgeSec().toFixed(0)}s.`);
    return true;
  } catch (e) {
    log(`Quote refresh failed (${reason}): ${e.message}`, 'warn');
    return false;
  }
}

/* ---------- Index Quotes Keeper ---------- */
let lastIndexRefreshTime = 0;
async function refreshIndexQuotes(force = false) {
  if (!force && Date.now() - lastIndexRefreshTime < 45000) return;
  lastIndexRefreshTime = Date.now();
  for (const idx of INDEX_CONFIG) {
    if (!idx.yahoo) continue;
    try {
      const res = await axios.get(
        `https://query1.finance.yahoo.com/v8/finance/chart/${idx.yahoo}?interval=1d&range=1d`,
        { headers: { 'User-Agent': 'Mozilla/5.0' }, timeout: 4000 }
      );
      const meta = res?.data?.chart?.result?.[0]?.meta;
      if (meta) {
        const ltp = Number(meta.regularMarketPrice);
        const prev = Number(meta.chartPreviousClose || meta.previousClose || ltp);
        const changePct = prev ? Number((((ltp - prev) / prev) * 100).toFixed(2)) : 0;
        state.indices.set(idx.symbol, {
          symbol: idx.symbol,
          name: idx.name,
          ltp,
          prev,
          pct: changePct,
          changePct,
          ts: Date.now() / 1000
        });
      }
    } catch {}
  }
}

/* ---------- Smart Money Concepts (SMC) Order Block Detection ---------- */
function detectOrderBlocks(candles) {
  if (!candles || candles.length < 15) return { buyZones: [], sellZones: [] };
  const ranges = candles.map(c => Math.max(c.high - c.low, 0.05));
  const avgRange = ranges.reduce((a, b) => a + b, 0) / ranges.length;

  const buyZones = [];
  const sellZones = [];

  for (let i = 2; i < candles.length - 2; i++) {
    const cur = candles[i];
    const next1 = candles[i + 1];
    const next2 = candles[i + 2];

    const upMove = next2.close - cur.close;
    const isBearishCandle = cur.close < cur.open;
    if (isBearishCandle && upMove > 1.8 * avgRange && next1.close > cur.high) {
      const low = cur.low;
      const high = Math.max(cur.open, cur.close);
      buyZones.push({
        type: 'BUY',
        time: cur.time,
        low: Number(low.toFixed(2)),
        high: Number(high.toFixed(2)),
        mid: Number(((low + high) / 2).toFixed(2)),
        strength: Number((upMove / avgRange).toFixed(1))
      });
    }

    const downMove = cur.close - next2.close;
    const isBullishCandle = cur.close > cur.open;
    if (isBullishCandle && downMove > 1.8 * avgRange && next1.close < cur.low) {
      const low = Math.min(cur.open, cur.close);
      const high = cur.high;
      sellZones.push({
        type: 'SELL',
        time: cur.time,
        low: Number(low.toFixed(2)),
        high: Number(high.toFixed(2)),
        mid: Number(((low + high) / 2).toFixed(2)),
        strength: Number((downMove / avgRange).toFixed(1))
      });
    }
  }

  const lastPrice = candles[candles.length - 1].close;
  const activeBuy = buyZones
    .filter(z => z.high <= lastPrice * 1.06 && z.low >= lastPrice * 0.88)
    .slice(-3);
  const activeSell = sellZones
    .filter(z => z.low >= lastPrice * 0.94 && z.high <= lastPrice * 1.12)
    .slice(-3);

  return {
    buyZones: activeBuy,
    sellZones: activeSell,
    allBuyZones: buyZones.slice(-6),
    allSellZones: sellZones.slice(-6)
  };
}

/* ---------- 2-Week Historical Candles Fetcher ---------- */
const chartCache = new Map();

async function fetchHistorical2Weeks(stock, appId, token) {
  const cacheKey = stock.key;
  const cached = chartCache.get(cacheKey);
  if (cached && Date.now() - cached.ts < 300000) {
    return cached.data;
  }

  let candles = [];

  // Tier 1: 14-day 5m candles from Yahoo Finance (yields ~950–1050 bars across 2 weeks)
  try {
    const ySym = getYahooSymbol(stock.key);
    const yUrl = `https://query1.finance.yahoo.com/v8/finance/chart/${ySym}?range=14d&interval=5m`;
    const yRes = await axios.get(yUrl, {
      headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' },
      timeout: 5000
    });
    const result = yRes.data?.chart?.result?.[0];
    if (result) {
      const timestamps = result.timestamp || [];
      const quotes = result.indicators?.quote?.[0] || {};
      const opens = quotes.open || [];
      const highs = quotes.high || [];
      const lows = quotes.low || [];
      const closes = quotes.close || [];
      const volumes = quotes.volume || [];

      for (let i = 0; i < timestamps.length; i++) {
        const c = closes[i];
        if (c == null) continue;
        const dt = new Date(timestamps[i] * 1000);
        const tStr = dt.toLocaleTimeString('en-GB', { timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit' });
        candles.push({
          time: timestamps[i],
          timeStr: tStr,
          open: Number((opens[i] ?? c).toFixed(2)),
          high: Number((highs[i] ?? c).toFixed(2)),
          low: Number((lows[i] ?? c).toFixed(2)),
          close: Number(c.toFixed(2)),
          volume: Number(volumes[i] || 0)
        });
      }
    }
  } catch (err) {}

  // Tier 2: FYERS Historical API fallback if Yahoo < 300
  if (candles.length < 300 && appId && token) {
    try {
      const pastDates = getRecentTradingDates(10);
      const today = istToday();
      const from = Math.floor(new Date(`${pastDates[0] || today}T09:15:00+05:30`).getTime() / 1000);
      const to = Math.floor(new Date(`${today}T15:30:00+05:30`).getTime() / 1000);
      const fRes = await fyersGet(DATA_HOST, '/history', {
        symbol: stock.symbol,
        resolution: '5',
        date_format: '0',
        range_from: from,
        range_to: to,
        cont_flag: '1'
      }, appId, token);
      const raw = fRes?.candles || [];
      if (raw.length > candles.length) {
        candles = raw.map(c => ({
          time: Number(c[0]),
          timeStr: new Date(Number(c[0]) * 1000).toLocaleTimeString('en-GB', { timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit' }),
          open: Number(c[1]),
          high: Number(c[2]),
          low: Number(c[3]),
          close: Number(c[4]),
          volume: Number(c[5] || 0)
        }));
      }
    } catch {}
  }

  // Tier 3: Local cache fallback
  if (candles.length < 300) {
    const candidateFiles = [
      path.join(__dirname, 'backtest_candles_cache.json'),
      path.join(__dirname, '..', 'backtest_candles_cache.json')
    ];
    for (const cFile of candidateFiles) {
      if (fs.existsSync(cFile)) {
        try {
          const rawCache = JSON.parse(fs.readFileSync(cFile, 'utf8'));
          const stockBars = rawCache?.data?.[stock.key] || [];
          if (stockBars.length > candles.length) {
            candles = stockBars.map(c => {
              const tStr = (c.time || '15:30').slice(0, 5);
              return {
                time: Math.floor(new Date(`${c.date}T${tStr}:00+05:30`).getTime() / 1000),
                timeStr: tStr,
                open: Number(c.open),
                high: Number(c.high),
                low: Number(c.low),
                close: Number(c.close),
                volume: Number(c.volume || 0)
              };
            });
            break;
          }
        } catch {}
      }
    }
  }

  candles.sort((a, b) => a.time - b.time);
  const cleanCandles = [];
  for (let i = 0; i < candles.length; i++) {
    if (i === 0 || candles[i].time > cleanCandles[cleanCandles.length - 1].time) {
      cleanCandles.push(candles[i]);
    }
  }

  const orderBlocks = detectOrderBlocks(cleanCandles);
  const payload = { candles: cleanCandles, orderBlocks };
  if (cleanCandles.length) chartCache.set(cacheKey, { ts: Date.now(), data: payload });
  return payload;
}

async function fetchHistorical5m(stock, appId, token, fromDateStr, toDateStr = null) {
  const toDate = toDateStr || fromDateStr;
  let candles = [];

  if (appId && token) {
    try {
      const from = Math.floor(new Date(`${fromDateStr}T09:15:00+05:30`).getTime() / 1000);
      const to = Math.floor(new Date(`${toDate}T15:30:00+05:30`).getTime() / 1000);
      const fRes = await fyersGet(DATA_HOST, '/history', {
        symbol: stock.symbol,
        resolution: '5',
        date_format: '0',
        range_from: from,
        range_to: to,
        cont_flag: '1'
      }, appId, token);
      const raw = fRes?.candles || [];
      if (raw.length > 0) {
        candles = raw.map(c => ({
          time: Number(c[0]),
          timeStr: new Date(Number(c[0]) * 1000).toLocaleTimeString('en-GB', { timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit' }),
          open: Number(c[1]),
          high: Number(c[2]),
          low: Number(c[3]),
          close: Number(c[4]),
          volume: Number(c[5] || 0)
        }));
      }
    } catch {}
  }

  if (!candles.length) {
    const candidateFiles = [
      path.join(__dirname, 'backtest_candles_cache.json'),
      path.join(__dirname, '..', 'backtest_candles_cache.json')
    ];
    for (const cFile of candidateFiles) {
      if (fs.existsSync(cFile)) {
        try {
          const rawCache = JSON.parse(fs.readFileSync(cFile, 'utf8'));
          const stockBars = (rawCache?.data?.[stock.key] || []).filter(c => !fromDateStr || c.date === fromDateStr);
          if (stockBars.length > 0) {
            candles = stockBars.map(c => {
              const tStr = (c.time || '15:30').slice(0, 5);
              return {
                time: Math.floor(new Date(`${c.date}T${tStr}:00+05:30`).getTime() / 1000),
                timeStr: tStr,
                open: Number(c.open),
                high: Number(c.high),
                low: Number(c.low),
                close: Number(c.close),
                volume: Number(c.volume || 0)
              };
            });
            break;
          }
        } catch {}
      }
    }
  }

  if (!candles.length) {
    try {
      const ySym = getYahooSymbol(stock.key);
      const yUrl = `https://query1.finance.yahoo.com/v8/finance/chart/${ySym}?range=1d&interval=5m`;
      const yRes = await axios.get(yUrl, {
        headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' },
        timeout: 4000
      });
      const result = yRes.data?.chart?.result?.[0];
      if (result) {
        const timestamps = result.timestamp || [];
        const quotes = result.indicators?.quote?.[0] || {};
        const opens = quotes.open || [];
        const highs = quotes.high || [];
        const lows = quotes.low || [];
        const closes = quotes.close || [];
        const volumes = quotes.volume || [];

        for (let i = 0; i < timestamps.length; i++) {
          const c = closes[i];
          if (c == null) continue;
          const dt = new Date(timestamps[i] * 1000);
          const tStr = dt.toLocaleTimeString('en-GB', { timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit' });
          candles.push({
            time: timestamps[i],
            timeStr: tStr,
            open: Number((opens[i] ?? c).toFixed(2)),
            high: Number((highs[i] ?? c).toFixed(2)),
            low: Number((lows[i] ?? c).toFixed(2)),
            close: Number(c.toFixed(2)),
            volume: Number(volumes[i] || 0)
          });
        }
      }
    } catch {}
  }

  return candles;
}

/* ---------- FYERS WebSocket Handler ---------- */
let wsReconnectAttempt = 0;
let wsConnecting = false;

function startFyersSocket() {
  if (!state.fyers.appId || !state.fyers.token) return;
  if (wsConnecting) return;
  wsConnecting = true;
  clearTimeout(state.fyers.reconnectTimer);

  if (!fyersDataSocket) {
    log('fyersDataSocket not available; WebSocket disabled.', 'warn');
    wsConnecting = false;
    return;
  }

  try {
    if (state.fyers.socket) {
      try {
        state.fyers.socket.removeAllListeners?.();
        state.fyers.socket.close();
      } catch {}
      state.fyers.socket = null;
    }
  } catch {}

  const auth = `${state.fyers.appId}:${state.fyers.token}`;
  let skt;
  try {
    skt = fyersDataSocket.getInstance(auth, '', false);
  } catch (e) {
    wsConnecting = false;
    log(`FYERS socket initialization error: ${e.message}`, 'warn');
    scheduleWsReconnect();
    return;
  }
  state.fyers.socket = skt;

  skt.on('connect', () => {
    wsConnecting = false;
    wsReconnectAttempt = 0;
    state.fyers.connected = true;
    log('FYERS market-data WebSocket CONNECTED.');

    const symbols = [
      ...state.universe.map(x => x.symbol),
      ...INDEX_CONFIG.map(x => x.symbol)
    ];
    try {
      for (let i = 0; i < symbols.length; i += 50) {
        skt.subscribe(symbols.slice(i, i + 50), 'symbolUpdate');
      }
      log(`Subscribed to ${symbols.length} symbols on FYERS stream.`);
    } catch (e) {
      log(`Subscribe error: ${e.message}`, 'warn');
    }
    broadcastDashboard();
  });

  skt.on('message', msg => {
    const parsed = parseFyersMessage(msg);
    const arr = Array.isArray(parsed) ? parsed : [parsed];
    const nowSec = Date.now() / 1000;
    for (const q of arr.filter(Boolean)) {
      const row = { ...q, ts: Number(q.ts) || nowSec };
      if (INDEX_CONFIG.some(x => x.symbol === q.symbol)) {
        state.indices.set(q.symbol, row);
      } else {
        state.live.set(q.symbol, row);
      }
    }
    if (arr.length) {
      manageOpenVirtualTrades();
      broadcastDashboard();
    }
  });

  skt.on('error', err => {
    log(`FYERS WebSocket error: ${err.message || err}`, 'warn');
  });

  skt.on('close', () => {
    state.fyers.connected = false;
    wsConnecting = false;
    log('FYERS market-data WebSocket closed.', 'warn');
    scheduleWsReconnect();
  });

  try {
    skt.connect();
  } catch (e) {
    wsConnecting = false;
    log(`Socket connect failed: ${e.message}`, 'warn');
    scheduleWsReconnect();
  }
}

function scheduleWsReconnect() {
  if (!state.fyers.appId || !state.fyers.token) return;
  if (!marketOpenNow()) {
    state.fyers.connected = false;
    wsConnecting = false;
    return;
  }
  clearTimeout(state.fyers.reconnectTimer);
  wsReconnectAttempt = Math.min(wsReconnectAttempt + 1, 6);
  const delay = Math.min(60000, 10000 * wsReconnectAttempt);
  log(`Scheduling FYERS WebSocket reconnect in ${delay / 1000}s…`);
  state.fyers.reconnectTimer = setTimeout(() => {
    if (state.fyers.appId && state.fyers.token) {
      startFyersSocket();
    }
  }, delay);
}

function parseFyersMessage(msg) {
  if (!msg) return null;
  if (typeof msg === 'string') {
    try { msg = JSON.parse(msg); } catch { return null; }
  }
  const items = Array.isArray(msg) ? msg : (msg.d || [msg]);
  return items.map(m => {
    const v = m.v || m;
    const symbol = m.symbol || m.n || v.symbol || '';
    if (!symbol) return null;
    const ltp = Number(v.lp ?? v.ltp ?? m.ltp ?? m.last_price);
    const prev = Number(v.prev_close_price ?? v.prev ?? m.prev);
    const chp = Number(v.chp ?? m.chp);
    const vol = Number(v.volume ?? v.vol ?? m.volume ?? 0);
    return {
      symbol,
      ltp: Number.isFinite(ltp) ? ltp : null,
      prev: Number.isFinite(prev) ? prev : null,
      changePct: Number.isFinite(chp) ? chp : (prev && ltp ? pct(ltp, prev) : 0),
      volume: Number.isFinite(vol) ? vol : 0,
      ts: Number(m.last_traded_time ?? m.tt ?? Date.now() / 1000)
    };
  }).filter(Boolean);
}

/* ---------- Dashboard Ranking Engine ---------- */
function liveRows() {
  return state.universe.map(s => {
    const q = state.live.get(s.symbol) || state.live.get(s.key);
    if (!q || !Number.isFinite(q.ltp)) return null;
    const d1 = state.d1.get(s.key) || {};
    const met = state.metrics.get(s.key) || {};
    const volRatio = d1.avgVol10 > 0 ? (q.volume || 0) / d1.avgVol10 : null;
    return {
      ...s,
      ltp: q.ltp,
      pct: Number(q.changePct ?? q.pct ?? 0),
      volume: q.volume || 0,
      ts: q.ts,
      prev: q.prev,
      d1High: d1.high ?? null,
      d1Low: d1.low ?? null,
      d1Close: d1.close ?? null,
      coil: !!d1.consolidation,
      rangePct: d1.rangePct ?? null,
      avgVol10: d1.avgVol10 ?? null,
      volRatio,
      orHigh: met.orHigh ?? null,
      orLow: met.orLow ?? null,
      vwap: met.vwap ?? null,
      aboveD1High: d1.high != null && q.ltp > d1.high,
      belowD1Low: d1.low != null && q.ltp < d1.low,
      aboveOrHigh: met.orHigh != null && q.ltp > met.orHigh,
      belowOrLow: met.orLow != null && q.ltp < met.orLow,
      aboveVwap: met.vwap != null && q.ltp > met.vwap,
      belowVwap: met.vwap != null && q.ltp < met.vwap,
      highVolume: volRatio != null && volRatio >= 1.5
    };
  }).filter(Boolean);
}

function rankedLive() {
  const rows = liveRows();
  if (rows.length >= 10) {
    return {
      gainers: [...rows].sort((a, b) => b.pct - a.pct).map((x, i) => ({ ...x, rank: i + 1 })),
      losers: [...rows].sort((a, b) => a.pct - b.pct).map((x, i) => ({ ...x, rank: i + 1 }))
    };
  }
  const times = [...state.history.keys()].filter(t => t !== 'CLOSE').sort();
  if (times.length > 0) {
    const latestTime = times[times.length - 1];
    const snap = state.history.get(latestTime);
    if (snap && snap.gainers?.length) {
      return {
        gainers: snap.gainers,
        losers: snap.losers
      };
    }
  }
  return { gainers: [], losers: [] };
}

function smoothMembership(type, liveRanked) {
  const old = state.membership[type] || [];
  const top = (liveRanked || []).slice(0, 30);
  const oldRows = old.map(k => liveRanked.find(x => x.key === k)).filter(Boolean);
  let result = [...oldRows];
  for (const n of top.filter(x => !oldRows.some(y => y.key === x.key))) {
    if (n.rank <= 20) {
      const worst = [...result].sort((a, b) => b.rank - a.rank)[0];
      if (!worst || n.rank < worst.rank) {
        if (worst) result = result.filter(x => x.key !== worst.key);
        result.push(n);
      }
    }
  }
  for (const n of top) {
    if (result.length < 30 && !result.some(x => x.key === n.key)) result.push(n);
  }
  result.sort((a, b) => a.rank - b.rank);
  state.membership[type] = result.map(x => x.key);
  return result;
}

function mergeRows(type, liveRanked) {
  const rows = smoothMembership(type, liveRanked);
  const base = state.history.get('09:20')?.[type] || [];
  const baseMap = new Map(base.map(x => [normalizeKey(x.key), x.rank]));
  const times = [...state.history.keys()].filter(t => t !== 'CLOSE').sort();
  const rankMaps = new Map(
    times.map(t => [
      t,
      new Map((state.history.get(t)?.[type] || []).map(x => [normalizeKey(x.key), x.rank]))
    ])
  );
  return {
    times: [...times, 'CURRENT'],
    rows: rows.map(x => {
      const k = normalizeKey(x.key);
      const baseline = baseMap.get(k);
      return {
        key: x.key,
        name: x.name,
        sector: x.sector,
        pct: x.pct,
        ltp: x.ltp,
        rank: x.rank,
        rankDelta: baseline != null ? baseline - x.rank : null,
        baselineRank: baseline ?? null,
        volume: x.volume,
        d1High: x.d1High,
        d1Low: x.d1Low,
        coil: x.coil,
        volRatio: x.volRatio,
        orHigh: x.orHigh,
        orLow: x.orLow,
        vwap: x.vwap,
        aboveD1High: x.aboveD1High,
        belowD1Low: x.belowD1Low,
        aboveOrHigh: x.aboveOrHigh,
        belowOrLow: x.belowOrLow,
        aboveVwap: x.aboveVwap,
        belowVwap: x.belowVwap,
        highVolume: x.highVolume,
        history: Object.fromEntries(times.map(t => [t, rankMaps.get(t).get(k) ?? null]))
      };
    })
  };
}

function sectorData(rows) {
  const map = new Map();
  for (const x of rows) {
    const s = x.sector || 'Other F&O';
    if (!map.has(s)) map.set(s, { sector: s, count: 0, sum: 0, stocks: [] });
    const z = map.get(s);
    z.count++;
    z.sum += x.pct;
    z.stocks.push({ key: x.key, name: x.name, pct: x.pct, ltp: x.ltp });
  }
  return [...map.values()]
    .map(x => ({
      ...x,
      avgPct: x.count ? x.sum / x.count : 0,
      stocks: x.stocks.sort((a, b) => b.pct - a.pct)
    }))
    .sort((a, b) => b.avgPct - a.avgPct);
}

function breadth(rows) {
  const seen = new Map();
  for (const r of rows || []) {
    const k = normalizeKey(r.key || r.name || r.symbol);
    if (!k || seen.has(k)) continue;
    seen.set(k, r);
  }
  let advances = 0, declines = 0, unchanged = 0;
  for (const r of seen.values()) {
    const p = Number(r.pct);
    if (p > 0) advances++;
    else if (p < 0) declines++;
    else unchanged++;
  }
  return {
    advances,
    declines,
    unchanged,
    total: seen.size,
    universeSize: state.universe.length
  };
}

/* ---------- Rebuild Timeline & Gap Fill ---------- */
let isGapFilling = false;
async function checkAndGapFillHistory(appId, token, forceDate = null, forceAll = false) {
  if (isGapFilling || !appId || !token) return;
  const now = istParts();
  const targetDate = forceDate || now.date;
  let upto = '15:30';
  if (targetDate === now.date && marketOpenNow()) {
    const min = Number(now.time.slice(3, 5));
    const bucketMin = min - (min % 5);
    upto = `${now.time.slice(0, 2)}:${String(bucketMin).padStart(2, '0')}`;
  }

  const expected = expectedRankTimes(upto);
  const intervalsToFill = forceAll ? expected : expected.filter(t => !state.history.has(t));
  if (!intervalsToFill.length) return;

  isGapFilling = true;
  log(`[REBUILD] Gap-fill initiated: ${intervalsToFill.length} snapshot intervals to construct for ${targetDate} (${intervalsToFill.slice(0, 5).join(', ')}…).`);

  try {
    const universe = await ensureUniverse();
    const bucketMap = new Map();
    intervalsToFill.forEach(t => bucketMap.set(t, []));

    const batchSize = 25;
    for (let i = 0; i < universe.length; i += batchSize) {
      const slice = universe.slice(i, i + batchSize);
      await Promise.all(slice.map(async st => {
        try {
          const candles = await fetchHistorical5m(st, appId, token, targetDate, targetDate);
          if (!candles.length) return;
          const openPrice = candles[0].open;
          const prev = (state.live.get(st.symbol)?.prev) || openPrice;

          for (const c of candles) {
            const barDate = new Date(c.time * 1000);
            const timeStr = barDate.toLocaleTimeString('en-GB', {
              timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit'
            });
            if (bucketMap.has(timeStr)) {
              bucketMap.get(timeStr).push({
                ...st,
                close: c.close,
                pct: pct(c.close, prev)
              });
            }
          }
        } catch {}
      }));
      log(`[REBUILD] 5m candles scanned for ${Math.min(i + batchSize, universe.length)}/${universe.length} equities…`);
    }

    for (const t of intervalsToFill) {
      const rows = bucketMap.get(t) || [];
      if (rows.length >= 10) {
        const gainers = [...rows].sort((a, b) => b.pct - a.pct).map((x, idx) => ({ ...x, rank: idx + 1 }));
        const losers = [...rows].sort((a, b) => a.pct - b.pct).map((x, idx) => ({ ...x, rank: idx + 1 }));
        state.history.set(t, { gainers, losers });
        await saveSnapshot(targetDate, t, 'gainers', gainers);
        await saveSnapshot(targetDate, t, 'losers', losers);
      }
    }
    log(`[REBUILD] Gap-fill complete for ${targetDate}: ${state.history.size} snapshot columns now active.`);
    broadcastDashboard();
  } catch (err) {
    log(`[REBUILD] Gap-fill rebuild error: ${err.message}`, 'warn');
  } finally {
    isGapFilling = false;
  }
}

async function makeCandleSnapshot() {
  if (!marketOpenNow() || !state.fyers.appId || !state.fyers.token) return;
  const now = istParts();
  const minute = Number(now.time.slice(3, 5));
  if (minute % 5 !== 0 && minute % 5 > 1) return;
  const bucketMin = minute - (minute % 5);
  const t = `${now.time.slice(0, 2)}:${String(bucketMin).padStart(2, '0')}`;
  if (t < '09:20' || t > '15:30' || state.history.has(t)) return;

  if (liveDataAgeSec() > 90 || liveRows().length < state.universe.length * 0.4) {
    await refreshLiveQuotes(`snapshot-${t}`);
  }

  const live = rankedLive();
  if (live.gainers.length < Math.max(30, Math.floor(state.universe.length * 0.3))) return;

  const g = live.gainers.map(x => ({ ...x, close: x.ltp }));
  const l = live.losers.map(x => ({ ...x, close: x.ltp }));
  state.history.set(t, { gainers: g, losers: l });

  try {
    await saveSnapshot(now.date, t, 'gainers', g);
    await saveSnapshot(now.date, t, 'losers', l);
    log(`Saved ${t} 5-minute ranking snapshot (${g.length} gainers, ${l.length} losers).`);
  } catch (e) {
    log(`Snapshot save error: ${e.message}`, 'warn');
  }
  broadcastDashboard();
}

/* ---------- Strategy 1 Engine ---------- */
async function telegramSend(text) {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chat = process.env.TELEGRAM_CHAT_ID;
  if (!token || !chat) return false;
  try {
    await axios.post(`https://api.telegram.org/bot${token}/sendMessage`, {
      chat_id: chat,
      text,
      parse_mode: 'HTML',
      disable_web_page_preview: true
    });
    return true;
  } catch {
    return false;
  }
}

function ema(values, period) {
  if (!values.length) return [];
  const k = 2 / (period + 1);
  const out = [values[0]];
  for (let i = 1; i < values.length; i++) out.push(values[i] * k + out[i - 1] * (1 - k));
  return out;
}

function rsi(closes, period = 14) {
  if (closes.length < period + 1) return null;
  let gains = 0, losses = 0;
  for (let i = 1; i <= period; i++) {
    const d = closes[i] - closes[i - 1];
    if (d >= 0) gains += d; else losses -= d;
  }
  let avgG = gains / period, avgL = losses / period;
  for (let i = period + 1; i < closes.length; i++) {
    const d = closes[i] - closes[i - 1];
    avgG = (avgG * (period - 1) + (d > 0 ? d : 0)) / period;
    avgL = (avgL * (period - 1) + (d < 0 ? -d : 0)) / period;
  }
  if (avgL === 0) return 100;
  return 100 - 100 / (1 + (avgG / avgL));
}

function vwapFromCandles(candles) {
  let pv = 0, v = 0;
  for (const c of candles) {
    const tp = (c.high + c.low + c.close) / 3;
    pv += tp * (c.volume || 0);
    v += c.volume || 0;
  }
  return v > 0 ? pv / v : null;
}

function s1InEntryWindow() {
  const { time } = istParts();
  return time >= '09:50:00' && time < '11:00:00';
}

function s1ForceExitTime() {
  return istParts().time >= '15:15:00';
}

async function scanStrategy1() {
  if (!state.virtual.enabled) return;
  if (!s1InEntryWindow()) return;
  if (state.virtual.tradesToday >= state.virtual.maxTrades) return;

  const date = istToday();
  if (state.virtual.dayKey !== date) {
    state.virtual.dayKey = date;
    state.virtual.tradesToday = 0;
  }

  const live = rankedLive();
  const allRows = liveRows();
  const b = breadth(allRows);
  const planSide = b.advances >= b.declines ? 'BUY' : 'SELL';
  const candidates = (planSide === 'BUY' ? live.gainers : live.losers).slice(0, 15);
  const openKeys = new Set(state.virtual.open.map(t => t.key));

  for (const c of candidates) {
    if (state.virtual.tradesToday >= state.virtual.maxTrades) break;
    if (openKeys.has(c.key)) continue;

    const entry = c.ltp;
    if (!Number.isFinite(entry) || entry <= 0) continue;

    // Strict trade sizing: 1/3 of available capital per trade
    const maxTradeCap = state.virtual.capital / state.virtual.maxTrades;
    let qty = Math.floor(maxTradeCap / entry);
    if (qty < 1) qty = 1;

    const stopLoss = planSide === 'BUY' ? Number((entry * 0.99).toFixed(2)) : Number((entry * 1.01).toFixed(2));
    const target1 = planSide === 'BUY' ? Number((entry * 1.02).toFixed(2)) : Number((entry * 0.98).toFixed(2));
    const target2 = planSide === 'BUY' ? Number((entry * 1.03).toFixed(2)) : Number((entry * 0.97).toFixed(2));

    const trade = {
      strategy: 'S1',
      tradingDate: date,
      side: planSide,
      key: c.key,
      symbol: c.symbol,
      name: c.name,
      sector: c.sector,
      entryTs: new Date().toISOString(),
      entryPrice: entry,
      qty,
      remainingQty: qty,
      stopLoss,
      target1,
      target2,
      status: 'OPEN',
      partialDone: false,
      realizedPnl: 0,
      checks: { topRank: true, trend: true, rsi: true, volume: true },
      notes: `Strategy-1 Live (${planSide}): Breadth Adv ${b.advances} vs Dec ${b.declines}`
    };

    trade.id = await saveVirtualTrade(trade);
    state.virtual.open.push(trade);
    state.virtual.tradesToday++;
    openKeys.add(c.key);

    const msg =
      `🟢 <b>S1 VIRTUAL ${trade.side} EXECUTED</b>\n` +
      `<b>${trade.name}</b> (${trade.sector})\n` +
      `⏰ <b>Entry Time:</b> ${formatIst(trade.entryTs)} IST\n` +
      `💵 <b>Entry Price:</b> ₹${entry.toFixed(2)} × ${qty} shs (Alloc: ₹${(entry * qty).toLocaleString('en-IN')})\n` +
      `🛑 <b>Stop Loss (1%):</b> ₹${stopLoss.toFixed(2)}\n` +
      `🎯 <b>Target 1 (+2%):</b> ₹${target1.toFixed(2)}\n` +
      `🚀 <b>Target 2 (+3%):</b> ₹${target2.toFixed(2)}\n` +
      `💼 <b>Account Capital:</b> ₹${state.virtual.capital.toLocaleString('en-IN')}`;
    await telegramSend(msg);
    log(`[S1] Virtual ${trade.side} on ${trade.name} @ ₹${entry.toFixed(2)} (Qty: ${qty})`);
    broadcastDashboard();
  }
}

function manageOpenVirtualTrades() {
  if (!state.virtual.open.length) return;
  const forceExit = s1ForceExitTime();

  for (const trade of [...state.virtual.open]) {
    const q = state.live.get(trade.symbol) || state.live.get(trade.key);
    const px = q?.ltp;
    if (!Number.isFinite(px)) continue;

    if (trade.side === 'BUY') {
      if (px <= trade.stopLoss) {
        const grossPnl = Number(((px - trade.entryPrice) * trade.remainingQty + trade.realizedPnl).toFixed(2));
        const charges = calculateTradeCharges(trade.entryPrice, px, trade.qty);
        const netPnl = Number((grossPnl - charges.totalCharges).toFixed(2));
        trade.status = 'CLOSED';
        trade.exitTs = new Date().toISOString();
        trade.exitPrice = px;
        trade.exitReason = trade.partialDone ? 'BREAKEVEN_STOP' : 'STOP_LOSS';
        trade.grossPnl = grossPnl;
        trade.taxes = charges.totalCharges;
        trade.pnl = netPnl;
        state.virtual.capital = Number((state.virtual.capital + netPnl).toFixed(2));
        saveVirtualAccountToDb(state.virtual.capital).catch(() => {});
        state.virtual.closed.unshift(trade);
        state.virtual.open = state.virtual.open.filter(t => t !== trade);
        updateVirtualTradeDb(trade).catch(() => {});
        log(`[S1] SL hit on ${trade.name} @ ₹${px.toFixed(2)} | Net PnL: ₹${netPnl}`);
        telegramSend(`🔴 <b>S1 SL HIT</b>: ${trade.name} @ ₹${px.toFixed(2)} | Net PnL: ₹${netPnl}`);
        continue;
      }

      if (!trade.partialDone && px >= trade.target1) {
        const halfQty = Math.floor(trade.remainingQty / 2);
        if (halfQty > 0) {
          trade.partialDone = true;
          trade.remainingQty -= halfQty;
          trade.realizedPnl += (px - trade.entryPrice) * halfQty;
          trade.stopLoss = trade.entryPrice;
          log(`[S1] Target 1 on ${trade.name} @ ₹${px.toFixed(2)}. 50% booked, SL moved to breakeven.`);
          telegramSend(`🎯 <b>S1 TARGET 1</b>: ${trade.name} @ ₹${px.toFixed(2)}. Booked 50%, SL to breakeven.`);
        }
      }

      if (px >= trade.target2 || forceExit) {
        const exitReason = px >= trade.target2 ? 'TARGET_2' : 'EOD_SQUAREOFF';
        const grossPnl = Number(((px - trade.entryPrice) * trade.remainingQty + trade.realizedPnl).toFixed(2));
        const charges = calculateTradeCharges(trade.entryPrice, px, trade.qty);
        const netPnl = Number((grossPnl - charges.totalCharges).toFixed(2));
        trade.status = 'CLOSED';
        trade.exitTs = new Date().toISOString();
        trade.exitPrice = px;
        trade.exitReason = exitReason;
        trade.grossPnl = grossPnl;
        trade.taxes = charges.totalCharges;
        trade.pnl = netPnl;
        state.virtual.capital = Number((state.virtual.capital + netPnl).toFixed(2));
        saveVirtualAccountToDb(state.virtual.capital).catch(() => {});
        state.virtual.closed.unshift(trade);
        state.virtual.open = state.virtual.open.filter(t => t !== trade);
        updateVirtualTradeDb(trade).catch(() => {});
        log(`[S1] Closed ${trade.name} (${exitReason}) @ ₹${px.toFixed(2)} | Net PnL: ₹${netPnl}`);
        telegramSend(`🚀 <b>S1 CLOSED (${exitReason})</b>: ${trade.name} @ ₹${px.toFixed(2)} | Net PnL: ₹${netPnl}`);
        continue;
      }
    } else {
      // SELL (Short)
      if (px >= trade.stopLoss) {
        const grossPnl = Number(((trade.entryPrice - px) * trade.remainingQty + trade.realizedPnl).toFixed(2));
        const charges = calculateTradeCharges(trade.entryPrice, px, trade.qty);
        const netPnl = Number((grossPnl - charges.totalCharges).toFixed(2));
        trade.status = 'CLOSED';
        trade.exitTs = new Date().toISOString();
        trade.exitPrice = px;
        trade.exitReason = trade.partialDone ? 'BREAKEVEN_STOP' : 'STOP_LOSS';
        trade.grossPnl = grossPnl;
        trade.taxes = charges.totalCharges;
        trade.pnl = netPnl;
        state.virtual.capital = Number((state.virtual.capital + netPnl).toFixed(2));
        saveVirtualAccountToDb(state.virtual.capital).catch(() => {});
        state.virtual.closed.unshift(trade);
        state.virtual.open = state.virtual.open.filter(t => t !== trade);
        updateVirtualTradeDb(trade).catch(() => {});
        log(`[S1] SL hit on short ${trade.name} @ ₹${px.toFixed(2)} | Net PnL: ₹${netPnl}`);
        telegramSend(`🔴 <b>S1 SL HIT</b>: Short ${trade.name} @ ₹${px.toFixed(2)} | Net PnL: ₹${netPnl}`);
        continue;
      }

      if (!trade.partialDone && px <= trade.target1) {
        const halfQty = Math.floor(trade.remainingQty / 2);
        if (halfQty > 0) {
          trade.partialDone = true;
          trade.remainingQty -= halfQty;
          trade.realizedPnl += (trade.entryPrice - px) * halfQty;
          trade.stopLoss = trade.entryPrice;
          log(`[S1] Target 1 on short ${trade.name} @ ₹${px.toFixed(2)}. 50% booked, SL moved to breakeven.`);
          telegramSend(`🎯 <b>S1 TARGET 1</b>: Short ${trade.name} @ ₹${px.toFixed(2)}. Booked 50%, SL to breakeven.`);
        }
      }

      if (px <= trade.target2 || forceExit) {
        const exitReason = px <= trade.target2 ? 'TARGET_2' : 'EOD_SQUAREOFF';
        const grossPnl = Number(((trade.entryPrice - px) * trade.remainingQty + trade.realizedPnl).toFixed(2));
        const charges = calculateTradeCharges(trade.entryPrice, px, trade.qty);
        const netPnl = Number((grossPnl - charges.totalCharges).toFixed(2));
        trade.status = 'CLOSED';
        trade.exitTs = new Date().toISOString();
        trade.exitPrice = px;
        trade.exitReason = exitReason;
        trade.grossPnl = grossPnl;
        trade.taxes = charges.totalCharges;
        trade.pnl = netPnl;
        state.virtual.capital = Number((state.virtual.capital + netPnl).toFixed(2));
        saveVirtualAccountToDb(state.virtual.capital).catch(() => {});
        state.virtual.closed.unshift(trade);
        state.virtual.open = state.virtual.open.filter(t => t !== trade);
        updateVirtualTradeDb(trade).catch(() => {});
        log(`[S1] Closed short ${trade.name} (${exitReason}) @ ₹${px.toFixed(2)} | Net PnL: ₹${netPnl}`);
        telegramSend(`🚀 <b>S1 CLOSED (${exitReason})</b>: Short ${trade.name} @ ₹${px.toFixed(2)} | Net PnL: ₹${netPnl}`);
        continue;
      }
    }
  }
}

/* ---------- Strategy-1 End-of-Day Backtest Simulator ---------- */
async function runDayBacktest(targetDate, appId, token) {
  log(`[DAY-BACKTEST] Initiating Strategy-1 backtest simulation for ${targetDate}…`);
  const initialCap = 300000;
  state.virtual.capital = initialCap;
  await saveVirtualAccountToDb(initialCap);

  if (pool) {
    try {
      await pool.query(`DELETE FROM virtual_trades WHERE trading_date = $1`, [targetDate]);
    } catch {}
  }
  state.virtual.open = [];
  state.virtual.closed = state.virtual.closed.filter(t => t.tradingDate !== targetDate);
  state.virtual.tradesToday = 0;

  const universe = await ensureUniverse();
  await checkAndGapFillHistory(appId, token, targetDate, false);

  const baseSnap = state.history.get('09:20');
  let planSide = 'BUY';
  let marketAdv = 0, marketDec = 0;

  if (baseSnap && baseSnap.gainers && baseSnap.gainers.length) {
    marketAdv = baseSnap.gainers.filter(x => (x.pct || 0) > 0).length;
    marketDec = baseSnap.gainers.filter(x => (x.pct || 0) < 0).length;
    if (marketDec > marketAdv) planSide = 'SELL';
  } else {
    const allRows = liveRows();
    const b = breadth(allRows);
    marketAdv = b.advances;
    marketDec = b.declines;
    if (marketDec > marketAdv) planSide = 'SELL';
  }

  log(`[DAY-BACKTEST] 09:20 Market Breadth: ${marketAdv} Adv vs ${marketDec} Dec -> Mode: ${planSide}`);

  const candidatePool = baseSnap ? (planSide === 'BUY' ? baseSnap.gainers : baseSnap.losers) : universe;
  const candidates = (candidatePool || []).slice(0, 15);

  const maxTrades = 3;
  let currentCap = initialCap;
  const executedTrades = [];

  for (const cand of candidates) {
    if (executedTrades.length >= maxTrades) break;

    const candles = await fetchHistorical5m(cand, appId, token, targetDate, targetDate);
    if (!candles || candles.length < 10) continue;

    const entryBar = candles.find(c => {
      const hm = c.timeStr || new Date(c.time * 1000).toLocaleTimeString('en-GB', { timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit' });
      return hm >= '09:50' && hm <= '10:30';
    });
    if (!entryBar) continue;

    const entryIdx = candles.indexOf(entryBar);
    const priorCloses = candles.slice(0, entryIdx + 1).map(c => c.close);
    const rsiVal = rsi(priorCloses, 14);
    const vwapVal = vwapFromCandles(candles.slice(0, entryIdx + 1));
    const ema9Arr = ema(priorCloses, 9);
    const ema21Arr = ema(priorCloses, 21);
    const ema50Arr = ema(priorCloses, 50);

    const ema9 = ema9Arr[ema9Arr.length - 1];
    const ema21 = ema21Arr[ema21Arr.length - 1];
    const ema50 = ema50Arr[ema50Arr.length - 1];

    if (planSide === 'BUY') {
      if (vwapVal && entryBar.close < vwapVal) continue;
      if (ema9 && ema21 && ema9 < ema21) continue;
      if (rsiVal && (rsiVal < 50 || rsiVal > 75)) continue;
    } else {
      if (vwapVal && entryBar.close > vwapVal) continue;
      if (ema9 && ema21 && ema9 > ema21) continue;
      if (rsiVal && (rsiVal > 55 || rsiVal < 25)) continue;
    }

    const entry = entryBar.close;
    const maxTradeCap = currentCap / maxTrades;
    let qty = Math.floor(maxTradeCap / entry);
    if (qty < 1) qty = 1;

    const stopLoss = planSide === 'BUY' ? Number((entry * 0.99).toFixed(2)) : Number((entry * 1.01).toFixed(2));
    const target1 = planSide === 'BUY' ? Number((entry * 1.02).toFixed(2)) : Number((entry * 0.98).toFixed(2));
    const target2 = planSide === 'BUY' ? Number((entry * 1.03).toFixed(2)) : Number((entry * 0.97).toFixed(2));

    let remainingQty = qty;
    let partialDone = false;
    let realizedPnl = 0;
    let exitPrice = entry;
    let exitReason = 'EOD_SQUAREOFF';
    let exitTs = new Date(candles[candles.length - 1].time * 1000).toISOString();
    let currentSl = stopLoss;

    for (let k = entryIdx + 1; k < candles.length; k++) {
      const bar = candles[k];
      const hm = bar.timeStr || new Date(bar.time * 1000).toLocaleTimeString('en-GB', { timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit' });

      if (planSide === 'BUY') {
        if (bar.low <= currentSl) {
          exitPrice = currentSl;
          exitReason = partialDone ? 'BREAKEVEN_STOP' : 'STOP_LOSS';
          exitTs = new Date(bar.time * 1000).toISOString();
          break;
        }
        if (!partialDone && bar.high >= target1) {
          const halfQty = Math.floor(remainingQty / 2);
          if (halfQty > 0) {
            partialDone = true;
            remainingQty -= halfQty;
            realizedPnl += (target1 - entry) * halfQty;
            currentSl = entry;
          }
        }
        if (bar.high >= target2) {
          exitPrice = target2;
          exitReason = 'TARGET_2';
          exitTs = new Date(bar.time * 1000).toISOString();
          break;
        }
      } else {
        if (bar.high >= currentSl) {
          exitPrice = currentSl;
          exitReason = partialDone ? 'BREAKEVEN_STOP' : 'STOP_LOSS';
          exitTs = new Date(bar.time * 1000).toISOString();
          break;
        }
        if (!partialDone && bar.low <= target1) {
          const halfQty = Math.floor(remainingQty / 2);
          if (halfQty > 0) {
            partialDone = true;
            remainingQty -= halfQty;
            realizedPnl += (entry - target1) * halfQty;
            currentSl = entry;
          }
        }
        if (bar.low <= target2) {
          exitPrice = target2;
          exitReason = 'TARGET_2';
          exitTs = new Date(bar.time * 1000).toISOString();
          break;
        }
      }

      if (hm >= '15:15') {
        exitPrice = bar.close;
        exitReason = 'EOD_SQUAREOFF';
        exitTs = new Date(bar.time * 1000).toISOString();
        break;
      }
    }

    const grossPnl = planSide === 'BUY'
      ? Number(((exitPrice - entry) * remainingQty + realizedPnl).toFixed(2))
      : Number(((entry - exitPrice) * remainingQty + realizedPnl).toFixed(2));

    const charges = calculateTradeCharges(entry, exitPrice, qty);
    const netPnl = Number((grossPnl - charges.totalCharges).toFixed(2));
    currentCap = Number((currentCap + netPnl).toFixed(2));

    const tradeRecord = {
      strategy: 'S1',
      tradingDate: targetDate,
      side: planSide,
      key: cand.key,
      symbol: cand.symbol,
      name: cand.name,
      sector: cand.sector,
      entryTs: new Date(entryBar.time * 1000).toISOString(),
      entryPrice: entry,
      qty,
      stopLoss,
      target1,
      target2,
      exitTs,
      exitPrice,
      exitReason,
      grossPnl,
      taxes: charges.totalCharges,
      pnl: netPnl,
      status: 'CLOSED',
      checks: { topRank: true, trend: true, rsi: true, volume: true },
      notes: `Backtested (${planSide}): Entry @ ${formatIst(entryBar.time * 1000)} | Exit @ ${formatIst(exitTs)}`
    };

    try {
      tradeRecord.id = await saveVirtualTrade(tradeRecord);
      await updateVirtualTradeDb(tradeRecord);
    } catch {}

    executedTrades.push(tradeRecord);
    state.virtual.closed.unshift(tradeRecord);
  }

  state.virtual.capital = currentCap;
  await saveVirtualAccountToDb(currentCap);

  const totalGross = executedTrades.reduce((a, b) => a + (b.grossPnl || 0), 0);
  const totalTax = executedTrades.reduce((a, b) => a + (b.taxes || 0), 0);
  const totalNet = executedTrades.reduce((a, b) => a + (b.pnl || 0), 0);

  let tgMsg = `📊 <b>S1 END-OF-DAY BACKTEST REPORT (${targetDate})</b>\n`;
  tgMsg += `━━━━━━━━━━━━━━━━━━━━━\n`;
  tgMsg += `💼 <b>Starting Capital:</b> ₹${initialCap.toLocaleString('en-IN')}\n`;
  tgMsg += `📈 <b>Ending Capital:</b> ₹${currentCap.toLocaleString('en-IN')} (${totalNet >= 0 ? '+' : ''}${(((currentCap - initialCap)/initialCap)*100).toFixed(2)}%)\n`;
  tgMsg += `🎯 <b>Trades Executed:</b> ${executedTrades.length}/${maxTrades} (${planSide} Mode)\n\n`;

  executedTrades.forEach((t, idx) => {
    tgMsg += `<b>${idx + 1}. ${t.name}</b> (${t.side})\n`;
    tgMsg += `  Entry: ₹${t.entryPrice.toFixed(2)} × ${t.qty} shs\n`;
    tgMsg += `  Exit: ₹${t.exitPrice.toFixed(2)} (${t.exitReason})\n`;
    tgMsg += `  Net PnL: <b>${t.pnl >= 0 ? '+' : ''}₹${t.pnl.toFixed(2)}</b> (Taxes: ₹${t.taxes.toFixed(2)})\n\n`;
  });

  tgMsg += `━━━━━━━━━━━━━━━━━━━━━\n`;
  tgMsg += `💵 <b>Gross PnL:</b> ₹${totalGross.toFixed(2)}\n`;
  tgMsg += `🏛 <b>Taxes & Brokerage:</b> ₹${totalTax.toFixed(2)}\n`;
  tgMsg += `✨ <b>Net PnL:</b> <b>${totalNet >= 0 ? '+' : ''}₹${totalNet.toFixed(2)}</b>`;

  await telegramSend(tgMsg);
  log(`[DAY-BACKTEST] Completed for ${targetDate}: ${executedTrades.length} trades (${planSide}), Net PnL: ₹${totalNet.toFixed(2)}`);
  broadcastDashboard();

  return {
    ok: true,
    date: targetDate,
    startingCapital: initialCap,
    endingCapital: currentCap,
    trades: executedTrades,
    summary: { totalGross, totalTax, totalNet }
  };
}

/* ---------- Dashboard Payload Builder ---------- */
async function dashboardData() {
  const date = istToday();
  if (state.virtual.dayKey !== date) {
    state.virtual.dayKey = date;
    state.virtual.tradesToday = 0;
  }
  if (!state.indices.size || Date.now() - lastIndexRefreshTime > 60000) {
    refreshIndexQuotes().catch(() => {});
  }

  const live = rankedLive();
  const gainers = mergeRows('gainers', live.gainers);
  const losers = mergeRows('losers', live.losers);

  let allRows = liveRows();
  if (allRows.length < 10) {
    if (state.live.size >= 10) {
      allRows = [...state.live.values()].map(q => {
        const key = normalizeKey(q.symbol || q.key || q.name);
        return {
          key,
          name: key,
          symbol: q.symbol,
          pct: Number(q.changePct ?? q.pct ?? 0),
          ltp: q.ltp,
          sector: SECTOR_MAP[key] || 'Other F&O'
        };
      });
    } else {
      const times = [...state.history.keys()].filter(t => t !== 'CLOSE').sort();
      if (times.length > 0) {
        const snap = state.history.get(times[times.length - 1]);
        if (snap) {
          const combined = [...(snap.gainers || []), ...(snap.losers || [])];
          const seen = new Set();
          allRows = [];
          for (const item of combined) {
            const k = normalizeKey(item.key || item.symbol || item.name);
            if (!seen.has(k)) {
              seen.add(k);
              allRows.push(item);
            }
          }
        }
      }
    }
  }

  const b = breadth(allRows);
  const sectors = sectorData(allRows);

  const idxCards = INDEX_CONFIG.map(cfg => {
    const q = state.indices.get(cfg.symbol) || state.live.get(cfg.symbol) || {};
    let ltp = q.ltp ?? null;
    let pctVal = q.changePct ?? q.pct ?? null;
    let adv = b.advances;
    let dec = b.declines;

    if (cfg.name === 'NIFTY 50') {
      const n50Stocks = [...NIFTY50].map(k => state.live.get(`NSE:${k}-EQ`) || state.live.get(k)).filter(Boolean);
      if (n50Stocks.length) {
        adv = n50Stocks.filter(x => (x.changePct || 0) > 0).length;
        dec = n50Stocks.filter(x => (x.changePct || 0) < 0).length;
        if (pctVal == null) {
          pctVal = Number((n50Stocks.reduce((a, x) => a + (x.changePct || 0), 0) / n50Stocks.length).toFixed(2));
        }
      }
    }

    return {
      name: cfg.name,
      symbol: cfg.symbol,
      ltp,
      pct: pctVal,
      changePct: pctVal,
      advances: adv,
      declines: dec
    };
  });

  return {
    updatedAt: new Date().toISOString(),
    displayDate: state.displayDate || date,
    displayMode: state.displayMode,
    isLiveSession: marketOpenNow(),
    isPreOpen: isPreOpenSession(),
    sessionFinished: regularSessionFinished(),
    indices: idxCards,
    breadth: b,
    universeSize: state.universe.length,
    sector: sectors,
    sectors: sectors,
    gainers: gainers.rows,
    losers: losers.rows,
    scannerUniverse: allRows,
    times: gainers.times,
    virtual: {
      enabled: state.virtual.enabled,
      capital: state.virtual.capital,
      tradesToday: state.virtual.tradesToday,
      maxTrades: state.virtual.maxTrades,
      open: state.virtual.open,
      closed: state.virtual.closed.slice(0, 30),
      signals: state.virtual.signals.slice(-20).reverse()
    },
    logs: state.log.slice(-30)
  };
}

function broadcastDashboard() {
  if (!browserSockets.size) return;
  dashboardData().then(payload => {
    const raw = JSON.stringify({ type: 'dashboard', data: payload });
    for (const ws of browserSockets) {
      try { if (ws.readyState === 1) ws.send(raw); } catch {}
    }
  }).catch(() => {});
}

/* ---------- HTTP Request Router ---------- */
function send(res, code, data, type = 'application/json') {
  res.writeHead(code, {
    'Content-Type': type,
    'Access-Control-Allow-Origin': '*',
    'Cache-Control': 'no-store'
  });
  res.end(typeof data === 'string' ? data : JSON.stringify(data));
}

function cors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', '*');
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let b = '';
    req.on('data', c => { b += c; });
    req.on('end', () => {
      try { resolve(b ? JSON.parse(b) : {}); }
      catch { resolve({}); }
    });
    req.on('error', reject);
  });
}

function virtualReportSummary(trades) {
  const closed = trades.filter(t => (t.status || '') === 'CLOSED' || t.exit_ts || t.exitTs);
  let wins = 0, losses = 0, pnl = 0;
  for (const t of closed) {
    const p = Number(t.pnl);
    if (!Number.isFinite(p)) continue;
    pnl += p;
    if (p >= 0) wins++; else losses++;
  }
  return {
    trades: closed.length,
    open: trades.filter(t => (t.status || '') === 'OPEN' || (!t.exit_ts && !t.exitTs && t.status !== 'CLOSED')).length,
    wins,
    losses,
    winRate: closed.length ? Number(((wins / closed.length) * 100).toFixed(1)) : 0,
    totalPnl: Number(pnl.toFixed(2))
  };
}

async function route(req, res) {
  cors(res);
  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    return res.end();
  }

  const u = new URL(req.url, `http://${req.headers.host}`);

  try {
    if (u.pathname === '/api/health') {
      return send(res, 200, { ok: true, broker: 'fyers', universe: state.universe.length, connected: state.fyers.connected });
    }

    if (u.pathname === '/api/login' && req.method === 'POST') {
      const body = await readBody(req);
      const appId = String(body.appId || body.api_key || body.apiKey || '').trim();
      let token = String(body.accessToken || body.access_token || body.token || '').trim();
      token = token.replace(/^(?:bearer)\s+/i, '').trim();

      if (!appId || !token) {
        return send(res, 400, { ok: false, error: 'Both App ID and Access Token are required.' });
      }

      state.fyers.appId = appId;
      state.fyers.token = token;
      log('FYERS credentials updated via browser.');

      await saveSessionToDb(appId, token);
      await ensureUniverse();
      await loadOpenVirtualFromDb();
      await seedInitialQuotes(appId, token);
      startFyersSocket();
      checkAndGapFillHistory(appId, token).catch(() => {});

      return send(res, 200, { ok: true, universe: state.universe.length });
    }

    if (u.pathname === '/api/logout' && req.method === 'POST') {
      state.fyers.appId = '';
      state.fyers.token = '';
      if (state.fyers.socket) {
        try { state.fyers.socket.close(); } catch {}
      }
      if (pool) {
        try { await pool.query("DELETE FROM server_sessions WHERE broker = 'fyers'"); } catch {}
      }
      log('User logged out. FYERS session cleared.', 'warn');
      return send(res, 200, { ok: true });
    }

    if (u.pathname === '/api/dashboard') {
      const hAppId = req.headers['x-fyers-app-id'] || req.headers['x-api-key'];
      let hToken = req.headers['x-fyers-access-token'] || req.headers['authorization'];
      if (hToken) hToken = hToken.replace(/^(?:bearer)\s+/i, '').trim();

      if (hAppId && hToken && (!state.fyers.appId || !state.fyers.token)) {
        state.fyers.appId = hAppId;
        state.fyers.token = hToken;
        await ensureUniverse();
        await loadOpenVirtualFromDb();
        await seedInitialQuotes(hAppId, hToken);
        startFyersSocket();
      }
      return send(res, 200, await dashboardData());
    }

    if (u.pathname === '/api/logs') {
      return send(res, 200, { logs: state.log.slice(-100) });
    }

    if (u.pathname === '/api/chart' && req.method === 'GET') {
      const key = u.searchParams.get('key') || '';
      const stock = state.universe.find(x => normalizeKey(x.key) === normalizeKey(key));
      if (!stock) return send(res, 404, { error: 'Unknown stock symbol' });

      let candles = [];
      let orderBlocks = { buyZones: [], sellZones: [] };
      const res2w = await fetchHistorical2Weeks(stock, state.fyers.appId, state.fyers.token);
      candles = res2w.candles || [];
      orderBlocks = res2w.orderBlocks || orderBlocks;
      const liveQ = state.live.get(stock.symbol) || state.live.get(stock.key);
      const ltp = liveQ?.ltp || null;
      return send(res, 200, { stock, candles, orderBlocks, ltp, days: 14 });
    }

    if (u.pathname === '/api/virtual/status' && req.method === 'GET') {
      return send(res, 200, {
        enabled: state.virtual.enabled,
        capital: state.virtual.capital,
        tradesToday: state.virtual.tradesToday,
        maxTrades: state.virtual.maxTrades,
        open: state.virtual.open,
        closed: state.virtual.closed.slice(0, 50),
        signals: state.virtual.signals.slice(-30).reverse(),
        summary: virtualReportSummary([...state.virtual.open, ...state.virtual.closed]),
        telegramConfigured: !!(process.env.TELEGRAM_BOT_TOKEN && process.env.TELEGRAM_CHAT_ID)
      });
    }

    if (u.pathname === '/api/virtual/report' && req.method === 'GET') {
      const from = u.searchParams.get('from') || istToday();
      const to = u.searchParams.get('to') || istToday();
      let rows = [...state.virtual.closed, ...state.virtual.open];

      if (pool) {
        try {
          const resDb = await pool.query(
            `SELECT * FROM virtual_trades WHERE trading_date >= $1 AND trading_date <= $2 ORDER BY entry_ts DESC`,
            [from, to]
          );
          rows = resDb.rows.map(r => ({
            id: r.id,
            strategy: r.strategy,
            date: toISODate(r.trading_date),
            side: r.side,
            name: r.name,
            symbol: r.symbol,
            sector: r.sector,
            entryTs: r.entry_ts,
            entryPrice: Number(r.entry_price),
            qty: Number(r.qty),
            stopLoss: Number(r.stop_loss),
            target1: Number(r.target1),
            target2: Number(r.target2),
            exitTs: r.exit_ts,
            exitPrice: r.exit_price != null ? Number(r.exit_price) : null,
            exitReason: r.exit_reason,
            pnl: r.pnl != null ? Number(r.pnl) : null,
            status: r.status,
            checks: r.checks,
            notes: r.notes
          }));
        } catch (e) {
          log(`Report DB load error: ${e.message}`, 'warn');
        }
      }

      return send(res, 200, {
        from, to, summary: virtualReportSummary(rows), trades: rows
      });
    }

    if (u.pathname === '/api/virtual/test-telegram' && req.method === 'POST') {
      const body = await readBody(req);
      const token = body.botToken || process.env.TELEGRAM_BOT_TOKEN;
      const chat = body.chatId || process.env.TELEGRAM_CHAT_ID;

      if (!token || !chat) {
        return send(res, 400, {
          ok: false,
          error: 'Telegram credentials missing. Please set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in Environment Variables.'
        });
      }

      const testMsg =
        `🔔 <b>FYERS F&O Intraday Dashboard — Telegram Test</b>\n` +
        `✅ Telegram notifications connected successfully!\n` +
        `⏰ <b>Timestamp:</b> ${formatIst(new Date())} IST\n` +
        `📊 <b>Active Universe:</b> ${state.universe.length} F&O Equities\n` +
        `🤖 <b>Strategy-1 Engine:</b> ${state.virtual.enabled ? 'ACTIVE (Armed)' : 'STANDBY'}\n` +
        `💼 <b>Virtual Capital:</b> ₹${Number(state.virtual.capital).toLocaleString('en-IN')}\n` +
        `📡 <b>Session Status:</b> ${isPreOpenSession() ? 'Pre-Open' : marketOpenNow() ? 'Market Live' : 'Market Closed'}`;

      try {
        await axios.post(`https://api.telegram.org/bot${token}/sendMessage`, {
          chat_id: chat,
          text: testMsg,
          parse_mode: 'HTML',
          disable_web_page_preview: true
        });
        log('Telegram test message dispatched successfully.');
        return send(res, 200, { ok: true, message: 'Test message sent successfully to your Telegram group!' });
      } catch (err) {
        const desc = err.response?.data?.description || err.message;
        log(`Telegram test send failed: ${desc}`, 'warn');
        return send(res, 502, { ok: false, error: `Telegram API Error: ${desc}` });
      }
    }

    if (u.pathname === '/api/virtual/toggle' && req.method === 'POST') {
      const body = await readBody(req);
      if (typeof body.enabled === 'boolean') state.virtual.enabled = body.enabled;
      if (Number.isFinite(Number(body.capital))) {
        state.virtual.capital = Number(body.capital);
        await saveVirtualAccountToDb(state.virtual.capital);
      }
      log(`Virtual trading ${state.virtual.enabled ? 'ENABLED' : 'DISABLED'}; Capital: ₹${state.virtual.capital}`);
      return send(res, 200, { ok: true, enabled: state.virtual.enabled, capital: state.virtual.capital });
    }

    if (u.pathname === '/api/virtual/scan' && req.method === 'POST') {
      await scanStrategy1();
      return send(res, 200, { ok: true, tradesToday: state.virtual.tradesToday, open: state.virtual.open.length });
    }

    if (u.pathname === '/api/rebuild' && req.method === 'POST') {
      const appId = state.fyers.appId;
      const token = state.fyers.token;
      if (!appId || !token) {
        return send(res, 400, { ok: false, error: 'FYERS credentials required to rebuild timeline. Please login first.' });
      }
      log('Triggered manual timeline rebuild via UI button…');
      checkAndGapFillHistory(appId, token, istToday(), true)
        .then(() => {
          log('Manual timeline rebuild finished successfully.');
          if (browserSockets.size) broadcastDashboard();
        })
        .catch(err => {
          log(`Manual rebuild error: ${err.message}`, 'error');
        });
      return send(res, 200, { ok: true, message: 'Timeline rebuild started in the background. Check logs window for progress!' });
    }

    if (u.pathname === '/api/virtual/backtest-day' && req.method === 'POST') {
      const appId = state.fyers.appId;
      const token = state.fyers.token;
      if (!appId || !token) {
        return send(res, 400, { ok: false, error: 'FYERS credentials required to run EOD backtest. Please login first.' });
      }
      const now = istParts(new Date());
      const force = u.searchParams.get('force') === '1';
      if (!force && now.time < '15:40:00') {
        return send(res, 400, {
          ok: false,
          error: `Day backtest is only permitted after 15:40 IST (Market Close + Candle Settlement). Current IST time: ${now.time}`
        });
      }
      log(`Starting Strategy-1 End-of-Day Backtest for ${now.date} (Post 15:40 IST)...`);
      try {
        const result = await runDayBacktest(now.date, appId, token);
        if (browserSockets.size) broadcastDashboard();
        return send(res, 200, { ok: true, ...result });
      } catch (err) {
        log(`Day backtest failed: ${err.message}`, 'error');
        return send(res, 500, { ok: false, error: err.message });
      }
    }

    return send(res, 404, { error: 'Not found' });
  } catch (err) {
    log(`HTTP Route Error: ${err.message}`, 'error');
    return send(res, 500, { ok: false, error: err.message });
  }
}

/* ---------- Start HTTP Server & Browser WebSockets ---------- */
const htmlPath = path.join(__dirname, 'index.html');
const server = http.createServer((req, res) => {
  const u = new URL(req.url, `http://${req.headers.host}`);
  if (u.pathname.startsWith('/api/')) return route(req, res);
  if (req.method === 'GET' && (u.pathname === '/' || u.pathname === '/index.html')) {
    if (!fs.existsSync(htmlPath)) {
      return send(res, 404, 'index.html missing', 'text/plain');
    }
    const html = fs.readFileSync(htmlPath);
    cors(res);
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
    return res.end(html);
  }
  send(res, 404, 'Not found', 'text/plain');
});

const wss = new WebSocket.Server({ noServer: true });
wss.on('connection', async ws => {
  browserSockets.add(ws);
  try {
    ws.send(JSON.stringify({ type: 'dashboard', data: await dashboardData() }));
  } catch {}
  ws.on('close', () => browserSockets.delete(ws));
});

server.on('upgrade', (req, socket, head) => {
  const u = new URL(req.url, `http://${req.headers.host}`);
  if (u.pathname === '/ws') {
    wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, req));
  } else {
    socket.destroy();
  }
});

/* ---------- Timers ---------- */
// Regular index quote keeper (every 45s)
setInterval(() => {
  refreshIndexQuotes().catch(() => {});
}, 45000);

// 5-min snapshot timer (evaluates within first 90s of each 5m mark)
setInterval(() => {
  makeCandleSnapshot().catch(e => log(`Snapshot timer err: ${e.message}`, 'warn'));
}, 15000);

// Anti-freeze server-side quote keeper (every 60s)
setInterval(() => {
  if (!state.fyers.appId || !state.fyers.token) return;
  if (!marketOpenNow()) return;
  if (!state.fyers.connected || liveDataAgeSec() > 75) {
    refreshLiveQuotes(state.fyers.connected ? 'stale' : 'ws-down')
      .then(ok => {
        if (ok) {
          manageOpenVirtualTrades();
          if (browserSockets.size) broadcastDashboard();
        }
      })
      .catch(() => {});
  }
}, 60000);

// Strategy 1 scan timer (runs every 45s during market hours)
setInterval(() => {
  scanStrategy1().catch(e => log(`S1 scan err: ${e.message}`, 'warn'));
  manageOpenVirtualTrades();
}, 45000);

// Open positions monitor (every 5s)
setInterval(() => {
  if (state.virtual.open.length && marketOpenNow()) {
    manageOpenVirtualTrades();
    if (browserSockets.size) broadcastDashboard();
  }
}, 5000);

/* ---------- Boot ---------- */
(async () => {
  try {
    await initDb();
    await ensureUniverse();
    await loadVirtualAccountFromDb();
    await loadDbHistory(istToday());
    if (state.history.size === 0) {
      loadSeedHistory();
    }
    await loadOpenVirtualFromDb();

    // Check saved session in DB first
    const savedSession = await loadSessionFromDb();
    if (savedSession && !state.fyers.token) {
      state.fyers.appId = savedSession.appId;
      state.fyers.token = savedSession.token;
      log('Restored FYERS background session credentials from database.');
    }

    if (state.fyers.appId && state.fyers.token) {
      log('Bootstrapping FYERS background session…');
      await seedInitialQuotes(state.fyers.appId, state.fyers.token);
      startFyersSocket();
      checkAndGapFillHistory(state.fyers.appId, state.fyers.token).catch(() => {});
    }

    server.listen(PORT, HOST, () => {
      log(`FYERS F&O Intraday Dashboard listening on http://${HOST}:${PORT}`);
    });
  } catch (e) {
    console.error('Fatal initialization error:', e);
    process.exit(1);
  }
})();
