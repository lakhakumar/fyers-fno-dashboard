/**
 * F&O Intraday Rank Dashboard + Strategy-1 Virtual Forward Testing
 * Node HTTP + pg + ws + fyers-api-v3
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const { URL } = require('url');
const { Pool } = require('pg');
const { fyersDataSocket } = require('fyers-api-v3');

const PORT = Number(process.env.PORT || 10000);
const HOST = '0.0.0.0';
const DATA_HOST = 'https://api-t1.fyers.in/data';
const API_HOST = 'https://api-t1.fyers.in/api/v3';
const MASTER_BASE = 'https://public.fyers.in/sym_details/';

const INDEX_SYMBOLS = [
  ['NIFTY 50', 'NSE:NIFTY50-INDEX'],
  ['BANK NIFTY', 'NSE:NIFTYBANK-INDEX'],
  ['SENSEX', 'BSE:SENSEX-INDEX']
];

const NIFTY50 = new Set([
  'RELIANCE','TCS','HDFCBANK','INFY','ICICIBANK','HINDUNILVR','ITC','SBIN','BHARTIARTL',
  'KOTAKBANK','LT','AXISBANK','ASIANPAINT','MARUTI','SUNPHARMA','TITAN','BAJFINANCE',
  'NESTLEIND','ULTRACEMCO','WIPRO','HCLTECH','POWERGRID','NTPC','TATASTEEL','M&M',
  'ADANIENT','JSWSTEEL','INDUSINDBK','TECHM','GRASIM','HINDALCO','ONGC','CIPLA','DRREDDY',
  'COALINDIA','BPCL','SBILIFE','BAJAJFINSV','EICHERMOT','DIVISLAB','BRITANNIA','HEROMOTOCO',
  'APOLLOHOSP','HDFCLIFE','TATACONSUM','ADANIPORTS','BAJAJ-AUTO','UPL','TATAMOTORS'
]);

/* ---------- Sector map (subset + Other F&O fallback) ---------- */
const SECTOR_MAP = {
  ADANIENT:'Metals & Mining',ADANIPORTS:'Infrastructure',APOLLOHOSP:'Healthcare',ASIANPAINT:'Consumer Durables',
  AXISBANK:'Financial Services','BAJAJ-AUTO':'Automobile',BAJFINANCE:'Financial Services',BAJAJFINSV:'Financial Services',
  BEL:'Defence',BHARTIARTL:'Telecommunication',BPCL:'Oil & Gas',BRITANNIA:'FMCG',CIPLA:'Healthcare',
  COALINDIA:'Metals & Mining',DABUR:'FMCG',DIVISLAB:'Healthcare',DRREDDY:'Healthcare',EICHERMOT:'Automobile',
  GAIL:'Oil & Gas',GRASIM:'Cement',HCLTECH:'Information Technology',HDFCBANK:'Financial Services',HDFCLIFE:'Financial Services',
  HEROMOTOCO:'Automobile',HINDALCO:'Metals & Mining',HINDPETRO:'Oil & Gas',HINDUNILVR:'FMCG',ICICIBANK:'Financial Services',
  INDUSINDBK:'Financial Services',INFY:'Information Technology',IOC:'Oil & Gas',ITC:'FMCG',JINDALSTEL:'Metals & Mining',
  JSWSTEEL:'Metals & Mining',KOTAKBANK:'Financial Services',LT:'Infrastructure','M&M':'Automobile',MARUTI:'Automobile',
  NESTLEIND:'FMCG',NTPC:'Power',ONGC:'Oil & Gas',POWERGRID:'Power',RELIANCE:'Oil & Gas',SBIN:'Financial Services',
  SUNPHARMA:'Healthcare',TATAMOTORS:'Automobile',TATASTEEL:'Metals & Mining',TCS:'Information Technology',
  TECHM:'Information Technology',TITAN:'Consumer Durables',ULTRACEMCO:'Cement',WIPRO:'Information Technology',
  BANKBARODA:'Financial Services',CANBK:'Financial Services',FEDERALBNK:'Financial Services',PNB:'Financial Services',
  UNIONBANK:'Financial Services',YESBANK:'Financial Services',IDFCFIRSTB:'Financial Services',AUBANK:'Financial Services',
  PERSISTENT:'Information Technology',LTIM:'Information Technology',COFORGE:'Information Technology',MPHASIS:'Information Technology',
  ZOMATO:'Consumer Services',TRENT:'Consumer Services',DELHIVERY:'Logistics',PAYTM:'Financial Services',
  MANKIND:'Healthcare',GLENMARK:'Healthcare',LUPIN:'Healthcare',BIOCON:'Healthcare',
  SOLARINDS:'Defence',HAL:'Defence',BHEL:'Industrials',SIEMENS:'Industrials',
  AMBUJACEM:'Cement',DALBHARAT:'Cement',SHREECEM:'Cement',
  VEDL:'Metals & Mining',HINDZINC:'Metals & Mining',NATIONALUM:'Metals & Mining',SAIL:'Metals & Mining',
  TVSMOTOR:'Automobile',MOTHERSON:'Automobile',BOSCHLTD:'Automobile',
  DIXON:'Consumer Durables',HAVELLS:'Consumer Durables',VOLTAS:'Consumer Durables',
  IRCTC:'Consumer Services',INDHOTEL:'Consumer Services',JUBLFOOD:'Consumer Services',
  PETRONET:'Oil & Gas',OIL:'Oil & Gas',GAIL:'Oil & Gas',
  RECLTD:'Financial Services',PFC:'Financial Services',IRFC:'Financial Services',
  PREMIERENE:'Power',SUZLON:'Power',INOXWIND:'Power',TATAPOWER:'Power',
  POLICYBZR:'Financial Services',KFINTECH:'Financial Services',BSE:'Financial Services',MCX:'Financial Services',
  AMBER:'Consumer Durables',PNBHOUSING:'Financial Services',LICHSGFIN:'Financial Services',
  ABB:'Industrials',POLYCAB:'Industrials',KEI:'Industrials',CGPOWER:'Industrials'
};

/* ---------- State ---------- */
const state = {
  masters: { fo: null, cm: null, loadedAt: 0 },
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
  fyers: { appId: '', token: '', socket: null, connected: false, reconnectTimer: null },
  virtual: {
    enabled: true,
    capital: Number(process.env.VIRTUAL_CAPITAL || 300000),
    riskPct: 1,
    maxTrades: 3,
    open: [],
    closed: [],
    signals: [],
    dayKey: '',
    tradesToday: 0,
    lastScanAt: 0,
    reportCache: null
  }
};

const browserSockets = new Set();
let broadcastTimer = null;
let historyRebuildPromise = null;
let historyRebuildStartedAt = 0;

/* ---------- DB ---------- */
const pool = process.env.DATABASE_URL
  ? new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: process.env.DATABASE_URL.includes('localhost') ? false : { rejectUnauthorized: false },
      max: 3
    })
  : null;

function log(msg, level = 'info') {
  const line = { ts: new Date().toISOString(), level, msg: String(msg) };
  state.log.push(line);
  if (state.log.length > 400) state.log.shift();
  console.log(`[${level}] ${msg}`);
  broadcast({ type: 'log', line });
}

async function initDb() {
  if (!pool) {
    log('DATABASE_URL not set: persistence disabled.', 'warn');
    return;
  }
  await pool.query(`
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
    CREATE INDEX IF NOT EXISTS rank_snapshots_lookup
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
      pnl numeric,
      status varchar(20) NOT NULL DEFAULT 'OPEN',
      checks jsonb,
      notes text
    );
    CREATE INDEX IF NOT EXISTS virtual_trades_day
      ON virtual_trades (trading_date, strategy, status);
  `);
  log('Postgres persistence ready (ranks + virtual_trades).');
}

/* ---------- IST helpers ---------- */
function istParts(date = new Date()) {
  const s = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Kolkata',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false
  }).format(date);
  const [d, t] = s.split(', ');
  return { date: d, time: t };
}
function istToday() { return istParts().date; }
function istWeekday(d = new Date()) {
  const day = new Intl.DateTimeFormat('en-US', { timeZone: 'Asia/Kolkata', weekday: 'short' }).format(d);
  return !['Sat', 'Sun'].includes(day);
}
function isTradingSessionDay() { return istWeekday(); }
function marketOpenNow() {
  if (!isTradingSessionDay()) return false;
  const { time } = istParts();
  return time >= '09:15:00' && time < '15:30:00';
}
function regularSessionStarted() {
  if (!isTradingSessionDay()) return false;
  return istParts().time >= '09:15:00';
}
function regularSessionFinished() {
  return istParts().time >= '15:30:00';
}
function toISODate(v) {
  if (!v) return istToday();
  if (typeof v === 'string' && /^\d{4}-\d{2}-\d{2}/.test(v)) return v.slice(0, 10);
  try {
    return new Date(v).toLocaleDateString('en-CA', { timeZone: 'Asia/Kolkata' });
  } catch {
    return istToday();
  }
}
function epochForISTDate(dateStr, hm) {
  const [y, m, d] = dateStr.split('-').map(Number);
  const [hh, mm] = hm.split(':').map(Number);
  // Approximate IST as UTC+5:30
  return Math.floor(Date.UTC(y, m - 1, d, hh - 5, mm - 30) / 1000);
}
function pct(a, b) {
  if (!Number.isFinite(a) || !Number.isFinite(b) || !b) return 0;
  return ((a - b) / b) * 100;
}
function normalizeKey(s) {
  return String(s || '')
    .toUpperCase()
    .replace(/^NSE:/, '')
    .replace(/-EQ$/, '')
    .replace(/\s+/g, '');
}
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

/* ---------- FYERS rate limiter ---------- */
const FYERS_SAFE_REQUESTS_PER_MINUTE = 100;
const FYERS_MIN_REQUEST_GAP_MS = 450;
let fyersRequestTimes = [];
let fyersLastRequestAt = 0;
let fyersRateQueue = Promise.resolve();
let fyersCooldownUntil = 0;

function fyersHeaders(appId, token) {
  return { Authorization: `${appId}:${token}`, 'Content-Type': 'application/json' };
}

function acquireFyersRequestSlot() {
  const job = fyersRateQueue.then(async () => {
    while (true) {
      const now = Date.now();
      if (now < fyersCooldownUntil) {
        await sleep(fyersCooldownUntil - now);
        continue;
      }
      fyersRequestTimes = fyersRequestTimes.filter(t => now - t < 60000);
      const waitForGap = Math.max(0, fyersLastRequestAt + FYERS_MIN_REQUEST_GAP_MS - now);
      const waitForMinute =
        fyersRequestTimes.length >= FYERS_SAFE_REQUESTS_PER_MINUTE
          ? fyersRequestTimes[0] + 60001 - now
          : 0;
      const wait = Math.max(waitForGap, waitForMinute);
      if (wait <= 0) break;
      await sleep(wait);
    }
    const sentAt = Date.now();
    fyersLastRequestAt = sentAt;
    fyersRequestTimes.push(sentAt);
  });
  fyersRateQueue = job.catch(() => {});
  return job;
}

async function fyersGet(base, endpoint, params, appId, token, retries = 1) {
  await acquireFyersRequestSlot();
  const u = new URL(base + endpoint);
  for (const [k, v] of Object.entries(params || {})) u.searchParams.set(k, String(v));
  const r = await fetch(u, { headers: fyersHeaders(appId, token) });
  const text = await r.text();
  let data;
  try { data = JSON.parse(text); } catch {
    if (r.status === 429 && retries > 0) {
      fyersCooldownUntil = Date.now() + 60000;
      log('FYERS rate-limit cooldown 60s', 'warn');
      await sleep(60000);
      return fyersGet(base, endpoint, params, appId, token, retries - 1);
    }
    throw new Error(`FYERS non-JSON ${r.status}: ${text.slice(0, 120)}`);
  }
  if (!r.ok || data.s === 'error') {
    const code = data.code ?? '';
    const msg = data.message || 'request failed';
    if ((r.status === 429 || String(msg).toLowerCase().includes('rate')) && retries > 0) {
      fyersCooldownUntil = Date.now() + 60000;
      log('FYERS rate-limit cooldown 60s', 'warn');
      await sleep(60000);
      return fyersGet(base, endpoint, params, appId, token, retries - 1);
    }
    throw new Error(`FYERS ${r.status}/${code}: ${msg}`);
  }
  return data;
}

/* ---------- Symbol masters / universe ---------- */
function getField(v, names) {
  for (const n of names) {
    if (v?.[n] !== undefined && v?.[n] !== null && v?.[n] !== '') return v[n];
  }
  return '';
}

async function loadMaster(kind) {
  const now = Date.now();
  if (state.masters[kind] && now - state.masters.loadedAt < 6 * 60 * 60 * 1000) {
    return state.masters[kind];
  }
  const name = kind === 'fo' ? 'NSE_FO_sym_master.json' : 'NSE_CM_sym_master.json';
  const r = await fetch(MASTER_BASE + name, { headers: { 'User-Agent': 'fno-rank-dashboard/2.1' } });
  if (!r.ok) throw new Error(`Symbol master ${name}: HTTP ${r.status}`);
  const data = await r.json();
  state.masters[kind] = data;
  state.masters.loadedAt = now;
  log(`Loaded ${name} (${Object.keys(data).length} instruments).`);
  return data;
}

function growwSlug(name) {
  let s = String(name || '')
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9\s-]/g, ' ')
    .replace(/\blimited\b/g, 'ltd')
    .replace(/\s+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');
  // preserve india / of-india (do not collapse to "i")
  s = s
    .replace(/\bstate-bank-of-india\b/g, 'state-bank-of-india')
    .replace(/\bbank-of-india\b/g, 'bank-of-india')
    .replace(/\bunion-bank-of-india\b/g, 'union-bank-of-india')
    .replace(/\bindian\b/g, 'indian')
    .replace(/\bindia\b/g, 'india');
  return s;
}

function buildUniverse(fo, cm) {
  const out = new Map();
  let futstk = 0, skippedIdx = 0, noCm = 0, dup = 0;
  for (const [, v] of Object.entries(fo)) {
    const typ = String(getField(v, ['exInstType', 'instrumentType', 'instrument_type', 'type'])).toUpperCase();
    if (!(typ === '13' || typ === 'FUTSTK' || typ.includes('FUTSTK'))) continue;
    futstk++;
    let underlying = String(getField(v, ['underSym', 'underlyingSymbol', 'underlying', 'shortSym', 'exSymName']))
      .toUpperCase().trim();
    if (underlying.includes(':')) underlying = underlying.split(':').pop();
    underlying = underlying.replace(/-EQ$/, '').replace(/\s+/g, '');
    if (!underlying) continue;
    if (['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'SENSEX', 'BANKEX'].includes(underlying)) {
      skippedIdx++;
      continue;
    }
    let eq = [`NSE:${underlying}-EQ`, String(getField(v, ['cashSymbol', 'equitySymbol', 'eqSymbol']))]
      .filter(Boolean).find(s => cm[s]);
    if (!eq) {
      const hit = Object.entries(cm).find(([s, cv]) =>
        String(getField(cv, ['shortSymbol', 'shortSym', 'exSymName', 'symDetails'])).toUpperCase() === underlying &&
        s.startsWith('NSE:') && /-EQ$/.test(s)
      );
      if (hit) eq = hit[0];
    }
    if (!eq) { eq = `NSE:${underlying}-EQ`; noCm++; }
    if (out.has(underlying)) { dup++; continue; }
    const cmRow = cm[eq] || {};
    const displayName = String(getField(cmRow, ['exSymName', 'symTicker', 'shortSym']) || underlying);
    out.set(underlying, {
      key: underlying,
      symbol: eq,
      name: underlying,
      displayName,
      sector: SECTOR_MAP[underlying] || 'Other F&O',
      growwUrl: `https://groww.in/charts/stocks/${growwSlug(displayName)}?exchange=NSE`
    });
  }
  const list = [...out.values()].sort((a, b) => a.name.localeCompare(b.name));
  log(`F&O universe build: FUTSTK=${futstk}, unique equities=${list.length}, skipped index underlyings=${skippedIdx}, no CM EQ match≈${noCm}, duplicates=${dup}`);
  const pe = list.find(x => x.key === 'PREMIERENE');
  log(pe ? `Validation: PREMIERENE -> ${pe.symbol} (OK)` : 'Validation: PREMIERENE not in universe', pe ? 'info' : 'warn');
  log(`Sample F&O equity symbols: ${list.slice(0, 8).map(x => x.symbol).join(', ')}`);
  return list;
}

async function ensureUniverse() {
  if (state.universe.length) return state.universe;
  const fo = await loadMaster('fo');
  const cm = await loadMaster('cm');
  state.universe = buildUniverse(fo, cm);
  log(`F&O equity universe ready: ${state.universe.length} stocks.`);
  return state.universe;
}

/* ---------- Quotes / parse ---------- */
async function quotes(symbols, appId, token) {
  const out = [];
  for (let i = 0; i < symbols.length; i += 40) {
    const batch = symbols.slice(i, i + 40);
    const d = await fyersGet(DATA_HOST, '/quotes', { symbols: batch.join(',') }, appId, token);
    const arr = d.d || d.data || [];
    for (const row of arr) {
      const v = row.v || row;
      const sym = row.n || v.symbol || '';
      const ltp = Number(v.lp ?? v.ltp ?? v.last_price);
      const prev = Number(v.prev_close_price ?? v.prev ?? v.open_price);
      const chp = Number(v.chp ?? v.change_percentage);
      const vol = Number(v.volume ?? v.vol ?? 0);
      out.push({
        symbol: sym,
        ltp: Number.isFinite(ltp) ? ltp : null,
        prev: Number.isFinite(prev) ? prev : null,
        changePct: Number.isFinite(chp) ? chp : (prev && ltp ? pct(ltp, prev) : 0),
        volume: Number.isFinite(vol) ? vol : 0
      });
    }
  }
  return out;
}

function parseFyersMessage(msg) {
  if (!msg) return null;
  if (typeof msg === 'string') {
    try { msg = JSON.parse(msg); } catch { return null; }
  }
  const items = Array.isArray(msg) ? msg : [msg];
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

function liveRows() {
  return state.universe.map(s => {
    const q = state.live.get(s.symbol);
    return q ? { ...s, ltp: q.ltp, pct: q.changePct, volume: q.volume || 0, ts: q.ts } : null;
  }).filter(Boolean);
}

function rankedLive() {
  const rows = liveRows();
  return {
    gainers: [...rows].sort((a, b) => b.pct - a.pct).map((x, i) => ({ ...x, rank: i + 1 })),
    losers: [...rows].sort((a, b) => a.pct - b.pct).map((x, i) => ({ ...x, rank: i + 1 }))
  };
}

function smoothMembership(type, ranked) {
  const old = state.membership[type] || [];
  const top = ranked.slice(0, 30);
  const oldRows = old.map(k => ranked.find(x => x.key === k)).filter(Boolean);
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
        growwUrl: x.growwUrl,
        history: Object.fromEntries(times.map(t => [t, rankMaps.get(t).get(k) ?? null]))
      };
    })
  };
}

function sectorData(live) {
  const map = new Map();
  for (const x of live) {
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
  // Deduplicate by key — never count gainers+losers lists twice
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

/* ---------- Rank snapshots ---------- */
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
    throw e;
  } finally {
    client.release();
  }
}

async function loadDbHistory(date) {
  if (!pool) return;
  state.history.clear();
  const { rows } = await pool.query(
    `SELECT trading_date, candle_time, side, rank, symbol, name, sector, pct, close
     FROM rank_snapshots WHERE trading_date=$1 ORDER BY candle_time, side, rank`,
    [toISODate(date)]
  );
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
  if (rows.length) {
    const times = [...state.history.keys()].sort().join(', ');
    const g920 = state.history.get('09:20')?.gainers?.length || 0;
    log(`Restored ${rows.length} ranking rows from Postgres for ${toISODate(date)}. Timeline: ${times}`);
    log(`09:20 gainers in DB: ${g920}; sample keys: ${(state.history.get('09:20')?.gainers || []).slice(0, 5).map(x => x.name).join(', ') || '(none)'}`);
  }
}

async function latestStoredTradingDate() {
  if (!pool) return null;
  const today = istToday();
  const { rows } = await pool.query(
    `SELECT MAX(trading_date) AS d FROM rank_snapshots WHERE trading_date < $1`,
    [today]
  );
  return rows[0]?.d ? toISODate(rows[0].d) : null;
}

function resetSession(date) {
  state.currentDay = date;
  state.displayDate = date;
  state.displayMode = 'current';
  state.history.clear();
  state.membership = { gainers: [], losers: [] };
  state.live.clear();
  state.indices.clear();
  state.virtual.dayKey = date;
  state.virtual.tradesToday = 0;
  state.virtual.open = [];
  state.virtual.signals = [];
}

/* ---------- History rebuild (simplified full-depth) ---------- */
async function history5m(stock, appId, token, date) {
  const from = epochForISTDate(date, '09:10');
  const to = epochForISTDate(date, '15:35');
  const d = await fyersGet(DATA_HOST, '/history', {
    symbol: stock.symbol,
    resolution: '5',
    date_format: '0',
    range_from: from,
    range_to: to,
    cont_flag: '1'
  }, appId, token);
  return (d.candles || []).map(c => ({
    ts: Number(c[0]),
    open: Number(c[1]),
    high: Number(c[2]),
    low: Number(c[3]),
    close: Number(c[4]),
    volume: Number(c[5] || 0)
  }));
}

async function priorDayClose(stock, appId, token, date) {
  const from = epochForISTDate(date, '09:15') - 20 * 86400;
  const to = epochForISTDate(date, '09:15') - 60;
  const d = await fyersGet(DATA_HOST, '/history', {
    symbol: stock.symbol,
    resolution: 'D',
    date_format: '0',
    range_from: from,
    range_to: to,
    cont_flag: '1'
  }, appId, token);
  const cs = d.candles || [];
  if (!cs.length) return null;
  // last bar strictly before date
  for (let i = cs.length - 1; i >= 0; i--) {
    const barDate = new Date(Number(cs[i][0]) * 1000).toLocaleDateString('en-CA', { timeZone: 'Asia/Kolkata' });
    if (barDate < date) return Number(cs[i][4]);
  }
  return null;
}

async function rebuildHistoryFromFyers(appId, token) {
  const date = istToday();
  if (!regularSessionStarted()) {
    await loadLatestCompletedSession(appId, token);
    return;
  }
  if (state.currentDay !== date || state.displayMode !== 'current') resetSession(date);
  await loadDbHistory(date);
  const g920 = state.history.get('09:20')?.gainers?.length || 0;
  if (g920 >= Math.min(100, Math.floor(state.universe.length * 0.5))) {
    log(`Restored today's ranking history from PostgreSQL (${state.history.size} snapshots); FYERS rebuild not required.`);
    return;
  }
  log(`No/sparse stored ranking history for ${date}; rebuilding from FYERS…`);
  const universe = await ensureUniverse();
  const prevMap = new Map();
  try {
    const qs = await quotes(universe.map(s => s.symbol), appId, token);
    for (const q of qs) {
      if (q.symbol && Number.isFinite(q.prev) && q.prev > 0) prevMap.set(q.symbol, q.prev);
    }
  } catch (e) {
    log(`Bulk quotes for prev-close: ${e.message}`, 'warn');
  }

  const rows = [];
  for (let i = 0; i < universe.length; i++) {
    const stock = universe[i];
    try {
      let prev = prevMap.get(stock.symbol);
      if (!Number.isFinite(prev) || prev <= 0) {
        prev = await priorDayClose(stock, appId, token, date);
      }
      const candles = await history5m(stock, appId, token, date);
      if (Number.isFinite(prev) && prev > 0) rows.push({ stock, cs: candles, prev });
    } catch (e) {
      log(`${stock.name}: history rebuild failed: ${e.message}`, 'warn');
    }
    if ((i + 1) % 30 === 0) log(`History rebuild progress: ${i + 1}/${universe.length}`);
  }

  const buckets = new Map();
  for (const row of rows) {
    for (const candle of row.cs) {
      const closeEpoch = candle.ts + 300;
      const time = new Date(closeEpoch * 1000).toLocaleTimeString('en-GB', {
        timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit'
      });
      if (time < '09:20' || time > '15:30') continue;
      if (!buckets.has(time)) buckets.set(time, []);
      buckets.get(time).push({
        ...row.stock,
        close: candle.close,
        pct: pct(candle.close, row.prev)
      });
    }
  }

  for (const time of [...buckets.keys()].sort()) {
    const arr = buckets.get(time);
    const gainers = [...arr].sort((a, b) => b.pct - a.pct).map((s, i) => ({ ...s, rank: i + 1 }));
    const losers = [...arr].sort((a, b) => a.pct - b.pct).map((s, i) => ({ ...s, rank: i + 1 }));
    state.history.set(time, { gainers, losers });
    await saveSnapshot(date, time, 'gainers', gainers);
    await saveSnapshot(date, time, 'losers', losers);
  }
  log(`Historical ranking rebuild complete for ${date}: ${state.history.size} five-minute snapshots. Stocks: ${rows.length}/${universe.length}`);
}

async function loadLatestCompletedSession(appId, token) {
  const stored = await latestStoredTradingDate();
  if (stored) {
    state.currentDay = stored;
    state.displayDate = stored;
    state.displayMode = 'previous-close';
    state.history.clear();
    state.membership = { gainers: [], losers: [] };
    await loadDbHistory(stored);
    if (state.history.size) {
      log(`Pre-open / non-trading: showing last completed session ${stored}.`);
      return stored;
    }
  }
  log('No prior session in DB; previous-close ranking unavailable until first trading day is stored.', 'warn');
  return null;
}

async function makeCandleSnapshot() {
  if (!marketOpenNow() || !state.fyers.appId || state.displayMode !== 'current') return;
  const now = istParts();
  const minute = Number(now.time.slice(3, 5));
  if (minute % 5 !== 0) return;
  const t = now.time.slice(0, 5);
  if (t < '09:20' || t > '15:30' || state.history.has(t)) return;
  const live = rankedLive();
  if (live.gainers.length < Math.max(50, Math.floor(state.universe.length * 0.4))) {
    log(`Skipped ${t} snapshot: only ${live.gainers.length}/${state.universe.length} live symbols.`, 'warn');
    return;
  }
  const g = live.gainers.map(x => ({ ...x, close: x.ltp }));
  const l = live.losers.map(x => ({ ...x, close: x.ltp }));
  state.history.set(t, { gainers: g, losers: l });
  try {
    await saveSnapshot(now.date, t, 'gainers', g);
    await saveSnapshot(now.date, t, 'losers', l);
    log(`Saved ${t} 5-minute ranking snapshot.`);
  } catch (e) {
    log(`Snapshot DB error: ${e.message}`, 'warn');
  }
  broadcastDashboard();
}

/** Expected 5-min labels from 09:20 through now (or 15:30) */
function expectedRankTimes(upto) {
  const out = [];
  let h = 9, m = 20;
  const end = upto || '15:30';
  while (true) {
    const t = `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;
    if (t > end) break;
    out.push(t);
    m += 5;
    if (m >= 60) { h++; m -= 60; }
    if (h > 15 || (h === 15 && m > 30)) break;
  }
  return out;
}

async function gapFillMissingTimes(appId, token) {
  if (!appId || !token || Date.now() < fyersCooldownUntil) return;
  if (historyRebuildPromise) return;
  const date = state.displayDate || istToday();
  if (state.displayMode !== 'current' && date !== istToday()) {
    // still allow fill for displayed session day
  }
  const nowT = istParts().time.slice(0, 5);
  const cap = marketOpenNow() ? nowT : '15:30';
  const expected = expectedRankTimes(cap < '09:20' ? '09:20' : cap);
  const missing = expected.filter(t => !state.history.has(t));
  if (!missing.length) return;

  log(`[GAP-FILL] ${missing.length} missing times (${missing[0]}…${missing[missing.length - 1]}) for ${date}`);
  historyRebuildStartedAt = Date.now();
  historyRebuildPromise = (async () => {
    const universe = await ensureUniverse();
    const prevMap = new Map();
    try {
      const qs = await quotes(universe.map(s => s.symbol), appId, token);
      for (const q of qs) {
        if (q.symbol && Number.isFinite(q.prev) && q.prev > 0) prevMap.set(q.symbol, q.prev);
      }
    } catch (e) {
      log(`[GAP-FILL] quotes: ${e.message}`, 'warn');
    }

    const buckets = new Map(missing.map(t => [t, []]));
    let ok = 0;
    for (let i = 0; i < universe.length; i++) {
      if (Date.now() < fyersCooldownUntil) {
        log('[GAP-FILL] paused — FYERS rate limit', 'warn');
        break;
      }
      const stock = universe[i];
      try {
        let prev = prevMap.get(stock.symbol);
        if (!Number.isFinite(prev) || prev <= 0) {
          prev = await priorDayClose(stock, appId, token, date);
        }
        if (!Number.isFinite(prev) || prev <= 0) continue;
        const candles = await history5m(stock, appId, token, date);
        for (const candle of candles) {
          const time = new Date((candle.ts + 300) * 1000).toLocaleTimeString('en-GB', {
            timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit'
          });
          if (!buckets.has(time)) continue;
          buckets.get(time).push({
            ...stock,
            close: candle.close,
            pct: pct(candle.close, prev)
          });
        }
        ok++;
      } catch (e) {
        if (String(e.message).includes('429')) break;
      }
      if ((i + 1) % 40 === 0) log(`[GAP-FILL] progress ${i + 1}/${universe.length}`);
    }

    for (const time of missing) {
      const arr = buckets.get(time) || [];
      if (arr.length < 30) continue;
      const gainers = [...arr].sort((a, b) => b.pct - a.pct).map((s, i) => ({ ...s, rank: i + 1 }));
      const losers = [...arr].sort((a, b) => a.pct - b.pct).map((s, i) => ({ ...s, rank: i + 1 }));
      state.history.set(time, { gainers, losers });
      try {
        await saveSnapshot(date, time, 'gainers', gainers);
        await saveSnapshot(date, time, 'losers', losers);
      } catch (e) {
        log(`[GAP-FILL] save ${time}: ${e.message}`, 'warn');
      }
    }
    log(`[GAP-FILL] done for ${date}. Timeline now: ${[...state.history.keys()].sort().join(', ')}`);
    broadcastDashboard();
  })()
    .catch(e => log(`[GAP-FILL] error: ${e.message}`, 'warn'))
    .finally(() => { historyRebuildPromise = null; });

  return historyRebuildPromise;
}

/* ---------- Virtual trades DB ---------- */
async function saveVirtualTrade(trade) {
  if (!pool) return null;
  const { rows } = await pool.query(
    `INSERT INTO virtual_trades
     (strategy, trading_date, side, symbol, name, sector, entry_ts, entry_price, qty,
      stop_loss, target1, target2, status, checks, notes)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
     RETURNING id`,
    [
      trade.strategy || 'S1',
      trade.tradingDate,
      trade.side,
      trade.symbol,
      trade.name,
      trade.sector,
      trade.entryTs,
      trade.entryPrice,
      trade.qty,
      trade.stopLoss,
      trade.target1,
      trade.target2,
      'OPEN',
      JSON.stringify(trade.checks || {}),
      trade.notes || null
    ]
  );
  return rows[0]?.id;
}

async function updateVirtualTrade(id, patch) {
  if (!pool || !id) return;
  await pool.query(
    `UPDATE virtual_trades SET
       exit_ts = COALESCE($2, exit_ts),
       exit_price = COALESCE($3, exit_price),
       exit_reason = COALESCE($4, exit_reason),
       pnl = COALESCE($5, pnl),
       status = COALESCE($6, status),
       stop_loss = COALESCE($7, stop_loss),
       qty = COALESCE($8, qty)
     WHERE id=$1`,
    [id, patch.exitTs || null, patch.exitPrice ?? null, patch.exitReason || null,
      patch.pnl ?? null, patch.status || null, patch.stopLoss ?? null, patch.qty ?? null]
  );
}

async function loadVirtualTrades(fromDate, toDate) {
  if (!pool) return [...state.virtual.closed, ...state.virtual.open];
  const { rows } = await pool.query(
    `SELECT * FROM virtual_trades
     WHERE trading_date >= $1 AND trading_date <= $2
     ORDER BY entry_ts DESC`,
    [fromDate, toDate]
  );
  return rows;
}

/* ---------- Telegram ---------- */
async function telegramSend(text) {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chat = process.env.TELEGRAM_CHAT_ID;
  if (!token || !chat) {
    log('Telegram not configured (set TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID).', 'warn');
    return false;
  }
  try {
    const r = await fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: chat, text, parse_mode: 'HTML', disable_web_page_preview: true })
    });
    if (!r.ok) {
      const t = await r.text();
      log(`Telegram send failed: ${t.slice(0, 120)}`, 'warn');
      return false;
    }
    return true;
  } catch (e) {
    log(`Telegram error: ${e.message}`, 'warn');
    return false;
  }
}

/* ---------- Strategy 1 indicators ---------- */
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
    const g = d > 0 ? d : 0;
    const l = d < 0 ? -d : 0;
    avgG = (avgG * (period - 1) + g) / period;
    avgL = (avgL * (period - 1) + l) / period;
  }
  if (avgL === 0) return 100;
  const rs = avgG / avgL;
  return 100 - 100 / (1 + rs);
}

function atr(candles, period = 14) {
  if (candles.length < period + 1) return null;
  const trs = [];
  for (let i = 1; i < candles.length; i++) {
    const h = candles[i].high, l = candles[i].low, pc = candles[i - 1].close;
    trs.push(Math.max(h - l, Math.abs(h - pc), Math.abs(l - pc)));
  }
  const slice = trs.slice(-period);
  return slice.reduce((a, b) => a + b, 0) / slice.length;
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

async function fetchToday5m(stock, appId, token) {
  const date = istToday();
  return history5m(stock, appId, token, date);
}

/* ---------- Strategy 1 engine ---------- */
function s1InWatchWindow() {
  const { time } = istParts();
  return time >= '09:20:00' && time < '09:50:00';
}
function s1InEntryWindow() {
  const { time } = istParts();
  return time >= '09:50:00' && time < '11:00:00';
}
function s1ForceExitTime() {
  return istParts().time >= '15:15:00';
}

async function evaluateS1Candidate(stock, rank, rank920, sectorAvg, marketAdvPct, appId, token) {
  const checks = {
    topRank: false,
    rankAccel: false,
    sector: false,
    volume: false,
    trend: false,
    rsi: false,
    market: false
  };
  const notes = [];

  // 1 Top rank (top 15)
  checks.topRank = rank >= 1 && rank <= 15;
  if (!checks.topRank) notes.push(`rank ${rank} not in top 15`);

  // 2 Rank acceleration
  if (rank920 != null) {
    const delta = rank920 - rank; // positive = improved
    checks.rankAccel = delta >= 10;
    notes.push(`rankΔ ${delta >= 0 ? '+' : ''}${delta} (09:20→now)`);
  } else {
    checks.rankAccel = rank <= 10;
    notes.push('no 09:20 rank; using top-10 proxy');
  }

  // 3 Sector backing
  checks.sector = Number.isFinite(sectorAvg) && sectorAvg > 0.3;
  notes.push(`sectorAvg ${sectorAvg?.toFixed?.(2) ?? 'n/a'}%`);

  // 4–6 need candles
  let candles = [];
  try {
    candles = await fetchToday5m(stock, appId, token);
  } catch (e) {
    notes.push(`candles fail: ${e.message}`);
    return { pass: false, checks, notes, side: null };
  }
  if (candles.length < 20) {
    notes.push('insufficient 5m bars');
    return { pass: false, checks, notes, side: null };
  }

  const closes = candles.map(c => c.close);
  const last = candles[candles.length - 1];
  const e9 = ema(closes, 9);
  const e21 = ema(closes, 21);
  const e50 = ema(closes, 50);
  const vw = vwapFromCandles(candles);
  const r = rsi(closes, 14);
  const a = atr(candles, 14);

  const live = state.live.get(stock.symbol);
  const ltp = live?.ltp || last.close;
  const volToday = live?.volume || last.volume || 0;
  const d1 = state.d1.get(stock.key);
  const avgVol = d1?.avgVol10 || 0;
  // volume vs typical full-day: early session scale ~ minutes/375
  const mins = Math.max(1, (Number(istParts().time.slice(0, 2)) * 60 + Number(istParts().time.slice(3, 5))) - (9 * 60 + 15));
  const expectedFrac = Math.min(1, mins / 375);
  const volRatio = avgVol > 0 ? volToday / (avgVol * Math.max(0.15, expectedFrac)) : 0;
  checks.volume = volRatio >= 1.5;
  notes.push(`volRatio ${volRatio.toFixed(2)}`);

  const bullTrend = ltp > (vw || 0) && e9[e9.length - 1] > e21[e21.length - 1] && e21[e21.length - 1] > e50[e50.length - 1];
  const bearTrend = ltp < (vw || Infinity) && e9[e9.length - 1] < e21[e21.length - 1] && e21[e21.length - 1] < e50[e50.length - 1];
  checks.trend = bullTrend || bearTrend;
  notes.push(bullTrend ? 'EMA/VWAP bull' : bearTrend ? 'EMA/VWAP bear' : 'no trend align');

  checks.rsi = r != null && r >= 55 && r <= 68;
  if (r != null && r > 75) notes.push(`RSI ${r.toFixed(1)} overbought`);
  else notes.push(`RSI ${r?.toFixed?.(1) ?? 'n/a'}`);

  // 7 Market mood
  const declineHeavy = marketAdvPct < 30; // <30% advancing → avoid buys
  checks.market = !declineHeavy || bearTrend;
  notes.push(`adv% ${marketAdvPct.toFixed(0)}`);

  const longPass = checks.topRank && checks.rankAccel && checks.sector && checks.volume && bullTrend && checks.rsi && !declineHeavy;
  const shortPass = checks.topRank && checks.rankAccel && sectorAvg < -0.3 && checks.volume && bearTrend && r != null && r <= 45 && r >= 32 && declineHeavy;

  if (longPass) {
    return { pass: true, side: 'BUY', checks: { ...checks, trend: true }, notes, ltp, atr: a, rsi: r, vwap: vw };
  }
  if (shortPass) {
    return { pass: true, side: 'SELL', checks: { ...checks, trend: true, rsi: true }, notes, ltp, atr: a, rsi: r, vwap: vw };
  }
  return { pass: false, checks, notes, side: null, ltp, atr: a };
}

async function scanStrategy1() {
  if (!state.virtual.enabled || !state.fyers.appId || !state.fyers.token) return;
  if (!marketOpenNow()) return;
  if (!s1InEntryWindow()) {
    if (s1InWatchWindow()) {
      // only leaderboard — no trades
      return;
    }
    return;
  }
  if (state.virtual.tradesToday >= state.virtual.maxTrades) return;
  if (Date.now() - state.virtual.lastScanAt < 60000) return; // min 1 min between scans
  state.virtual.lastScanAt = Date.now();

  const live = rankedLive();
  if (live.gainers.length < 50) return;

  const b = breadth(live.gainers.concat(live.losers));
  const marketAdvPct = b.total ? (b.advances / b.total) * 100 : 50;
  const sectors = sectorData(live.gainers.concat(live.losers));
  const sectorAvg = new Map(sectors.map(s => [s.sector, s.avgPct]));
  const baseG = state.history.get('09:20')?.gainers || [];
  const baseMap = new Map(baseG.map(x => [normalizeKey(x.key), x.rank]));

  // Candidates: top 15 gainers + top 10 losers if market weak
  const candidates = [
    ...live.gainers.slice(0, 15),
    ...(marketAdvPct < 35 ? live.losers.slice(0, 10) : [])
  ];

  const openKeys = new Set(state.virtual.open.map(t => t.key));
  log(`S1 scan: ${candidates.length} candidates, adv%=${marketAdvPct.toFixed(0)}, tradesToday=${state.virtual.tradesToday}`);

  for (const c of candidates) {
    if (state.virtual.tradesToday >= state.virtual.maxTrades) break;
    if (openKeys.has(c.key)) continue;

    const evalRes = await evaluateS1Candidate(
      c,
      c.rank,
      baseMap.get(normalizeKey(c.key)),
      sectorAvg.get(c.sector) || 0,
      marketAdvPct,
      state.fyers.appId,
      state.fyers.token
    );

    state.virtual.signals.push({
      ts: new Date().toISOString(),
      key: c.key,
      name: c.name,
      side: evalRes.side,
      pass: evalRes.pass,
      rank: c.rank,
      pct: c.pct,
      checks: evalRes.checks,
      notes: evalRes.notes
    });
    if (state.virtual.signals.length > 100) state.virtual.signals.shift();

    if (!evalRes.pass || !evalRes.side) continue;

    // Position size: 1% risk
    const entry = evalRes.ltp;
    if (!Number.isFinite(entry) || entry <= 0) continue;
    let atrVal = evalRes.atr;
    if (!Number.isFinite(atrVal) || atrVal <= 0) atrVal = entry * 0.01;
    const slDist = Math.min(Math.max(atrVal, entry * 0.008), entry * 0.012);
    const stopLoss = evalRes.side === 'BUY' ? entry - slDist : entry + slDist;
    const riskPerShare = Math.abs(entry - stopLoss);
    if (riskPerShare <= 0) continue;
    const riskAmt = state.virtual.capital * (state.virtual.riskPct / 100);
    let qty = Math.floor(riskAmt / riskPerShare);
    if (qty < 1) qty = 1;
    const target1 = evalRes.side === 'BUY' ? entry + 2 * riskPerShare : entry - 2 * riskPerShare;
    const target2 = evalRes.side === 'BUY' ? entry + 3 * riskPerShare : entry - 3 * riskPerShare;

    const trade = {
      strategy: 'S1',
      tradingDate: istToday(),
      side: evalRes.side,
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
      checks: evalRes.checks,
      notes: evalRes.notes.join('; '),
      realizedPnl: 0
    };

    try {
      trade.id = await saveVirtualTrade(trade);
    } catch (e) {
      log(`Virtual trade DB save: ${e.message}`, 'warn');
    }
    state.virtual.open.push(trade);
    state.virtual.tradesToday++;
    openKeys.add(c.key);

    const msg =
      `🟢 <b>S1 VIRTUAL ${trade.side}</b>\n` +
      `<b>${trade.name}</b> (${trade.sector})\n` +
      `Entry: ₹${entry.toFixed(2)} × ${qty}\n` +
      `SL: ₹${stopLoss.toFixed(2)} | T1: ₹${target1.toFixed(2)} | T2: ₹${target2.toFixed(2)}\n` +
      `Rank: ${c.rank} | %: ${c.pct?.toFixed?.(2)}%\n` +
      `Risk ~1% | RR 1:2 then trail\n` +
      `${trade.notes}`;
    await telegramSend(msg);
    log(`S1 virtual ${trade.side} ${trade.name} @ ${entry.toFixed(2)} qty=${qty}`);
    broadcastDashboard();
  }
}

function manageOpenVirtualTrades() {
  if (!state.virtual.open.length) return;
  const force = s1ForceExitTime() || (regularSessionFinished() && isTradingSessionDay());

  for (const trade of [...state.virtual.open]) {
    const q = state.live.get(trade.symbol);
    const px = q?.ltp;
    if (!Number.isFinite(px)) {
      if (force) closeVirtualTrade(trade, trade.entryPrice, 'EOD_NO_QUOTE');
      continue;
    }

    if (trade.side === 'BUY') {
      if (px <= trade.stopLoss) {
        closeVirtualTrade(trade, trade.stopLoss, trade.partialDone ? 'BE_STOP' : 'STOP');
        continue;
      }
      if (!trade.partialDone && px >= trade.target1) {
        // book 60%
        const bookQty = Math.max(1, Math.floor(trade.remainingQty * 0.6));
        const pnlPart = (px - trade.entryPrice) * bookQty;
        trade.realizedPnl += pnlPart;
        trade.remainingQty -= bookQty;
        trade.partialDone = true;
        trade.stopLoss = trade.entryPrice; // breakeven
        log(`S1 partial exit ${trade.name}: ${bookQty} @ ${px.toFixed(2)}, SL→BE`);
        telegramSend(
          `💰 <b>S1 PARTIAL 60%</b> ${trade.name}\n` +
          `Exit: ₹${px.toFixed(2)} × ${bookQty}\n` +
          `PnL part: ₹${pnlPart.toFixed(0)}\n` +
          `Rest ${trade.remainingQty} SL@BE → T2 ₹${trade.target2.toFixed(2)}`
        );
        if (trade.id) {
          updateVirtualTrade(trade.id, { stopLoss: trade.stopLoss, qty: trade.remainingQty }).catch(() => {});
        }
      }
      if (trade.partialDone && px >= trade.target2) {
        closeVirtualTrade(trade, px, 'TARGET2');
        continue;
      }
    } else {
      // SELL
      if (px >= trade.stopLoss) {
        closeVirtualTrade(trade, trade.stopLoss, trade.partialDone ? 'BE_STOP' : 'STOP');
        continue;
      }
      if (!trade.partialDone && px <= trade.target1) {
        const bookQty = Math.max(1, Math.floor(trade.remainingQty * 0.6));
        const pnlPart = (trade.entryPrice - px) * bookQty;
        trade.realizedPnl += pnlPart;
        trade.remainingQty -= bookQty;
        trade.partialDone = true;
        trade.stopLoss = trade.entryPrice;
        log(`S1 partial exit ${trade.name}: ${bookQty} @ ${px.toFixed(2)}, SL→BE`);
        telegramSend(
          `💰 <b>S1 PARTIAL 60%</b> ${trade.name} SHORT\n` +
          `Exit: ₹${px.toFixed(2)} × ${bookQty}\n` +
          `PnL part: ₹${pnlPart.toFixed(0)}`
        );
      }
      if (trade.partialDone && px <= trade.target2) {
        closeVirtualTrade(trade, px, 'TARGET2');
        continue;
      }
    }

    if (force) closeVirtualTrade(trade, px, 'EOD_FLAT');
  }
}

function closeVirtualTrade(trade, exitPrice, reason) {
  const qty = trade.remainingQty || trade.qty;
  const pnlRest = trade.side === 'BUY'
    ? (exitPrice - trade.entryPrice) * qty
    : (trade.entryPrice - exitPrice) * qty;
  const pnl = (trade.realizedPnl || 0) + pnlRest;
  trade.exitTs = new Date().toISOString();
  trade.exitPrice = exitPrice;
  trade.exitReason = reason;
  trade.pnl = pnl;
  trade.status = 'CLOSED';
  state.virtual.open = state.virtual.open.filter(t => t !== trade);
  state.virtual.closed.unshift(trade);
  if (state.virtual.closed.length > 200) state.virtual.closed.pop();
  if (trade.id) {
    updateVirtualTrade(trade.id, {
      exitTs: trade.exitTs,
      exitPrice,
      exitReason: reason,
      pnl,
      status: 'CLOSED',
      qty
    }).catch(() => {});
  }
  const emoji = pnl >= 0 ? '✅' : '❌';
  telegramSend(
    `${emoji} <b>S1 CLOSED</b> ${trade.side} ${trade.name}\n` +
    `Exit: ₹${exitPrice.toFixed(2)} (${reason})\n` +
    `PnL: ₹${pnl.toFixed(0)}`
  );
  log(`S1 closed ${trade.name} ${reason} PnL=${pnl.toFixed(0)}`);
  broadcastDashboard();
}

function virtualReportSummary(trades) {
  const closed = trades.filter(t => (t.status || t.status) === 'CLOSED' || t.exit_ts || t.exitTs);
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
    winRate: closed.length ? (wins / closed.length) * 100 : 0,
    totalPnl: pnl
  };
}

/* ---------- Dashboard payload ---------- */
function snapshotAtCurrent() {
  const live = rankedLive();
  let displayLive = live;
  if (state.displayMode === 'previous-close' || (!live.gainers.length && state.history.size)) {
    const times = [...state.history.keys()].filter(t => t !== 'CLOSE').sort();
    const key = times.length ? times[times.length - 1] : 'CLOSE';
    const h = state.history.get(key);
    if (h) {
      displayLive = {
        gainers: (h.gainers || []).map(x => ({ ...x, ltp: x.close, pct: x.pct })),
        losers: (h.losers || []).map(x => ({ ...x, ltp: x.close, pct: x.pct }))
      };
    }
  }
  const g = mergeRows('gainers', displayLive.gainers);
  const l = mergeRows('losers', displayLive.losers);
  // Unique F&O rows only (liveRows or one side of ranked list — not gainers+losers concat)
  const uniqueLive = liveRows().length
    ? liveRows()
    : (() => {
        const m = new Map();
        for (const x of (displayLive.gainers || [])) m.set(normalizeKey(x.key), x);
        return [...m.values()];
      })();
  const times = [...new Set([...g.times, ...l.times])].sort((a, b) =>
    a === 'CURRENT' ? 1 : b === 'CURRENT' ? -1 : a.localeCompare(b)
  );
  return {
    gainers: g.rows,
    losers: l.rows,
    times,
    sector: sectorData(uniqueLive),
    breadth: breadth(uniqueLive),
    niftyBreadth: breadth(uniqueLive.filter(x => NIFTY50.has(normalizeKey(x.key))))
  };
}

async function dashboardData() {
  const now = istParts();
  const preOpen = !regularSessionStarted();
  const postClose = regularSessionFinished();
  const snap = snapshotAtCurrent();
  const indices = INDEX_SYMBOLS.map(([name, symbol]) => ({
    name,
    symbol,
    ...(state.indices.get(symbol) || { ltp: null, changePct: null })
  }));

  let displayLabel = "Today's live trading session";
  if (preOpen || !isTradingSessionDay()) displayLabel = 'Last completed / pre-open session';
  else if (postClose) displayLabel = "Today's completed trading session";

  return {
    date: state.displayDate || istToday(),
    calendarDate: now.date,
    marketOpen: marketOpenNow(),
    sessionStarted: regularSessionStarted(),
    sessionFinished: regularSessionFinished(),
    displayMode: state.displayMode,
    displayLabel,
    updatedAt: new Date().toISOString(),
    indices,
    universeSize: state.universe.length,
    connected: state.fyers.connected,
    histSnapshots: state.history.size,
    ...snap,
    virtual: {
      enabled: state.virtual.enabled,
      capital: state.virtual.capital,
      riskPct: state.virtual.riskPct,
      maxTrades: state.virtual.maxTrades,
      tradesToday: state.virtual.tradesToday,
      open: state.virtual.open,
      closed: state.virtual.closed.slice(0, 30),
      signals: state.virtual.signals.slice(-20).reverse(),
      summary: virtualReportSummary([...state.virtual.open, ...state.virtual.closed])
    },
    logs: state.log.slice(-80)
  };
}

function broadcast(obj) {
  const text = JSON.stringify(obj);
  for (const ws of browserSockets) {
    try { if (ws.readyState === 1) ws.send(text); } catch {}
  }
}

function broadcastDashboard() {
  if (broadcastTimer) return;
  broadcastTimer = setTimeout(async () => {
    broadcastTimer = null;
    try {
      const d = await dashboardData();
      broadcast({ type: 'dashboard', data: d });
    } catch (e) {
      log(`Broadcast error: ${e.message}`, 'warn');
    }
  }, 300);
}

/* ---------- FYERS socket ---------- */
let wsReconnectAttempt = 0;
let wsConnecting = false;

function startFyersSocket() {
  if (!state.fyers.appId || !state.fyers.token) return;
  if (wsConnecting) return;
  wsConnecting = true;

  clearTimeout(state.fyers.reconnectTimer);

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
    log(`FYERS socket getInstance: ${e.message}`, 'warn');
    scheduleWsReconnect();
    return;
  }
  state.fyers.socket = skt;

  skt.on('connect', () => {
    wsConnecting = false;
    wsReconnectAttempt = 0;
    state.fyers.connected = true;
    log('FYERS market-data WebSocket connected.');
    const symbols = [
      ...state.universe.map(x => x.symbol),
      ...INDEX_SYMBOLS.map(x => x[1])
    ];
    try {
      // Subscribe in chunks to reduce disconnect risk
      for (let i = 0; i < symbols.length; i += 80) {
        skt.subscribe(symbols.slice(i, i + 80));
      }
    } catch (e) {
      log(`Subscribe error: ${e.message || e}`, 'warn');
    }
    broadcastDashboard();
  });

  skt.on('message', msg => {
    const parsed = parseFyersMessage(msg);
    const arr = Array.isArray(parsed) ? parsed : [parsed];
    for (const q of arr.filter(Boolean)) {
      if (INDEX_SYMBOLS.some(x => x[1] === q.symbol)) state.indices.set(q.symbol, q);
      else state.live.set(q.symbol, q);
    }
    if (arr.length) {
      manageOpenVirtualTrades();
      broadcastDashboard();
    }
  });

  skt.on('error', e => {
    const s = typeof e === 'string' ? e : JSON.stringify(e);
    log(`FYERS WebSocket error: ${s}`, 'error');
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
    log(`Socket start: ${e.message}`, 'warn');
    scheduleWsReconnect();
  }
}

function scheduleWsReconnect() {
  if (!state.fyers.appId || !state.fyers.token) return;
  clearTimeout(state.fyers.reconnectTimer);
  wsReconnectAttempt = Math.min(wsReconnectAttempt + 1, 6);
  // 15s, 30s, 45s, 60s, 90s, 120s — stop 12s reconnect storm
  const delay = Math.min(120000, 15000 * wsReconnectAttempt);
  state.fyers.reconnectTimer = setTimeout(() => {
    if (state.fyers.appId && state.fyers.token) {
      log(`Attempting FYERS WebSocket reconnect (attempt ${wsReconnectAttempt}, wait ${delay / 1000}s)…`);
      startFyersSocket();
    }
  }, delay);
}

/* ---------- Chart API (14d 5m) ---------- */
async function chartData(key, days = 14) {
  const stock = state.universe.find(x => normalizeKey(x.key) === normalizeKey(key));
  if (!stock) throw new Error('Unknown symbol');
  if (!state.fyers.appId || !state.fyers.token) throw new Error('Not logged in');
  const today = istToday();
  const from = epochForISTDate(today, '09:15') - days * 86400;
  const to = epochForISTDate(today, '15:35');
  const d = await fyersGet(DATA_HOST, '/history', {
    symbol: stock.symbol,
    resolution: '5',
    date_format: '0',
    range_from: from,
    range_to: to,
    cont_flag: '1'
  }, state.fyers.appId, state.fyers.token, 0);
  const candles = (d.candles || []).map(c => ({
    time: Number(c[0]),
    open: Number(c[1]),
    high: Number(c[2]),
    low: Number(c[3]),
    close: Number(c[4]),
    volume: Number(c[5] || 0)
  }));
  let ltp = state.live.get(stock.symbol)?.ltp || null;
  return { stock, candles, ltp, days };
}

/* ---------- HTTP ---------- */
function readBody(req) {
  return new Promise((resolve, reject) => {
    let b = '';
    req.on('data', c => {
      b += c;
      if (b.length > 1e6) { req.destroy(); reject(new Error('Body too large')); }
    });
    req.on('end', () => {
      try { resolve(b ? JSON.parse(b) : {}); } catch (e) { reject(e); }
    });
    req.on('error', reject);
  });
}
function cors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, x-fyers-app-id, x-fyers-access-token');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
}
function send(res, status, data, type = 'application/json') {
  cors(res);
  res.writeHead(status, { 'Content-Type': type, 'Cache-Control': 'no-store' });
  res.end(type === 'application/json' ? JSON.stringify(data) : data);
}

async function seedQuotes(appId, token) {
  const universe = await ensureUniverse();
  try {
    const qs = await quotes(universe.map(s => s.symbol).concat(INDEX_SYMBOLS.map(x => x[1])), appId, token);
    for (const q of qs) {
      if (INDEX_SYMBOLS.some(x => x[1] === q.symbol)) state.indices.set(q.symbol, q);
      else state.live.set(q.symbol, q);
    }
    log(`Background: seeded live quotes for ${universe.length} symbols.`);
  } catch (e) {
    log(`Quote seed: ${e.message}`, 'warn');
  }
}

async function loadD1Levels(appId, token) {
  const universe = await ensureUniverse();
  const date = istToday();
  let n = 0;
  for (const stock of universe) {
    try {
      const from = epochForISTDate(date, '09:15') - 25 * 86400;
      const to = epochForISTDate(date, '09:15') - 60;
      const d = await fyersGet(DATA_HOST, '/history', {
        symbol: stock.symbol, resolution: 'D', date_format: '0',
        range_from: from, range_to: to, cont_flag: '1'
      }, appId, token, 0);
      const cs = (d.candles || []).filter(c => {
        const bd = new Date(Number(c[0]) * 1000).toLocaleDateString('en-CA', { timeZone: 'Asia/Kolkata' });
        return bd < date;
      });
      if (!cs.length) continue;
      const last = cs[cs.length - 1];
      const volSlice = cs.slice(-10);
      const avgVol10 = volSlice.reduce((a, c) => a + Number(c[5] || 0), 0) / volSlice.length;
      const high = Number(last[2]), low = Number(last[3]), open = Number(last[1]), close = Number(last[4]);
      const rangePct = close ? Math.abs(high - low) / close * 100 : 0;
      const chgPct = open ? Math.abs(close - open) / open * 100 : 0;
      state.d1.set(stock.key, {
        high, low, open, close, avgVol10,
        consolidation: rangePct <= 1.5 && chgPct <= 1.5,
        rangePct, chgPct
      });
      n++;
    } catch {}
  }
  log(`D-1 levels + volume MA(10) loaded for ${n}/${universe.length} stocks.`);
}

async function route(req, res) {
  cors(res);
  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    return res.end();
  }
  const u = new URL(req.url, `http://${req.headers.host}`);
  const appId = req.headers['x-fyers-app-id'] || state.fyers.appId;
  const token = req.headers['x-fyers-access-token'] || state.fyers.token;

  try {
    if (u.pathname === '/api/health') {
      return send(res, 200, {
        ok: true,
        universe: state.universe.length,
        connected: state.fyers.connected,
        history: state.history.size,
        virtualOpen: state.virtual.open.length
      });
    }

    if (u.pathname === '/api/login' && req.method === 'POST') {
      const body = await readBody(req);
      const id = String(body.appId || '').trim();
      const tok = String(body.accessToken || '').trim();
      if (!id || !tok) return send(res, 400, { ok: false, error: 'App ID and access token required' });

      let probeOk = false;
      try {
        await fyersGet(DATA_HOST, '/quotes', { symbols: 'NSE:NIFTY50-INDEX' }, id, tok, 0);
        probeOk = true;
      } catch (e) {
        const msg = e.message || '';
        if (msg.includes('401') || msg.toLowerCase().includes('valid token') || msg.includes('-15')) {
          return send(res, 401, { ok: false, error: msg });
        }
        log(`Login probe: ${msg} — accepting credentials; ranks from DB when possible.`, 'warn');
      }

      state.fyers.appId = id;
      state.fyers.token = tok;
      log('FYERS credentials accepted (token not logged).');
      await ensureUniverse();

      const today = istToday();
      if (!regularSessionStarted() || !isTradingSessionDay()) {
        await loadLatestCompletedSession(id, tok);
      } else {
        if (state.currentDay !== today) resetSession(today);
        await loadDbHistory(today);
        if (!state.history.size || !(state.history.get('09:20')?.gainers?.length > 30)) {
          if (!historyRebuildPromise) {
            historyRebuildStartedAt = Date.now();
            historyRebuildPromise = rebuildHistoryFromFyers(id, tok)
              .catch(e => log(`Background rebuild: ${e.message}`, 'warn'))
              .finally(() => { historyRebuildPromise = null; });
          }
        } else {
          log(`Post-login DB ranks: ${state.history.size} snapshot times for ${today}.`);
          // Fill holes like missing 09:25 without full rebuild
          setTimeout(() => {
            gapFillMissingTimes(id, tok).catch(e => log(`Gap-fill: ${e.message}`, 'warn'));
          }, 5000);
        }
      }

      startFyersSocket();
      setTimeout(() => {
        if (Date.now() >= fyersCooldownUntil) {
          seedQuotes(id, tok).catch(() => {});
        }
        setTimeout(() => {
          if (Date.now() >= fyersCooldownUntil) {
            loadD1Levels(id, tok).catch(e => log(`D1 load: ${e.message}`, 'warn'));
          }
        }, 90000);
      }, 3000);

      return send(res, 200, { ok: true, probeOk, universe: state.universe.length });
    }

    if (u.pathname === '/api/logout' && req.method === 'POST') {
      try { if (state.fyers.socket) state.fyers.socket.close(); } catch {}
      state.fyers = { appId: '', token: '', socket: null, connected: false, reconnectTimer: null };
      log('Logged out; FYERS credentials cleared.', 'warn');
      return send(res, 200, { ok: true });
    }

    if (u.pathname === '/api/dashboard' && req.method === 'GET') {
      if (appId && token && !state.fyers.appId) {
        state.fyers.appId = appId;
        state.fyers.token = token;
        log('Session restore from browser headers (DB ranks + WebSocket).');
        await ensureUniverse();
        const today = istToday();
        if (!regularSessionStarted() || !isTradingSessionDay()) {
          await loadLatestCompletedSession(appId, token);
        } else {
          await loadDbHistory(today);
        }
        startFyersSocket();
      }
      return send(res, 200, await dashboardData());
    }

    if (u.pathname === '/api/logs') {
      return send(res, 200, { logs: state.log.slice(-100) });
    }

    if (u.pathname === '/api/chart' && req.method === 'GET') {
      const key = u.searchParams.get('key') || '';
      const days = Math.min(20, Math.max(1, Number(u.searchParams.get('days') || 14)));
      const data = await chartData(key, days);
      return send(res, 200, data);
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
      const rows = await loadVirtualTrades(from, to);
      const mapped = rows.map(r => ({
        id: r.id,
        strategy: r.strategy || 'S1',
        date: toISODate(r.trading_date || r.tradingDate),
        side: r.side,
        name: r.name,
        symbol: r.symbol,
        sector: r.sector,
        entryTs: r.entry_ts || r.entryTs,
        entryPrice: Number(r.entry_price ?? r.entryPrice),
        qty: Number(r.qty),
        stopLoss: Number(r.stop_loss ?? r.stopLoss),
        target1: Number(r.target1),
        target2: Number(r.target2),
        exitTs: r.exit_ts || r.exitTs,
        exitPrice: r.exit_price != null ? Number(r.exit_price) : r.exitPrice,
        exitReason: r.exit_reason || r.exitReason,
        pnl: r.pnl != null ? Number(r.pnl) : null,
        status: r.status,
        checks: r.checks,
        notes: r.notes
      }));
      return send(res, 200, {
        from,
        to,
        summary: virtualReportSummary(mapped),
        trades: mapped
      });
    }

    if (u.pathname === '/api/virtual/toggle' && req.method === 'POST') {
      const body = await readBody(req);
      if (typeof body.enabled === 'boolean') state.virtual.enabled = body.enabled;
      if (Number.isFinite(Number(body.capital))) state.virtual.capital = Number(body.capital);
      log(`Virtual trading ${state.virtual.enabled ? 'ENABLED' : 'DISABLED'}; capital=₹${state.virtual.capital}`);
      return send(res, 200, { ok: true, enabled: state.virtual.enabled, capital: state.virtual.capital });
    }

    if (u.pathname === '/api/virtual/scan' && req.method === 'POST') {
      await scanStrategy1();
      return send(res, 200, { ok: true, tradesToday: state.virtual.tradesToday, open: state.virtual.open.length });
    }

    return send(res, 404, { error: 'Not found' });
  } catch (e) {
    log(e.message, 'error');
    return send(res, 500, { ok: false, error: e.message, logs: state.log.slice(-20) });
  }
}

/* ---------- Server ---------- */
const htmlPath = path.join(__dirname, 'index.html');
const server = http.createServer((req, res) => {
  const u = new URL(req.url, `http://${req.headers.host}`);
  if (u.pathname.startsWith('/api/')) return route(req, res);
  if (req.method === 'GET' && (u.pathname === '/' || u.pathname === '/index.html')) {
    const html = fs.readFileSync(htmlPath);
    cors(res);
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
    return res.end(html);
  }
  send(res, 404, 'Not found', 'text/plain');
});

const { WebSocketServer } = require('ws');
const wss = new WebSocketServer({ noServer: true });
wss.on('connection', async ws => {
  browserSockets.add(ws);
  try {
    ws.send(JSON.stringify({ type: 'dashboard', data: await dashboardData() }));
  } catch {}
  ws.on('close', () => browserSockets.delete(ws));
});
server.on('upgrade', (req, socket, head) => {
  const u = new URL(req.url, `http://${req.headers.host}`);
  if (u.pathname !== '/ws') { socket.destroy(); return; }
  wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, req));
});

/* ---------- Timers ---------- */
setInterval(async () => {
  if (!state.fyers.appId) return;
  try {
    const today = istToday();
    if (!regularSessionStarted() || !isTradingSessionDay()) {
      if (state.displayMode !== 'previous-close') {
        await loadLatestCompletedSession(state.fyers.appId, state.fyers.token);
        broadcastDashboard();
      }
      return;
    }
    if (state.currentDay !== today || state.displayMode !== 'current') {
      resetSession(today);
      if (!historyRebuildPromise) {
        historyRebuildPromise = rebuildHistoryFromFyers(state.fyers.appId, state.fyers.token)
          .catch(e => log(`Session rebuild: ${e.message}`, 'warn'))
          .finally(() => { historyRebuildPromise = null; });
      }
      broadcastDashboard();
    }
  } catch (e) {
    log(`Session monitor: ${e.message}`, 'warn');
  }
}, 5000);

setInterval(() => {
  makeCandleSnapshot().catch(e => log(`Snapshot timer: ${e.message}`, 'warn'));
}, 15000);

setInterval(() => {
  // Always push UI updates during market hours if logged in (even if FYERS WS is down)
  if (state.fyers.appId && (marketOpenNow() || state.live.size)) {
    manageOpenVirtualTrades();
    broadcastDashboard();
  }
}, 5000);

setInterval(() => {
  if (Date.now() < fyersCooldownUntil) return; // don't scan while REST rate-limited
  scanStrategy1().catch(e => log(`S1 scan: ${e.message}`, 'warn'));
}, 45000);

/* ---------- Start ---------- */
(async () => {
  try {
    await initDb();
    await ensureUniverse();
    log(`Dashboard server listening on ${HOST}:${PORT}`);
    server.listen(PORT, HOST);
  } catch (e) {
    console.error(e);
    process.exit(1);
  }
})();
