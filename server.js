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

/*
 * Nifty 50 equity constituents (NSE tickers).
 * Used only for advance/decline counts inside the NIFTY 50 card.
 * Keep reasonably current; missing names simply reduce the matched count.
 */
const NIFTY50_TICKERS = [
  'ADANIENT', 'ADANIPORTS', 'APOLLOHOSP', 'ASIANPAINT', 'AXISBANK',
  'BAJAJ-AUTO', 'BAJFINANCE', 'BAJAJFINSV', 'BEL', 'BHARTIARTL',
  'BPCL', 'BRITANNIA', 'CIPLA', 'COALINDIA', 'DRREDDY',
  'EICHERMOT', 'GRASIM', 'HCLTECH', 'HDFCBANK', 'HDFCLIFE',
  'HEROMOTOCO', 'HINDALCO', 'HINDUNILVR', 'ICICIBANK', 'INDUSINDBK',
  'INFY', 'ITC', 'JIOFIN', 'JSWSTEEL', 'KOTAKBANK',
  'LT', 'M&M', 'MARUTI', 'MAXHEALTH', 'NESTLEIND',
  'NTPC', 'ONGC', 'POWERGRID', 'RELIANCE', 'SBILIFE',
  'SBIN', 'SHRIRAMFIN', 'SUNPHARMA', 'TCS', 'TATACONSUM',
  'TATAMOTORS', 'TATASTEEL', 'TECHM', 'TITAN', 'TRENT',
  'ULTRACEMCO', 'WIPRO'
];
const NIFTY50_SET = new Set(NIFTY50_TICKERS);

/*
 * Sector classification for F&O equities.
 * Kept server-side so gainers, losers and sector drill-down share the same map.
 */
const SECTOR_MAP = {
  ADANIENT: 'Metals & Mining',
  ADANIPORTS: 'Infrastructure',
  APOLLOHOSP: 'Healthcare',
  ASIANPAINT: 'Consumer Durables',
  AXISBANK: 'Financial Services',
  'BAJAJ-AUTO': 'Automobile',
  BAJFINANCE: 'Financial Services',
  BAJAJFINSV: 'Financial Services',
  BEL: 'Defence',
  BHARTIARTL: 'Telecommunication',
  BPCL: 'Oil & Gas',
  BRITANNIA: 'FMCG',
  CIPLA: 'Healthcare',
  COALINDIA: 'Metals & Mining',
  DABUR: 'FMCG',
  DIVISLAB: 'Healthcare',
  DRREDDY: 'Healthcare',
  EICHERMOT: 'Automobile',
  ETERNAL: 'Consumer Services',
  GAIL: 'Oil & Gas',
  GRASIM: 'Cement',
  HCLTECH: 'Information Technology',
  HDFCBANK: 'Financial Services',
  HDFCLIFE: 'Financial Services',
  HEROMOTOCO: 'Automobile',
  HINDALCO: 'Metals & Mining',
  HINDPETRO: 'Oil & Gas',
  HINDUNILVR: 'FMCG',
  HINDZINC: 'Metals & Mining',
  ICICIBANK: 'Financial Services',
  INDUSINDBK: 'Financial Services',
  INFY: 'Information Technology',
  IOC: 'Oil & Gas',
  ITC: 'FMCG',
  JINDALSTEL: 'Metals & Mining',
  JSWSTEEL: 'Metals & Mining',
  KOTAKBANK: 'Financial Services',
  LT: 'Infrastructure',
  LTM: 'Information Technology',
  LTIM: 'Information Technology',
  'M&M': 'Automobile',
  'M&MFIN': 'Financial Services',
  MARICO: 'FMCG',
  MARUTI: 'Automobile',
  MAXHEALTH: 'Healthcare',
  MCX: 'Financial Services',
  NESTLEIND: 'FMCG',
  NHPC: 'Power',
  NMDC: 'Metals & Mining',
  NTPC: 'Power',
  OIL: 'Oil & Gas',
  ONGC: 'Oil & Gas',
  PAGEIND: 'Consumer Durables',
  PERSISTENT: 'Information Technology',
  PETRONET: 'Oil & Gas',
  PFC: 'Financial Services',
  POWERGRID: 'Power',
  RBLBANK: 'Financial Services',
  RELIANCE: 'Oil & Gas',
  SBICARD: 'Financial Services',
  SBILIFE: 'Financial Services',
  SBIN: 'Financial Services',
  SHREECEM: 'Cement',
  SHRIRAMFIN: 'Financial Services',
  SIEMENS: 'Industrials',
  SOLARINDS: 'Defence',
  SRF: 'Chemicals',
  SUNPHARMA: 'Healthcare',
  TATACONSUM: 'FMCG',
  TATAMOTORS: 'Automobile',
  TATASTEEL: 'Metals & Mining',
  TCS: 'Information Technology',
  TECHM: 'Information Technology',
  TITAN: 'Consumer Durables',
  TORNTPHARM: 'Healthcare',
  TRENT: 'Consumer Services',
  TVSMOTOR: 'Automobile',
  UBL: 'FMCG',
  ULTRACEMCO: 'Cement',
  UNIONBANK: 'Financial Services',
  VEDL: 'Metals & Mining',
  VOLTAS: 'Consumer Durables',
  WIPRO: 'Information Technology',
  YESBANK: 'Financial Services',
  ZYDUSLIFE: 'Healthcare',
  ABB: 'Industrials',
  ABCAPITAL: 'Financial Services',
  ABFRL: 'Consumer Services',
  ALKEM: 'Healthcare',
  AMBUJACEM: 'Cement',
  AUROPHARMA: 'Healthcare',
  AUBANK: 'Financial Services',
  BALKRISIND: 'Automobile',
  BANKBARODA: 'Financial Services',
  BHEL: 'Industrials',
  BIOCON: 'Healthcare',
  BOSCHLTD: 'Automobile',
  CANBK: 'Financial Services',
  CESC: 'Power',
  CGPOWER: 'Industrials',
  CHAMBLFERT: 'Chemicals',
  COLPAL: 'FMCG',
  CONCOR: 'Infrastructure',
  CROMPTON: 'Consumer Durables',
  DALBHARAT: 'Cement',
  DELHIVERY: 'Logistics',
  DIXON: 'Consumer Durables',
  FEDERALBNK: 'Financial Services',
  GLENMARK: 'Healthcare',
  GODREJCP: 'FMCG',
  GODREJPROP: 'Real Estate',
  HAVELLS: 'Consumer Durables',
  ICICIGI: 'Financial Services',
  IDFCFIRSTB: 'Financial Services',
  INDIACEM: 'Cement',
  INDIAMART: 'Consumer Services',
  INDHOTEL: 'Consumer Services',
  INDUSTOWER: 'Telecommunication',
  IRCTC: 'Consumer Services',
  IRFC: 'Financial Services',
  JUBLFOOD: 'Consumer Services',
  KALYANKJIL: 'Consumer Durables',
  KEI: 'Industrials',
  LICHSGFIN: 'Financial Services',
  LODHA: 'Real Estate',
  LUPIN: 'Healthcare',
  MANKIND: 'Healthcare',
  MANAPPURAM: 'Financial Services',
  MGL: 'Oil & Gas',
  MFSL: 'Financial Services',
  MOTHERSON: 'Automobile',
  MPHASIS: 'Information Technology',
  NATIONALUM: 'Metals & Mining',
  NAVINFLUOR: 'Chemicals',
  OFSS: 'Information Technology',
  PAYTM: 'Financial Services',
  PIIND: 'Chemicals',
  POLYCAB: 'Industrials',
  PVRINOX: 'Consumer Services',
  RAIN: 'Chemicals',
  RAMCOCEM: 'Cement',
  RECLTD: 'Financial Services',
  SAIL: 'Metals & Mining',
  SCHAEFFLER: 'Automobile',
  SUPREMEIND: 'Industrials',
  TATACHEM: 'Chemicals',
  TATAPOWER: 'Power',
  TIINDIA: 'Automobile',
  TORNTPOWER: 'Power',
  UPL: 'Chemicals',
  VBL: 'FMCG',
  WHIRLPOOL: 'Consumer Durables',
  ZOMATO: 'Consumer Services',
  PREMIERENE: 'Power',
  '360ONE': 'Financial Services',
  PNB: 'Financial Services',
  BANDHANBNK: 'Financial Services',
  CHOLAFIN: 'Financial Services',
  MUTHOOTFIN: 'Financial Services',
  LICI: 'Financial Services',
  HUDCO: 'Financial Services',
  IEX: 'Power',
  IGIL: 'Oil & Gas',
  NYKAA: 'Consumer Services',
  POLICYBZR: 'Financial Services',
  ASTRAL: 'Industrials',
  KFINTECH: 'Financial Services',
  BSE: 'Financial Services',
  CAMS: 'Financial Services',
  CDSL: 'Financial Services',
  ANGELONE: 'Financial Services',
  NUVAMA: 'Financial Services',
  PATANJALI: 'FMCG',
  UNITDSPR: 'FMCG',
  DMART: 'Consumer Services',
  JSWENERGY: 'Power',
  ADANIGREEN: 'Power',
  ADANIENSOL: 'Power',
  ATGL: 'Oil & Gas',
  GUJGASLTD: 'Oil & Gas',
  GSPL: 'Oil & Gas',
  PETRONET: 'Oil & Gas',
  OBEROIRLTY: 'Real Estate',
  PRESTIGE: 'Real Estate',
  PHOENIXLTD: 'Real Estate',
  BRIGADE: 'Real Estate',
  SOBHA: 'Real Estate',
  EXIDEIND: 'Automobile',
  ASHOKLEY: 'Automobile',
  BHARATFORG: 'Automobile',
  SONACOMS: 'Automobile',
  UNOMINDA: 'Automobile',
  ENDURANCE: 'Automobile',
  APOLLOTYRE: 'Automobile',
  MRF: 'Automobile',
  CEATLTD: 'Automobile',
  ESCORTS: 'Automobile',
  FORCEMOT: 'Automobile',
  MAHINDCIE: 'Automobile',
  SUNDRMFAST: 'Automobile',
  LTTS: 'Information Technology',
  COFORGE: 'Information Technology',
  CYIENT: 'Information Technology',
  KPITTECH: 'Information Technology',
  TATAELXSI: 'Information Technology',
  SONATSOFTW: 'Information Technology',
  BSOFT: 'Information Technology',
  RITES: 'Infrastructure',
  RVNL: 'Infrastructure',
  IRCON: 'Infrastructure',
  NBCC: 'Infrastructure',
  KEC: 'Infrastructure',
  HFCL: 'Telecommunication',
  TATACOMM: 'Telecommunication',
  IDEA: 'Telecommunication',
  TEJASNET: 'Telecommunication',
  DEEPAKNTR: 'Chemicals',
  CLEAN: 'Chemicals',
  AARTIIND: 'Chemicals',
  FLUOROCHEM: 'Chemicals',
  GNFC: 'Chemicals',
  COROMANDEL: 'Chemicals',
  FACT: 'Chemicals',
  DEEPAKFERT: 'Chemicals',
  LAURUSLABS: 'Healthcare',
  SYNGENE: 'Healthcare',
  PPLPHARMA: 'Healthcare',
  IPCALAB: 'Healthcare',
  AJANTPHARM: 'Healthcare',
  ABBOTINDIA: 'Healthcare',
  SANOFI: 'Healthcare',
  FORTIS: 'Healthcare',
  MEDANTA: 'Healthcare',
  KIMS: 'Healthcare',
  RAINBOW: 'Healthcare',
  METROPOLIS: 'Healthcare',
  LALPATHLAB: 'Healthcare',
  GRANULES: 'Healthcare',
  NATCOPHARM: 'Healthcare',
  BLS: 'Healthcare',
  CONCORDBIO: 'Healthcare',
  EMCURE: 'Healthcare',
  COHANCE: 'Healthcare',
  PFIZER: 'Healthcare',
  GLAND: 'Healthcare',
  JBCHEPHARM: 'Healthcare',
  NEULANDLAB: 'Healthcare',
  AMBER: 'Consumer Durables',
  BLUESTARCO: 'Consumer Durables',
  VGUARD: 'Consumer Durables',
  KAJARIACER: 'Consumer Durables',
  CERA: 'Consumer Durables',
  HINDCOPPER: 'Metals & Mining',
  APLAPOLLO: 'Metals & Mining',
  JINDALSAW: 'Metals & Mining',
  WELCORP: 'Metals & Mining',
  JSL: 'Metals & Mining',
  MOIL: 'Metals & Mining',
  GMRAIRPORT: 'Infrastructure',
  BDL: 'Defence',
  HAL: 'Defence',
  MAZDOCK: 'Defence',
  COCHINSHIP: 'Defence',
  GRSE: 'Defence',
  DATAPATTNS: 'Defence',
  ZENTEC: 'Defence',
  AETHER: 'Chemicals',
  ANANDRATHI: 'Financial Services',
  AFFLE: 'Information Technology',
  KAYNES: 'Industrials',
  SYRMA: 'Industrials',
  PGEL: 'Consumer Durables',
  AADHARHFC: 'Financial Services',
  HOMEFIRST: 'Financial Services',
  POONAWALLA: 'Financial Services',
  SAMMAANCAP: 'Financial Services',
  IREDA: 'Financial Services',
  INOXWIND: 'Power',
  SUZLON: 'Power',
  WAAREEENER: 'Power',
  OLAELEC: 'Automobile',
  JIOFIN: 'Financial Services',
  SWIGGY: 'Consumer Services',
  FIRSTCRY: 'Consumer Services',
  SHYAMMETL: 'Metals & Mining',
  NESTLEIND: 'FMCG',
  RADICO: 'FMCG',
  'MCDOWELL-N': 'FMCG',
  UNITEDSPIR: 'FMCG',
  GILLETTE: 'FMCG',
  PGHH: 'FMCG',
  EMAMILTD: 'FMCG',
  JYOTHYLAB: 'FMCG',
  VSTIND: 'FMCG',
  MANYAVAR: 'Consumer Services',
  SHOPERSTOP: 'Consumer Services',
  WESTLIFE: 'Consumer Services',
  DEVYANI: 'Consumer Services',
  SAPPHIRE: 'Consumer Services',
  BIKAJI: 'FMCG',
  LTFOODS: 'FMCG',
  KRBL: 'FMCG',
  CCL: 'FMCG',
  TRIVENI: 'FMCG',
  BALRAMCHIN: 'FMCG',
  EIDPARRY: 'FMCG',
  RENUKA: 'FMCG',
  DHANUKA: 'Chemicals',
  RALLIS: 'Chemicals',
  SUMICHEM: 'Chemicals',
  BAYERCROP: 'Chemicals',
  UJJIVANSFB: 'Financial Services',
  EQUITASBNK: 'Financial Services',
  CUB: 'Financial Services',
  KARURVYSYA: 'Financial Services',
  SOUTHBANK: 'Financial Services',
  CSBBANK: 'Financial Services',
  'J&KBANK': 'Financial Services',
  MAHABANK: 'Financial Services',
  CENTRALBK: 'Financial Services',
  IOB: 'Financial Services',
  UCOBANK: 'Financial Services',
  BANKINDIA: 'Financial Services',
  CANFINHOME: 'Financial Services',
  PNBHOUSING: 'Financial Services',
  REPCOHOME: 'Financial Services',
  APTUS: 'Financial Services',
  AAVAS: 'Financial Services',
  CREDITACC: 'Financial Services',
  FUSION: 'Financial Services',
  SPANDANA: 'Financial Services',
  SATIN: 'Financial Services',
  UJJIVAN: 'Financial Services',
  'FIVE-STAR': 'Financial Services',
  SBFC: 'Financial Services',
  MASFIN: 'Financial Services',
  IIFL: 'Financial Services',
  IIFLCAPS: 'Financial Services',
  MOTILALOFS: 'Financial Services',
  EDELWEISS: 'Financial Services',
  JMCPROJECT: 'Infrastructure',
  PNCINFRA: 'Infrastructure',
  HGINFRA: 'Infrastructure',
  ASHOKA: 'Infrastructure',
  IRB: 'Infrastructure',
  GMRINFRA: 'Infrastructure',
  ADANIPOWER: 'Power',
  RPOWER: 'Power',
  JPPOWER: 'Power',
  SJVN: 'Power',
  NHPC: 'Power',
  NLCINDIA: 'Power',
  THERMAX: 'Industrials',
  BHEL: 'Industrials',
  CUMMINSIND: 'Industrials',
  KIRLOSENG: 'Industrials',
  ELGIEQUIP: 'Industrials',
  AIAENG: 'Industrials',
  SKFINDIA: 'Industrials',
  TIMKEN: 'Industrials',
  CARBORUNIV: 'Industrials',
  GRINDWELL: 'Industrials',
  ESABINDIA: 'Industrials',
  FINPIPE: 'Industrials',
  RATNAMANI: 'Industrials',
  WELSPUNIND: 'Consumer Durables',
  TRIDENT: 'Consumer Durables',
  VTL: 'Consumer Durables',
  RAYMOND: 'Consumer Durables',
  PAGEIND: 'Consumer Durables',
  KPRMILL: 'Consumer Durables',
  GOKEX: 'Consumer Durables',
  SAREGAMA: 'Consumer Services',
  TIPSINDLTD: 'Consumer Services',
  PVRINOX: 'Consumer Services',
  INOXLEISUR: 'Consumer Services',
  ZEEL: 'Consumer Services',
  SUNTV: 'Consumer Services',
  TVTODAY: 'Consumer Services',
  NDTV: 'Consumer Services',
  NETWORK18: 'Consumer Services',
  TV18BRDCST: 'Consumer Services',
  DISHTV: 'Consumer Services',
  HATHWAY: 'Telecommunication',
  GTPL: 'Telecommunication',
  DEN: 'Telecommunication',
  SITI: 'Telecommunication'
};

const state = {
  masters: {
    fo: null,
    cm: null,
    loadedAt: 0
  },
  universe: [],
  log: [],
  currentDay: '',
  displayDate: '',
  displayMode: 'current',
  live: new Map(),
  indices: new Map(),
  /** symbol -> previous trading day OHLC + volume MA */
  d1Levels: new Map(),
  /** symbol -> { orHigh, orLow, vwap, maxBarVol, highVolume, ... } from today's 5m bars */
  intradaily: new Map(),
  metrics: {
    d1LoadedFor: '',      // ISO date key when D-1 levels finished
    intraLoadedFor: '',   // session date key for OR/VWAP/vol
    d1Loading: false,
    intraLoading: false
  },
  membership: {
    gainers: [],
    losers: []
  },
  history: new Map(),
  fyers: {
    appId: '',
    token: '',
    socket: null,
    connected: false,
    reconnectTimer: null
  }
};

/* ---------------------------------------------------------
   DATABASE
--------------------------------------------------------- */

const pool = process.env.DATABASE_URL
  ? new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: process.env.DATABASE_URL.includes('localhost')
        ? false
        : { rejectUnauthorized: false },
      max: 3
    })
  : null;

function log(msg, level = 'info') {
  const line = {
    ts: new Date().toISOString(),
    level,
    msg: String(msg)
  };
  state.log.push(line);
  if (state.log.length > 300) state.log.shift();
  console.log(`[${level}] ${msg}`);
  broadcast({ type: 'log', line });
}

/* ---------------------------------------------------------
   IST / MARKET SESSION HELPERS
--------------------------------------------------------- */

function istParts(date = new Date()) {
  const s = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Kolkata',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false
  }).format(date);
  const [d, t] = s.split(', ');
  return { date: d, time: t };
}

function istToday() {
  return istParts().date;
}

/** Normalize any Date / string to YYYY-MM-DD for Postgres and state. */
function toISODate(value) {
  if (value == null || value === '') return null;
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value.toLocaleDateString('en-CA', { timeZone: 'Asia/Kolkata' });
  }
  const s = String(value).trim();
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0, 10);
  const parsed = new Date(s);
  if (!Number.isNaN(parsed.getTime())) {
    return parsed.toLocaleDateString('en-CA', { timeZone: 'Asia/Kolkata' });
  }
  return null;
}

/**
 * NSE equity cash/F&O regular session is Mon–Fri.
 * Holidays still need calendar handling; weekends must never be treated as a live session day.
 */
function istWeekday() {
  const wd = new Intl.DateTimeFormat('en-US', {
    timeZone: 'Asia/Kolkata',
    weekday: 'short'
  }).format(new Date());
  return wd !== 'Sat' && wd !== 'Sun';
}

function isTradingSessionDay() {
  return istWeekday();
}

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
  // Weekend / non-session: treat as "session finished" so UI shows last completed data
  if (!isTradingSessionDay()) return true;
  return istParts().time >= '15:30:00';
}

function fyersHeaders(appId, token) {
  return {
    Authorization: `${appId}:${token}`,
    'Content-Type': 'application/json'
  };
}

/* ---------------------------------------------------------
   FYERS HTTP / RATE LIMITER
   Safety margin: max 180 req/min, min 350 ms between starts.
--------------------------------------------------------- */

/*
 * FYERS data API is stricter in practice than documented "burst" numbers.
 * Keep a conservative global limiter shared by quotes + history.
 * On HTTP 429, raise a cooldown so the whole queue slows down.
 */
const FYERS_SAFE_REQUESTS_PER_MINUTE = 40;
const FYERS_MIN_REQUEST_GAP_MS = 1200;
const FYERS_HISTORY_BATCH_SIZE = 1; // serial history — avoids burst 429s

let fyersRequestTimes = [];
let fyersLastRequestAt = 0;
let fyersRateQueue = Promise.resolve();
let fyersCooldownUntil = 0;
let fyers429Count = 0;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function noteFyers429() {
  fyers429Count += 1;
  // Escalating cooldown: 30s, 60s, 120s, 180s — short cooldowns were still hammering FYERS
  const cool = Math.min(180000, 30000 * Math.pow(2, Math.min(fyers429Count - 1, 3)));
  fyersCooldownUntil = Math.max(fyersCooldownUntil, Date.now() + cool);
  log(
    `FYERS rate-limit cooldown ${Math.round(cool / 1000)}s (429 count=${fyers429Count}). Pause login/rebuild until this clears.`,
    'warn'
  );
}

function noteFyersSuccess() {
  // Slowly recover after clean requests
  if (fyers429Count > 0 && Date.now() > fyersCooldownUntil) {
    fyers429Count = Math.max(0, fyers429Count - 1);
  }
}

function acquireFyersRequestSlot() {
  const job = fyersRateQueue.then(async () => {
    while (true) {
      const now = Date.now();
      fyersRequestTimes = fyersRequestTimes.filter(t => now - t < 60000);

      const waitCooldown = Math.max(0, fyersCooldownUntil - now);
      const waitForGap = Math.max(
        0,
        fyersLastRequestAt + FYERS_MIN_REQUEST_GAP_MS - now
      );
      const waitForMinute =
        fyersRequestTimes.length >= FYERS_SAFE_REQUESTS_PER_MINUTE
          ? fyersRequestTimes[0] + 60001 - now
          : 0;

      const wait = Math.max(waitCooldown, waitForGap, waitForMinute);
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
  for (const [k, v] of Object.entries(params || {})) {
    u.searchParams.set(k, String(v));
  }

  let r;
  let text;
  try {
    r = await fetch(u, { headers: fyersHeaders(appId, token) });
    text = await r.text();
  } catch (e) {
    if (retries > 0) {
      log(`FYERS network error, retrying: ${e.message}`, 'warn');
      await sleep(1500 + Math.random() * 1000);
      return fyersGet(base, endpoint, params, appId, token, retries - 1);
    }
    throw e;
  }

  // HTML or plain 429 pages
  if (r.status === 429) {
    noteFyers429();
    if (retries > 0) {
      const wait = Math.max(0, fyersCooldownUntil - Date.now()) + 1000 + Math.random() * 1000;
      log(`FYERS 429 — waiting ${Math.round(wait / 1000)}s before single retry…`, 'warn');
      await sleep(wait);
      return fyersGet(base, endpoint, params, appId, token, 0);
    }
    throw new Error(
      `FYERS 429: rate limited. Wait 1–2 minutes, then try login again. ` +
        `Do not refresh repeatedly.`
    );
  }

  let data;
  try {
    data = JSON.parse(text);
  } catch {
    // Some gateways return HTML 429 without proper status
    if (/429|too many requests|rate limit/i.test(text)) {
      noteFyers429();
      if (retries > 0) {
        await sleep(Math.max(0, fyersCooldownUntil - Date.now()) + 500);
        return fyersGet(base, endpoint, params, appId, token, retries - 1);
      }
    }
    throw new Error(`FYERS non-JSON ${r.status}: ${text.slice(0, 180)}`);
  }

  if (!r.ok || data.s === 'error') {
    const msg = String(data.message || data.msg || 'request failed');
    if (
      r.status === 429 ||
      /rate limit|too many|throttle/i.test(msg) ||
      data.code === 429
    ) {
      noteFyers429();
      if (retries > 0) {
        await sleep(Math.max(0, fyersCooldownUntil - Date.now()) + 500);
        return fyersGet(base, endpoint, params, appId, token, retries - 1);
      }
    }
    throw new Error(`FYERS ${r.status}/${data.code ?? ''}: ${msg}`);
  }

  noteFyersSuccess();
  return data;
}

/* ---------------------------------------------------------
   DATABASE INITIALIZATION
--------------------------------------------------------- */

async function initDb() {
  if (!pool) {
    log(
      'DATABASE_URL not set: persistence disabled; history will be rebuilt from FYERS after restart.',
      'warn'
    );
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
  `);

  log('Postgres persistence ready.');
}

async function saveSnapshot(date, time, side, rows) {
  if (!pool || !rows?.length) return;

  const iso = toISODate(date);
  if (!iso) {
    log(`saveSnapshot: invalid date ${date}`, 'warn');
    return;
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    for (const r of rows) {
      await client.query(
        `
        INSERT INTO rank_snapshots
          (trading_date, candle_time, side, rank, symbol, name, sector, pct, close)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        ON CONFLICT (trading_date, candle_time, side, rank)
        DO UPDATE SET
          symbol = EXCLUDED.symbol,
          name = EXCLUDED.name,
          sector = EXCLUDED.sector,
          pct = EXCLUDED.pct,
          close = EXCLUDED.close
        `,
        [
          iso,
          time,
          side,
          r.rank,
          r.key,
          r.name,
          r.sector,
          r.pct,
          r.close ?? null
        ]
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

async function latestStoredTradingDate() {
  if (!pool) return null;
  const today = istToday();
  const { rows } = await pool.query(
    `SELECT MAX(trading_date) AS d FROM rank_snapshots WHERE trading_date < $1`,
    [today]
  );
  return toISODate(rows[0]?.d);
}

/* ---------------------------------------------------------
   SESSION RESET
--------------------------------------------------------- */

function resetSession(date) {
  state.currentDay = date;
  state.displayDate = date;
  state.displayMode = 'current';
  state.history.clear();
  state.membership = { gainers: [], losers: [] };
  state.live.clear();
  state.indices.clear();
}

/* ---------------------------------------------------------
   PREVIOUS COMPLETED SESSION
--------------------------------------------------------- */

async function loadLatestCompletedSession(appId, token) {
  const stored = await latestStoredTradingDate();

  if (stored) {
    const storedIso = toISODate(stored);
    state.currentDay = storedIso;
    state.displayDate = storedIso;
    state.displayMode = 'previous-close';
    state.history.clear();
    state.membership = { gainers: [], losers: [] };
    await loadDbHistory(storedIso);
    if (state.history.size) {
      log(`Pre-open mode: showing last completed trading session ${stored}.`);
      return stored;
    }
  }

  const universe = await ensureUniverse();
  const today = istToday();
  const rows = [];

  for (let i = 0; i < universe.length; i += FYERS_HISTORY_BATCH_SIZE) {
    const batch = universe.slice(i, i + FYERS_HISTORY_BATCH_SIZE);
    const result = await Promise.all(
      batch.map(async stock => {
        try {
          const from = epochForISTDate(today, '09:15') - 21 * 86400;
          const to = epochForISTDate(today, '09:15');
          const d = await fyersGet(
            DATA_HOST,
            '/history',
            {
              symbol: stock.symbol,
              resolution: 'D',
              date_format: '0',
              range_from: from,
              range_to: to,
              cont_flag: '1'
            },
            appId,
            token
          );
          const cs = d.candles || [];
          if (cs.length < 2) return null;
          const last = cs[cs.length - 1];
          const prev = cs[cs.length - 2];
          const close = Number(last[4]);
          const prevClose = Number(prev[4]);
          if (!Number.isFinite(close) || !Number.isFinite(prevClose) || !prevClose) {
            return null;
          }
          return {
            ...stock,
            close,
            pct: pct(close, prevClose),
            sourceDate: new Date(Number(last[0]) * 1000).toLocaleDateString('en-CA', {
              timeZone: 'Asia/Kolkata'
            })
          };
        } catch (e) {
          log(`${stock.name}: previous-session lookup failed: ${e.message}`, 'warn');
          return null;
        }
      })
    );
    rows.push(...result.filter(Boolean));
  }

  if (!rows.length) {
    throw new Error('Could not obtain a previous completed trading session from FYERS.');
  }

  const date = rows.map(x => x.sourceDate).sort().pop();
  const g = [...rows].sort((a, b) => b.pct - a.pct).map((x, i) => ({ ...x, rank: i + 1 }));
  const l = [...rows].sort((a, b) => a.pct - b.pct).map((x, i) => ({ ...x, rank: i + 1 }));

  state.currentDay = date;
  state.displayDate = date;
  state.displayMode = 'previous-close';
  state.history.clear();
  state.history.set('CLOSE', { gainers: g, losers: l });

  log(
    `Pre-open mode: no persisted history found; showing ${date} last-close ranking from FYERS daily candles.`,
    'warn'
  );
  return date;
}

/* ---------------------------------------------------------
   LOAD DATABASE HISTORY
--------------------------------------------------------- */

async function loadDbHistory(date) {
  if (!pool) return;

  const iso = toISODate(date);
  if (!iso) {
    log(`loadDbHistory: invalid date ${date}`, 'warn');
    return;
  }

  const { rows } = await pool.query(
    `
    SELECT trading_date, candle_time, side, rank, symbol, name, sector, pct, close
    FROM rank_snapshots
    WHERE trading_date = $1
    ORDER BY candle_time, side, rank
    `,
    [iso]
  );

  // Replace in-memory history — never append across multiple restores (was 210→840 dups)
  state.history.clear();
  state.membership = { gainers: [], losers: [] };

  for (const r of rows) {
    let t = r.candle_time;
    if (t instanceof Date) {
      // pg sometimes returns time as Date; format in IST wall-clock is unreliable —
      // prefer ISO time portion if present, else HH:MM from UTC components is wrong.
      // Fall back to stringifying via known time string if driver gave a string-like value.
      t = t.toISOString().slice(11, 16);
    } else {
      t = String(t).slice(0, 5);
    }
    // Guard against bad parses like "1970-"
    if (!/^\d{2}:\d{2}$/.test(t)) continue;

    const side = String(r.side || '').toLowerCase();
    if (side !== 'gainers' && side !== 'losers') continue;

    if (!state.history.has(t)) {
      state.history.set(t, { gainers: [], losers: [] });
    }

    const key = normalizeKey(r.symbol) || normalizeKey(r.name);
    state.history.get(t)[side].push({
      key,
      name: normalizeKey(r.name) || key,
      sector: r.sector || 'Other F&O',
      pct: Number(r.pct),
      close: r.close == null ? null : Number(r.close),
      rank: Number(r.rank)
    });
  }

  if (rows.length) {
    const times = [...state.history.keys()].sort();
    const g920 = state.history.get('09:20')?.gainers?.length || 0;
    const sampleKeys = (state.history.get('09:20')?.gainers || [])
      .slice(0, 5)
      .map(x => x.key)
      .join(', ');
    log(
      `Restored ${rows.length} ranking rows from Postgres for ${iso}. ` +
        `Timeline: ${times.join(', ')}`
    );
    log(
      `09:20 gainers in DB: ${g920}; sample keys: ${sampleKeys || '(none)'}`
    );
  }
}

/** Serialize FYERS full-day history rebuilds (login + session monitor). */
let historyRebuildPromise = null;

/**
 * In-memory history is usable if we already have solid snapshot coverage.
 * Avoid full FYERS rebuild when live snapshots exist in DB/memory.
 */
function historyIsSparse() {
  const times = [...state.history.keys()].filter(t => t !== 'CLOSE');
  if (!times.length) return true;
  const g920 = state.history.get('09:20')?.gainers?.length || 0;
  const anyG = Math.max(
    0,
    ...times.map(t => (state.history.get(t)?.gainers || []).length)
  );
  if (g920 >= 30 || anyG >= 50) return false;
  if (times.length >= 2 && anyG >= 20) return false;
  return true;
}

/** True when Render Postgres already has ranking rows for this trading date. */
async function dbHasDayHistory(date) {
  if (!pool) return false;
  const iso = toISODate(date);
  if (!iso) return false;
  try {
    const { rows } = await pool.query(
      `
      SELECT
        COUNT(*)::int AS total_rows,
        COUNT(DISTINCT candle_time)::int AS times
      FROM rank_snapshots
      WHERE trading_date = $1
      `,
      [iso]
    );
    const total = Number(rows[0]?.total_rows || 0);
    const times = Number(rows[0]?.times || 0);
    // One 5-min snapshot of full F&O list ≈ 400+ rows; even partial is enough to skip wipe
    const ok = total >= 80 || times >= 1;
    if (ok) {
      log(
        `DB already has rank history for ${iso}: rows=${total}, distinct times=${times} — skip FYERS full rebuild.`
      );
    }
    return ok;
  } catch (e) {
    log(`dbHasDayHistory: ${e.message}`, 'warn');
    return false;
  }
}

async function clearDbHistoryForDate(date) {
  if (!pool) return;
  const iso = toISODate(date);
  if (!iso) return;
  try {
    const r = await pool.query(
      `DELETE FROM rank_snapshots WHERE trading_date = $1`,
      [iso]
    );
    log(`Cleared ${r.rowCount || 0} sparse/incomplete rank_snapshots rows for ${iso}.`);
  } catch (e) {
    log(`Failed to clear sparse history for ${iso}: ${e.message}`, 'warn');
  }
}

/* ---------------------------------------------------------
   SYMBOL MASTER — correct FO fields: exInstType=13, underSym
--------------------------------------------------------- */

async function loadMaster(kind) {
  const now = Date.now();
  if (state.masters[kind] && now - state.masters.loadedAt < 6 * 60 * 60 * 1000) {
    return state.masters[kind];
  }

  const name = kind === 'fo' ? 'NSE_FO_sym_master.json' : 'NSE_CM_sym_master.json';
  const r = await fetch(MASTER_BASE + name, {
    headers: { 'User-Agent': 'fno-rank-dashboard/2.0' }
  });
  if (!r.ok) throw new Error(`Symbol master ${name}: HTTP ${r.status}`);
  const data = await r.json();
  state.masters[kind] = data;
  state.masters.loadedAt = now;
  log(`Loaded ${name} (${Object.keys(data).length} instruments).`);
  return data;
}

function getField(v, names) {
  for (const n of names) {
    if (v?.[n] !== undefined && v?.[n] !== null && v?.[n] !== '') return v[n];
  }
  return '';
}

/** Normalize any symbol/key to bare underlying ticker (e.g. NSE:RELIANCE-EQ -> RELIANCE). */
function normalizeKey(s) {
  let k = String(s || '').toUpperCase().trim();
  if (k.includes(':')) k = k.split(':').pop();
  k = k.replace(/-EQ$/i, '').replace(/\s+/g, '');
  return k;
}

function slugifyCompanyName(fullName, fallbackTicker) {
  let s = String(fullName || '')
    .toLowerCase()
    // Groww uses "ltd" not "limited"
    .replace(/\blimited\b/g, 'ltd')
    .replace(/\bpvt\.?\b/g, 'pvt')
    .replace(/\bprivate\b/g, 'pvt')
    .replace(/&/g, ' and ')
    .replace(/\+/g, ' plus ')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .replace(/-+/g, '-');
  s = s.replace(/-limited-/g, '-ltd-').replace(/-limited$/g, '-ltd');
  s = s.replace(/-ltd-ltd/g, '-ltd');
  // Common Groww "of-india" / bank patterns
  s = s.replace(/state-bank-india$/g, 'state-bank-of-india');
  s = s.replace(/(^|-)bank-india$/g, '$1bank-of-india');
  s = s.replace(/bank-baroda$/g, 'bank-of-baroda');
  s = s.replace(/union-bank-india$/g, 'union-bank-of-india');
  s = s.replace(/indian-bank$/g, 'indian-bank');
  if (!s || s.length < 2) {
    s = String(fallbackTicker || 'stock')
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-');
  }
  return s;
}

function buildUniverse(fo, cm) {
  const out = new Map();
  let futstkCount = 0;
  let skippedIndex = 0;
  let skippedNoEq = 0;
  let duplicates = 0;

  const INDEX_UNDERLYINGS = new Set([
    'NIFTY',
    'BANKNIFTY',
    'FINNIFTY',
    'MIDCPNIFTY',
    'SENSEX',
    'BANKEX',
    'NIFTYNXT50'
  ]);

  for (const [key, v] of Object.entries(fo)) {
    const instType = Number(getField(v, ['exInstType', 'instrumentType', 'instrument_type', 'type']));
    // 13 = FUTSTK (stock futures)
    if (instType !== 13) continue;
    futstkCount++;

    let underlying = String(getField(v, ['underSym', 'underlyingSymbol', 'underlying', 'exSymbol']))
      .toUpperCase()
      .trim();

    if (underlying.includes(':')) underlying = underlying.split(':').pop();
    underlying = underlying.replace(/-EQ$/i, '').replace(/\s+/g, '');

    if (!underlying || INDEX_UNDERLYINGS.has(underlying)) {
      skippedIndex++;
      continue;
    }

    const preferred = `NSE:${underlying}-EQ`;
    let eq = null;
    if (cm[preferred]) {
      eq = preferred;
    } else {
      const hit = Object.entries(cm).find(([s, cv]) => {
        const series = String(getField(cv, ['exSeries', 'series'])).toUpperCase();
        const sym = String(getField(cv, ['exSymbol', 'underSym', 'short_name'])).toUpperCase();
        return (
          s.startsWith('NSE:') &&
          series === 'EQ' &&
          (sym === underlying || s === preferred)
        );
      });
      if (hit) eq = hit[0];
    }

    if (!eq) {
      skippedNoEq++;
      continue;
    }

    if (out.has(underlying)) {
      duplicates++;
      continue;
    }

    const cmRow = cm[eq] || {};
    const displayName =
      String(getField(cmRow, ['exSymbol', 'underSym', 'short_name'])).toUpperCase() ||
      underlying;
    const fullName = String(
      getField(cmRow, ['exSymName', 'symDetails', 'symbolDesc']) || displayName
    );
    const growwSlug = slugifyCompanyName(fullName, underlying);

    out.set(underlying, {
      key: underlying,
      symbol: eq,
      name: displayName,
      fullName,
      growwSlug,
      growwUrl: `https://groww.in/charts/stocks/${growwSlug}?exchange=NSE`,
      sector: SECTOR_MAP[underlying] || 'Other F&O'
    });
  }

  const list = [...out.values()].sort((a, b) => a.name.localeCompare(b.name));

  log(
    `F&O universe build: FUTSTK=${futstkCount}, unique equities=${list.length}, ` +
      `skipped index underlyings=${skippedIndex}, no CM EQ match=${skippedNoEq}, duplicates=${duplicates}`
  );

  const premier = list.find(x => x.key === 'PREMIERENE');
  if (premier) {
    log(`Validation: PREMIERENE -> ${premier.symbol} (OK)`);
  } else {
    log('Validation: PREMIERENE not found in F&O equity universe', 'warn');
  }

  const sample = list.slice(0, 8).map(x => x.symbol).join(', ');
  log(`Sample F&O equity symbols: ${sample}`);

  return list;
}

async function ensureUniverse() {
  if (state.universe.length) return state.universe;
  const [fo, cm] = await Promise.all([loadMaster('fo'), loadMaster('cm')]);
  state.universe = buildUniverse(fo, cm);
  log(`F&O equity universe ready: ${state.universe.length} stocks.`);
  return state.universe;
}

/* ---------------------------------------------------------
   QUOTES / HISTORY
--------------------------------------------------------- */

function chunks(a, n) {
  const r = [];
  for (let i = 0; i < a.length; i += n) r.push(a.slice(i, i + n));
  return r;
}

async function quotes(symbols, appId, token) {
  const all = [];
  for (const c of chunks(symbols, 50)) {
    const d = await fyersGet(
      DATA_HOST,
      '/quotes',
      { symbols: c.join(',') },
      appId,
      token
    );
    for (const x of d.d || []) {
      const v = x.v || {};
      all.push({
        symbol: x.symbol || x.n || x.s || '',
        ltp: Number(v.lp ?? v.ltp ?? 0),
        prev: Number(v.prev_close_price ?? v.prev_close ?? 0),
        changePct: Number(v.chp ?? 0),
        volume: Number(v.volume ?? 0),
        ts: Number(v.tt ?? v.timestamp ?? 0)
      });
    }
  }
  return all;
}

async function indexQuotes(appId, token) {
  const q = await quotes(
    INDEX_SYMBOLS.map(x => x[1]),
    appId,
    token
  );
  const m = new Map(q.map(x => [x.symbol, x]));
  return INDEX_SYMBOLS.map(([name, symbol]) => ({
    name,
    symbol,
    ...(m.get(symbol) || { ltp: null, changePct: null })
  }));
}

function epochForISTDate(date, hhmm) {
  const [h, m] = hhmm.split(':').map(Number);
  return Math.floor(
    Date.parse(
      `${date}T${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:00+05:30`
    ) / 1000
  );
}

async function history5m(stock, appId, token, date) {
  const from = epochForISTDate(date, '09:15');
  const to = Math.min(epochForISTDate(date, '15:40'), Math.floor(Date.now() / 1000));
  const d = await fyersGet(
    DATA_HOST,
    '/history',
    {
      symbol: stock.symbol,
      resolution: '5',
      date_format: '0',
      range_from: from,
      range_to: to,
      cont_flag: '1'
    },
    appId,
    token
  );
  return (d.candles || []).map(x => ({
    ts: Number(x[0]),
    open: Number(x[1]),
    high: Number(x[2]),
    low: Number(x[3]),
    close: Number(x[4]),
    volume: Number(x[5])
  }));
}

async function previousCloseFor(stock, appId, token, date) {
  const from = epochForISTDate(date, '09:15') - 5 * 86400;
  const to = epochForISTDate(date, '09:20');
  const d = await fyersGet(
    DATA_HOST,
    '/history',
    {
      symbol: stock.symbol,
      resolution: 'D',
      date_format: '0',
      range_from: from,
      range_to: to,
      cont_flag: '1'
    },
    appId,
    token
  );
  const cs = d.candles || [];
  return cs.length ? Number(cs[cs.length - 1][4]) : null;
}

function pct(close, prev) {
  return prev ? ((close - prev) / prev) * 100 : 0;
}

/* ---------------------------------------------------------
   PREVIOUS DAY (D-1) HIGH / LOW
--------------------------------------------------------- */

async function loadD1Levels(appId, token) {
  const universe = await ensureUniverse();
  const today = istToday();
  const VOL_MA_DAYS = 10;

  // Cache: reuse if already loaded for this calendar day and coverage is good
  if (
    state.metrics.d1LoadedFor === today &&
    state.d1Levels.size >= Math.min(50, universe.length * 0.5)
  ) {
    log(`D-1 levels cache hit for ${today} (${state.d1Levels.size} stocks).`);
    return;
  }
  if (state.metrics.d1Loading) {
    log('D-1 levels already loading; skipping duplicate run.');
    return;
  }
  state.metrics.d1Loading = true;
  let ok = 0;
  let fail = 0;

  try {
  for (let i = 0; i < universe.length; i += FYERS_HISTORY_BATCH_SIZE) {
    const batch = universe.slice(i, i + FYERS_HISTORY_BATCH_SIZE);
    const result = await Promise.all(
      batch.map(async stock => {
        try {
          const from = epochForISTDate(today, '09:15') - 25 * 86400;
          const to = epochForISTDate(today, '15:40');
          const d = await fyersGet(
            DATA_HOST,
            '/history',
            {
              symbol: stock.symbol,
              resolution: 'D',
              date_format: '0',
              range_from: from,
              range_to: to,
              cont_flag: '1'
            },
            appId,
            token
          );
          const cs = d.candles || [];
          if (!cs.length) return null;

          // Split candles: completed days before today, and optional today bar
          const prior = [];
          let todayBar = null;
          for (const c of cs) {
            const day = new Date(Number(c[0]) * 1000).toLocaleDateString('en-CA', {
              timeZone: 'Asia/Kolkata'
            });
            const bar = {
              day,
              open: Number(c[1]),
              high: Number(c[2]),
              low: Number(c[3]),
              close: Number(c[4]),
              volume: Number(c[5]) || 0
            };
            if (day < today) prior.push(bar);
            else if (day === today) todayBar = bar;
          }

          if (!prior.length) return null;

          const d1 = prior[prior.length - 1];
          const open = d1.open;
          const high = d1.high;
          const low = d1.low;
          const close = d1.close;
          if (
            !Number.isFinite(high) ||
            !Number.isFinite(low) ||
            !Number.isFinite(close) ||
            !close ||
            !Number.isFinite(open) ||
            !open
          ) {
            return null;
          }

          /*
           * Consolidation (quiet D-1 session):
           * - Range uses absolute |high − low| (same whether you think high-low or low-high)
           * - Net change uses absolute |close − open| so both +ve and −ve days count
           * Both must be within 1.5%.
           */
          const rangePct = (Math.abs(high - low) / close) * 100;
          const changePct = ((close - open) / open) * 100; // signed (+ up / − down)
          const absChangePct = Math.abs(changePct);
          const consolidation = rangePct <= 1.5 && absChangePct <= 1.5;

          // Volume MA of last VOL_MA_DAYS completed sessions (exclude today)
          const volWindow = prior.slice(-VOL_MA_DAYS);
          const volSum = volWindow.reduce((s, b) => s + (b.volume || 0), 0);
          const avgVolume = volWindow.length ? volSum / volWindow.length : null;
          const d1Volume = d1.volume || null;

          return {
            symbol: stock.symbol,
            key: stock.key,
            open,
            high,
            low,
            close,
            date: d1.day,
            rangePct,
            changePct,
            absChangePct,
            consolidation,
            d1Volume,
            avgVolume,
            volMaDays: volWindow.length,
            todayVolumeFromDaily: todayBar ? todayBar.volume : null
          };
        } catch (e) {
          return null;
        }
      })
    );

    for (const row of result) {
      if (!row) {
        fail++;
        continue;
      }
      state.d1Levels.set(row.symbol, row);
      ok++;
    }
  }

  state.metrics.d1LoadedFor = today;
  log(
    `D-1 levels + volume MA(${VOL_MA_DAYS}) loaded for ${ok}/${universe.length} stocks` +
      (fail ? ` (${fail} unavailable)` : '') +
      '.'
  );
  } finally {
    state.metrics.d1Loading = false;
  }
}

/**
 * Load today's (or last session day's) 5-minute bars to compute:
 * - Opening range (09:15–09:30 IST): orHigh / orLow
 * - Session VWAP from 5m bars
 * - High-volume spike: any bar vol >= 5 × (10d avg daily vol / 75)
 */
async function loadIntradayMetrics(appId, token) {
  const universe = await ensureUniverse();
  let sessionDate = istToday();
  if (!isTradingSessionDay() || !regularSessionStarted()) {
    const histDates = [];
    for (const v of state.d1Levels.values()) {
      if (v.date) histDates.push(v.date);
    }
    histDates.sort();
    if (histDates.length) sessionDate = histDates[histDates.length - 1];
  }

  if (
    state.metrics.intraLoadedFor === sessionDate &&
    state.intradaily.size >= Math.min(50, universe.length * 0.5)
  ) {
    log(`Intraday metrics cache hit for ${sessionDate} (${state.intradaily.size} stocks).`);
    return;
  }
  if (state.metrics.intraLoading) {
    log('Intraday metrics already loading; skipping duplicate run.');
    return;
  }
  state.metrics.intraLoading = true;

  let ok = 0;
  let fail = 0;
  const BARS_PER_DAY = 75;

  try {
  for (let i = 0; i < universe.length; i += FYERS_HISTORY_BATCH_SIZE) {
    const batch = universe.slice(i, i + FYERS_HISTORY_BATCH_SIZE);
    const result = await Promise.all(
      batch.map(async stock => {
        try {
          const cs = await history5m(stock, appId, token, sessionDate);
          if (!cs.length) return null;

          let orHigh = null;
          let orLow = null;
          let pv = 0;
          let vv = 0;
          let maxBarVol = 0;

          for (const c of cs) {
            const closeEpoch = c.ts + 300;
            const t = new Date(closeEpoch * 1000).toLocaleTimeString('en-GB', {
              timeZone: 'Asia/Kolkata',
              hour: '2-digit',
              minute: '2-digit'
            });
            // Opening range: bars whose close time is in 09:20..09:30
            // (covers 09:15-09:20 and 09:20-09:25 and 09:25-09:30 opens)
            if (t >= '09:20' && t <= '09:30') {
              orHigh =
                orHigh == null ? c.high : Math.max(orHigh, c.high);
              orLow = orLow == null ? c.low : Math.min(orLow, c.low);
            }

            const vol = Number(c.volume) || 0;
            if (vol > maxBarVol) maxBarVol = vol;

            const typical = (c.high + c.low + c.close) / 3;
            if (vol > 0 && Number.isFinite(typical)) {
              pv += typical * vol;
              vv += vol;
            }
          }

          // Also include first bar open 09:15 close 09:20 if labeled 09:20
          // OR window already covers 09:20-09:30 closes

          const vwap = vv > 0 ? pv / vv : null;
          const d1 = state.d1Levels.get(stock.symbol);
          const avgVolume = d1 && Number.isFinite(d1.avgVolume) ? d1.avgVolume : null;
          const typicalBar = avgVolume && avgVolume > 0 ? avgVolume / BARS_PER_DAY : null;
          const highVolume =
            typicalBar != null && maxBarVol >= 5 * typicalBar;
          const barVolRatio =
            typicalBar && typicalBar > 0 ? maxBarVol / typicalBar : null;

          return {
            symbol: stock.symbol,
            key: stock.key,
            sessionDate,
            orHigh,
            orLow,
            vwap,
            maxBarVol,
            barVolRatio,
            highVolume,
            bars: cs.length
          };
        } catch (e) {
          return null;
        }
      })
    );

    for (const row of result) {
      if (!row) {
        fail++;
        continue;
      }
      state.intradaily.set(row.symbol, row);
      ok++;
    }
  }

  state.metrics.intraLoadedFor = sessionDate;
  log(
    `Intraday metrics (OR 15m / VWAP / vol spike) loaded for ${ok}/${universe.length} on ${sessionDate}` +
      (fail ? ` (${fail} unavailable)` : '') +
      '.'
  );
  } finally {
    state.metrics.intraLoading = false;
  }
}


function lookupD1(row) {
  let d1 = row.symbol ? state.d1Levels.get(row.symbol) : null;
  if (!d1 && row.key) {
    for (const v of state.d1Levels.values()) {
      if (v.key === row.key) {
        d1 = v;
        break;
      }
    }
  }
  return d1 || null;
}

function lookupIntraday(row) {
  let m = row.symbol ? state.intradaily.get(row.symbol) : null;
  if (!m && row.key) {
    for (const v of state.intradaily.values()) {
      if (v.key === row.key) {
        m = v;
        break;
      }
    }
  }
  return m || null;
}

function attachD1Flags(row) {
  const d1 = lookupD1(row);
  const intra = lookupIntraday(row);
  const ltp = Number(row.ltp ?? row.close);
  const d1High = d1 && Number.isFinite(d1.high) ? d1.high : null;
  const d1Low = d1 && Number.isFinite(d1.low) ? d1.low : null;
  const d1Close = d1 && Number.isFinite(d1.close) ? d1.close : null;
  const rangePct = d1 && Number.isFinite(d1.rangePct) ? d1.rangePct : null;
  const changePct = d1 && Number.isFinite(d1.changePct) ? d1.changePct : null;
  const absChangePct = d1 && Number.isFinite(d1.absChangePct) ? d1.absChangePct : null;
  const consolidation = !!(d1 && d1.consolidation);
  const avgVolume = d1 && Number.isFinite(d1.avgVolume) ? d1.avgVolume : null;

  const liveVol = Number(row.volume);
  const todayVolume =
    Number.isFinite(liveVol) && liveVol > 0
      ? liveVol
      : d1 && Number.isFinite(d1.todayVolumeFromDaily)
        ? d1.todayVolumeFromDaily
        : null;

  // Day volume vs 10d MA (informational)
  const volumeRatio =
    avgVolume && todayVolume != null && avgVolume > 0
      ? todayVolume / avgVolume
      : null;

  /*
   * High volume (scanner): any of today's 5-minute candles has volume
   * >= 5 × typical bar size, where typical bar ≈ 10-day avg daily volume / 75
   * (≈ number of 5m bars in 09:15–15:30).
   */
  const typicalBarVol =
    avgVolume && avgVolume > 0 ? avgVolume / 75 : null;
  const maxBarVol = intra && Number.isFinite(intra.maxBarVol) ? intra.maxBarVol : null;
  const barVolRatio =
    typicalBarVol && maxBarVol != null && typicalBarVol > 0
      ? maxBarVol / typicalBarVol
      : null;
  const highVolume =
    (intra && intra.highVolume) ||
    (barVolRatio != null && barVolRatio >= 5);

  const orHigh = intra && Number.isFinite(intra.orHigh) ? intra.orHigh : null;
  const orLow = intra && Number.isFinite(intra.orLow) ? intra.orLow : null;
  const vwap = intra && Number.isFinite(intra.vwap) ? intra.vwap : null;

  const aboveD1High =
    d1High != null && Number.isFinite(ltp) ? ltp > d1High : false;
  const belowD1Low =
    d1Low != null && Number.isFinite(ltp) ? ltp < d1Low : false;
  const aboveOrHigh =
    orHigh != null && Number.isFinite(ltp) ? ltp > orHigh : false;
  const belowOrLow =
    orLow != null && Number.isFinite(ltp) ? ltp < orLow : false;
  const aboveVwap =
    vwap != null && Number.isFinite(ltp) ? ltp > vwap : false;
  const belowVwap =
    vwap != null && Number.isFinite(ltp) ? ltp < vwap : false;

  const stockMeta =
    state.universe.find(s => s.key === row.key || s.symbol === row.symbol) || {};

  return {
    ...row,
    d1High,
    d1Low,
    d1Close,
    d1Date: d1?.date || null,
    rangePct,
    changePct,
    absChangePct,
    consolidation,
    avgVolume,
    todayVolume,
    volumeRatio,
    maxBarVol,
    barVolRatio,
    highVolume,
    orHigh,
    orLow,
    vwap,
    aboveD1High,
    belowD1Low,
    aboveOrHigh,
    belowOrLow,
    aboveVwap,
    belowVwap,
    growwUrl: row.growwUrl || stockMeta.growwUrl || null,
    fullName: row.fullName || stockMeta.fullName || row.name
  };
}

function nifty50BreadthFromLive() {
  let advances = 0;
  let declines = 0;
  let unchanged = 0;
  let matched = 0;

  for (const stock of state.universe) {
    if (!NIFTY50_SET.has(stock.key)) continue;
    const q = state.live.get(stock.symbol);
    if (!q || !Number.isFinite(q.changePct)) continue;
    matched++;
    const p = q.changePct;
    if (p > 0) advances++;
    else if (p < 0) declines++;
    else unchanged++;
  }

  return { advances, declines, unchanged, matched, total: NIFTY50_TICKERS.length };
}


/* ---------------------------------------------------------
   REBUILD HISTORY
--------------------------------------------------------- */


/** Five-minute rank times from 09:20 through 15:30 inclusive. */
function allSessionRankTimes() {
  const out = [];
  for (let mins = 9 * 60 + 20; mins <= 15 * 60 + 30; mins += 5) {
    const h = Math.floor(mins / 60);
    const m = mins % 60;
    out.push(`${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`);
  }
  return out;
}

/** Times that should already exist for `date` (up to "now" if today). */
function expectedRankTimes(date) {
  const all = allSessionRankTimes();
  const iso = toISODate(date) || date;
  if (iso !== istToday()) return all;
  const nowHm = istParts().time.slice(0, 5);
  // Only completed buckets (strictly before current minute's open bucket if mid-candle)
  return all.filter(t => t <= nowHm);
}

function missingRankTimes(date) {
  const expected = expectedRankTimes(date);
  return expected.filter(t => {
    const snap = state.history.get(t);
    const n = snap?.gainers?.length || 0;
    return n < 30; // treat missing or tiny as gap
  });
}

/**
 * Top-up only missing 09:20…now buckets from FYERS 5m history.
 * Does not wipe existing DB snapshots. One history call per stock.
 */
async function fillMissingSnapshotsFromFyers(appId, token, date) {
  if (Date.now() < fyersCooldownUntil) {
    log(
      `Skip gap-fill: FYERS cooldown ${Math.ceil((fyersCooldownUntil - Date.now()) / 1000)}s remaining.`,
      'warn'
    );
    return;
  }

  const iso = toISODate(date) || date;
  const missing = missingRankTimes(iso);
  if (!missing.length) {
    log(`Rank timeline complete for ${iso} — no gap-fill needed.`);
    return;
  }

  if (historyRebuildPromise) {
    log('Gap-fill waiting on existing history job…');
    await historyRebuildPromise;
    return;
  }

  historyRebuildPromise = (async () => {
    try {
      const universe = await ensureUniverse();
      log(
        `Gap-fill for ${iso}: missing ${missing.length} times (${missing[0]}…${missing[missing.length - 1]}). ` +
          `~${universe.length} history calls.`
      );

      const previousCloseMap = new Map();
      try {
        const quoteRows = await quotes(
          universe.map(x => x.symbol),
          appId,
          token
        );
        for (const q of quoteRows) {
          if (q.symbol && Number.isFinite(q.prev) && q.prev > 0) {
            previousCloseMap.set(q.symbol, q.prev);
          }
        }
      } catch (e) {
        log(`Gap-fill quotes: ${e.message}`, 'warn');
      }

      const missingSet = new Set(missing);
      const buckets = new Map(); // time -> [{key,name,sector,pct,close}]

      for (let i = 0; i < universe.length; i += FYERS_HISTORY_BATCH_SIZE) {
        const batch = universe.slice(i, i + FYERS_HISTORY_BATCH_SIZE);
        await Promise.all(
          batch.map(async stock => {
            try {
              const cs = await history5m(stock, appId, token, iso);
              let prev = previousCloseMap.get(stock.symbol);
              if (!Number.isFinite(prev) || prev <= 0) {
                if (cs.length) prev = cs[0].open; // weak fallback
              }
              for (const c of cs) {
                const closeEpoch = c.ts + 300;
                const t = new Date(closeEpoch * 1000).toLocaleTimeString('en-GB', {
                  timeZone: 'Asia/Kolkata',
                  hour: '2-digit',
                  minute: '2-digit'
                });
                if (!missingSet.has(t)) continue;
                if (!Number.isFinite(prev) || prev <= 0) continue;
                const pctVal = ((c.close - prev) / prev) * 100;
                if (!buckets.has(t)) buckets.set(t, []);
                buckets.get(t).push({
                  key: stock.key,
                  name: stock.name,
                  sector: stock.sector,
                  pct: pctVal,
                  close: c.close
                });
              }
            } catch (e) {
              // skip stock
            }
          })
        );
        if ((i + FYERS_HISTORY_BATCH_SIZE) % 30 === 0) {
          log(`Gap-fill progress: ${Math.min(i + FYERS_HISTORY_BATCH_SIZE, universe.length)}/${universe.length}`);
        }
      }

      const filled = [...buckets.keys()].sort();
      for (const t of filled) {
        const arr = buckets.get(t) || [];
        if (arr.length < 20) continue;
        const gainers = arr
          .slice()
          .sort((a, b) => b.pct - a.pct)
          .map((x, i) => ({ ...x, rank: i + 1 }));
        const losers = arr
          .slice()
          .sort((a, b) => a.pct - b.pct)
          .map((x, i) => ({ ...x, rank: i + 1 }));
        state.history.set(t, { gainers, losers });
        await saveSnapshot(iso, t, 'gainers', gainers.slice(0, 50));
        await saveSnapshot(iso, t, 'losers', losers.slice(0, 50));
      }

      log(
        `Gap-fill complete for ${iso}: added ${filled.length} times. Timeline now: ${[...state.history.keys()].sort().join(', ')}`
      );
      broadcastDashboard();
    } finally {
      historyRebuildPromise = null;
    }
  })();

  return historyRebuildPromise;
}

async function rebuildHistoryFromFyers(appId, token) {
  if (historyRebuildPromise) {
    log('History rebuild already running — reusing that job (avoids duplicate FYERS load / 429).');
    return historyRebuildPromise;
  }

  historyRebuildPromise = (async () => {
  try {
  const date = istToday();

  /*
   * Weekends / before open: never rebuild "today" — show last completed session.
   */
  if (!isTradingSessionDay() || !regularSessionStarted()) {
    log(
      !isTradingSessionDay()
        ? `Non-trading day (${date}): loading last completed session instead of rebuilding today.`
        : `Pre-open (${date}): loading last completed session.`
    );
    await loadLatestCompletedSession(appId, token);
    return;
  }

  if (state.currentDay !== date || state.displayMode !== 'current') {
    if (state.currentDay === date && state.history.size) {
      state.displayMode = 'current';
      state.displayDate = date;
    } else {
      resetSession(date);
    }
  }

  await loadDbHistory(date);

  if (state.history.size && !historyIsSparse()) {
    log(
      `Restored today's ranking history from PostgreSQL (${state.history.size} snapshots); FYERS rebuild not required.`
    );
    return;
  }

  // Use DB history, then top-up any missing 5-minute buckets (e.g. after sleep/offline)
  if (await dbHasDayHistory(date)) {
    if (!state.history.size) await loadDbHistory(date);
    const gaps = missingRankTimes(date);
    if (gaps.length) {
      log(
        `DB has partial timeline for ${date}; gap-fill ${gaps.length} missing times (not a full wipe/rebuild).`
      );
      await fillMissingSnapshotsFromFyers(appId, token, date);
    } else {
      log(`Persisted DB history for ${date} is complete through latest bucket — no FYERS rebuild.`);
    }
    return;
  }

  if (state.history.size && historyIsSparse()) {
    const g = state.history.get('09:20')?.gainers?.length || 0;
    log(
      `In-memory history thin (09:20 gainers=${g}); DB empty — one FYERS rebuild. Not deleting DB rows.`,
      'warn'
    );
  }

  const universe = await ensureUniverse();
  if (!universe.length) {
    throw new Error('F&O universe is empty; cannot rebuild history.');
  }

  log(
    `No DB rank history for ${date}; one-time FYERS rebuild (~${universe.length} five-minute history calls + bulk quotes, not per-candle)…`,
    'info'
  );

  const previousCloseMap = new Map();
  try {
    const quoteRows = await quotes(
      universe.map(x => x.symbol),
      appId,
      token
    );
    for (const q of quoteRows) {
      if (q.symbol && Number.isFinite(q.prev) && q.prev > 0) {
        previousCloseMap.set(q.symbol, q.prev);
      }
    }
    log(
      `Previous-close values loaded from FYERS quotes: ${previousCloseMap.size}/${universe.length} stocks.`
    );
  } catch (e) {
    log(`Bulk previous-close quote request failed: ${e.message}`, 'warn');
  }

  const rows = [];
  let successCount = 0;
  let failCount = 0;

  for (let i = 0; i < universe.length; i += FYERS_HISTORY_BATCH_SIZE) {
    const batch = universe.slice(i, i + FYERS_HISTORY_BATCH_SIZE);
    const result = await Promise.all(
      batch.map(async stock => {
        try {
          const cs = await history5m(stock, appId, token, date);
          let prev = previousCloseMap.get(stock.symbol);
          if (!Number.isFinite(prev) || prev <= 0) {
            try {
              prev = await previousCloseFor(stock, appId, token, date);
            } catch (e) {
              log(`${stock.name}: previous-close fallback failed: ${e.message}`, 'warn');
              prev = null;
            }
          }
          if (cs.length) {
            log(`Historical candles received for ${stock.name}: ${cs.length}`);
          }
          return { stock, cs, prev };
        } catch (e) {
          failCount++;
          log(`${stock.name}: history rebuild failed: ${e.message}`, 'warn');
          return null;
        }
      })
    );
    const ok = result.filter(Boolean);
    successCount += ok.length;
    rows.push(...ok);
    if ((i / 6) % 5 === 0) {
      log(`History rebuild progress: ${Math.min(i + 6, universe.length)}/${universe.length}`);
    }
  }

  const buckets = new Map();
  for (const r of rows) {
    if (!Number.isFinite(r.prev) || r.prev <= 0) continue;
    for (const c of r.cs) {
      // FYERS 5-min candle timestamp is the OPEN of the bar; close time = open + 300s
      const closeEpoch = c.ts + 300;
      const t = new Date(closeEpoch * 1000).toLocaleTimeString('en-GB', {
        timeZone: 'Asia/Kolkata',
        hour: '2-digit',
        minute: '2-digit'
      });
      if (t < '09:20' || t > '15:30') continue;
      if (!buckets.has(t)) buckets.set(t, []);
      buckets.get(t).push({
        ...r.stock,
        close: c.close,
        pct: pct(c.close, r.prev)
      });
    }
  }

  const sortedTimes = [...buckets.keys()].sort();
  log(
    `Snapshots generated for date ${date}: ${sortedTimes.length} times ` +
      `(${sortedTimes.slice(0, 5).join(', ')}${sortedTimes.length > 5 ? '…' : ''})`
  );

  for (const t of sortedTimes) {
    const g = [...buckets.get(t)]
      .sort((a, b) => b.pct - a.pct)
      .map((x, i) => ({ ...x, rank: i + 1 }));
    const l = [...buckets.get(t)]
      .sort((a, b) => a.pct - b.pct)
      .map((x, i) => ({ ...x, rank: i + 1 }));

    state.history.set(t, { gainers: g, losers: l });

    try {
      await saveSnapshot(date, t, 'gainers', g);
      await saveSnapshot(date, t, 'losers', l);
    } catch (e) {
      log(`Snapshot DB save error at ${t}: ${e.message}`, 'warn');
    }
  }

  log(
    `Historical ranking rebuild complete for ${date}: ${state.history.size} five-minute snapshots.`
  );
  log(
    `Historical stocks successfully loaded: ${successCount}/${universe.length} (failures: ${failCount}).`
  );
  if (sortedTimes.length) {
    log(`Historical timeline available: ${sortedTimes.join(', ')}`);
  } else {
    log(
      `No 5-minute candles for ${date} (holiday or empty session). Falling back to last completed session.`,
      'warn'
    );
    await loadLatestCompletedSession(appId, token);
  }
  } catch (e) {
    log(`History rebuild error: ${e.message}`, 'error');
    throw e;
  } finally {
    historyRebuildPromise = null;
  }
  })();

  return historyRebuildPromise;
}

/* ---------------------------------------------------------
   FYERS WEBSOCKET MESSAGE PARSER
--------------------------------------------------------- */

function parseFyersMessage(msg) {
  if (!msg) return null;
  if (typeof msg === 'string') {
    try {
      msg = JSON.parse(msg);
    } catch {
      return null;
    }
  }
  if (Array.isArray(msg)) {
    return msg.map(parseFyersMessage).filter(Boolean);
  }
  const symbol = msg.symbol || msg.n || msg.s;
  if (!symbol) return null;
  const ltp = Number(msg.ltp ?? msg.lp ?? msg.v?.lp ?? msg.data?.ltp);
  const chp = Number(msg.chp ?? msg.v?.chp ?? msg.data?.chp);
  const prev = Number(
    msg.prev_close_price ?? msg.v?.prev_close_price ?? msg.data?.prev_close_price
  );
  if (!Number.isFinite(ltp)) return null;
  return {
    symbol,
    ltp,
    changePct: Number.isFinite(chp) ? chp : prev ? ((ltp - prev) / prev) * 100 : 0,
    prev,
    volume: Number(msg.volume ?? msg.v?.volume ?? 0),
    ts: Number(msg.last_traded_time ?? msg.tt ?? msg.v?.tt ?? Date.now() / 1000)
  };
}

/* ---------------------------------------------------------
   LIVE RANKING
--------------------------------------------------------- */

function liveRows() {
  return state.universe
    .map(s => {
      const q = state.live.get(s.symbol);
      if (!q) return null;
      return attachD1Flags({
        ...s,
        ltp: q.ltp,
        pct: q.changePct,
        volume: q.volume || 0,
        ts: q.ts
      });
    })
    .filter(Boolean);
}

function rankedLive() {
  const rows = liveRows();
  return {
    gainers: [...rows].sort((a, b) => b.pct - a.pct).map((x, i) => ({ ...x, rank: i + 1 })),
    losers: [...rows].sort((a, b) => a.pct - b.pct).map((x, i) => ({ ...x, rank: i + 1 }))
  };
}

/* ---------------------------------------------------------
   DISPLAY MEMBERSHIP STABILITY
--------------------------------------------------------- */

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
    if (result.length < 30 && !result.some(x => x.key === n.key)) {
      result.push(n);
    }
  }

  result.sort((a, b) => a.rank - b.rank);
  state.membership[type] = result.map(x => x.key);
  return result;
}

/* ---------------------------------------------------------
   MERGE HISTORICAL + CURRENT
--------------------------------------------------------- */

function mergeRows(type, liveRanked) {
  const rows = smoothMembership(type, liveRanked);
  const base = state.history.get('09:20')?.[type] || [];
  const baseMap = new Map(
    base.map(x => [normalizeKey(x.key) || normalizeKey(x.name), x.rank])
  );
  const times = [...state.history.keys()].filter(t => t !== 'CLOSE').sort();
  const rankMaps = new Map(
    times.map(t => [
      t,
      new Map(
        (state.history.get(t)?.[type] || []).map(x => [
          normalizeKey(x.key) || normalizeKey(x.name),
          x.rank
        ])
      )
    ])
  );

  function lookupRank(map, stock) {
    if (!map) return null;
    const k1 = normalizeKey(stock.key);
    if (k1 && map.has(k1)) return map.get(k1);
    const k2 = normalizeKey(stock.name);
    if (k2 && map.has(k2)) return map.get(k2);
    return null;
  }

  return {
    times: [...times, 'CURRENT'],
    rows: rows.map(x => {
      const baseline = lookupRank(baseMap, x);
      const enriched = attachD1Flags({
        ...x,
        ltp: x.ltp ?? x.close ?? null
      });
      return {
        key: x.key,
        name: x.name,
        sector: x.sector,
        pct: x.pct,
        ltp: enriched.ltp,
        rank: x.rank,
        rankDelta: baseline != null ? baseline - x.rank : null,
        baselineRank: baseline,
        d1High: enriched.d1High,
        d1Low: enriched.d1Low,
        d1Close: enriched.d1Close,
        d1Date: enriched.d1Date,
        rangePct: enriched.rangePct,
        changePct: enriched.changePct,
        absChangePct: enriched.absChangePct,
        consolidation: enriched.consolidation,
        avgVolume: enriched.avgVolume,
        todayVolume: enriched.todayVolume,
        volumeRatio: enriched.volumeRatio,
        highVolume: enriched.highVolume,
        maxBarVol: enriched.maxBarVol,
        barVolRatio: enriched.barVolRatio,
        orHigh: enriched.orHigh,
        orLow: enriched.orLow,
        vwap: enriched.vwap,
        aboveD1High: enriched.aboveD1High,
        belowD1Low: enriched.belowD1Low,
        aboveOrHigh: enriched.aboveOrHigh,
        belowOrLow: enriched.belowOrLow,
        aboveVwap: enriched.aboveVwap,
        belowVwap: enriched.belowVwap,
        growwUrl: enriched.growwUrl,
        history: Object.fromEntries(
          times.map(t => [t, lookupRank(rankMaps.get(t), x)])
        )
      };
    })
  };
}

/* ---------------------------------------------------------
   SECTOR + BREADTH
--------------------------------------------------------- */

function sectorData(live) {
  const map = new Map();
  for (const x of live) {
    const s = x.sector || 'Other F&O';
    if (!map.has(s)) {
      map.set(s, { sector: s, count: 0, sum: 0, positive: 0, stocks: [] });
    }
    const z = map.get(s);
    z.count++;
    z.sum += x.pct;
    if (x.pct > 0) z.positive++;
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

function breadthFrom(rows) {
  let advances = 0;
  let declines = 0;
  let unchanged = 0;
  for (const x of rows) {
    const p = Number(x.pct);
    if (!Number.isFinite(p) || p === 0) unchanged++;
    else if (p > 0) advances++;
    else declines++;
  }
  return { advances, declines, unchanged };
}

/* ---------------------------------------------------------
   CURRENT SNAPSHOT
--------------------------------------------------------- */

function snapshotAtCurrent() {
  const live = rankedLive();
  let displayLive = live;

  const useHistorySnapshot = () => {
    const times = [...state.history.keys()].filter(t => t !== 'CLOSE').sort();
    const key = times.length ? times[times.length - 1] : 'CLOSE';
    const h = state.history.get(key);
    if (!h) return false;
    displayLive = {
      gainers: (h.gainers || []).map(x => ({
        ...x,
        key: normalizeKey(x.key) || normalizeKey(x.name),
        name: normalizeKey(x.name) || normalizeKey(x.key),
        pct: x.pct,
        rank: x.rank,
        ltp: x.close
      })),
      losers: (h.losers || []).map(x => ({
        ...x,
        key: normalizeKey(x.key) || normalizeKey(x.name),
        name: normalizeKey(x.name) || normalizeKey(x.key),
        pct: x.pct,
        rank: x.rank,
        ltp: x.close
      }))
    };
    return true;
  };

  /*
   * Pre-open: previous completed session.
   * After close: prefer last stored 5-min snapshot so historical columns align.
   * During session with no live quotes yet: fall back to last snapshot.
   */
  if (
    state.displayMode === 'previous-close' ||
    !isTradingSessionDay()
  ) {
    useHistorySnapshot();
  } else if (regularSessionFinished() && state.history.size) {
    useHistorySnapshot();
  } else if (!live.gainers.length && state.history.size) {
    useHistorySnapshot();
  }

  // Last resort: live quotes alone (e.g. weekend seed before history loads)
  if (
    (!displayLive.gainers || !displayLive.gainers.length) &&
    live.gainers.length
  ) {
    displayLive = live;
  }

  const g = mergeRows('gainers', displayLive.gainers);
  const l = mergeRows('losers', displayLive.losers);
  const allForBreadth = displayLive.gainers.concat(displayLive.losers);
  // de-dupe by key for breadth
  const seen = new Set();
  const unique = [];
  for (const x of allForBreadth) {
    if (seen.has(x.key)) continue;
    seen.add(x.key);
    unique.push(x);
  }

  const times = [...new Set([...g.times, ...l.times])].sort((a, b) =>
    a === 'CURRENT' ? 1 : b === 'CURRENT' ? -1 : a.localeCompare(b)
  );

  // Full universe rows for Scanner tab (live flags + ranks)
  const gRank = new Map(displayLive.gainers.map(x => [x.key, x.rank]));
  const lRank = new Map(displayLive.losers.map(x => [x.key, x.rank]));
  const scanner = displayLive.gainers.map(x => {
    const enriched = attachD1Flags({
      ...x,
      ltp: x.ltp ?? x.close ?? null,
      volume: x.volume || 0
    });
    return {
      key: enriched.key,
      name: enriched.name,
      sector: enriched.sector,
      ltp: enriched.ltp,
      pct: enriched.pct,
      volume: enriched.todayVolume ?? enriched.volume ?? null,
      gainerRank: gRank.get(x.key) ?? null,
      loserRank: lRank.get(x.key) ?? null,
      rankDelta:
        (state.history.get('09:20')?.gainers || []).find(y => y.key === x.key)
          ? (state.history.get('09:20').gainers.find(y => y.key === x.key).rank -
              (gRank.get(x.key) || 0))
          : null,
      d1High: enriched.d1High,
      d1Low: enriched.d1Low,
      d1Close: enriched.d1Close,
      d1Date: enriched.d1Date,
      rangePct: enriched.rangePct,
      changePct: enriched.changePct,
      absChangePct: enriched.absChangePct,
      consolidation: enriched.consolidation,
      avgVolume: enriched.avgVolume,
      todayVolume: enriched.todayVolume,
      volumeRatio: enriched.volumeRatio,
      highVolume: enriched.highVolume,
      maxBarVol: enriched.maxBarVol,
      barVolRatio: enriched.barVolRatio,
      orHigh: enriched.orHigh,
      orLow: enriched.orLow,
      vwap: enriched.vwap,
      aboveD1High: enriched.aboveD1High,
      belowD1Low: enriched.belowD1Low,
      aboveOrHigh: enriched.aboveOrHigh,
      belowOrLow: enriched.belowOrLow,
      aboveVwap: enriched.aboveVwap,
      belowVwap: enriched.belowVwap,
      growwUrl: enriched.growwUrl,
      fullName: enriched.fullName,
      isGainer: (enriched.pct ?? 0) > 0,
      isLoser: (enriched.pct ?? 0) < 0
    };
  });

  return {
    gainers: g.rows,
    losers: l.rows,
    times,
    currentTime: istParts().time.slice(0, 5),
    sector: sectorData(unique),
    breadth: breadthFrom(unique),
    scanner
  };
}

/* ---------------------------------------------------------
   FINAL CLOSE SNAPSHOT (15:30)
   NSE continuous trading ends at 15:30. If history only reaches
   15:25, materialize a 15:30 ranking from the latest live quotes
   so day-end ranks reflect the official close.
--------------------------------------------------------- */

async function ensureFinalCloseSnapshot() {
  if (!state.fyers.appId || state.displayMode !== 'current') return;
  if (!isTradingSessionDay()) return;
  if (!regularSessionFinished()) return;

  const date = istToday();
  if (state.currentDay && state.currentDay !== date) return;

  const times = [...state.history.keys()].filter(t => t !== 'CLOSE').sort();
  const last = times.length ? times[times.length - 1] : '';
  if (last >= '15:30') return;

  const live = rankedLive();
  if (
    live.gainers.length <
    Math.max(30, Math.floor((state.universe.length || 100) * 0.25))
  ) {
    log(
      `Cannot build 15:30 close ranking yet: only ${live.gainers.length} live symbols.`,
      'warn'
    );
    return;
  }

  const g = live.gainers.map(x => ({ ...x, close: x.ltp }));
  const l = live.losers.map(x => ({ ...x, close: x.ltp }));
  state.history.set('15:30', { gainers: g, losers: l });

  try {
    await saveSnapshot(date, '15:30', 'gainers', g);
    await saveSnapshot(date, '15:30', 'losers', l);
    log(
      `Saved 15:30 day-end ranking snapshot (previous last was ${last || 'none'}). ` +
        `Top gainers: ${g
          .slice(0, 3)
          .map(x => x.name)
          .join(', ')}`
    );
  } catch (e) {
    log(`15:30 close snapshot DB error: ${e.message}`, 'warn');
  }

  broadcastDashboard();
}

/* ---------------------------------------------------------
   5-MINUTE SNAPSHOT
--------------------------------------------------------- */

async function makeCandleSnapshot() {
  if (!state.fyers.appId || state.displayMode !== 'current' || !isTradingSessionDay()) {
    return;
  }

  const now = istParts();
  const t = now.time.slice(0, 5);
  const date = now.date;

  /*
   * Rank window: 09:20 .. 15:30 IST (NSE continuous session ends 15:30).
   * Allow the 15:30 snapshot even though marketOpenNow() becomes false at 15:30.
   * Closing-session prices (15:40 call) are captured via history rebuild using
   * range_to 15:40 when FYERS returns a final bar; live WS may already be quiet.
   */
  if (t < '09:20' || t > '15:30') return;

  const minute = Number(now.time.slice(3, 5));
  // Fire on :00/:05/.../:30, and also once in the first minute after 15:30 for safety
  const atCloseGrace = t === '15:30' && Number(now.time.slice(6, 8)) <= 45;
  if (minute % 5 !== 0 && !atCloseGrace) return;

  if (state.history.has(t)) return;

  const live = rankedLive();
  if (live.gainers.length < Math.max(50, Math.floor(state.universe.length * 0.5))) {
    log(
      `Skipped ${t} snapshot: only ${live.gainers.length}/${state.universe.length} live symbols available.`,
      'warn'
    );
    return;
  }

  const g = live.gainers.map(x => ({ ...x, close: x.ltp }));
  const l = live.losers.map(x => ({ ...x, close: x.ltp }));
  state.history.set(t, { gainers: g, losers: l });

  try {
    await saveSnapshot(date, t, 'gainers', g);
    await saveSnapshot(date, t, 'losers', l);
    log(`Saved ${t} 5-minute ranking snapshot.`);
  } catch (e) {
    log(`Snapshot DB error: ${e.message}`, 'warn');
  }

  broadcastDashboard();
}

/* ---------------------------------------------------------
   FYERS DATA WEBSOCKET
--------------------------------------------------------- */

function startFyersSocket() {
  if (!state.fyers.appId || !state.fyers.token) return;

  try {
    if (state.fyers.socket) {
      try {
        state.fyers.socket.close();
      } catch {}
    }
  } catch {}

  const auth = `${state.fyers.appId}:${state.fyers.token}`;
  const skt = fyersDataSocket.getInstance(auth, '', false);
  state.fyers.socket = skt;

  skt.on('connect', () => {
    state.fyers.connected = true;
    log('FYERS market-data WebSocket connected.');
    const symbols = [
      ...state.universe.map(x => x.symbol),
      ...INDEX_SYMBOLS.map(x => x[1])
    ];
    skt.subscribe(symbols);
  });

  skt.on('message', msg => {
    const parsed = parseFyersMessage(msg);
    const arr = Array.isArray(parsed) ? parsed : [parsed];
    for (const q of arr.filter(Boolean)) {
      if (INDEX_SYMBOLS.some(x => x[1] === q.symbol)) {
        state.indices.set(q.symbol, q);
      } else {
        state.live.set(q.symbol, q);
      }
    }
    if (arr.length) broadcastDashboard();
  });

  skt.on('error', e =>
    log(
      `FYERS WebSocket error: ${typeof e === 'string' ? e : JSON.stringify(e)}`,
      'error'
    )
  );

  skt.on('close', () => {
    state.fyers.connected = false;
    log('FYERS market-data WebSocket closed.', 'warn');
    if (state.fyers.appId && !state.fyers.reconnectTimer) {
      state.fyers.reconnectTimer = setTimeout(() => {
        state.fyers.reconnectTimer = null;
        log('Attempting FYERS WebSocket reconnect…');
        startFyersSocket();
      }, 5000);
    }
  });

  skt.connect();
}

/* ---------------------------------------------------------
   BROWSER WEBSOCKET
--------------------------------------------------------- */

const browserSockets = new Set();

function broadcast(obj) {
  const text = JSON.stringify(obj);
  for (const ws of browserSockets) {
    try {
      if (ws.readyState === 1) ws.send(text);
    } catch {}
  }
}

let broadcastTimer = null;

function broadcastDashboard() {
  if (broadcastTimer) return;
  broadcastTimer = setTimeout(async () => {
    broadcastTimer = null;
    try {
      const d = await dashboardData();
      broadcast({ type: 'dashboard', data: d });
    } catch (e) {
      log(`Broadcast dashboard error: ${e.message}`, 'warn');
    }
  }, 250);
}

/* ---------------------------------------------------------
   DASHBOARD DATA
--------------------------------------------------------- */

async function dashboardData() {
  const now = istParts();
  const preOpen = !regularSessionStarted();
  const postClose = regularSessionFinished();
  const snap = snapshotAtCurrent();

  const n50b = nifty50BreadthFromLive();
  const indices = INDEX_SYMBOLS.map(([name, symbol]) => {
    const base = {
      name,
      symbol,
      ...(state.indices.get(symbol) || { ltp: null, changePct: null })
    };
    if (name === 'NIFTY 50') {
      base.nifty50Breadth = n50b;
    }
    return base;
  });

  let mode = state.displayMode;
  let displayDate = state.displayDate || istToday();
  let displayLabel = 'Current trading session';

  if (!isTradingSessionDay()) {
    mode = 'previous-close';
    displayLabel = 'Market closed (weekend/holiday) • last completed session';
  } else if (preOpen) {
    mode = 'previous-close';
    displayLabel = 'Last completed trading session (pre-open)';
  } else if (postClose) {
    mode = 'current';
    displayLabel = "Today's completed trading session";
  } else {
    mode = 'current';
    displayLabel = "Today's live trading session";
  }

  return {
    date: displayDate,
    calendarDate: now.date,
    marketOpen: marketOpenNow(),
    sessionStarted: regularSessionStarted(),
    sessionFinished: regularSessionFinished(),
    displayMode: mode,
    displayLabel,
    updatedAt: new Date().toISOString(),
    indices,
    universeSize: state.universe.length,
    connected: state.fyers.connected,
    historyTimes: [...state.history.keys()].filter(t => t !== 'CLOSE').sort(),
    ...snap,
    logs: state.log.slice(-100)
  };
}

/* ---------------------------------------------------------
   HTTP HELPERS
--------------------------------------------------------- */

function readBody(req) {
  return new Promise((resolve, reject) => {
    let b = '';
    req.on('data', c => {
      b += c;
      if (b.length > 1e6) {
        req.destroy();
        reject(new Error('Body too large'));
      }
    });
    req.on('end', () => {
      try {
        resolve(b ? JSON.parse(b) : {});
      } catch (e) {
        reject(e);
      }
    });
    req.on('error', reject);
  });
}

function cors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader(
    'Access-Control-Allow-Headers',
    'Content-Type, x-fyers-app-id, x-fyers-access-token'
  );
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
}

function send(res, status, data, type = 'application/json') {
  cors(res);
  res.writeHead(status, {
    'Content-Type': type,
    'Cache-Control': 'no-store'
  });
  res.end(type === 'application/json' ? JSON.stringify(data) : data);
}

/* ---------------------------------------------------------
   HTTP ROUTER
--------------------------------------------------------- */

async function route(req, res) {
  cors(res);
  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    return res.end();
  }

  const u = new URL(req.url, `http://${req.headers.host}`);
  const pathName = u.pathname;

  try {
    if (pathName === '/api/health' && req.method === 'GET') {
      return send(res, 200, {
        ok: true,
        time: istParts(),
        universe: state.universe.length,
        historySnapshots: state.history.size,
        connected: state.fyers.connected,
        db: !!pool
      });
    }

    if (pathName === '/api/login' && req.method === 'POST') {
      const body = await readBody(req);
      const appId = String(body.appId || '').trim();
      const accessToken = String(body.accessToken || body.token || '').trim();
      if (!appId || !accessToken) {
        return send(res, 400, { ok: false, error: 'App ID and access token are required.' });
      }

      /*
       * Soft login: never block the user behind REST validation when FYERS is 429'ing.
       * Accept credentials → load ranks from Postgres → start WebSocket.
       * Heavy REST (quotes seed / history rebuild / D-1) runs only after cooldown.
       */
      let rateLimited = Date.now() < fyersCooldownUntil;
      let validated = false;

      if (!rateLimited) {
        try {
          await fyersGet(
            DATA_HOST,
            '/quotes',
            { symbols: 'NSE:NIFTY50-INDEX' },
            appId,
            accessToken,
            0
          );
          validated = true;
          noteFyersSuccess();
        } catch (e) {
          const is429 = /429|rate limit/i.test(e.message);
          log(`Login validation: ${e.message}`, is429 ? 'warn' : 'error');
          if (!is429) {
            return send(res, 401, {
              ok: false,
              error: `FYERS auth failed: ${e.message}`
            });
          }
          rateLimited = true;
        }
      } else {
        log(
          `Login during FYERS cooldown (${Math.ceil((fyersCooldownUntil - Date.now()) / 1000)}s left) — soft accept, DB-only.`,
          'warn'
        );
      }

      state.fyers.appId = appId;
      state.fyers.token = accessToken;
      log(
        rateLimited
          ? 'FYERS credentials stored (soft login — REST deferred until rate limit clears).'
          : 'FYERS credentials accepted (token not logged).'
      );

      await ensureUniverse();

      // Always prefer Postgres ranks first — zero FYERS REST
      try {
        const today = istToday();
        if (isTradingSessionDay() && regularSessionStarted()) {
          await loadDbHistory(today);
          state.displayMode = 'current';
          state.displayDate = today;
          state.currentDay = today;
          if (!state.history.size) {
            // previous day from DB only
            const prev = await latestStoredTradingDate();
            if (prev) {
              await loadDbHistory(prev);
              state.displayMode = 'previous-close';
              state.displayDate = prev;
            }
          }
        } else {
          const prev = await latestStoredTradingDate();
          if (prev) {
            await loadDbHistory(prev);
            state.displayMode = 'previous-close';
            state.displayDate = prev;
            state.currentDay = prev;
          }
        }
        log(
          `Post-login DB ranks: ${state.history.size} snapshot times for ${state.displayDate || today}.`
        );
      } catch (e) {
        log(`Post-login DB history: ${e.message}`, 'warn');
      }

      // WebSocket often works even when REST is limited
      try {
        startFyersSocket();
      } catch (e) {
        log(`WebSocket start: ${e.message}`, 'warn');
      }

      broadcastDashboard();

      // Defer ALL REST until cooldown ends (plus buffer)
      const deferMs = rateLimited
        ? Math.max(120000, fyersCooldownUntil - Date.now() + 5000)
        : 5000;

      setTimeout(() => {
        (async () => {
          if (!state.fyers.appId) return;
          if (Date.now() < fyersCooldownUntil) {
            log('Deferred REST still in cooldown — waiting longer…', 'warn');
            await sleep(Math.max(0, fyersCooldownUntil - Date.now()) + 5000);
          }
          if (!state.fyers.appId) return;

          try {
            const day = istToday();
            await loadDbHistory(day);
            const gaps = missingRankTimes(day);
            if (gaps.length) {
              log(`Deferred: gap-fill ${gaps.length} missing rank times…`);
              await fillMissingSnapshotsFromFyers(
                state.fyers.appId,
                state.fyers.token,
                day
              );
            } else if (!state.history.size) {
              log('Deferred: full history rebuild (DB empty)…');
              await rebuildHistoryFromFyers(state.fyers.appId, state.fyers.token);
            }
          } catch (e) {
            log(`Deferred history: ${e.message}`, 'warn');
          }

          try {
            const iq = await indexQuotes(state.fyers.appId, state.fyers.token);
            for (const x of iq) {
              if (x.symbol) state.indices.set(x.symbol, x);
            }
          } catch (e) {
            log(`Deferred index quotes: ${e.message}`, 'warn');
          }

          try {
            const q = await quotes(
              state.universe.map(x => x.symbol),
              state.fyers.appId,
              state.fyers.token
            );
            for (const row of q) {
              if (row.symbol) state.live.set(row.symbol, row);
            }
            log(`Deferred: seeded live quotes for ${q.length} symbols.`);
            if (!state.history.size) {
              const live = rankedLive();
              if (live.gainers.length) {
                state.history.set('CLOSE', {
                  gainers: live.gainers,
                  losers: live.losers
                });
              }
            }
          } catch (e) {
            log(`Deferred quote seed: ${e.message}`, 'warn');
          }

          try {
            if (historyRebuildPromise) await historyRebuildPromise;
            await sleep(10000);
            await loadD1Levels(state.fyers.appId, state.fyers.token);
            await sleep(15000);
            await loadIntradayMetrics(state.fyers.appId, state.fyers.token);
            log('Deferred Scanner metrics complete.');
          } catch (e) {
            log(`Deferred metrics: ${e.message}`, 'warn');
          }

          broadcastDashboard();
        })();
      }, deferMs);

      return send(res, 200, {
        ok: true,
        softLogin: rateLimited || !validated,
        rateLimited,
        universeSize: state.universe.length,
        historySnapshots: state.history.size,
        message: rateLimited
          ? 'Logged in with DB ranks. Live REST paused until FYERS rate limit clears (~1–3 min).'
          : undefined
      });
    }

    if (pathName === '/api/logout' && req.method === 'POST') {
      if (state.fyers.socket) {
        try {
          state.fyers.socket.close();
        } catch {}
      }
      if (state.fyers.reconnectTimer) {
        clearTimeout(state.fyers.reconnectTimer);
        state.fyers.reconnectTimer = null;
      }
      state.fyers.appId = '';
      state.fyers.token = '';
      state.fyers.connected = false;
      state.fyers.socket = null;
      log('Logged out; FYERS credentials cleared.', 'warn');
      return send(res, 200, { ok: true });
    }

    if (pathName === '/api/dashboard' && req.method === 'GET') {
      const appId = req.headers['x-fyers-app-id'] || '';
      const token = req.headers['x-fyers-access-token'] || '';
      if (!state.fyers.appId && appId && token) {
        /*
         * Soft restore after Render sleep: restore credentials + DB ranks only.
         * Never run full FYERS history rebuild here — that + login caused 429 storms.
         */
        state.fyers.appId = String(appId);
        state.fyers.token = String(token);
        log('Soft session restore from browser headers (DB only; no FYERS rebuild).');
        try {
          await ensureUniverse();
          const today = istToday();
          if (isTradingSessionDay() && regularSessionStarted()) {
            await loadDbHistory(today);
            state.displayMode = 'current';
            state.displayDate = today;
            state.currentDay = today;
          } else {
            await loadLatestCompletedSession(state.fyers.appId, state.fyers.token);
          }
        } catch (e) {
          log(`Dashboard soft restore: ${e.message}`, 'warn');
        }
        // Socket only — live quotes via WS; REST seed deferred to avoid 429
        try {
          startFyersSocket();
        } catch (e) {
          log(`Socket start on restore: ${e.message}`, 'warn');
        }
      }
      if (!state.fyers.appId) {
        return send(res, 401, { error: 'Not authenticated. Please log in.' });
      }
      const d = await dashboardData();
      return send(res, 200, d);
    }

    if (pathName === '/api/logs' && req.method === 'GET') {
      return send(res, 200, { logs: state.log.slice(-150) });
    }

    return send(res, 404, { error: 'Not found' });
  } catch (e) {
    log(e.message, 'error');
    return send(res, 500, {
      ok: false,
      error: e.message,
      logs: state.log.slice(-30)
    });
  }
}

/* ---------------------------------------------------------
   HTTP SERVER
--------------------------------------------------------- */

const htmlPath = path.join(__dirname, 'index.html');

const server = http.createServer((req, res) => {
  const u = new URL(req.url, `http://${req.headers.host}`);

  if (u.pathname.startsWith('/api/')) {
    return route(req, res);
  }

  if (req.method === 'GET' && (u.pathname === '/' || u.pathname === '/index.html')) {
    try {
      const html = fs.readFileSync(htmlPath);
      cors(res);
      res.writeHead(200, {
        'Content-Type': 'text/html; charset=utf-8',
        'Cache-Control': 'no-store'
      });
      return res.end(html);
    } catch (e) {
      return send(res, 500, `index.html missing: ${e.message}`, 'text/plain');
    }
  }

  send(res, 404, 'Not found', 'text/plain');
});

/* ---------------------------------------------------------
   BROWSER WEBSOCKET SERVER
--------------------------------------------------------- */

const { WebSocketServer } = require('ws');

const wss = new WebSocketServer({ noServer: true });

wss.on('connection', async ws => {
  browserSockets.add(ws);
  try {
    ws.send(
      JSON.stringify({
        type: 'dashboard',
        data: await dashboardData()
      })
    );
  } catch (e) {
    log(`Browser WebSocket initial send: ${e.message}`, 'warn');
  }
  ws.on('close', () => browserSockets.delete(ws));
});

server.on('upgrade', (req, socket, head) => {
  const u = new URL(req.url, `http://${req.headers.host}`);
  if (u.pathname !== '/ws') {
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, req));
});

/* ---------------------------------------------------------
   SESSION MONITOR
--------------------------------------------------------- */

setInterval(async () => {
  if (!state.fyers.appId) return;
  const today = istToday();
  try {
    if (!regularSessionStarted()) {
      if (state.displayDate !== today || state.displayMode !== 'previous-close') {
        // Only switch if we have not already loaded a previous session
        if (state.displayMode !== 'previous-close' || !state.history.size) {
          await loadLatestCompletedSession(state.fyers.appId, state.fyers.token);
          broadcastDashboard();
        }
      }
      return;
    }

    if (state.currentDay !== today || state.displayMode !== 'current') {
      if (state.currentDay && state.currentDay !== today) {
        resetSession(today);
      } else {
        state.displayMode = 'current';
        state.displayDate = today;
        state.currentDay = today;
      }
      await rebuildHistoryFromFyers(state.fyers.appId, state.fyers.token);
      log(
        `Trading session ${today} active (DB history reused when present; no duplicate FYERS rebuild).`
      );
      broadcastDashboard();
    }
  } catch (e) {
    log(`Session/date rollover error: ${e.message}`, 'warn');
  }
}, 5000);

/* ---------------------------------------------------------
   5-MINUTE SNAPSHOT TIMER
--------------------------------------------------------- */

setInterval(() => {
  makeCandleSnapshot().catch(e => log(`Snapshot timer: ${e.message}`, 'warn'));
}, 15000);

setInterval(() => {
  ensureFinalCloseSnapshot().catch(e =>
    log(`Final close snapshot: ${e.message}`, 'warn')
  );
}, 30000);

/* ---------------------------------------------------------
   DASHBOARD BROADCAST
--------------------------------------------------------- */

setInterval(() => {
  if (marketOpenNow() && state.fyers.connected) {
    broadcastDashboard();
  }
}, 5000);

/* ---------------------------------------------------------
   START
--------------------------------------------------------- */

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
