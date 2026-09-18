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
 * Sector classification.
 * This is intentionally kept on the server so the frontend receives
 * the same sector information for gainers, losers and sector drill-down.
 */
const SECTOR_MAP = {
  ADANIENT:"Metals & Mining",
  ADANIPORTS:"Infrastructure",
  APOLLOHOSP:"Healthcare",
  ASIANPAINT:"Consumer Durables",
  AXISBANK:"Financial Services",
  "BAJAJ-AUTO":"Automobile",
  BAJFINANCE:"Financial Services",
  BAJAJFINSV:"Financial Services",
  BEL:"Defence",
  BHARTIARTL:"Telecommunication",
  BPCL:"Oil & Gas",
  BRITANNIA:"FMCG",
  CIPLA:"Healthcare",
  COALINDIA:"Metals & Mining",
  DABUR:"FMCG",
  DIVISLAB:"Healthcare",
  DRREDDY:"Healthcare",
  EICHERMOT:"Automobile",
  ETERNAL:"Consumer Services",
  GAIL:"Oil & Gas",
  GRASIM:"Cement",
  HCLTECH:"Information Technology",
  HDFCBANK:"Financial Services",
  HDFCLIFE:"Financial Services",
  HEROMOTOCO:"Automobile",
  HINDALCO:"Metals & Mining",
  HINDPETRO:"Oil & Gas",
  HINDUNILVR:"FMCG",
  HINDZINC:"Metals & Mining",
  ICICIBANK:"Financial Services",
  INDUSINDBK:"Financial Services",
  INFY:"Information Technology",
  IOC:"Oil & Gas",
  ITC:"FMCG",
  JINDALSTEL:"Metals & Mining",
  JSWSTEEL:"Metals & Mining",
  KOTAKBANK:"Financial Services",
  LT:"Infrastructure",
  LTM:"Information Technology",
  LTIM:"Information Technology",
  "M&M":"Automobile",
  "M&MFIN":"Financial Services",
  MARICO:"FMCG",
  MARUTI:"Automobile",
  MAXHEALTH:"Healthcare",
  MCX:"Financial Services",
  NESTLEIND:"FMCG",
  NHPC:"Power",
  NMDC:"Metals & Mining",
  NTPC:"Power",
  OIL:"Oil & Gas",
  ONGC:"Oil & Gas",
  PAGEIND:"Consumer Durables",
  PERSISTENT:"Information Technology",
  PETRONET:"Oil & Gas",
  PFC:"Financial Services",
  POWERGRID:"Power",
  RBLBANK:"Financial Services",
  RELIANCE:"Oil & Gas",
  SBICARD:"Financial Services",
  SBILIFE:"Financial Services",
  SBIN:"Financial Services",
  SHREECEM:"Cement",
  SHRIRAMFIN:"Financial Services",
  SIEMENS:"Industrials",
  SOLARINDS:"Defence",
  SRF:"Chemicals",
  SUNPHARMA:"Healthcare",
  TATACONSUM:"FMCG",
  TATAMOTORS:"Automobile",
  TATASTEEL:"Metals & Mining",
  TCS:"Information Technology",
  TECHM:"Information Technology",
  TITAN:"Consumer Durables",
  TORNTPHARM:"Healthcare",
  TRENT:"Consumer Services",
  TVSMOTOR:"Automobile",
  UBL:"FMCG",
  ULTRACEMCO:"Cement",
  UNIONBANK:"Financial Services",
  VEDL:"Metals & Mining",
  VOLTAS:"Consumer Durables",
  WIPRO:"Information Technology",
  YESBANK:"Financial Services",
  ZYDUSLIFE:"Healthcare",
  ABB:"Industrials",
  ABCAPITAL:"Financial Services",
  ABFRL:"Consumer Services",
  ALKEM:"Healthcare",
  AMBUJACEM:"Cement",
  AUROPHARMA:"Healthcare",
  AUBANK:"Financial Services",
  BALKRISIND:"Automobile",
  BANKBARODA:"Financial Services",
  BHEL:"Industrials",
  BIOCON:"Healthcare",
  BOSCHLTD:"Automobile",
  CANBK:"Financial Services",
  CESC:"Power",
  CGPOWER:"Industrials",
  CHAMBLFERT:"Chemicals",
  COLPAL:"FMCG",
  CONCOR:"Infrastructure",
  CROMPTON:"Consumer Durables",
  DALBHARAT:"Cement",
  DELHIVERY:"Logistics",
  DIXON:"Consumer Durables",
  FEDERALBNK:"Financial Services",
  GLENMARK:"Healthcare",
  GODREJCP:"FMCG",
  GODREJPROP:"Real Estate",
  HAVELLS:"Consumer Durables",
  ICICIGI:"Financial Services",
  IDFCFIRSTB:"Financial Services",
  INDIACEM:"Cement",
  INDIAMART:"Consumer Services",
  INDHOTEL:"Consumer Services",
  INDUSTOWER:"Telecommunication",
  IRCTC:"Consumer Services",
  IRFC:"Financial Services",
  JUBLFOOD:"Consumer Services",
  KALYANKJIL:"Consumer Durables",
  KEI:"Industrials",
  LICHSGFIN:"Financial Services",
  LODHA:"Real Estate",
  LUPIN:"Healthcare",
  MANKIND:"Healthcare",
  MANAPPURAM:"Financial Services",
  MGL:"Oil & Gas",
  MFSL:"Financial Services",
  MOTHERSON:"Automobile",
  MPHASIS:"Information Technology",
  NATIONALUM:"Metals & Mining",
  NAVINFLUOR:"Chemicals",
  OFSS:"Information Technology",
  PAYTM:"Financial Services",
  PIIND:"Chemicals",
  POLYCAB:"Industrials",
  PVRINOX:"Consumer Services",
  RAIN:"Chemicals",
  RAMCOCEM:"Cement",
  RECLTD:"Financial Services",
  SAIL:"Metals & Mining",
  SCHAEFFLER:"Automobile",
  SUPREMEIND:"Industrials",
  TATACHEM:"Chemicals",
  TATAPOWER:"Power",
  TIINDIA:"Automobile",
  TORNTPOWER:"Power",
  UPL:"Chemicals",
  VBL:"FMCG",
  WHIRLPOOL:"Consumer Durables",
  ZOMATO:"Consumer Services"
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

  if (state.log.length > 300) {
    state.log.shift();
  }

  console.log(`[${level}] ${msg}`);

  broadcast({
    type: 'log',
    line
  });
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

  return {
    date: d,
    time: t
  };
}

function istToday() {
  return istParts().date;
}

function marketOpenNow() {
  const { time } = istParts();

  return (
    time >= '09:15:00' &&
    time < '15:30:00'
  );
}

function regularSessionStarted() {
  return istParts().time >= '09:15:00';
}

function regularSessionFinished() {
  return istParts().time >= '15:30:00';
}

function fyersHeaders(appId, token) {
  return {
    Authorization: `${appId}:${token}`,
    'Content-Type': 'application/json'
  };
}

/* ---------------------------------------------------------
   FYERS HTTP
--------------------------------------------------------- */

async function fyersGet(
  base,
  endpoint,
  params,
  appId,
  token
) {
  const u = new URL(base + endpoint);

  for (const [k, v] of Object.entries(params || {})) {
    u.searchParams.set(k, String(v));
  }

  const r = await fetch(u, {
    headers: fyersHeaders(appId, token)
  });

  const text = await r.text();

  let data;

  try {
    data = JSON.parse(text);
  } catch {
    throw new Error(
      `FYERS non-JSON ${r.status}: ${text.slice(0, 180)}`
    );
  }

  if (
    !r.ok ||
    data.s === 'error'
  ) {
    throw new Error(
      `FYERS ${r.status}/${data.code ?? ''}: ${
        data.message || 'request failed'
      }`
    );
  }

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
      PRIMARY KEY (
        trading_date,
        candle_time,
        side,
        rank
      )
    );

    CREATE INDEX IF NOT EXISTS rank_snapshots_lookup
    ON rank_snapshots(
      trading_date,
      side,
      candle_time
    );
  `);

  log('Postgres persistence ready.');
}

async function saveSnapshot(
  date,
  time,
  side,
  rows
) {
  if (!pool || !rows?.length) {
    return;
  }

  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    for (const r of rows) {
      await client.query(
        `
        INSERT INTO rank_snapshots
        (
          trading_date,
          candle_time,
          side,
          rank,
          symbol,
          name,
          sector,
          pct,
          close
        )
        VALUES (
          $1,$2,$3,$4,$5,$6,$7,$8,$9
        )

        ON CONFLICT (
          trading_date,
          candle_time,
          side,
          rank
        )

        DO UPDATE SET
          symbol=EXCLUDED.symbol,
          name=EXCLUDED.name,
          sector=EXCLUDED.sector,
          pct=EXCLUDED.pct,
          close=EXCLUDED.close
        `,
        [
          date,
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
  if (!pool) {
    return null;
  }

  const today = istToday();

  const { rows } = await pool.query(
    `
    SELECT MAX(trading_date) AS d
    FROM rank_snapshots
    WHERE trading_date < $1
    `,
    [today]
  );

  return rows[0]?.d
    ? String(rows[0].d).slice(0, 10)
    : null;
}

/* ---------------------------------------------------------
   SESSION RESET
--------------------------------------------------------- */

function resetSession(date) {
  state.currentDay = date;
  state.displayDate = date;
  state.displayMode = 'current';

  state.history.clear();

  state.membership = {
    gainers: [],
    losers: []
  };

  state.live.clear();
  state.indices.clear();
}

/* ---------------------------------------------------------
   PREVIOUS COMPLETED SESSION
--------------------------------------------------------- */

async function loadLatestCompletedSession(
  appId,
  token
) {
  const stored =
    await latestStoredTradingDate();

  if (stored) {
    state.currentDay = stored;
    state.displayDate = stored;
    state.displayMode = 'previous-close';

    state.history.clear();

    state.membership = {
      gainers: [],
      losers: []
    };

    await loadDbHistory(stored);

    if (state.history.size) {
      log(
        `Pre-open mode: showing last completed trading session ${stored}.`
      );

      return stored;
    }
  }

  /*
   * Fallback when there is no persisted ranking history.
   *
   * We use daily candles to identify the most recent
   * completed trading day and build a last-close ranking.
   */

  const universe =
    await ensureUniverse();

  const today = istToday();

  const rows = [];

  for (
    let i = 0;
    i < universe.length;
    i += 6
  ) {
    const batch =
      universe.slice(i, i + 6);

    const result =
      await Promise.all(
        batch.map(async stock => {
          try {
            const from =
              epochForISTDate(today, '09:15') -
              21 * 86400;

            const to =
              epochForISTDate(today, '09:15');

            const d =
              await fyersGet(
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

            const cs =
              d.candles || [];

            if (cs.length < 2) {
              return null;
            }

            const last =
              cs[cs.length - 1];

            const prev =
              cs[cs.length - 2];

            const close =
              Number(last[4]);

            const prevClose =
              Number(prev[4]);

            if (
              !Number.isFinite(close) ||
              !Number.isFinite(prevClose) ||
              !prevClose
            ) {
              return null;
            }

            return {
              ...stock,

              close,

              pct: pct(
                close,
                prevClose
              ),

              sourceDate:
                new Date(
                  Number(last[0]) * 1000
                ).toLocaleDateString(
                  'en-CA',
                  {
                    timeZone:
                      'Asia/Kolkata'
                  }
                )
            };
          } catch (e) {
            log(
              `${stock.name}: previous-session lookup failed: ${e.message}`,
              'warn'
            );

            return null;
          }
        })
      );

    rows.push(
      ...result.filter(Boolean)
    );
  }

  if (!rows.length) {
    throw new Error(
      'Could not obtain a previous completed trading session from FYERS.'
    );
  }

  const date =
    rows
      .map(x => x.sourceDate)
      .sort()
      .pop();

  const g =
    [...rows]
      .sort((a, b) => b.pct - a.pct)
      .map((x, i) => ({
        ...x,
        rank: i + 1
      }));

  const l =
    [...rows]
      .sort((a, b) => a.pct - b.pct)
      .map((x, i) => ({
        ...x,
        rank: i + 1
      }));

  state.currentDay = date;
  state.displayDate = date;
  state.displayMode = 'previous-close';

  state.history.clear();

  state.history.set(
    'CLOSE',
    {
      gainers: g,
      losers: l
    }
  );

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
  if (!pool) {
    return;
  }

  const { rows } =
    await pool.query(
      `
      SELECT
        trading_date,
        candle_time,
        side,
        rank,
        symbol,
        name,
        sector,
        pct,
        close
      FROM rank_snapshots
      WHERE trading_date=$1
      ORDER BY candle_time, side, rank
      `,
      [date]
    );

  for (const r of rows) {
    const t =
      String(r.candle_time)
        .slice(0, 5);

    if (!state.history.has(t)) {
      state.history.set(
        t,
        {
          gainers: [],
          losers: []
        }
      );
    }

    state.history.get(t)[r.side].push({
      key: r.symbol,
      name: r.name,
      sector: r.sector,
      pct: Number(r.pct),
      close:
        r.close == null
          ? null
          : Number(r.close),
      rank: Number(r.rank)
    });
  }

  if (rows.length) {
    log(
      `Restored ${rows.length} ranking rows from Postgres for ${date}.`
    );
  }
}

/* ---------------------------------------------------------
   SYMBOL MASTER
--------------------------------------------------------- */

async function loadMaster(kind) {
  const now = Date.now();

  if (
    state.masters[kind] &&
    now - state.masters.loadedAt <
      6 * 60 * 60 * 1000
  ) {
    return state.masters[kind];
  }

  const name =
    kind === 'fo'
      ? 'NSE_FO_sym_master.json'
      : 'NSE_CM_sym_master.json';

  const r =
    await fetch(
      MASTER_BASE + name,
      {
        headers: {
          'User-Agent':
            'fno-rank-dashboard/2.0'
        }
      }
    );

  if (!r.ok) {
    throw new Error(
      `Symbol master ${name}: HTTP ${r.status}`
    );
  }

  const data =
    await r.json();

  state.masters[kind] = data;
  state.masters.loadedAt = now;

  log(
    `Loaded ${name} (${Object.keys(data).length} instruments).`
  );

  return data;
}

function getField(v, names) {
  for (const n of names) {
    if (
      v?.[n] !== undefined &&
      v?.[n] !== null &&
      v?.[n] !== ''
    ) {
      return v[n];
    }
  }

  return '';
}

function buildUniverse(fo, cm) {
  const out = new Map();

  for (const [key, v] of Object.entries(fo)) {
    const typ =
      String(
        getField(
          v,
          [
            'instrumentType',
            'instrument_type',
            'type'
          ]
        )
      ).toUpperCase();

    if (
      !(
        typ === '13' ||
        typ === 'FUTSTK' ||
        typ.includes('FUTSTK')
      )
    ) {
      continue;
    }

    let underlying =
      String(
        getField(
          v,
          [
            'underlyingSymbol',
            'underlying',
            'underlying_scrip',
            'shortSym',
            'short_sym',
            'exSymName'
          ]
        )
      )
        .toUpperCase()
        .trim();

    if (underlying.includes(':')) {
      underlying =
        underlying.split(':').pop();
    }

    underlying =
      underlying
        .replace(/-EQ$/, '')
        .replace(/\s+/g, '');

    if (
      !underlying ||
      [
        'NIFTY',
        'BANKNIFTY',
        'FINNIFTY',
        'MIDCPNIFTY',
        'SENSEX',
        'BANKEX'
      ].includes(underlying)
    ) {
      continue;
    }

    let eq = [
      `NSE:${underlying}-EQ`,
      String(
        getField(
          v,
          [
            'cashSymbol',
            'equitySymbol',
            'eqSymbol'
          ]
        )
      )
    ]
      .filter(Boolean)
      .find(s => cm[s]);

    if (!eq) {
      const hit =
        Object.entries(cm).find(
          ([s, cv]) =>
            String(
              getField(
                cv,
                [
                  'shortSymbol',
                  'shortSym',
                  'exSymName',
                  'symDetails'
                ]
              )
            )
              .toUpperCase() ===
              underlying &&
            s.startsWith('NSE:') &&
            /-EQ$/.test(s)
        );

      if (hit) {
        eq = hit[0];
      }
    }

    if (!eq) {
      eq =
        `NSE:${underlying}-EQ`;
    }

    if (!out.has(underlying)) {
      out.set(
        underlying,
        {
          key: underlying,
          symbol: eq,
          name: underlying,
          sector:
            SECTOR_MAP[underlying] ||
            'Other F&O'
        }
      );
    }
  }

  return [
    ...out.values()
  ].sort(
    (a, b) =>
      a.name.localeCompare(b.name)
  );
}

async function ensureUniverse() {
  if (state.universe.length) {
    return state.universe;
  }

  const [fo, cm] =
    await Promise.all([
      loadMaster('fo'),
      loadMaster('cm')
    ]);

  state.universe =
    buildUniverse(fo, cm);

  log(
    `F&O equity universe ready: ${state.universe.length} stocks.`
  );

  return state.universe;
}

/* ---------------------------------------------------------
   QUOTES / HISTORY
--------------------------------------------------------- */

function chunks(a, n) {
  const r = [];

  for (
    let i = 0;
    i < a.length;
    i += n
  ) {
    r.push(
      a.slice(i, i + n)
    );
  }

  return r;
}

async function quotes(
  symbols,
  appId,
  token
) {
  const all = [];

  for (
    const c of chunks(symbols, 50)
  ) {
    const d =
      await fyersGet(
        DATA_HOST,
        '/quotes',
        {
          symbols:
            c.join(',')
        },
        appId,
        token
      );

    for (const x of d.d || []) {
      const v = x.v || {};

      all.push({
        symbol:
          x.symbol ||
          x.n ||
          x.s ||
          '',

        ltp:
          Number(
            v.lp ??
            v.ltp ??
            0
          ),

        prev:
          Number(
            v.prev_close_price ??
            v.prev_close ??
            0
          ),

        changePct:
          Number(v.chp ?? 0),

        volume:
          Number(v.volume ?? 0),

        ts:
          Number(
            v.tt ??
            v.timestamp ??
            0
          )
      });
    }
  }

  return all;
}

async function indexQuotes(
  appId,
  token
) {
  const q =
    await quotes(
      INDEX_SYMBOLS.map(
        x => x[1]
      ),
      appId,
      token
    );

  const m =
    new Map(
      q.map(
        x => [
          x.symbol,
          x
        ]
      )
    );

  return INDEX_SYMBOLS.map(
    ([name, symbol]) => ({
      name,
      symbol,
      ...(
        m.get(symbol) || {
          ltp: null,
          changePct: null
        }
      )
    })
  );
}

function epochForISTDate(
  date,
  hhmm
) {
  const [h, m] =
    hhmm
      .split(':')
      .map(Number);

  return Math.floor(
    Date.parse(
      `${date}T${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:00+05:30`
    ) / 1000
  );
}

async function history5m(
  stock,
  appId,
  token,
  date
) {
  const from =
    epochForISTDate(
      date,
      '09:15'
    );

  const to =
    Math.min(
      epochForISTDate(
        date,
        '15:40'
      ),
      Math.floor(
        Date.now() / 1000
      )
    );

  const d =
    await fyersGet(
      DATA_HOST,
      '/history',
      {
        symbol:
          stock.symbol,
        resolution: '5',
        date_format: '0',
        range_from: from,
        range_to: to,
        cont_flag: '1'
      },
      appId,
      token
    );

  return (
    d.candles || []
  ).map(x => ({
    ts: Number(x[0]),
    open: Number(x[1]),
    high: Number(x[2]),
    low: Number(x[3]),
    close: Number(x[4]),
    volume: Number(x[5])
  }));
}

async function previousCloseFor(
  stock,
  appId,
  token,
  date
) {
  const from =
    epochForISTDate(
      date,
      '09:15'
    ) -
    5 * 86400;

  const to =
    epochForISTDate(
      date,
      '09:20'
    );

  const d =
    await fyersGet(
      DATA_HOST,
      '/history',
      {
        symbol:
          stock.symbol,
        resolution: 'D',
        date_format: '0',
        range_from: from,
        range_to: to,
        cont_flag: '1'
      },
      appId,
      token
    );

  const cs =
    d.candles || [];

  return cs.length
    ? Number(
        cs[cs.length - 1][4]
      )
    : null;
}

function pct(close, prev) {
  return prev
    ? ((close - prev) / prev) * 100
    : 0;
}

/* ---------------------------------------------------------
   REBUILD HISTORY
--------------------------------------------------------- */

async function rebuildHistoryFromFyers(
  appId,
  token
) {
  const date = istToday();

  if (!regularSessionStarted()) {
    await loadLatestCompletedSession(
      appId,
      token
    );

    return;
  }

  if (
    state.currentDay !== date ||
    state.displayMode !== 'current'
  ) {
    resetSession(date);
  }

  await loadDbHistory(date);

  if (state.history.size) {
    return;
  }

  const universe =
    await ensureUniverse();

  log(
    `No stored ranking history for ${date}; rebuilding 09:20 onward from FYERS history…`,
    'info'
  );

  const rows = [];

  for (
    let i = 0;
    i < universe.length;
    i += 6
  ) {
    const batch =
      universe.slice(i, i + 6);

    const result =
      await Promise.all(
        batch.map(
          async stock => {
            try {
              const [
                cs,
                prev
              ] =
                await Promise.all([
                  history5m(
                    stock,
                    appId,
                    token,
                    date
                  ),

                  previousCloseFor(
                    stock,
                    appId,
                    token,
                    date
                  )
                ]);

              return {
                stock,
                cs,
                prev
              };
            } catch (e) {
              log(
                `${stock.name}: history rebuild failed: ${e.message}`,
                'warn'
              );

              return null;
            }
          }
        )
      );

    rows.push(
      ...result.filter(Boolean)
    );
  }

  const buckets =
    new Map();

  for (const r of rows) {
    if (!r.prev) {
      continue;
    }

    for (const c of r.cs) {
      const closeEpoch =
        c.ts + 300;

      const t =
        new Date(
          closeEpoch * 1000
        ).toLocaleTimeString(
          'en-GB',
          {
            timeZone:
              'Asia/Kolkata',
            hour: '2-digit',
            minute: '2-digit'
          }
        );

      if (
        t < '09:20' ||
        t > '15:25'
      ) {
        continue;
      }

      if (!buckets.has(t)) {
        buckets.set(
          t,
          []
        );
      }

      buckets.get(t).push({
        ...r.stock,
        close: c.close,
        pct: pct(
          c.close,
          r.prev
        )
      });
    }
  }

  for (
    const t of [
      ...buckets.keys()
    ].sort()
  ) {
    const g =
      [
        ...buckets.get(t)
      ]
        .sort(
          (a, b) =>
            b.pct - a.pct
        )
        .map(
          (x, i) => ({
            ...x,
            rank: i + 1
          })
        );

    const l =
      [
        ...buckets.get(t)
      ]
        .sort(
          (a, b) =>
            a.pct - b.pct
        )
        .map(
          (x, i) => ({
            ...x,
            rank: i + 1
          })
        );

    state.history.set(
      t,
      {
        gainers: g,
        losers: l
      }
    );

    await saveSnapshot(
      date,
      t,
      'gainers',
      g
    );

    await saveSnapshot(
      date,
      t,
      'losers',
      l
    );
  }

  log(
    `Historical ranking rebuild complete for ${date}: ${state.history.size} five-minute snapshots.`
  );
}

/* ---------------------------------------------------------
   FYERS WEBSOCKET MESSAGE PARSER
--------------------------------------------------------- */

function parseFyersMessage(msg) {
  if (!msg) {
    return null;
  }

  if (typeof msg === 'string') {
    try {
      msg = JSON.parse(msg);
    } catch {
      return null;
    }
  }

  if (Array.isArray(msg)) {
    return msg
      .map(parseFyersMessage)
      .filter(Boolean);
  }

  const symbol =
    msg.symbol ||
    msg.n ||
    msg.s;

  if (!symbol) {
    return null;
  }

  const ltp =
    Number(
      msg.ltp ??
      msg.lp ??
      msg.v?.lp ??
      msg.data?.ltp
    );

  const chp =
    Number(
      msg.chp ??
      msg.v?.chp ??
      msg.data?.chp
    );

  const prev =
    Number(
      msg.prev_close_price ??
      msg.v?.prev_close_price ??
      msg.data?.prev_close_price
    );

  if (!Number.isFinite(ltp)) {
    return null;
  }

  return {
    symbol,

    ltp,

    changePct:
      Number.isFinite(chp)
        ? chp
        : (
            prev
              ? ((ltp - prev) / prev) * 100
              : 0
          ),

    prev,

    ts:
      Number(
        msg.last_traded_time ??
        msg.tt ??
        msg.v?.tt ??
        Date.now() / 1000
      )
  };
}

/* ---------------------------------------------------------
   LIVE RANKING
--------------------------------------------------------- */

function liveRows() {
  return state.universe
    .map(s => {
      const q =
        state.live.get(
          s.symbol
        );

      return q
        ? {
            ...s,
            ltp: q.ltp,
            pct: q.changePct,
            volume:
              q.volume || 0,
            ts: q.ts
          }
        : null;
    })
    .filter(Boolean);
}

function rankedLive() {
  const rows =
    liveRows();

  return {
    gainers:
      [...rows]
        .sort(
          (a, b) =>
            b.pct - a.pct
        )
        .map(
          (x, i) => ({
            ...x,
            rank: i + 1
          })
        ),

    losers:
      [...rows]
        .sort(
          (a, b) =>
            a.pct - b.pct
        )
        .map(
          (x, i) => ({
            ...x,
            rank: i + 1
          })
        )
  };
}

/* ---------------------------------------------------------
   DISPLAY MEMBERSHIP STABILITY
--------------------------------------------------------- */

function smoothMembership(
  type,
  ranked
) {
  const old =
    state.membership[type] ||
    [];

  const top =
    ranked.slice(0, 30);

  const oldRows =
    old
      .map(
        k =>
          ranked.find(
            x => x.key === k
          )
      )
      .filter(Boolean);

  let result =
    [...oldRows];

  for (
    const n of top.filter(
      x =>
        !oldRows.some(
          y =>
            y.key === x.key
        )
    )
  ) {
    if (n.rank <= 20) {
      const worst =
        [...result].sort(
          (a, b) =>
            b.rank - a.rank
        )[0];

      if (
        !worst ||
        n.rank < worst.rank
      ) {
        if (worst) {
          result =
            result.filter(
              x =>
                x.key !==
                worst.key
            );
        }

        result.push(n);
      }
    }
  }

  for (
    const n of top
  ) {
    if (
      result.length < 30 &&
      !result.some(
        x =>
          x.key === n.key
      )
    ) {
      result.push(n);
    }
  }

  result.sort(
    (a, b) =>
      a.rank - b.rank
  );

  state.membership[type] =
    result.map(
      x => x.key
    );

  return result;
}

/* ---------------------------------------------------------
   MERGE HISTORICAL + CURRENT
--------------------------------------------------------- */

function mergeRows(
  type,
  liveRanked
) {
  const rows =
    smoothMembership(
      type,
      liveRanked
    );

  const base =
    state.history
      .get('09:20')
      ?.[type] ||
    [];

  const baseMap =
    new Map(
      base.map(
        x => [
          x.key,
          x.rank
        ]
      )
    );

  const times =
    [
      ...state.history.keys()
    ].sort();

  const rankMaps =
    new Map(
      times.map(
        t => [
          t,
          new Map(
            (
              state.history.get(t)
                ?.[type] ||
              []
            ).map(
              x => [
                x.key,
                x.rank
              ]
            )
          )
        ]
      )
    );

  return {
    times: [
      ...times,
      'CURRENT'
    ],

    rows:
      rows.map(x => ({
        key: x.key,
        name: x.name,
        sector: x.sector,
        pct: x.pct,
        rank: x.rank,

        rankDelta:
          baseMap.has(x.key)
            ? baseMap.get(x.key) -
              x.rank
            : null,

        baselineRank:
          baseMap.get(x.key) ??
          null,

        history:
          Object.fromEntries(
            times.map(
              t => [
                t,
                rankMaps
                  .get(t)
                  .get(x.key) ??
                null
              ]
            )
          )
      }))
  };
}

/* ---------------------------------------------------------
   SECTOR DATA
--------------------------------------------------------- */

function sectorData(live) {
  const map =
    new Map();

  for (const x of live) {
    const s =
      x.sector ||
      'Other F&O';

    if (!map.has(s)) {
      map.set(
        s,
        {
          sector: s,
          count: 0,
          sum: 0,
          positive: 0,
          stocks: []
        }
      );
    }

    const z =
      map.get(s);

    z.count++;
    z.sum += x.pct;

    if (x.pct > 0) {
      z.positive++;
    }

    z.stocks.push({
      key: x.key,
      name: x.name,
      pct: x.pct,
      ltp: x.ltp,
      sector: s
    });
  }

  return [
    ...map.values()
  ]
    .map(z => ({
      ...z,

      avgPct:
        z.count
          ? z.sum / z.count
          : 0,

      stocks:
        z.stocks.sort(
          (a, b) =>
            b.pct - a.pct
        )
    }))
    .sort(
      (a, b) =>
        b.avgPct - a.avgPct
    );
}

/* ---------------------------------------------------------
   CURRENT SNAPSHOT
--------------------------------------------------------- */

function snapshotAtCurrent() {
  const live =
    rankedLive();

  let displayLive =
    live;

  /*
   * Before 09:15, show the previous completed session.
   */

  if (
    state.displayMode ===
    'previous-close'
  ) {
    const times =
      [
        ...state.history.keys()
      ]
        .filter(
          t => t !== 'CLOSE'
        )
        .sort();

    const key =
      times.length
        ? times[times.length - 1]
        : 'CLOSE';

    const h =
      state.history.get(
        key
      );

    if (h) {
      displayLive = {
        gainers:
          (h.gainers || [])
            .map(
              x => ({
                ...x,
                pct: x.pct,
                rank: x.rank,
                ltp: x.close
              })
            ),

        losers:
          (h.losers || [])
            .map(
              x => ({
                ...x,
                pct: x.pct,
                rank: x.rank,
                ltp: x.close
              })
            )
      };
    }
  }

  /*
   * If live data isn't available yet but historical data exists,
   * use the most recent completed five-minute snapshot.
   */

  else if (
    !live.gainers.length &&
    state.history.size
  ) {
    const times =
      [
        ...state.history.keys()
      ]
        .filter(
          t => t !== 'CLOSE'
        )
        .sort();

    const h =
      state.history.get(
        times[
          times.length - 1
        ] || 'CLOSE'
      );

    if (h) {
      displayLive = {
        gainers:
          (h.gainers || [])
            .map(
              x => ({
                ...x,
                pct: x.pct,
                rank: x.rank,
                ltp: x.close
              })
            ),

        losers:
          (h.losers || [])
            .map(
              x => ({
                ...x,
                pct: x.pct,
                rank: x.rank,
                ltp: x.close
              })
            )
      };
    }
  }

  const g =
    mergeRows(
      'gainers',
      displayLive.gainers
    );

  const l =
    mergeRows(
      'losers',
      displayLive.losers
    );

  const times =
    [
      ...new Set([
        ...g.times,
        ...l.times
      ])
    ].sort(
      (a, b) =>
        a === 'CURRENT'
          ? 1
          : b === 'CURRENT'
          ? -1
          : a.localeCompare(b)
    );

  return {
    gainers: g.rows,
    losers: l.rows,

    times,

    currentTime:
      istParts()
        .time.slice(0, 5),

    sector:
      sectorData(
        displayLive.gainers.concat(
          displayLive.losers
        )
      )
  };
}

/* ---------------------------------------------------------
   5-MINUTE SNAPSHOT
--------------------------------------------------------- */

async function makeCandleSnapshot() {
  if (
    !marketOpenNow() ||
    !state.fyers.appId ||
    state.displayMode !==
      'current'
  ) {
    return;
  }

  const now =
    istParts();

  const minute =
    Number(
      now.time.slice(3, 5)
    );

  if (minute % 5 !== 0) {
    return;
  }

  const t =
    now.time.slice(0, 5);

  const date =
    now.date;

  if (
    t < '09:20' ||
    t > '15:25' ||
    state.history.has(t)
  ) {
    return;
  }

  const live =
    rankedLive();

  if (
    live.gainers.length <
    Math.max(
      50,
      Math.floor(
        state.universe.length *
          0.5
      )
    )
  ) {
    log(
      `Skipped ${t} snapshot: only ${live.gainers.length}/${state.universe.length} live symbols available.`,
      'warn'
    );

    return;
  }

  const g =
    live.gainers.map(
      x => ({
        ...x,
        close: x.ltp
      })
    );

  const l =
    live.losers.map(
      x => ({
        ...x,
        close: x.ltp
      })
    );

  state.history.set(
    t,
    {
      gainers: g,
      losers: l
    }
  );

  try {
    await saveSnapshot(
      date,
      t,
      'gainers',
      g
    );

    await saveSnapshot(
      date,
      t,
      'losers',
      l
    );

    log(
      `Saved ${t} 5-minute ranking snapshot.`
    );
  } catch (e) {
    log(
      `Snapshot DB error: ${e.message}`,
      'warn'
    );
  }

  broadcastDashboard();
}

/* ---------------------------------------------------------
   FYERS DATA WEBSOCKET
--------------------------------------------------------- */

function startFyersSocket() {
  if (
    !state.fyers.appId ||
    !state.fyers.token
  ) {
    return;
  }

  try {
    if (state.fyers.socket) {
      try {
        state.fyers.socket.close();
      } catch {}
    }
  } catch {}

  const auth =
    `${state.fyers.appId}:${state.fyers.token}`;

  const skt =
    fyersDataSocket.getInstance(
      auth,
      '',
      false
    );

  state.fyers.socket =
    skt;

  skt.on(
    'connect',
    () => {
      state.fyers.connected =
        true;

      log(
        'FYERS market-data WebSocket connected.'
      );

      const symbols = [
        ...state.universe.map(
          x => x.symbol
        ),
        ...INDEX_SYMBOLS.map(
          x => x[1]
        )
      ];

      skt.subscribe(
        symbols
      );
    }
  );

  skt.on(
    'message',
    msg => {
      const parsed =
        parseFyersMessage(
          msg
        );

      const arr =
        Array.isArray(parsed)
          ? parsed
          : [parsed];

      for (
        const q of arr.filter(
          Boolean
        )
      ) {
        if (
          INDEX_SYMBOLS.some(
            x =>
              x[1] === q.symbol
          )
        ) {
          state.indices.set(
            q.symbol,
            q
          );
        } else {
          state.live.set(
            q.symbol,
            q
          );
        }
      }

      if (arr.length) {
        broadcastDashboard();
      }
    }
  );

  skt.on(
    'error',
    e =>
      log(
        `FYERS WebSocket error: ${
          typeof e === 'string'
            ? e
            : JSON.stringify(e)
        }`,
        'error'
      )
  );

  skt.on(
    'close',
    () => {
      state.fyers.connected =
        false;

      log(
        'FYERS market-data WebSocket closed.',
        'warn'
      );
    }
  );

  skt.connect();
}

/* ---------------------------------------------------------
   BROWSER WEBSOCKET
--------------------------------------------------------- */

const browserSockets =
  new Set();

function broadcast(obj) {
  const text =
    JSON.stringify(obj);

  for (
    const ws of browserSockets
  ) {
    try {
      if (
        ws.readyState === 1
      ) {
        ws.send(text);
      }
    } catch {}
  }
}

let broadcastTimer =
  null;

function broadcastDashboard() {
  if (broadcastTimer) {
    return;
  }

  broadcastTimer =
    setTimeout(
      async () => {
        broadcastTimer =
          null;

        try {
          const d =
            await dashboardData();

          broadcast({
            type: 'dashboard',
            data: d
          });
        } catch (e) {
          log(
            `Broadcast dashboard error: ${e.message}`,
            'warn'
          );
        }
      },
      250
    );
}

/* ---------------------------------------------------------
   DASHBOARD DATA
--------------------------------------------------------- */

async function dashboardData() {
  const now =
    istParts();

  const preOpen =
    !regularSessionStarted();

  const postClose =
    regularSessionFinished();

  const snap =
    snapshotAtCurrent();

  const indices =
    INDEX_SYMBOLS.map(
      ([name, symbol]) => ({
        name,
        symbol,
        ...(
          state.indices.get(
            symbol
          ) || {
            ltp: null,
            changePct: null
          }
        )
      })
    );

  let mode =
    state.displayMode;

  let displayDate =
    state.displayDate ||
    istToday();

  let displayLabel =
    'Current trading session';

  /*
   * BEFORE MARKET OPEN
   * Show last completed trading session.
   */

  if (preOpen) {
    mode =
      'previous-close';

    displayLabel =
      'Last completed trading session (pre-open)';
  }

  /*
   * AFTER MARKET CLOSE
   * Keep today's completed session.
   */

  else if (postClose) {
    mode =
      'current';

    displayLabel =
      "Today's completed trading session";
  }

  /*
   * MARKET OPEN
   */

  else {
    mode =
      'current';

    displayLabel =
      "Today's live trading session";
  }

  return {
    date: displayDate,

    calendarDate:
      now.date,

    marketOpen:
      marketOpenNow(),

    sessionStarted:
      regularSessionStarted(),

    sessionFinished:
      regularSessionFinished(),

    displayMode: mode,

    displayLabel,

    updatedAt:
      new Date().toISOString(),

    indices,

    universeSize:
      state.universe.length,

    connected:
      state.fyers.connected,

    ...snap,

    logs:
      state.log.slice(-100)
  };
}

/* ---------------------------------------------------------
   HTTP HELPERS
--------------------------------------------------------- */

function readBody(req) {
  return new Promise(
    (resolve, reject) => {
      let b = '';

      req.on(
        'data',
        c => {
          b += c;

          if (
            b.length > 1e6
          ) {
            req.destroy();

            reject(
              new Error(
                'Body too large'
              )
            );
          }
        }
      );

      req.on(
        'end',
        () => {
          try {
            resolve(
              b
                ? JSON.parse(b)
                : {}
            );
          } catch (e) {
            reject(e);
          }
        }
      );

      req.on(
        'error',
        reject
      );
    }
  );
}

function cors(res) {
  res.setHeader(
    'Access-Control-Allow-Origin',
    '*'
  );

  res.setHeader(
    'Access-Control-Allow-Headers',
    'Content-Type, x-fyers-app-id, x-fyers-access-token'
  );

  res.setHeader(
    'Access-Control-Allow-Methods',
    'GET,POST,OPTIONS'
  );
}

function send(
  res,
  status,
  data,
  type = 'application/json'
) {
  cors(res);

  res.writeHead(
    status,
    {
      'Content-Type': type,
      'Cache-Control':
        'no-store'
    }
  );

  res.end(
    type ===
      'application/json'
      ? JSON.stringify(data)
      : data
  );
}

/* ---------------------------------------------------------
   HTTP ROUTER
--------------------------------------------------------- */

async function route(
  req,
  res
) {
  if (
    req.method ===
    'OPTIONS'
  ) {
    return send(
      res,
      204,
      ''
    );
  }

  const u =
    new URL(
      req.url,
      `http://${req.headers.host}`
    );

  try {
    /*
     * Health endpoint
     */

    if (
      req.method === 'GET' &&
      u.pathname ===
        '/api/health'
    ) {
      return send(
        res,
        200,
        {
          ok: true,

          time:
            istParts(),

          marketOpen:
            marketOpenNow(),

          universeSize:
            state.universe.length,

          wsConnected:
            state.fyers.connected,

          persistence:
            !!pool
        }
      );
    }

    /*
     * LOGIN
     */

    if (
      req.method === 'POST' &&
      u.pathname ===
        '/api/login'
    ) {
      const body =
        await readBody(req);

      const appId =
        String(
          body.appId || ''
        ).trim();

      const token =
        String(
          body.accessToken || ''
        ).trim();

      if (
        !appId ||
        !token
      ) {
        return send(
          res,
          400,
          {
            ok: false,
            error:
              'App ID and access token are required.'
          }
        );
      }

      /*
       * Validate FYERS credentials
       */

      await fyersGet(
        API_HOST,
        '/profile',
        {},
        appId,
        token
      );

      state.fyers.appId =
        appId;

      state.fyers.token =
        token;

      state.currentDay =
        '';

      state.displayDate =
        '';

      state.displayMode =
        'current';

      state.history.clear();

      state.live.clear();

      state.indices.clear();

      state.membership = {
        gainers: [],
        losers: []
      };

      await ensureUniverse();

      /*
       * Important:
       * This determines whether the dashboard should show:
       *
       * previous completed session
       * OR
       * current trading session
       */

      await rebuildHistoryFromFyers(
        appId,
        token
      );

      startFyersSocket();

      log(
        `FYERS login validated for ${appId.slice(0, 10)}…`
      );

      return send(
        res,
        200,
        {
          ok: true
        }
      );
    }

    const appId =
      req.headers[
        'x-fyers-app-id'
      ];

    const token =
      req.headers[
        'x-fyers-access-token'
      ];

    if (
      !appId ||
      !token ||
      appId !==
        state.fyers.appId ||
      token !==
        state.fyers.token
    ) {
      return send(
        res,
        401,
        {
          ok: false,
          error:
            'Not authenticated.'
        }
      );
    }

    /*
     * LOGOUT
     */

    if (
      req.method === 'POST' &&
      u.pathname ===
        '/api/logout'
    ) {
      try {
        state.fyers.socket?.close();
      } catch {}

      state.fyers = {
        appId: '',
        token: '',
        socket: null,
        connected: false,
        reconnectTimer: null
      };

      state.live.clear();
      state.indices.clear();

      return send(
        res,
        200,
        {
          ok: true
        }
      );
    }

    /*
     * DASHBOARD
     */

    if (
      req.method === 'GET' &&
      u.pathname ===
        '/api/dashboard'
    ) {
      return send(
        res,
        200,
        await dashboardData()
      );
    }

    /*
     * LOGS
     */

    if (
      req.method === 'GET' &&
      u.pathname ===
        '/api/logs'
    ) {
      return send(
        res,
        200,
        {
          logs:
            state.log.slice(
              -100
            )
        }
      );
    }

    return send(
      res,
      404,
      {
        ok: false,
        error: 'Not found'
      }
    );
  } catch (e) {
    log(
      e.message,
      'error'
    );

    return send(
      res,
      500,
      {
        ok: false,
        error: e.message,
        logs:
          state.log.slice(
            -30
          )
      }
    );
  }
}

/* ---------------------------------------------------------
   HTTP SERVER
--------------------------------------------------------- */

const htmlPath =
  path.join(
    __dirname,
    'index.html'
  );

const server =
  http.createServer(
    (req, res) => {
      const u =
        new URL(
          req.url,
          `http://${req.headers.host}`
        );

      if (
        u.pathname.startsWith(
          '/api/'
        )
      ) {
        return route(
          req,
          res
        );
      }

      if (
        req.method === 'GET' &&
        (
          u.pathname === '/' ||
          u.pathname ===
            '/index.html'
        )
      ) {
        const html =
          fs.readFileSync(
            htmlPath
          );

        cors(res);

        res.writeHead(
          200,
          {
            'Content-Type':
              'text/html; charset=utf-8',
            'Cache-Control':
              'no-store'
          }
        );

        return res.end(
          html
        );
      }

      send(
        res,
        404,
        'Not found',
        'text/plain'
      );
    }
  );

/* ---------------------------------------------------------
   BROWSER WEBSOCKET SERVER
--------------------------------------------------------- */

const {
  WebSocketServer
} = require('ws');

const wss =
  new WebSocketServer({
    noServer: true
  });

wss.on(
  'connection',
  async ws => {
    browserSockets.add(
      ws
    );

    try {
      ws.send(
        JSON.stringify({
          type:
            'dashboard',

          data:
            await dashboardData()
        })
      );
    } catch (e) {
      log(
        `Browser WebSocket initial send: ${e.message}`,
        'warn'
      );
    }

    ws.on(
      'close',
      () =>
        browserSockets.delete(
          ws
        )
    );
  }
);

server.on(
  'upgrade',
  (
    req,
    socket,
    head
  ) => {
    const u =
      new URL(
        req.url,
        `http://${req.headers.host}`
      );

    if (
      u.pathname !==
      '/ws'
    ) {
      socket.destroy();
      return;
    }

    wss.handleUpgrade(
      req,
      socket,
      head,
      ws =>
        wss.emit(
          'connection',
          ws,
          req
        )
    );
  }
);

/* ---------------------------------------------------------
   SESSION MONITOR
--------------------------------------------------------- */

setInterval(
  async () => {
    if (
      !state.fyers.appId
    ) {
      return;
    }

    const today =
      istToday();

    try {
      /*
       * BEFORE MARKET OPEN
       *
       * Show last completed trading day.
       */

      if (
        !regularSessionStarted()
      ) {
        if (
          state.displayDate !==
            today ||
          state.displayMode !==
            'previous-close'
        ) {
          await loadLatestCompletedSession(
            state.fyers.appId,
            state.fyers.token
          );

          broadcastDashboard();
        }

        return;
      }

      /*
       * MARKET HAS STARTED
       *
       * Clear previous displayed session and initialize
       * the new current trading day.
       */

      if (
        state.currentDay !==
          today ||
        state.displayMode !==
          'current'
      ) {
        resetSession(
          today
        );

        await rebuildHistoryFromFyers(
          state.fyers.appId,
          state.fyers.token
        );

        log(
          `New trading session ${today} started: previous-session display cleared; current-day session is now active.`
        );

        broadcastDashboard();
      }
    } catch (e) {
      log(
        `Session/date rollover error: ${e.message}`,
        'warn'
      );
    }
  },
  5000
);

/* ---------------------------------------------------------
   5-MINUTE SNAPSHOT TIMER
--------------------------------------------------------- */

setInterval(
  () => {
    makeCandleSnapshot()
      .catch(
        e =>
          log(
            `Snapshot timer: ${e.message}`,
            'warn'
          )
      );
  },
  15000
);

/* ---------------------------------------------------------
   DASHBOARD BROADCAST
--------------------------------------------------------- */

setInterval(
  () => {
    if (
      marketOpenNow() &&
      state.fyers.connected
    ) {
      broadcastDashboard();
    }
  },
  5000
);

/* ---------------------------------------------------------
   START
--------------------------------------------------------- */

(async () => {
  try {
    await initDb();

    await ensureUniverse();

    log(
      `Dashboard server listening on ${HOST}:${PORT}`
    );

    server.listen(
      PORT,
      HOST
    );
  } catch (e) {
    console.error(e);
    process.exit(1);
  }
})();
