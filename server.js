const http = require("http");
const https = require("https");
const fs = require("fs");
const path = require("path");

const RAW_SYMBOLS = [
  "PATANJALI","POLICYBZR","MFSL","PAYTM","HDFCLIFE","ICICIPRULI","SBILIFE","RADICO",
  "SBIN","MARICO","ITC","OBEROIRLTY","UNOMINDA","COLPAL","LODHA","IOC","UNIONBANK",
  "PNB","MUTHOOTFIN","TIINDIA","NESTLEIND","GLENMARK","JSWSTEEL","BPCL","NATIONALUM",
  "PIDILITIND","BAJAJ-AUTO","AXISBANK","HINDALCO","TRENT","YESBANK","BLUESTARCO",
  "TATACONSUM","UNITDSPR","HINDPETRO","CUMMINSIND","RECLTD","HEROMOTOCO","M&M",
  "KOTAKBANK","HINDUNILVR","ADANIGREEN","MCX","HINDZINC","PHOENIXLTD","PGEL","DMART",
  "TITAN","JUBLFOOD","BRITANNIA","CONCOR","BOSCHLTD","LICI","SRF","SUNPHARMA",
  "GODFRYPHLP","BANKBARODA","NBCC","VMM","ICICIGI","MAHABANK","FORCEMOT","ZYDUSLIFE",
  "BEL","JINDALSTEL","COALINDIA","HYUNDAI","GODREJCP","HDFCBANK","CANBK","ABCAPITAL",
  "EICHERMOT","ICICIBANK","ADANIPORTS","PFC","ASIANPAINT","ETERNAL","CIPLA","AUBANK",
  "GRASIM","PRESTIGE","GVT&D","KFINTECH","BHARATFORG","OIL","INDIANB","RELIANCE",
  "GODREJPROP","ONGC","MANAPPURAM","SAIL","DLF","DELHIVERY","ALKEM","HAVELLS",
  "BHARTIARTL","IDFCFIRSTB","ASTRAL","VBL","INOXWIND","LICHSGFIN","ADANIPOWER",
  "BANKINDIA","KEI","ADANIENSOL","RBLBANK","BDL","MOTHERSON","PIIND","ULTRACEMCO",
  "APLAPOLLO","BANDHANBNK","DIXON","HAL","SUPREMEIND","POWERGRID","IEX","ATHERENERG",
  "HCLTECH","JSWENERGY","SHRIRAMFIN","SBICARD","ASHOKLEY","GAIL","MAZDOCK","AMBUJACEM",
  "APOLLOHOSP","KAYNES","INDHOTEL","NMDC","IRFC","POLYCAB","ADANIENT","BAJFINANCE",
  "JIOFIN","FEDERALBNK","TATASTEEL","ABB","VEDL","TVSMOTOR","DIVISLAB","NAUKRI",
  "DRREDDY","LTF","COCHINSHIP","MAXHEALTH","SHREECEM","COFORGE","MARUTI","VOLTAS",
  "BHEL","IREDA","GMRAIRPORT","360ONE","PETRONET","PERSISTENT","TATAPOWER","SIEMENS",
  "CGPOWER","DABUR","RVNL","NTPC","TORNTPHARM","NHPC","TMPV","PAGEIND","LUPIN",
  "INDIGO","INDUSTOWER","CDSL","PNBHOUSING","AMBER","BAJAJFINSV","AUROPHARMA",
  "INDUSINDBK","LAURUSLABS","FORTIS","LT","TECHM","CROMPTON","SAGILITY","IDEA",
  "ANGELONE","KPITTECH","MOTILALOFS","CHOLAFIN","HDFCAMC","MANKIND","BAJAJHLDNG",
  "INFY","CAMS","SWIGGY","SUZLON","SOLARINDS","UPL","WIPRO","POWERINDIA","TATAELXSI",
  "BIOCON","BSE","WAAREEENER","OFSS","NAM-INDIA","MPHASIS","LTM","TCS","SONACOMS",
  "NYKAA","KALYANKJIL","PREMIERENE"
];

const FNO_SYMBOLS = RAW_SYMBOLS.map(s => `NSE:${s}-EQ`);
const INDEX_SYMBOLS = ["NSE:NIFTY50-INDEX", "NSE:NIFTYBANK-INDEX"];

const SECTOR_MAP = {
  "RELIANCE":"Energy","ONGC":"Energy","BPCL":"Energy","IOC":"Energy","GAIL":"Energy",
  "PETRONET":"Energy","HINDPETRO":"Energy","ADANIGREEN":"Energy","ADANIENSOL":"Energy",
  "JSWENERGY":"Energy","TATAPOWER":"Power","NTPC":"Power","POWERGRID":"Power","NHPC":"Power",
  "IREDA":"Power","SUZLON":"Power","WAAREEENER":"Power","POWERINDIA":"Power",
  "TCS":"IT","INFY":"IT","HCLTECH":"IT","WIPRO":"IT","TECHM":"IT","COFORGE":"IT",
  "PERSISTENT":"IT","MPHASIS":"IT","TATAELXSI":"IT","OFSS":"IT","LTM":"IT","KPITTECH":"IT",
  "HDFCBANK":"Bank","ICICIBANK":"Bank","SBIN":"Bank","KOTAKBANK":"Bank","AXISBANK":"Bank",
  "INDUSINDBK":"Bank","BANKBARODA":"Bank","PNB":"Bank","CANBK":"Bank","FEDERALBNK":"Bank",
  "IDFCFIRSTB":"Bank","AUBANK":"Bank","BANDHANBNK":"Bank","BANKINDIA":"Bank","RBLBANK":"Bank",
  "YESBANK":"Bank","UNIONBANK":"Bank","INDIANB":"Bank","MAHABANK":"Bank",
  "BAJFINANCE":"Finance","BAJAJFINSV":"Finance","CHOLAFIN":"Finance","MUTHOOTFIN":"Finance",
  "PFC":"Finance","RECLTD":"Finance","MANAPPURAM":"Finance","LTF":"Finance","JIOFIN":"Finance",
  "IRFC":"Finance","SBICARD":"Finance","SHRIRAMFIN":"Finance","LICHSGFIN":"Finance",
  "PNBHOUSING":"Finance","ABCAPITAL":"Finance","ANGELONE":"Finance","MOTILALOFS":"Finance",
  "HDFCAMC":"Finance","BAJAJHLDNG":"Finance","MFSL":"Finance",
  "HDFCLIFE":"Insurance","SBILIFE":"Insurance","ICICIPRULI":"Insurance","ICICIGI":"Insurance","LICI":"Insurance",
  "MARUTI":"Auto","M&M":"Auto","EICHERMOT":"Auto","HEROMOTOCO":"Auto","BAJAJ-AUTO":"Auto",
  "TVSMOTOR":"Auto","ASHOKLEY":"Auto","MOTHERSON":"Auto","BHARATFORG":"Auto","SONACOMS":"Auto",
  "UNOMINDA":"Auto","HYUNDAI":"Auto","FORCEMOT":"Auto","TIINDIA":"Auto",
  "SUNPHARMA":"Pharma","DRREDDY":"Pharma","CIPLA":"Pharma","DIVISLAB":"Pharma","APOLLOHOSP":"Pharma",
  "LUPIN":"Pharma","AUROPHARMA":"Pharma","BIOCON":"Pharma","ALKEM":"Pharma","TORNTPHARM":"Pharma",
  "GLENMARK":"Pharma","LAURUSLABS":"Pharma","ZYDUSLIFE":"Pharma","MANKIND":"Pharma","FORTIS":"Pharma","MAXHEALTH":"Pharma",
  "TATASTEEL":"Metal","JSWSTEEL":"Metal","HINDALCO":"Metal","SAIL":"Metal","VEDL":"Metal",
  "NMDC":"Metal","COALINDIA":"Metal","NATIONALUM":"Metal","HINDZINC":"Metal","JINDALSTEL":"Metal",
  "ASIANPAINT":"Consumer","HINDUNILVR":"Consumer","ITC":"Consumer","NESTLEIND":"Consumer",
  "BRITANNIA":"Consumer","GODREJCP":"Consumer","DABUR":"Consumer","MARICO":"Consumer",
  "COLPAL":"Consumer","TATACONSUM":"Consumer","VBL":"Consumer","UNITDSPR":"Consumer",
  "JUBLFOOD":"Consumer","PATANJALI":"Consumer","RADICO":"Consumer","GODFRYPHLP":"Consumer",
  "LT":"Infra","ULTRACEMCO":"Infra","GRASIM":"Infra","ADANIPORTS":"Infra","ADANIENT":"Infra",
  "AMBUJACEM":"Infra","SHREECEM":"Infra","NBCC":"Infra","RVNL":"Infra","GMRAIRPORT":"Infra",
  "DLF":"Realty","GODREJPROP":"Realty","OBEROIRLTY":"Realty","PRESTIGE":"Realty","LODHA":"Realty","PHOENIXLTD":"Realty",
  "BHARTIARTL":"Telecom","IDEA":"Telecom","INDUSTOWER":"Telecom",
  "BEL":"Defence","HAL":"Defence","BDL":"Defence","MAZDOCK":"Defence","BHEL":"Capital Goods",
  "SIEMENS":"Capital Goods","ABB":"Capital Goods","CGPOWER":"Capital Goods","CUMMINSIND":"Capital Goods",
  "HAVELLS":"Consumer Durables","VOLTAS":"Consumer Durables","BLUESTARCO":"Consumer Durables",
  "DIXON":"Consumer Durables","AMBER":"Consumer Durables","CROMPTON":"Consumer Durables","PGEL":"Consumer Durables",
  "IRCTC":"Services","CONCOR":"Services","INDIGO":"Services","INDHOTEL":"Services","DELHIVERY":"Services",
  "ZOMATO":"Services","NYKAA":"Services","PAYTM":"Services","NAUKRI":"Services","POLICYBZR":"Services",
  "SWIGGY":"Services","DMART":"Retail","TRENT":"Retail","VMM":"Retail","KALYANKJIL":"Retail",
  "CDSL":"Finance","CAMS":"Finance","MCX":"Finance","BSE":"Finance","IEX":"Power","KEI":"Capital Goods",
  "POLYCAB":"Capital Goods","KAYNES":"Capital Goods","SOLARINDS":"Chemicals","UPL":"Chemicals",
  "PIDILITIND":"Chemicals","SRF":"Chemicals","PIIND":"Chemicals","APLAPOLLO":"Metal","SUPREMEIND":"Chemicals",
  "ASTRAL":"Chemicals","INOXWIND":"Power","OIL":"Energy","ADANIPOWER":"Power","ETERNAL":"Others",
  "GVT&D":"Others","KFINTECH":"Finance","SAGILITY":"IT","NAM-INDIA":"Finance","PREMIERENE":"Power",
  "TMPV":"Auto","360ONE":"Finance","COCHINSHIP":"Defence","ATHERENERG":"Auto"
};

let rankHistory = {};
let lastSnapshot = 0;
let backfillDone = false;
let backfillInProgress = false;
let lastBackfillAttempt = 0;
let currentDay = null;
const SNAPSHOT_MS = 5 * 60 * 1000;
const BACKFILL_RETRY_MS = 30 * 1000;

// ---------- Logging (surfaced to the dashboard via /api/logs) ----------
let logs = [];
function log(level, msg) {
  const entry = { ts: Date.now(), level, msg: String(msg) };
  logs.push(entry);
  if (logs.length > 500) logs.shift();
  if (level === "error") console.error(msg); else console.log(msg);
}

// ---------- Networking ----------
// "&" inside a symbol name (M&M, GVT&D) breaks the query string if left raw,
// since it would be read as a parameter separator. Escape it explicitly.
function safeSymbolList(symbols) {
  return symbols.map(s => s.replace(/&/g, "%26")).join(",");
}

function httpsGet(auth, urlPath) {
  return new Promise((resolve) => {
    const opts = {
      hostname: "api-t1.fyers.in",
      path: urlPath,
      method: "GET",
      headers: { Authorization: auth }
    };
    const req = https.request(opts, res => {
      let d = "";
      res.on("data", c => d += c);
      res.on("end", () => {
        try {
          const parsed = JSON.parse(d);
          resolve({ ok: true, status: res.statusCode, body: parsed });
        } catch (e) {
          log("error", `Fyers response was not valid JSON (HTTP ${res.statusCode}) for ${urlPath.split("?")[0]}: ${d.slice(0, 200)}`);
          resolve({ ok: false, status: res.statusCode, body: null, err: "bad_json" });
        }
      });
    });
    req.on("error", (e) => {
      log("error", `Network error calling Fyers ${urlPath.split("?")[0]}: ${e.message}`);
      resolve({ ok: false, status: 0, body: null, err: e.message });
    });
    req.setTimeout(12000, () => {
      req.destroy();
      log("error", `Timeout (12s) calling Fyers ${urlPath.split("?")[0]}`);
      resolve({ ok: false, status: 0, body: null, err: "timeout" });
    });
    req.end();
  });
}

/** Returns { data:[...], error: null|string } — never throws, but never silently
 *  swallows a total failure either. */
function fetchQuotes(auth, symbols) {
  return new Promise(async (resolve) => {
    const results = [];
    let lastError = null;
    let failedBatches = 0;
    const totalBatches = Math.ceil(symbols.length / 50);

    for (let i = 0; i < symbols.length; i += 50) {
      const batch = symbols.slice(i, i + 50);
      const r = await httpsGet(auth, `/data/quotes?symbols=${safeSymbolList(batch)}`);
      if (r.ok && r.body && r.body.s === "ok" && Array.isArray(r.body.d)) {
        results.push(...r.body.d);
      } else {
        failedBatches++;
        const fyersMsg = r.body && (r.body.message || r.body.s);
        lastError = fyersMsg || r.err || `HTTP ${r.status}`;
        log("error", `Quotes batch failed (${batch.length} symbols): ${lastError}`);
      }
    }
    if (failedBatches > 0) {
      log("error", `${failedBatches}/${totalBatches} quote batch(es) failed. Last error: ${lastError}`);
    }
    resolve({ data: results, error: failedBatches === totalBatches ? lastError : null });
  });
}

/** Last trading day in IST (handles weekends & pre-open) */
function getHistoryDate() {
  const istStr = new Date().toLocaleString("en-US", { timeZone: "Asia/Kolkata" });
  const ist = new Date(istStr);
  let d = new Date(ist);
  const day = d.getDay();   // 0=Sun … 6=Sat
  const hour = d.getHours();
  const min = d.getMinutes();

  if (day === 0) d.setDate(d.getDate() - 2);          // Sun → Fri
  else if (day === 6) d.setDate(d.getDate() - 1);     // Sat → Fri
  else if (hour < 9 || (hour === 9 && min < 15)) {
    d.setDate(d.getDate() - (day === 1 ? 3 : 1));     // Mon morning → Fri
  }
  return d.toLocaleDateString("en-CA", { timeZone: "Asia/Kolkata" }); // YYYY-MM-DD
}

function epochToTimeLabel(epochSec) {
  return new Date(epochSec * 1000).toLocaleTimeString("en-IN", {
    hour: "2-digit", minute: "2-digit", hour12: false, timeZone: "Asia/Kolkata"
  });
}

async function fetchHistory5(auth, symbol, day) {
  const pathStr =
    `/data/history?symbol=${encodeURIComponent(symbol)}&resolution=5&date_format=1&range_from=${day}&range_to=${day}&cont_flag=1`;
  const r = await httpsGet(auth, pathStr);
  if (!r.ok || !r.body || r.body.s !== "ok" || !Array.isArray(r.body.candles)) {
    return { candles: [], error: r.body && r.body.message ? r.body.message : (r.err || null) };
  }
  return { candles: r.body.candles.map(c => ({ epoch: c[0], close: c[4] })), error: null };
}

async function backfillRanks(auth, prevCloseMap) {
  if (backfillDone || backfillInProgress) return;
  backfillInProgress = true;
  const day = getHistoryDate();
  log("info", `Backfill start for date: ${day}`);

  try {
    const CONCURRENCY = 8;
    const histBySymbol = {};
    let failCount = 0;
    let lastErr = null;

    for (let i = 0; i < FNO_SYMBOLS.length; i += CONCURRENCY) {
      const batch = FNO_SYMBOLS.slice(i, i + CONCURRENCY);
      await Promise.all(batch.map(async sym => {
        const name = sym.replace("NSE:", "").replace("-EQ", "");
        const { candles, error } = await fetchHistory5(auth, sym, day);
        if (candles.length) histBySymbol[name] = candles;
        else if (error) { failCount++; lastErr = error; }
      }));
    }

    if (Object.keys(histBySymbol).length === 0) {
      // Nothing usable came back — most likely an invalid/expired token, or
      // history genuinely isn't available yet (e.g. market just opened).
      // Do NOT mark backfillDone: let it retry on a later request once the
      // token is fixed, instead of getting stuck empty for the whole process.
      log("error", `Backfill got zero usable symbols (${failCount} failed). Last error: ${lastErr || "unknown"}. Will retry.`);
      return;
    }

    const epochSet = new Set();
    Object.values(histBySymbol).forEach(arr => arr.forEach(c => epochSet.add(c.epoch)));
    const epochs = Array.from(epochSet).sort((a, b) => a - b);
    log("info", `Backfill candles found for ${Object.keys(histBySymbol).length}/${FNO_SYMBOLS.length} symbols, unique times: ${epochs.length}`);

    epochs.forEach(epoch => {
      const t = epochToTimeLabel(epoch);
      const rows = [];
      Object.keys(histBySymbol).forEach(name => {
        const candle = histBySymbol[name].find(c => c.epoch === epoch);
        if (!candle) return;
        const prev = prevCloseMap[name];
        if (!prev || prev <= 0) return;
        const chp = ((candle.close - prev) / prev) * 100;
        rows.push({ name, chp });
      });
      if (rows.length < 5) return;

      rows.sort((a, b) => b.chp - a.chp);
      const gainRank = {};
      rows.forEach((r, i) => { gainRank[r.name] = i + 1; });

      rows.sort((a, b) => a.chp - b.chp);
      const lossRank = {};
      rows.forEach((r, i) => { lossRank[r.name] = i + 1; });

      Object.keys(gainRank).forEach(name => {
        if (!rankHistory[name]) rankHistory[name] = [];
        if (!rankHistory[name].some(x => x.t === t)) {
          rankHistory[name].push({ t, g: gainRank[name], l: lossRank[name] });
        }
      });
    });

    Object.keys(rankHistory).forEach(name => {
      rankHistory[name].sort((a, b) => a.t.localeCompare(b.t));
    });

    backfillDone = true;
    log("info", `Backfill done. Sample times: ${JSON.stringify(Object.values(rankHistory)[0]?.map(x => x.t).slice(0, 5))}`);
  } catch (e) {
    log("error", `Backfill crashed: ${e.message}`);
  } finally {
    backfillInProgress = false;
  }
}

function processData(quotes, indexQuotes) {
  const now = Date.now();
  const stocks = [];
  const prevCloseMap = {};

  quotes.forEach(q => {
    const v = q.v || {};
    const name = (q.n || "").replace("NSE:", "").replace("-EQ", "");
    const ltp = Number(v.lp) || 0;
    const prev = Number(v.prev_close_price) || 0;
    if (ltp <= 0) return;
    prevCloseMap[name] = prev;
    stocks.push({
      symbol: q.n,
      name,
      ltp: +ltp.toFixed(2),
      ch: +(Number(v.ch) || 0).toFixed(2),
      chp: +(Number(v.chp) || 0).toFixed(2),
      sector: SECTOR_MAP[name] || "Others"
    });
  });

  const byGain = [...stocks].sort((a, b) => b.chp - a.chp);
  const byLoss = [...stocks].sort((a, b) => a.chp - b.chp);
  byGain.forEach((s, i) => (s.rankG = i + 1));
  byLoss.forEach((s, i) => (s.rankL = i + 1));

  const tNow = new Date().toLocaleTimeString("en-IN", {
    hour: "2-digit", minute: "2-digit", hour12: false, timeZone: "Asia/Kolkata"
  });
  if (now - lastSnapshot >= SNAPSHOT_MS || lastSnapshot === 0) {
    lastSnapshot = now;
    stocks.forEach(s => {
      if (!rankHistory[s.name]) rankHistory[s.name] = [];
      if (!rankHistory[s.name].some(x => x.t === tNow)) {
        rankHistory[s.name].push({ t: tNow, g: s.rankG, l: s.rankL });
      }
    });
  }

  stocks.forEach(s => {
    const h = rankHistory[s.name] || [];
    s.hist = h;
    s.diffG = h.length ? h[0].g - s.rankG : 0;
    s.diffL = h.length ? h[0].l - s.rankL : 0;
  });

  const advances = stocks.filter(s => s.chp > 0).length;
  const declines = stocks.filter(s => s.chp < 0).length;

  const sec = {};
  stocks.forEach(s => {
    if (!sec[s.sector]) sec[s.sector] = { sum: 0, n: 0, list: [] };
    sec[s.sector].sum += s.chp;
    sec[s.sector].n++;
    sec[s.sector].list.push(s);
  });
  const sectors = Object.entries(sec)
    .map(([name, v]) => ({
      name,
      avg: +(v.sum / v.n).toFixed(2),
      stocks: v.list.sort((a, b) => b.chp - a.chp)
    }))
    .sort((a, b) => {
      if (a.avg >= 0 && b.avg >= 0) return b.avg - a.avg;
      if (a.avg < 0 && b.avg < 0) return a.avg - b.avg;
      return b.avg - a.avg;
    });

  let nifty = { lp: 0, chp: 0 };
  let banknifty = { lp: 0, chp: 0 };
  (indexQuotes || []).forEach(q => {
    const v = q.v || {};
    if ((q.n || "").includes("NIFTY50")) nifty = { lp: +(Number(v.lp) || 0).toFixed(1), chp: +(Number(v.chp) || 0).toFixed(2) };
    if ((q.n || "").includes("NIFTYBANK")) banknifty = { lp: +(Number(v.lp) || 0).toFixed(1), chp: +(Number(v.chp) || 0).toFixed(2) };
  });

  const timeSet = new Set();
  Object.values(rankHistory).forEach(arr => arr.forEach(x => timeSet.add(x.t)));
  const allTimes = Array.from(timeSet).sort();

  return {
    nifty, banknifty,
    advances, declines,
    total: stocks.length,
    gainers: byGain.slice(0, 30),
    losers: byLoss.slice(0, 30),
    sectors,
    times: allTimes,
    backfillDone,
    backfillInProgress,
    historyDate: getHistoryDate()
  };
}

const server = http.createServer(async (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Headers", "Authorization, Content-Type");
  if (req.method === "OPTIONS") { res.writeHead(204); return res.end(); }

  if (req.url === "/" || req.url === "/index.html") {
    res.writeHead(200, { "Content-Type": "text/html" });
    return res.end(fs.readFileSync(path.join(__dirname, "index.html")));
  }

  if (req.url.startsWith("/api/logs")) {
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({ logs: logs.slice(-200) }));
  }

  if (req.url.startsWith("/api/dashboard")) {
    const auth = req.headers.authorization;
    if (!auth) {
      res.writeHead(401, { "Content-Type": "application/json" });
      return res.end(JSON.stringify({ error: "No token" }));
    }

    try {
      // Reset state at the start of a new trading session so old-day rank
      // history / a stuck backfill flag don't bleed into the new day.
      const today = getHistoryDate();
      if (currentDay !== null && currentDay !== today) {
        log("info", `New trading day detected (${currentDay} -> ${today}). Resetting history.`);
        rankHistory = {};
        backfillDone = false;
        backfillInProgress = false;
        lastSnapshot = 0;
      }
      currentDay = today;

      const [quotesResult, indexResult] = await Promise.all([
        fetchQuotes(auth, FNO_SYMBOLS),
        fetchQuotes(auth, INDEX_SYMBOLS)
      ]);
      const stockQuotes = quotesResult.data;
      const indexQuotes = indexResult.data;

      // If EVERY batch failed, this isn't "market is flat" — it's a real
      // failure (expired/invalid token, Fyers outage, etc). Surface it
      // instead of returning an empty-but-"successful" payload.
      if (stockQuotes.length === 0 && FNO_SYMBOLS.length > 0) {
        const reason = quotesResult.error || indexResult.error || "Fyers API returned no data";
        const msg = `Fyers API error: ${reason}. If this mentions a token/auth issue, your access token has likely expired — Fyers tokens are valid for one trading day only. Generate a fresh one and log in again.`;
        log("error", msg);
        res.writeHead(200, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ error: msg }));
      }

      const prevCloseMap = {};
      stockQuotes.forEach(q => {
        const name = (q.n || "").replace("NSE:", "").replace("-EQ", "");
        prevCloseMap[name] = Number(q.v?.prev_close_price) || 0;
      });

      // Fire-and-forget: don't block this response on backfill. The client
      // already polls and shows backfillInProgress/backfillDone.
      if (!backfillDone && !backfillInProgress && Date.now() - lastBackfillAttempt > BACKFILL_RETRY_MS) {
        lastBackfillAttempt = Date.now();
        backfillRanks(auth, prevCloseMap).catch(e => log("error", `Backfill promise rejected: ${e.message}`));
      }

      const data = processData(stockQuotes, indexQuotes);
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(data));
    } catch (e) {
      log("error", `/api/dashboard crashed: ${e.message}`);
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: e.message }));
    }
    return;
  }
  res.writeHead(404); res.end("Not found");
});

const port = process.env.PORT || 3000;
server.listen(port, () => log("info", `Dashboard on port ${port}`));
