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
const SNAPSHOT_MS = 5 * 60 * 1000;

function getISTNow() {
  return new Date(new Date().toLocaleString("en-US", { timeZone: "Asia/Kolkata" }));
}

function isMarketOpen() {
  const d = getISTNow();
  const day = d.getDay();
  if (day === 0 || day === 6) return false;
  const mins = d.getHours() * 60 + d.getMinutes();
  return mins >= (9 * 60 + 15) && mins <= (15 * 60 + 40);
}

function getHistoryDate() {
  const d = getISTNow();
  let dt = new Date(d);
  const day = dt.getDay();
  const mins = dt.getHours() * 60 + dt.getMinutes();
  if (day === 0) dt.setDate(dt.getDate() - 2);
  else if (day === 6) dt.setDate(dt.getDate() - 1);
  else if (mins < 9 * 60 + 15) dt.setDate(dt.getDate() - (day === 1 ? 3 : 1));
  return dt.toLocaleDateString("en-CA", { timeZone: "Asia/Kolkata" });
}

function epochToTimeLabel(epochSec) {
  return new Date(epochSec * 1000).toLocaleTimeString("en-IN", {
    hour: "2-digit", minute: "2-digit", hour12: false, timeZone: "Asia/Kolkata"
  });
}

function httpsGet(auth, urlPath) {
  return new Promise((resolve) => {
    const req = https.request({
      hostname: "api-t1.fyers.in",
      path: urlPath,
      method: "GET",
      headers: { Authorization: auth }
    }, res => {
      let d = "";
      res.on("data", c => d += c);
      res.on("end", () => {
        try { resolve(JSON.parse(d)); } catch (e) { resolve(null); }
      });
    });
    req.on("error", () => resolve(null));
    req.setTimeout(12000, () => { req.destroy(); resolve(null); });
    req.end();
  });
}

async function fetchQuotes(auth, symbols) {
  const results = [];
  for (let i = 0; i < symbols.length; i += 50) {
    const batch = symbols.slice(i, i + 50);
    const j = await httpsGet(auth, `/data/quotes?symbols=${batch.join(",")}`);
    if (j && j.s === "ok" && Array.isArray(j.d)) results.push(...j.d);
  }
  return results;
}

async function fetchHistory5(auth, symbol, day) {
  const p = `/data/history?symbol=${encodeURIComponent(symbol)}&resolution=5&date_format=1&range_from=${day}&range_to=${day}&cont_flag=1`;
  const j = await httpsGet(auth, p);
  if (!j || j.s !== "ok" || !Array.isArray(j.candles)) return [];
  return j.candles.map(c => ({ epoch: c[0], close: c[4] }));
}

// Background only — never blocks the HTTP response
async function backfillRanks(auth, prevCloseMap) {
  if (backfillDone || backfillInProgress) return;
  backfillInProgress = true;
  const day = getHistoryDate();
  console.log("Background backfill start:", day);

  try {
    const CONCURRENCY = 6;
    const histBySymbol = {};

    for (let i = 0; i < FNO_SYMBOLS.length; i += CONCURRENCY) {
      const batch = FNO_SYMBOLS.slice(i, i + CONCURRENCY);
      await Promise.all(batch.map(async sym => {
        const name = sym.replace("NSE:", "").replace("-EQ", "");
        const candles = await fetchHistory5(auth, sym, day);
        if (candles.length) histBySymbol[name] = candles;
      }));
    }

    const epochSet = new Set();
    Object.values(histBySymbol).forEach(arr => arr.forEach(c => epochSet.add(c.epoch)));
    const epochs = Array.from(epochSet).sort((a, b) => a - b);
    console.log("Bars:", epochs.length, "symbols:", Object.keys(histBySymbol).length);

    if (epochs.length === 0) {
      console.log("No history candles — skip");
      backfillInProgress = false;
      return;
    }

    // Build into a temporary object, then replace (never leave empty)
    const newHistory = {};

    epochs.forEach(epoch => {
      const t = epochToTimeLabel(epoch);
      const rows = [];
      Object.keys(histBySymbol).forEach(name => {
        const candle = histBySymbol[name].find(c => c.epoch === epoch);
        if (!candle) return;
        const prev = prevCloseMap[name];
        if (!prev || prev <= 0) return;
        rows.push({ name, chp: ((candle.close - prev) / prev) * 100 });
      });
      if (rows.length < 10) return;

      const byGain = [...rows].sort((a, b) => b.chp - a.chp);
      const byLoss = [...rows].sort((a, b) => a.chp - b.chp);
      const gainRank = {}, lossRank = {};
      byGain.forEach((r, i) => { gainRank[r.name] = i + 1; });
      byLoss.forEach((r, i) => { lossRank[r.name] = i + 1; });

      Object.keys(gainRank).forEach(name => {
        if (!newHistory[name]) newHistory[name] = [];
        newHistory[name].push({ t, g: gainRank[name], l: lossRank[name] });
      });
    });

    Object.keys(newHistory).forEach(n => newHistory[n].sort((a, b) => a.t.localeCompare(b.t)));

    // Only replace when we have real data
    if (Object.keys(newHistory).length > 20) {
      rankHistory = newHistory;
      backfillDone = true;
      console.log("Backfill applied, stocks with history:", Object.keys(rankHistory).length);
    } else {
      console.log("Backfill too thin, keeping previous history");
    }
  } catch (e) {
    console.error("Backfill error:", e.message);
  } finally {
    backfillInProgress = false;
  }
}

function processData(quotes, indexQuotes) {
  const marketOpen = isMarketOpen();
  const stocks = [];

  quotes.forEach(q => {
    const v = q.v || {};
    const name = (q.n || "").replace("NSE:", "").replace("-EQ", "");
    const ltp = Number(v.lp) || 0;
    // Accept LTP or previous close so after-hours still shows stocks
    const price = ltp > 0 ? ltp : (Number(v.prev_close_price) || 0);
    if (price <= 0) return;
    stocks.push({
      symbol: q.n,
      name,
      ltp: +(ltp || price).toFixed(2),
      ch: +(Number(v.ch) || 0).toFixed(2),
      chp: +(Number(v.chp) || 0).toFixed(2),
      sector: SECTOR_MAP[name] || "Others"
    });
  });

  const byGain = [...stocks].sort((a, b) => b.chp - a.chp);
  const byLoss = [...stocks].sort((a, b) => a.chp - b.chp);
  byGain.forEach((s, i) => (s.rankG = i + 1));
  byLoss.forEach((s, i) => (s.rankL = i + 1));

  // Live snapshot only when market is open
  if (marketOpen) {
    const now = Date.now();
    const tNow = getISTNow().toLocaleTimeString("en-IN", {
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
  }

  stocks.forEach(s => {
    s.hist = rankHistory[s.name] || [];
    if (s.hist.length) {
      s.diffG = s.hist[0].g - s.rankG;
      s.diffL = s.hist[0].l - s.rankL;
    } else {
      s.diffG = 0;
      s.diffL = 0;
    }
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
    .sort((a, b) => b.avg - a.avg);

  let nifty = { lp: 0, chp: 0 }, banknifty = { lp: 0, chp: 0 };
  (indexQuotes || []).forEach(q => {
    const v = q.v || {};
    if ((q.n || "").includes("NIFTY50")) nifty = { lp: +(Number(v.lp) || 0).toFixed(1), chp: +(Number(v.chp) || 0).toFixed(2) };
    if ((q.n || "").includes("NIFTYBANK")) banknifty = { lp: +(Number(v.lp) || 0).toFixed(1), chp: +(Number(v.chp) || 0).toFixed(2) };
  });

  const timeSet = new Set();
  Object.values(rankHistory).forEach(arr => arr.forEach(x => timeSet.add(x.t)));
  const allTimes = Array.from(timeSet).sort();

  return {
    marketOpen,
    historyDate: getHistoryDate(),
    nifty, banknifty,
    advances, declines,
    total: stocks.length,
    gainers: byGain.slice(0, 30),
    losers: byLoss.slice(0, 30),
    sectors,
    times: allTimes,
    backfillDone,
    backfillInProgress
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

  if (req.url.startsWith("/api/dashboard")) {
    const auth = req.headers.authorization;
    if (!auth) {
      res.writeHead(401, { "Content-Type": "application/json" });
      return res.end(JSON.stringify({ error: "No token" }));
    }
    try {
      const [stockQuotes, indexQuotes] = await Promise.all([
        fetchQuotes(auth, FNO_SYMBOLS),
        fetchQuotes(auth, INDEX_SYMBOLS)
      ]);

      // Always return live data first (never block)
      const data = processData(stockQuotes, indexQuotes);
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(data));

      // Start history build in background (does not block UI)
      if (!backfillDone && !backfillInProgress) {
        const prevCloseMap = {};
        stockQuotes.forEach(q => {
          const name = (q.n || "").replace("NSE:", "").replace("-EQ", "");
          prevCloseMap[name] = Number(q.v?.prev_close_price) || 0;
        });
        backfillRanks(auth, prevCloseMap); // no await
      }
    } catch (e) {
      console.error(e);
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: e.message }));
    }
    return;
  }
  res.writeHead(404); res.end("Not found");
});

const port = process.env.PORT || 3000;
server.listen(port, () => console.log("Dashboard on port", port));
