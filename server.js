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
        try { resolve(JSON.parse(d)); }
        catch (e) { resolve(null); }
      });
    });
    req.on("error", () => resolve(null));
    req.setTimeout(12000, () => { req.destroy(); resolve(null); });
    req.end();
  });
}

function fetchQuotes(auth, symbols) {
  return new Promise(async (resolve) => {
    const results = [];
    for (let i = 0; i < symbols.length; i += 50) {
      const batch = symbols.slice(i, i + 50);
      const j = await httpsGet(auth, `/data/quotes?symbols=${batch.join(",")}`);
      if (j && j.s === "ok" && Array.isArray(j.d)) results.push(...j.d);
    }
    resolve(results);
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

  // Weekend
  if (day === 0) d.setDate(d.getDate() - 2);          // Sun → Fri
  else if (day === 6) d.setDate(d.getDate() - 1);     // Sat → Fri
  // Before market open (~9:15)
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
  const j = await httpsGet(auth, pathStr);
  if (!j || j.s !== "ok" || !Array.isArray(j.candles)) return [];
  return j.candles.map(c => ({ epoch: c[0], close: c[4] }));
}

async function backfillRanks(auth, prevCloseMap) {
  if (backfillDone || backfillInProgress) return;
  backfillInProgress = true;
  const day = getHistoryDate();
  console.log("Backfill start for date:", day);

  try {
    const CONCURRENCY = 8;
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
    console.log("Candles found, unique times:", epochs.length);

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
    console.log("Backfill done. Sample times:", Object.values(rankHistory)[0]?.map(x => x.t).slice(0, 5));
  } catch (e) {
    console.error("Backfill error:", e.message);
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

  // ALWAYS add current live snapshot (so at least 1 time column exists)
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

      // Build prevClose map for backfill
      const prevCloseMap = {};
      stockQuotes.forEach(q => {
        const name = (q.n || "").replace("NSE:", "").replace("-EQ", "");
        prevCloseMap[name] = Number(q.v?.prev_close_price) || 0;
      });

      // Await backfill on first request so columns appear immediately
      if (!backfillDone && !backfillInProgress) {
        await backfillRanks(auth, prevCloseMap);
      }

      const data = processData(stockQuotes, indexQuotes);
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(data));
    } catch (e) {
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: e.message }));
    }
    return;
  }
  res.writeHead(404); res.end("Not found");
});

const port = process.env.PORT || 3000;
server.listen(port, () => console.log(`Dashboard on port ${port}`));
