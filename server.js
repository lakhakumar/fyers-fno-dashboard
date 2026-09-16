const http = require("http");
const https = require("https");
const fs = require("fs");
const path = require("path");

// Full F&O list provided by user
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
const SNAPSHOT_MS = 5 * 60 * 1000;

function fetchQuotes(auth, symbols) {
  return new Promise((resolve, reject) => {
    const batches = [];
    for (let i = 0; i < symbols.length; i += 50) {
      batches.push(symbols.slice(i, i + 50));
    }
    const results = [];
    let done = 0;
    if (batches.length === 0) return resolve([]);

    batches.forEach(batch => {
      const opts = {
        hostname: "api-t1.fyers.in",
        path: `/data/quotes?symbols=${batch.join(",")}`,
        method: "GET",
        headers: { Authorization: auth }
      };
      const req = https.request(opts, res => {
        let d = "";
        res.on("data", c => d += c);
        res.on("end", () => {
          try {
            const j = JSON.parse(d);
            if (j.s === "ok" && Array.isArray(j.d)) results.push(...j.d);
          } catch (e) {}
          if (++done === batches.length) resolve(results);
        });
      });
      req.on("error", reject);
      req.end();
    });
  });
}

function processData(quotes, indexQuotes) {
  const now = Date.now();
  const stocks = [];

  quotes.forEach(q => {
    const v = q.v || {};
    const name = (q.n || "").replace("NSE:", "").replace("-EQ", "");
    const ltp = Number(v.lp) || 0;
    const chp = Number(v.chp) || 0;
    if (ltp <= 0) return;
    stocks.push({
      symbol: q.n,
      name,
      ltp: +ltp.toFixed(2),
      ch: +(Number(v.ch) || 0).toFixed(2),
      chp: +chp.toFixed(2),
      sector: SECTOR_MAP[name] || "Others"
    });
  });

  const byGain = [...stocks].sort((a, b) => b.chp - a.chp);
  const byLoss = [...stocks].sort((a, b) => a.chp - b.chp);
  byGain.forEach((s, i) => (s.rankG = i + 1));
  byLoss.forEach((s, i) => (s.rankL = i + 1));

  if (now - lastSnapshot >= SNAPSHOT_MS || lastSnapshot === 0) {
    lastSnapshot = now;
    const t = new Date().toLocaleTimeString("en-IN", {
      hour: "2-digit", minute: "2-digit", hour12: false
    });
    stocks.forEach(s => {
      if (!rankHistory[s.name]) rankHistory[s.name] = [];
      rankHistory[s.name].push({ t, g: s.rankG, l: s.rankL });
      if (rankHistory[s.name].length > 48) rankHistory[s.name].shift();
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
    .sort((a, b) => b.avg - a.avg);

  let nifty = { lp: 0, chp: 0 };
  let banknifty = { lp: 0, chp: 0 };
  (indexQuotes || []).forEach(q => {
    const v = q.v || {};
    if ((q.n || "").includes("NIFTY50")) {
      nifty = { lp: +(Number(v.lp) || 0).toFixed(1), chp: +(Number(v.chp) || 0).toFixed(2) };
    }
    if ((q.n || "").includes("NIFTYBANK")) {
      banknifty = { lp: +(Number(v.lp) || 0).toFixed(1), chp: +(Number(v.chp) || 0).toFixed(2) };
    }
  });

  const allTimes = [];
  const seen = new Set();
  Object.values(rankHistory).forEach(arr => {
    arr.forEach(x => {
      if (!seen.has(x.t)) {
        seen.add(x.t);
        allTimes.push(x.t);
      }
    });
  });

  return {
    nifty, banknifty,
    advances, declines,
    total: stocks.length,
    gainers: byGain.slice(0, 30),
    losers: byLoss.slice(0, 30),
    sectors,
    times: allTimes
  };
}

const server = http.createServer(async (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Headers", "Authorization, Content-Type");

  if (req.method === "OPTIONS") {
    res.writeHead(204);
    return res.end();
  }

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
      const data = processData(stockQuotes, indexQuotes);
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(data));
    } catch (e) {
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: e.message }));
    }
    return;
  }

  res.writeHead(404);
  res.end("Not found");
});

const port = process.env.PORT || 3000;
server.listen(port, () => {
  console.log(`Dashboard running on port ${port}`);
});
