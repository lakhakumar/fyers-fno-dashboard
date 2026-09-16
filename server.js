const http = require("http");
const https = require("https");
const fs = require("fs");
const path = require("path");

const FNO_SYMBOLS = [
  "NSE:360ONE-EQ","NSE:ABB-EQ","NSE:ABCAPITAL-EQ","NSE:ADANIENSOL-EQ","NSE:ADANIENT-EQ",
  "NSE:ADANIGREEN-EQ","NSE:ADANIPORTS-EQ","NSE:ADANIPOWER-EQ","NSE:ALKEM-EQ","NSE:AMBER-EQ",
  "NSE:AMBUJACEM-EQ","NSE:ANGELONE-EQ","NSE:APLAPOLLO-EQ","NSE:APOLLOHOSP-EQ","NSE:ASHOKLEY-EQ",
  "NSE:ASIANPAINT-EQ","NSE:ASTRAL-EQ","NSE:ATHERENERG-EQ","NSE:AUROPHARMA-EQ","NSE:DMART-EQ",
  "NSE:AXISBANK-EQ","NSE:BSE-EQ","NSE:BAJAJ-AUTO-EQ","NSE:BAJFINANCE-EQ","NSE:BAJAJFINSV-EQ",
  "NSE:BAJAJHLDNG-EQ","NSE:BANDHANBNK-EQ","NSE:BANKBARODA-EQ","NSE:BANKINDIA-EQ","NSE:BDL-EQ",
  "NSE:BEL-EQ","NSE:BHARATFORG-EQ","NSE:BHEL-EQ","NSE:BPCL-EQ","NSE:BHARTIARTL-EQ",
  "NSE:BIOCON-EQ","NSE:BLUESTARCO-EQ","NSE:BOSCHLTD-EQ","NSE:BRITANNIA-EQ","NSE:CGPOWER-EQ",
  "NSE:CANBK-EQ","NSE:CDSL-EQ","NSE:CHOLAFIN-EQ","NSE:CIPLA-EQ","NSE:COALINDIA-EQ",
  "NSE:COCHINSHIP-EQ","NSE:COFORGE-EQ","NSE:COLPAL-EQ","NSE:CAMS-EQ","NSE:CONCOR-EQ",
  "NSE:CUMMINSIND-EQ","NSE:CYIENT-EQ","NSE:DABUR-EQ","NSE:DALBHARAT-EQ","NSE:DELHIVERY-EQ",
  "NSE:DIVISLAB-EQ","NSE:DIXON-EQ","NSE:DLF-EQ","NSE:DRREDDY-EQ","NSE:EICHERMOT-EQ",
  "NSE:ESCORTS-EQ","NSE:EXIDEIND-EQ","NSE:FEDERALBNK-EQ","NSE:GAIL-EQ","NSE:GLENMARK-EQ",
  "NSE:GMRINFRA-EQ","NSE:GODREJCP-EQ","NSE:GODREJPROP-EQ","NSE:GRASIM-EQ","NSE:HAVELLS-EQ",
  "NSE:HCLTECH-EQ","NSE:HDFCBANK-EQ","NSE:HDFCLIFE-EQ","NSE:HEROMOTOCO-EQ","NSE:HINDALCO-EQ",
  "NSE:HAL-EQ","NSE:HINDUNILVR-EQ","NSE:HINDZINC-EQ","NSE:HUDCO-EQ","NSE:ICICIBANK-EQ",
  "NSE:ICICIGI-EQ","NSE:ICICIPRULI-EQ","NSE:IDEA-EQ","NSE:IDFCFIRSTB-EQ","NSE:IEX-EQ",
  "NSE:IGL-EQ","NSE:INDHOTEL-EQ","NSE:INDIANB-EQ","NSE:INDIGO-EQ","NSE:INDUSINDBK-EQ",
  "NSE:INDUSTOWER-EQ","NSE:INFY-EQ","NSE:IOC-EQ","NSE:IPCALAB-EQ","NSE:IRCTC-EQ",
  "NSE:IRFC-EQ","NSE:ITC-EQ","NSE:JINDALSTEL-EQ","NSE:JIOFIN-EQ","NSE:JSWENERGY-EQ",
  "NSE:JSWSTEEL-EQ","NSE:JUBLFOOD-EQ","NSE:KOTAKBANK-EQ","NSE:LALPATHLAB-EQ","NSE:LAURUSLABS-EQ",
  "NSE:LICI-EQ","NSE:LT-EQ","NSE:LTF-EQ","NSE:LTIM-EQ","NSE:LTTS-EQ",
  "NSE:LUPIN-EQ","NSE:M&M-EQ","NSE:M&MFIN-EQ","NSE:MANAPPURAM-EQ","NSE:MARICO-EQ",
  "NSE:MARUTI-EQ","NSE:MCX-EQ","NSE:METROPOLIS-EQ","NSE:MFSL-EQ","NSE:MGL-EQ",
  "NSE:MOTHERSON-EQ","NSE:MPHASIS-EQ","NSE:MRF-EQ","NSE:MUTHOOTFIN-EQ","NSE:NATIONALUM-EQ",
  "NSE:NAUKRI-EQ","NSE:NAVINFLUOR-EQ","NSE:NESTLEIND-EQ","NSE:NMDC-EQ","NSE:NTPC-EQ",
  "NSE:NYKAA-EQ","NSE:OBEROIRLTY-EQ","NSE:OFSS-EQ","NSE:ONGC-EQ","NSE:PAGEIND-EQ",
  "NSE:PATANJALI-EQ","NSE:PAYTM-EQ","NSE:PERSISTENT-EQ","NSE:PETRONET-EQ","NSE:PFC-EQ",
  "NSE:PIDILITIND-EQ","NSE:PIIND-EQ","NSE:PNB-EQ","NSE:POLYCAB-EQ","NSE:POWERGRID-EQ",
  "NSE:PPLPHARMA-EQ","NSE:PRESTIGE-EQ","NSE:RBLBANK-EQ","NSE:RECLTD-EQ","NSE:RELIANCE-EQ",
  "NSE:SAIL-EQ","NSE:SBICARD-EQ","NSE:SBILIFE-EQ","NSE:SBIN-EQ","NSE:SHREECEM-EQ",
  "NSE:SIEMENS-EQ","NSE:SOLARINDS-EQ","NSE:SONACOMS-EQ","NSE:SRF-EQ","NSE:SUNPHARMA-EQ",
  "NSE:SUNTV-EQ","NSE:SUPREMEIND-EQ","NSE:SYNGENE-EQ","NSE:TATACHEM-EQ","NSE:TATACOMM-EQ",
  "NSE:TATACONSUM-EQ","NSE:TATAELXSI-EQ","NSE:TATAMOTORS-EQ","NSE:TATAPOWER-EQ","NSE:TATASTEEL-EQ",
  "NSE:TCS-EQ","NSE:TECHM-EQ","NSE:TITAN-EQ","NSE:TORNTPHARM-EQ","NSE:TRENT-EQ",
  "NSE:TVSMOTOR-EQ","NSE:ULTRACEMCO-EQ","NSE:UNIONBANK-EQ","NSE:UNITDSPR-EQ","NSE:UPL-EQ",
  "NSE:VBL-EQ","NSE:VEDL-EQ","NSE:VOLTAS-EQ","NSE:WIPRO-EQ","NSE:YESBANK-EQ",
  "NSE:ZOMATO-EQ","NSE:ZYDUSLIFE-EQ","NSE:POLICYBZR-EQ","NSE:RADICO-EQ"
];

const INDEX_SYMBOLS = ["NSE:NIFTY50-INDEX","NSE:NIFTYBANK-INDEX"];

const SECTOR_MAP = {
  "RELIANCE":"Energy","ONGC":"Energy","BPCL":"Energy","IOC":"Energy","GAIL":"Energy","PETRONET":"Energy",
  "ADANIGREEN":"Energy","ADANIENSOL":"Energy","JSWENERGY":"Energy","TATAPOWER":"Power","NTPC":"Power","POWERGRID":"Power",
  "TCS":"IT","INFY":"IT","HCLTECH":"IT","WIPRO":"IT","TECHM":"IT","COFORGE":"IT","LTIM":"IT","PERSISTENT":"IT","MPHASIS":"IT","LTTS":"IT","TATAELXSI":"IT","OFSS":"IT","CYIENT":"IT",
  "HDFCBANK":"Bank","ICICIBANK":"Bank","SBIN":"Bank","KOTAKBANK":"Bank","AXISBANK":"Bank","INDUSINDBK":"Bank",
  "BANKBARODA":"Bank","PNB":"Bank","CANBK":"Bank","FEDERALBNK":"Bank","IDFCFIRSTB":"Bank","AUBANK":"Bank",
  "BANDHANBNK":"Bank","BANKINDIA":"Bank","RBLBANK":"Bank","YESBANK":"Bank","UNIONBANK":"Bank","INDIANB":"Bank",
  "BAJFINANCE":"Finance","BAJAJFINSV":"Finance","CHOLAFIN":"Finance","MUTHOOTFIN":"Finance","PFC":"Finance","RECLTD":"Finance",
  "M&MFIN":"Finance","MANAPPURAM":"Finance","LTF":"Finance","JIOFIN":"Finance","IRFC":"Finance","SBICARD":"Finance",
  "HDFCLIFE":"Insurance","SBILIFE":"Insurance","ICICIPRULI":"Insurance","ICICIGI":"Insurance","LICI":"Insurance",
  "MARUTI":"Auto","M&M":"Auto","TATAMOTORS":"Auto","EICHERMOT":"Auto","HEROMOTOCO":"Auto","BAJAJ-AUTO":"Auto",
  "TVSMOTOR":"Auto","ASHOKLEY":"Auto","MOTHERSON":"Auto","BHARATFORG":"Auto","SONACOMS":"Auto",
  "SUNPHARMA":"Pharma","DRREDDY":"Pharma","CIPLA":"Pharma","DIVISLAB":"Pharma","APOLLOHOSP":"Pharma",
  "LUPIN":"Pharma","AUROPHARMA":"Pharma","BIOCON":"Pharma","ALKEM":"Pharma","TORNTPHARM":"Pharma","GLENMARK":"Pharma",
  "LAURUSLABS":"Pharma","ZYDUSLIFE":"Pharma","IPCALAB":"Pharma","PPLPHARMA":"Pharma","SYNGENE":"Pharma",
  "TATASTEEL":"Metal","JSWSTEEL":"Metal","HINDALCO":"Metal","SAIL":"Metal","VEDL":"Metal","NMDC":"Metal",
  "COALINDIA":"Metal","NATIONALUM":"Metal","HINDZINC":"Metal","JINDALSTEL":"Metal",
  "ASIANPAINT":"Consumer","HINDUNILVR":"Consumer","ITC":"Consumer","NESTLEIND":"Consumer","BRITANNIA":"Consumer",
  "GODREJCP":"Consumer","DABUR":"Consumer","MARICO":"Consumer","COLPAL":"Consumer","TATACONSUM":"Consumer",
  "VBL":"Consumer","UNITDSPR":"Consumer","JUBLFOOD":"Consumer","PATANJALI":"Consumer","RADICO":"Consumer",
  "LT":"Infra","ULTRACEMCO":"Infra","GRASIM":"Infra","ADANIPORTS":"Infra","ADANIENT":"Infra","AMBUJACEM":"Infra",
  "SHREECEM":"Infra","DALBHARAT":"Infra",
  "DLF":"Realty","GODREJPROP":"Realty","OBEROIRLTY":"Realty","PRESTIGE":"Realty",
  "BHARTIARTL":"Telecom","IDEA":"Telecom","INDUSTOWER":"Telecom",
  "BEL":"Defence","HAL":"Defence","BDL":"Defence","BHEL":"Capital Goods","SIEMENS":"Capital Goods","ABB":"Capital Goods",
  "CGPOWER":"Capital Goods","CUMMINSIND":"Capital Goods","HAVELLS":"Consumer Durables","VOLTAS":"Consumer Durables",
  "BLUESTARCO":"Consumer Durables","DIXON":"Consumer Durables","AMBER":"Consumer Durables",
  "IRCTC":"Services","CONCOR":"Services","INDIGO":"Services","INDHOTEL":"Services","DELHIVERY":"Services",
  "ZOMATO":"Services","NYKAA":"Services","PAYTM":"Services","NAUKRI":"Services","POLICYBZR":"Services",
  "DMART":"Retail","TRENT":"Retail","ABCAPITAL":"Finance","ANGELONE":"Finance","CDSL":"Finance","CAMS":"Finance",
  "MCX":"Finance","BSE":"Finance","MFSL":"Finance","TATACOMM":"Telecom"
};

let rankHistory = {};   // name -> [{t, g, l}]
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

  // Ranking
  const byGain = [...stocks].sort((a, b) => b.chp - a.chp);
  const byLoss = [...stocks].sort((a, b) => a.chp - b.chp);
  byGain.forEach((s, i) => (s.rankG = i + 1));
  byLoss.forEach((s, i) => (s.rankL = i + 1));

  // 5-min snapshot
  if (now - lastSnapshot >= SNAPSHOT_MS || lastSnapshot === 0) {
    lastSnapshot = now;
    const t = new Date().toLocaleTimeString("en-IN", {
      hour: "2-digit",
      minute: "2-digit",
      hour12: false
    });
    stocks.forEach(s => {
      if (!rankHistory[s.name]) rankHistory[s.name] = [];
      rankHistory[s.name].push({ t, g: s.rankG, l: s.rankL });
      if (rankHistory[s.name].length > 48) rankHistory[s.name].shift(); // keep ~4 hrs
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

  // Sector averages
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

  // Index values
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

  // Collect all unique times for table headers
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
    nifty,
    banknifty,
    advances,
    declines,
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
