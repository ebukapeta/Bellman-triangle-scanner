use axum::{
    routing::get,
    extract::Query,
    response::Html,
    Json, Router,
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::Write;
use chrono::Utc;
use tokio_tungstenite::connect_async;

/* ================= CONFIG ================= */
const TRADE_SIZE_USDT: f64 = 100.0;
const FEE: f64 = 0.001;
const SLIPPAGE: f64 = 0.0008;
const MIN_LIQ_MULT: f64 = 3.0;
const MAX_SPREAD: f64 = 0.002;

/* ================= LOGGING ================= */
fn log_scan_activity(message: &str) {
    let timestamp = chrono::Utc::now().to_rfc3339();
    let log_line = format!("[{}] {}\n", timestamp, message);

    // 1️⃣ Log to stdout (Render captures this automatically)
    println!("{}", log_line);

    // 2️⃣ Optional: log to a local file if writable, but ignore errors
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("scanner_activity.log") 
    {
        let _ = file.write_all(log_line.as_bytes());
    }
}

/* ================= MODELS ================= */
#[derive(Clone)]
struct OrderBook { bid: f64, ask: f64, bid_vol: f64, ask_vol: f64 }

#[derive(Clone)]
struct Edge { from: String, to: String, weight: f64, rate: f64, liquidity: f64 }

struct Graph { vertices: Vec<String>, edges: Vec<Edge> }

impl Graph {
    fn new() -> Self { Self { vertices: vec![], edges: vec![] } }
    fn add_edge(&mut self, from: &str, to: &str, rate: f64, liq: f64) {
        self.vertices.push(from.into());
        self.vertices.push(to.into());
        self.edges.push(Edge { from: from.into(), to: to.into(), weight: -rate.ln(), rate, liquidity: liq });
    }
    fn dedup(&mut self) {
        let mut set = HashSet::new();
        self.vertices.retain(|v| set.insert(v.clone()));
    }
}

fn effective_rate(price: f64) -> f64 { price * (1.0-FEE)*(1.0-SLIPPAGE) }

fn executable(liq: f64, spread: f64) -> bool {
    liq >= TRADE_SIZE_USDT*MIN_LIQ_MULT && spread <= MAX_SPREAD
}

/* ================= BELLMAN FORD ================= */
fn find_negative_cycle(graph: &Graph) -> Option<Vec<String>> {
    let n = graph.vertices.len();
    if n==0 { return None; }

    let mut dist = vec![0.0;n];
    let mut parent: Vec<Option<usize>> = vec![None;n];

    for _ in 0..n {
        for e in &graph.edges {
            let u=graph.vertices.iter().position(|x|x==&e.from).unwrap();
            let v=graph.vertices.iter().position(|x|x==&e.to).unwrap();
            if dist[u]+e.weight < dist[v] {
                dist[v]=dist[u]+e.weight;
                parent[v]=Some(u);
            }
        }
    }

    for e in &graph.edges {
        let u=graph.vertices.iter().position(|x|x==&e.from).unwrap();
        let v=graph.vertices.iter().position(|x|x==&e.to).unwrap();
        if dist[u]+e.weight < dist[v] {
            let mut cur=v;
            for _ in 0..n { cur=parent[cur].unwrap(); }
            let start=cur;
            let mut path=vec![start];
            let mut next=parent[start].unwrap();
            while next!=start { path.push(next); next=parent[next].unwrap(); }
            path.push(start);
            return Some(path.iter().map(|i| graph.vertices[*i].clone()).collect());
        }
    }
    None
}

/* ================= PROFIT ================= */
fn evaluate_cycle(graph: &Graph, cycle: &Vec<String>) -> (f64,String,f64) {
    let mut rate=1.0;
    let mut min_liq=f64::MAX;

    for i in 0..cycle.len()-1 {
        let from=&cycle[i];
        let to=&cycle[i+1];
        if let Some(e)=graph.edges.iter().find(|x|&x.from==from&&&x.to==to){
            rate*=e.rate;
            if e.liquidity<min_liq { min_liq=e.liquidity; }
        }
    }

    let profit=(rate-1.0)*100.0;
    let direction=format!("{} → {} → {} → {}",cycle[0],cycle[1],cycle[2],cycle[0]);
    let confidence=(min_liq/(TRADE_SIZE_USDT*5.0)).min(1.0)*100.0;

    (profit,direction,confidence)
}

/* ================= BINANCE WS ================= */
async fn collect_pairs_binance(duration_secs:u64) -> HashMap<String, OrderBook> {
    let mut pairs = HashMap::new();
    log_scan_activity("Connecting to Binance WS");

    // 1️⃣ Connect
    let (mut ws, _) = connect_async("wss://stream.binance.com:9443/ws").await
        .expect("Failed to connect to Binance WS");

    // 2️⃣ Subscribe to ALL USDT pairs dynamically (simplified demo: still can hardcode some)
    let symbols = vec!["BTCUSDT", "ETHUSDT", "ETHBTC"];
    for sym in &symbols {
        let sub = serde_json::json!({
            "method": "SUBSCRIBE",
            "params": [format!("{}@depth5@100ms", sym.to_lowercase())],
            "id": 1
        });
        if let Err(e) = ws.send(tokio_tungstenite::tungstenite::Message::Text(sub.to_string())).await {
            log_scan_activity(&format!("WS send error: {}", e));
        }
    }

    // 3️⃣ Collect messages safely
    let start = std::time::Instant::now();
    while start.elapsed().as_secs() < duration_secs {
        if let Some(msg) = ws.next().await {
            match msg {
                Ok(tokio_tungstenite::tungstenite::Message::Text(txt)) => {
                    if txt.contains("bids") && txt.contains("asks") {
                        if let Ok(v) = serde_json::from_str::<Value>(&txt) {
                            if let Some(s) = v.get("s").and_then(|x| x.as_str()) {
                                if let (Some(bid), Some(ask)) = (
                                    v.get("bids").and_then(|b| b[0].get(0).and_then(|x| x.as_str()).and_then(|x| x.parse::<f64>().ok())),
                                    v.get("asks").and_then(|a| a[0].get(0).and_then(|x| x.as_str()).and_then(|x| x.parse::<f64>().ok())),
                                ) {
                                    let bid_vol = v.get("bids").and_then(|b| b[0].get(1).and_then(|x| x.as_str()).and_then(|x| x.parse::<f64>().ok())).unwrap_or(0.0);
                                    let ask_vol = v.get("asks").and_then(|a| a[0].get(1).and_then(|x| x.as_str()).and_then(|x| x.parse::<f64>().ok())).unwrap_or(0.0);
                                    pairs.insert(s.to_string(), OrderBook { bid, ask, bid_vol, ask_vol });
                                }
                            }
                        }
                    }
                }
                Ok(_) => {}
                Err(e) => log_scan_activity(&format!("WS read error: {}", e)),
            }
        }
    }

    log_scan_activity(&format!("Binance collected {} pairs", pairs.len()));
    pairs
}

/* ================= BYBIT WS ================= */
async fn collect_pairs_bybit(duration_secs:u64) -> HashMap<String, OrderBook> {
    let mut pairs = HashMap::new();
    log_scan_activity("Connecting Bybit WS");

    let (mut ws, _) = connect_async("wss://stream.bybit.com/realtime_public")
        .await
        .expect("Failed to connect Bybit WS");

    // Subscribe to top pairs
    let symbols = vec!["BTCUSDT", "ETHUSDT"];
    for sym in &symbols {
        let sub = serde_json::json!({
            "op": "subscribe",
            "args": [format!("orderBookL2_25.{}", sym)]
        });
        if let Err(e) = ws.send(tokio_tungstenite::tungstenite::Message::Text(sub.to_string())).await {
            log_scan_activity(&format!("Bybit WS send error: {}", e));
        }
    }

    let start = std::time::Instant::now();
    while start.elapsed().as_secs() < duration_secs {
        if let Some(msg) = ws.next().await {
            match msg {
                Ok(tokio_tungstenite::tungstenite::Message::Text(txt)) => {
                    if let Ok(v) = serde_json::from_str::<Value>(&txt) {
                        if let Some(data) = v.get("data").and_then(|d| d.as_array()) {
                            for item in data {
                                if let (Some(symbol), Some(price), Some(size), Some(side)) = (
                                    item.get("symbol").and_then(|x| x.as_str()),
                                    item.get("price").and_then(|x| x.as_str()).and_then(|x| x.parse::<f64>().ok()),
                                    item.get("size").and_then(|x| x.as_f64()),
                                    item.get("side").and_then(|x| x.as_str())
                                ) {
                                    let entry = pairs.entry(symbol.to_string()).or_insert(OrderBook { bid:0.0, ask:0.0, bid_vol:0.0, ask_vol:0.0 });
                                    if side=="Buy" { entry.bid=price; entry.bid_vol=size; }
                                    if side=="Sell" { entry.ask=price; entry.ask_vol=size; }
                                }
                            }
                        }
                    }
                }
                Ok(_) => {}
                Err(e) => log_scan_activity(&format!("Bybit WS read error: {}", e)),
            }
        }
    }

    log_scan_activity(&format!("Bybit collected {} pairs", pairs.len()));
    pairs
}

/* ================= KUCOIN WS ================= */
async fn collect_pairs_kucoin(duration_secs:u64) -> HashMap<String, OrderBook> {
    let mut pairs = HashMap::new();
    log_scan_activity("Connecting KuCoin WS");

    // Example: connect to KuCoin WS endpoint (replace <TOKEN> with your actual token from REST)
    let ws_url = "wss://push1-v2.kucoin.com/endpoint?token=<TOKEN>";
    let (mut ws, _) = connect_async(ws_url)
        .await
        .expect("Failed to connect KuCoin WS");

    let symbols = vec!["BTC-USDT", "ETH-USDT"];
    for sym in &symbols {
        let sub = serde_json::json!({
            "id": "1",
            "type": "subscribe",
            "topic": format!("/market/level2:{}", sym),
            "response": true
        });
        if let Err(e) = ws.send(tokio_tungstenite::tungstenite::Message::Text(sub.to_string())).await {
            log_scan_activity(&format!("KuCoin WS send error: {}", e));
        }
    }

    let start = std::time::Instant::now();
    while start.elapsed().as_secs() < duration_secs {
        if let Some(msg) = ws.next().await {
            match msg {
                Ok(tokio_tungstenite::tungstenite::Message::Text(txt)) => {
                    if let Ok(v) = serde_json::from_str::<Value>(&txt) {
                        if let Some(data) = v.get("data") {
                            let symbol = data.get("s").and_then(|x| x.as_str()).unwrap_or_default();
                            let bids = data.get("b").and_then(|b| b.as_array()).unwrap_or(&vec![]);
                            let asks = data.get("a").and_then(|a| a.as_array()).unwrap_or(&vec![]);
                            if !bids.is_empty() && !asks.is_empty() {
                                let bid = bids[0][0].as_str().and_then(|x| x.parse::<f64>().ok()).unwrap_or(0.0);
                                let bid_vol = bids[0][1].as_str().and_then(|x| x.parse::<f64>().ok()).unwrap_or(0.0);
                                let ask = asks[0][0].as_str().and_then(|x| x.parse::<f64>().ok()).unwrap_or(0.0);
                                let ask_vol = asks[0][1].as_str().and_then(|x| x.parse::<f64>().ok()).unwrap_or(0.0);
                                pairs.insert(symbol.to_string(), OrderBook { bid, ask, bid_vol, ask_vol });
                            }
                        }
                    }
                }
                Ok(_) => {}
                Err(e) => log_scan_activity(&format!("KuCoin WS read error: {}", e)),
            }
        }
    }

    log_scan_activity(&format!("KuCoin collected {} pairs", pairs.len()));
    pairs
}

/* ================= SCAN ================= */
#[derive(Deserialize)]
struct ScanParams { exchanges: Option<String>, min_profit: Option<f64> }

async fn run_scan(params:ScanParams)->Value {
    let min_profit=params.min_profit.unwrap_or(0.3);
    let exchanges=params.exchanges.unwrap_or("binance,bybit,kucoin".into());
    log_scan_activity("Scan triggered");

    let mut results=vec![];

    for ex in exchanges.split(',') {
        let books=match ex {
            "binance"=>collect_pairs_binance(10).await,
            "bybit"=>collect_pairs_bybit(10).await,
            "kucoin"=>collect_pairs_kucoin(10).await,
            _=>HashMap::new()
        };

        let mut graph=Graph::new();
        let mut valid=0;

        for (sym,book) in &books {
            if sym.len()<6 { continue; }
            let spread=(book.ask-book.bid)/book.ask;
            if executable(book.bid_vol,spread){
                valid+=1;
                let base=&sym[0..3];
                let quote=&sym[3..];
                graph.add_edge(base,quote,effective_rate(book.bid),book.bid_vol);
                graph.add_edge(quote,base,effective_rate(1.0/book.ask),book.ask_vol);
            }
        }

        graph.dedup();
        log_scan_activity(&format!("{} valid pairs {}",ex,valid));

        if let Some(cycle)=find_negative_cycle(&graph){
            let (profit,dir,conf)=evaluate_cycle(&graph,&cycle);
            if profit>=min_profit{
                results.push(serde_json::json!({
                    "exchange":ex,
                    "triangle":cycle,
                    "direction":dir,
                    "profit_percent":profit,
                    "confidence":conf
                }));
            }
        }
    }

    log_scan_activity(&format!("Scan finished opportunities {}",results.len()));
    serde_json::json!({ "opportunities":results })
}

async fn scan_handler(Query(params):Query<ScanParams>)->Json<Value>{
    Json(run_scan(params).await)
}

/* ================= UI ================= */
async fn ui() -> Html<&'static str> {
Html(r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Real-Time DEX Arbitrage Scanner</title>
<script src="https://cdn.tailwindcss.com"></script>

<style>
body { background:#020617; color:#e5e7eb; font-family:system-ui; }
.card { background:#020617; border:1px solid #1e293b; border-radius:10px; padding:20px; }
button { transition: all 0.2s ease; }
button:hover { transform: scale(1.02); }
.status-dot {
    width:10px; height:10px; border-radius:50%;
    display:inline-block; margin-right:6px;
}
.status-idle { background:#64748b; }
.status-run { background:#22c55e; animation:pulse 1s infinite; }
@keyframes pulse {
    0% { opacity:1 } 50% { opacity:0.4 } 100% { opacity:1 }
}
table { width:100%; border-collapse: collapse; }
th, td { padding:10px; border-bottom:1px solid #1e293b; text-align:center; }
th { background:#020617; }
tr:hover { background:#020617; }
</style>
</head>

<body class="p-6">

<div class="max-w-5xl mx-auto">

<h1 class="text-2xl font-bold mb-4">Triangular Arbitrage Scanner</h1>

<div class="card mb-4">

<div class="flex flex-wrap gap-6 items-center">

<div>
<label class="font-semibold">Exchanges</label><br>
<label><input type="checkbox" value="binance" checked> Binance</label><br>
<label><input type="checkbox" value="bybit" checked> Bybit</label><br>
<label><input type="checkbox" value="kucoin" checked> KuCoin</label>
</div>

<div>
<label class="font-semibold">Min Profit %</label><br>
<input id="min_profit" type="number" value="0.3" step="0.1"
class="bg-black border border-slate-700 rounded px-3 py-2 w-24">
</div>

<div>
<button onclick="runScan()"
class="bg-blue-600 px-6 py-3 rounded font-semibold">
Run Scan
</button>
</div>

<div class="ml-auto text-sm">
<span id="statusDot" class="status-dot status-idle"></span>
<span id="statusText">Idle</span>
</div>

</div>
</div>

<div class="card">
<div id="results">No scan executed yet.</div>
</div>

</div>

<script>
async function runScan() {

    const dot = document.getElementById("statusDot");
    const status = document.getElementById("statusText");
    const results = document.getElementById("results");

    dot.className = "status-dot status-run";
    status.textContent = "Scanning...";
    results.innerHTML = "Scanning exchanges...";

    const exchanges = Array.from(
        document.querySelectorAll("input[type=checkbox]:checked")
    ).map(e => e.value);

    const minProfit = document.getElementById("min_profit").value;

    try {
        const url = `/scan?exchanges=${exchanges.join(",")}&min_profit=${minProfit}`;
        const res = await fetch(url);
        const data = await res.json();

        dot.className = "status-dot status-idle";
        status.textContent = "Idle";

        if (!data.opportunities || data.opportunities.length === 0) {
            results.innerHTML = "<p>No arbitrage opportunities found</p>";
            return;
        }

        let html = `
        <table>
        <thead>
        <tr>
            <th>Exchange</th>
            <th>Triangle</th>
            <th>Direction</th>
            <th>Profit %</th>
            <th>Confidence</th>
        </tr>
        </thead>
        <tbody>`;

        data.opportunities.forEach(o => {
            html += `
            <tr>
                <td>${o.exchange}</td>
                <td>${o.triangle.join(" → ")}</td>
                <td>${o.direction}</td>
                <td>${o.profit_percent.toFixed(4)}</td>
                <td>${o.confidence.toFixed(1)}%</td>
            </tr>`;
        });

        html += "</tbody></table>";
        results.innerHTML = html;

    } catch (err) {
        dot.className = "status-dot status-idle";
        status.textContent = "Error";
        results.innerHTML = "<p style='color:#f87171'>Scanner connection failed</p>";
    }
}
</script>

</body>
</html>"#)
}

/* ================= SERVER ================= */
#[tokio::main]
async fn main(){
    log_scan_activity("Scanner service started");
    let app=Router::new()
        .route("/", get(ui))
        .route("/scan", get(scan_handler));

    let listener=tokio::net::TcpListener::bind("0.0.0.0:10000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
             }
