use axum::{
    routing::{get},
    extract::Query,
    response::Html,
    Json, Router,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio_tungstenite::connect_async;
use std::fs::OpenOptions;
use std::io::Write;
use chrono::Utc;

/* ================= CONFIG ================= */
const TRADE_SIZE_USDT: f64 = 100.0;
const FEE: f64 = 0.001;
const SLIPPAGE: f64 = 0.0008;
const MIN_LIQ_MULT: f64 = 3.0;
const MAX_SPREAD: f64 = 0.002;

/* ================= LOGGING ================= */
fn log_scan_activity(message: &str) {
    let timestamp = Utc::now().to_rfc3339();
    let log_line = format!("[{}] {}\n", timestamp, message);
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("scanner_activity.log")
        .unwrap();
    file.write_all(log_line.as_bytes()).unwrap();
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
fn executable(liq: f64, spread: f64) -> bool { liq >= TRADE_SIZE_USDT*MIN_LIQ_MULT && spread <= MAX_SPREAD }

/* ================= BELLMAN-FORD ================= */
fn find_negative_cycle(graph: &Graph) -> Option<Vec<String>> {
    let n = graph.vertices.len(); if n==0 { return None; }
    let mut dist = vec![0.0;n];
    let mut parent: Vec<Option<usize>> = vec![None;n];
    for _ in 0..n { for e in &graph.edges {
        let u=graph.vertices.iter().position(|x|x==&e.from).unwrap();
        let v=graph.vertices.iter().position(|x|x==&e.to).unwrap();
        if dist[u]+e.weight < dist[v] { dist[v]=dist[u]+e.weight; parent[v]=Some(u); }
    }}
    for e in &graph.edges {
        let u=graph.vertices.iter().position(|x|x==&e.from).unwrap();
        let v=graph.vertices.iter().position(|x|x==&e.to).unwrap();
        if dist[u]+e.weight < dist[v] {
            let mut cur=v; for _ in 0..n{cur=parent[cur].unwrap();}
            let start=cur;
            let mut path=vec![start];
            let mut next=parent[start].unwrap();
            while next!=start{path.push(next); next=parent[next].unwrap();}
            path.push(start);
            return Some(path.iter().map(|i| graph.vertices[*i].clone()).collect());
        }
    }
    None
}

/* ================= PROFIT ================= */
fn evaluate_cycle(graph: &Graph, cycle: &Vec<String>) -> (f64,String,f64) {
    let mut rate=1.0; let mut min_liq=f64::MAX;
    for i in 0..cycle.len()-1 {
        let from=&cycle[i]; let to=&cycle[i+1];
        if let Some(e)=graph.edges.iter().find(|x|&x.from==from&&&x.to==to){ rate*=e.rate; if e.liquidity<min_liq{min_liq=e.liquidity;} }
    }
    let profit=(rate-1.0)*100.0;
    let direction=format!("{} → {} → {} → {}",cycle[0],cycle[1],cycle[2],cycle[0]);
    let confidence=(min_liq/(TRADE_SIZE_USDT*5.0)).min(1.0)*100.0;
    (profit,direction,confidence)
}

/* ================= WS COLLECTION ================= */
async fn collect_pairs_binance(duration_secs:u64)->HashMap<String,OrderBook>{
    let mut pairs=HashMap::new();
    log_scan_activity("Connecting to Binance WS");
    let (mut ws,_)=connect_async("wss://stream.binance.com:9443/ws").await.unwrap();
    let symbols=vec!["BTCUSDT","ETHUSDT","ETHBTC"];
    for sym in &symbols{
        let sub=serde_json::json!({"method":"SUBSCRIBE","params":[format!("{}@depth5",sym.to_lowercase())],"id":1});
        ws.send(tokio_tungstenite::tungstenite::Message::Text(sub.to_string())).await.unwrap();
    }
    let start=std::time::Instant::now();
    while start.elapsed().as_secs()<duration_secs {
        if let Some(msg)=ws.next().await {
            let txt=msg.unwrap().to_string();
            if txt.contains("bids") {
                let v:Value=serde_json::from_str(&txt).unwrap();
                let s=v["s"].as_str().unwrap().to_string();
                let bid=v["bids"][0][0].as_str().unwrap().parse::<f64>().unwrap();
                let ask=v["asks"][0][0].as_str().unwrap().parse::<f64>().unwrap();
                let bid_vol=v["bids"][0][1].as_str().unwrap().parse::<f64>().unwrap();
                let ask_vol=v["asks"][0][1].as_str().unwrap().parse::<f64>().unwrap();
                pairs.insert(s,OrderBook{bid,ask,bid_vol,ask_vol});
            }
        }
    }
    log_scan_activity(&format!("Binance collected {} pairs",pairs.len()));
    pairs
}

// Dummy Bybit + KuCoin (replace with WS live collection)
async fn collect_pairs_bybit(_u:u64)->HashMap<String,OrderBook>{
    let mut pairs=HashMap::new();
    pairs.insert("BTCUSDT".into(),OrderBook{bid:27950.0,ask:27951.0,bid_vol:10.0,ask_vol:10.0});
    pairs.insert("ETHUSDT".into(),OrderBook{bid:1820.0,ask:1821.0,bid_vol:15.0,ask_vol:15.0});
    log_scan_activity("Bybit collected 2 pairs"); pairs
}
async fn collect_pairs_kucoin(_u:u64)->HashMap<String,OrderBook>{
    let mut pairs=HashMap::new();
    pairs.insert("BTCUSDT".into(),OrderBook{bid:27952.0,ask:27953.0,bid_vol:8.0,ask_vol:8.0});
    pairs.insert("ETHUSDT".into(),OrderBook{bid:1822.0,ask:1823.0,bid_vol:12.0,ask_vol:12.0});
    log_scan_activity("KuCoin collected 2 pairs"); pairs
}

/* ================= SCAN EXCHANGE ================= */
async fn scan_exchange(name:&str,min_profit:f64)->serde_json::Value{
    log_scan_activity(&format!("Starting scan on {}",name));
    let books:HashMap<String,OrderBook>=match name{
        "binance"=>collect_pairs_binance(10).await,
        "bybit"=>collect_pairs_bybit(10).await,
        "kucoin"=>collect_pairs_kucoin(10).await,
        _=>HashMap::new()
    };
    let mut valid_pairs=0; let mut graph=Graph::new();
    for (sym,book) in &books{
        let spread=(book.ask-book.bid)/book.ask;
        if executable(book.bid_vol,spread){
            valid_pairs+=1;
            let base=&sym[0..3]; let quote=&sym[3..];
            graph.add_edge(base,quote,effective_rate(book.bid),book.bid_vol);
            graph.add_edge(quote,base,effective_rate(1.0/book.ask),book.ask_vol);
        }
    }
    graph.dedup();
    log_scan_activity(&format!("{} valid pairs for {}",valid_pairs,name));
    if let Some(cycle)=find_negative_cycle(&graph){
        let (profit,dir,conf)=evaluate_cycle(&graph,&cycle);
        if profit>=min_profit{
            log_scan_activity(&format!("Opportunity on {}: {} profit {:.4}%",name,dir,profit));
            return serde_json::json!({"exchange":name,"triangle":cycle,"direction":dir,"profit_percent":profit,"confidence":conf});
        }
    }
    log_scan_activity(&format!("No opportunities on {}",name));
    serde_json::json!(null)
}

/* ================= GLOBAL SCAN ================= */
#[derive(Deserialize)]
struct ScanParams{ exchanges:Option<Vec<String>>, min_profit:Option<f64> }

async fn run_scan(params:ScanParams)->serde_json::Value{
    let min_profit=params.min_profit.unwrap_or(0.3);
    let exchanges=params.exchanges.unwrap_or(vec!["binance".into(),"bybit".into(),"kucoin".into()]);
    log_scan_activity(&format!("Scan triggered for exchanges {:?} with min_profit {}",exchanges,min_profit));
    let start=std::time::Instant::now();
    let mut results=vec![];
    for ex in &exchanges{
        let v=scan_exchange(ex,min_profit).await;
        if v!=Value::Null{results.push(v);}
    }
    let duration=start.elapsed().as_millis();
    log_scan_activity(&format!("Scan finished in {} ms, total opportunities: {}",duration,results.len()));
    serde_json::json!({"opportunities":results,"duration_ms":duration,"timestamp":Utc::now().to_rfc3339()})
}

/* ================= API ================= */
async fn scan_handler(Query(params):Query<ScanParams>)->Json<Value>{ Json(run_scan(params).await) }

/* ================= FRONTEND ================= */
async fn ui()->Html<&'static str>{
Html(r#"
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Triangular Arbitrage Scanner</title>
<script src="https://cdn.tailwindcss.com"></script>
<style>
body { background:#0f172a; color:white; font-family:Arial,sans-serif; }
button { padding:12px 24px; font-size:16px; margin-top:10px; }
table { border-collapse: collapse; width: 100%; margin-top: 20px; }
th, td { border: 1px solid #555; padding: 8px; text-align: center; }
th { background-color: #1e293b; }
tr:nth-child(even) { background-color: #1e293b; }
tr:nth-child(odd) { background-color: #0f172a; }
</style>
</head>
<body class="p-4">

<h1 class="text-2xl font-bold mb-4">Triangular Arbitrage Scanner</h1>

<div class="mb-4">
<label class="font-bold mr-2">Select Exchanges:</label><br>
<label><input type="checkbox" value="binance" checked> Binance</label><br>
<label><input type="checkbox" value="bybit" checked> Bybit</label><br>
<label><input type="checkbox" value="kucoin" checked> KuCoin</label>
</div>

<div class="mb-4">
<label class="mr-2 font-bold">Min Profit %:</label>
<input type="number" id="min_profit" value="0.3" step="0.1" class="bg-gray-800 p-2 rounded w-20">
</div>

<button onclick="runScan()" class="bg-blue-600 p-2 rounded">Run Scan</button>

<div id="results" class="mt-4"></div>

<script>
async function runScan(){
    const checkboxes = document.querySelectorAll("input[type=checkbox]:checked");
    const selectedEx = Array.from(checkboxes).map(c => c.value);
    const minProfit = document.getElementById("min_profit").value;

    const resDiv = document.getElementById("results");
    resDiv.innerHTML = "<p>Scanning, please wait...</p>";

    try {
        const url = '/scan?exchanges=' + selectedEx.join(',') + '&min_profit=' + minProfit;
        const response = await fetch(url);
        const data = await response.json();

        if(!data.opportunities || data.opportunities.length===0){
            resDiv.innerHTML = "<p>No arbitrage opportunities found</p>";
            return;
        }

        // Build table
        let html = `<table>
        <thead>
            <tr>
                <th>Exchange</th>
                <th>Triangle Path</th>
                <th>Direction</th>
                <th>Profit %</th>
                <th>Confidence %</th>
            </tr>
        </thead>
        <tbody>`;

        data.opportunities.forEach(o=>{
            html += `<tr>
                <td>${o.exchange}</td>
                <td>${o.triangle.join(' → ')}</td>
                <td>${o.direction}</td>
                <td>${o.profit_percent.toFixed(4)}</td>
                <td>${o.confidence.toFixed(1)}</td>
            </tr>`;
        });

        html += `</tbody></table>`;
        resDiv.innerHTML = html;

    } catch(err){
        resDiv.innerHTML = "<p style='color:red'>Error connecting to scanner.</p>";
        console.error(err);
    }
}
</script>

</body>
</html>

/* ================= SERVER ================= */
#[tokio::main]
async fn main(){
    let app=Router::new()
        .route("/", get(ui))
        .route("/scan", get(scan_handler));
    log_scan_activity("Scanner service started");
    let listener=tokio::net::TcpListener::bind("0.0.0.0:10000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
    }
