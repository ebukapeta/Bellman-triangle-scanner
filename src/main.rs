use axum::{routing::get, Router, response::Html};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use tokio_tungstenite::connect_async;

/* ================= CONFIG ================= */

const TRADE_SIZE_USDT: f64 = 100.0;
const FEE: f64 = 0.001;
const SLIPPAGE: f64 = 0.0008;
const MIN_LIQ_MULT: f64 = 3.0;
const MAX_SPREAD: f64 = 0.002;

/* ================= MODELS ================= */

#[derive(Clone)]
struct OrderBook {
    bid: f64,
    ask: f64,
    bid_vol: f64,
    ask_vol: f64,
}

#[derive(Clone)]
struct Edge {
    from: String,
    to: String,
    weight: f64,
    rate: f64,
    liquidity: f64,
}

struct Graph {
    vertices: Vec<String>,
    edges: Vec<Edge>,
}

impl Graph {
    fn new() -> Self {
        Self { vertices: vec![], edges: vec![] }
    }

    fn add_edge(&mut self, from: &str, to: &str, rate: f64, liq: f64) {
        self.vertices.push(from.into());
        self.vertices.push(to.into());
        self.edges.push(Edge {
            from: from.into(),
            to: to.into(),
            weight: -rate.ln(),
            rate,
            liquidity: liq,
        });
    }

    fn dedup(&mut self) {
        let mut set = HashSet::new();
        self.vertices.retain(|v| set.insert(v.clone()));
    }
}

/* ================= EXECUTION MODEL ================= */

fn effective_rate(price: f64) -> f64 {
    price * (1.0 - FEE) * (1.0 - SLIPPAGE)
}

fn executable(liq: f64, spread: f64) -> bool {
    liq >= TRADE_SIZE_USDT * MIN_LIQ_MULT && spread <= MAX_SPREAD
}

/* ================= BELLMAN FORD ================= */

fn find_negative_cycle(graph: &Graph) -> Option<Vec<String>> {
    let n = graph.vertices.len();
    if n == 0 { return None; }

    let mut dist = vec![0.0; n];
    let mut parent: Vec<Option<usize>> = vec![None; n];

    for _ in 0..n {
        for e in &graph.edges {
            let u = graph.vertices.iter().position(|x| x == &e.from).unwrap();
            let v = graph.vertices.iter().position(|x| x == &e.to).unwrap();
            if dist[u] + e.weight < dist[v] {
                dist[v] = dist[u] + e.weight;
                parent[v] = Some(u);
            }
        }
    }

    for e in &graph.edges {
        let u = graph.vertices.iter().position(|x| x == &e.from).unwrap();
        let v = graph.vertices.iter().position(|x| x == &e.to).unwrap();

        if dist[u] + e.weight < dist[v] {
            let mut cur = v;
            for _ in 0..n { cur = parent[cur].unwrap(); }
            let start = cur;
            let mut path = vec![start];
            let mut next = parent[start].unwrap();
            while next != start {
                path.push(next);
                next = parent[next].unwrap();
            }
            path.push(start);

            return Some(path.iter().map(|i| graph.vertices[*i].clone()).collect());
        }
    }
    None
}

/* ================= PROFIT + SCORE ================= */

fn evaluate_cycle(graph: &Graph, cycle: &Vec<String>) -> (f64, String, f64) {
    let mut rate = 1.0;
    let mut min_liq = f64::MAX;

    for i in 0..cycle.len()-1 {
        let from = &cycle[i];
        let to = &cycle[i+1];

        if let Some(e) = graph.edges.iter().find(|x| &x.from==from && &x.to==to) {
            rate *= e.rate;
            if e.liquidity < min_liq { min_liq = e.liquidity; }
        }
    }

    let profit = (rate - 1.0) * 100.0;
    let direction = format!("{} → {} → {} → {}", cycle[0], cycle[1], cycle[2], cycle[0]);

    let confidence = (min_liq / (TRADE_SIZE_USDT * 5.0)).min(1.0) * 100.0;

    (profit, direction, confidence)
}

/* ================= WS SNAPSHOT ================= */

async fn binance_book(symbol: &str) -> anyhow::Result<OrderBook> {
    let (mut ws, _) = connect_async("wss://stream.binance.com:9443/ws").await?;

    let sub = serde_json::json!({
        "method": "SUBSCRIBE",
        "params": [format!("{}@depth5", symbol.to_lowercase())],
        "id": 1
    });

    ws.send(tokio_tungstenite::tungstenite::Message::Text(sub.to_string())).await?;

    while let Some(msg) = ws.next().await {
        let txt = msg?.to_string();
        if txt.contains("bids") {
            let v: Value = serde_json::from_str(&txt)?;
            return Ok(OrderBook {
                bid: v["bids"][0][0].as_str().unwrap().parse()?,
                ask: v["asks"][0][0].as_str().unwrap().parse()?,
                bid_vol: v["bids"][0][1].as_str().unwrap().parse()?,
                ask_vol: v["asks"][0][1].as_str().unwrap().parse()?,
            });
        }
    }
    Err(anyhow::anyhow!("no data"))
}

/* ================= TRIANGLE SCAN ================= */

async fn scan_exchange(name: &str) -> serde_json::Value {
    let pairs = vec![
        ("BTC","USDT","BTCUSDT"),
        ("ETH","USDT","ETHUSDT"),
        ("ETH","BTC","ETHBTC"),
    ];

    let mut books = HashMap::new();

    for (_,_,sym) in &pairs {
        if let Ok(b) = binance_book(sym).await {
            books.insert(sym.to_string(), b);
        }
    }

    let mut graph = Graph::new();

    for (base,quote,sym) in &pairs {
        if let Some(book) = books.get(*sym) {
            let spread = (book.ask-book.bid)/book.ask;
            if !executable(book.bid_vol, spread) { continue; }

            graph.add_edge(base,quote,effective_rate(book.bid),book.bid_vol);
            graph.add_edge(quote,base,effective_rate(1.0/book.ask),book.ask_vol);
        }
    }

    graph.dedup();

    if let Some(cycle)=find_negative_cycle(&graph) {
        let (profit,dir,conf)=evaluate_cycle(&graph,&cycle);
        return serde_json::json!({
            "exchange": name,
            "triangle": cycle,
            "direction": dir,
            "profit_percent": profit,
            "confidence": conf
        });
    }

    serde_json::json!(null)
}

/* ================= GLOBAL SCAN ================= */

async fn run_scan() -> serde_json::Value {
    let mut results = vec![];

    if let v @ Value::Object(_) = scan_exchange("binance").await {
        results.push(v);
    }

    serde_json::json!({
        "opportunities": results,
        "timestamp": chrono::Utc::now().to_rfc3339()
    })
}

/* ================= UI ================= */

async fn ui() -> Html<&'static str> {
    Html(r#"
<!DOCTYPE html>
<html>
<head>
<title>Triangular Arbitrage</title>
<style>
body{background:#0f172a;color:white;font-family:Arial;text-align:center}
button{padding:12px 24px;font-size:18px;margin-top:40px}
.card{background:#1e293b;margin:20px auto;padding:20px;width:420px;border-radius:12px}
</style>
</head>
<body>
<h1>Triangular Arbitrage Scanner</h1>
<button onclick="scan()">Run Scan</button>
<div id="results"></div>
<script>
async function scan(){
 const res=await fetch('/scan');
 const data=await res.json();
 const div=document.getElementById('results');
 div.innerHTML='';
 if(data.opportunities.length===0){
   div.innerHTML='<p>No executable arbitrage</p>'; return;
 }
 data.opportunities.forEach(o=>{
   div.innerHTML+=`
   <div class="card">
   <h3>${o.exchange}</h3>
   <p><b>Path:</b> ${o.direction}</p>
   <p><b>Profit:</b> ${o.profit_percent.toFixed(4)}%</p>
   <p><b>Execution Confidence:</b> ${o.confidence.toFixed(1)}%</p>
   </div>`;
 });
}
</script>
</body>
</html>
"#)
}

/* ================= SERVER ================= */

async fn scan_handler() -> String {
    run_scan().await.to_string()
}

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/", get(ui))
        .route("/scan", get(scan_handler));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:10000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
