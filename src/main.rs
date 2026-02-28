// src/main.rs - Two-Phase Arbitrage Scanner (Market Price + Order Book Validation)
#![warn(clippy::all)]

use actix_web::{web, App, HttpServer, HttpResponse, Responder, http};
use actix_cors::Cors;
use actix_files as fs;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant, timeout};
use chrono::{Utc, Local};
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::visit::EdgeRef;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

// ==================== Data Models ====================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArbitrageOpportunity {
    pub pair: String,
    pub triangle: Vec<String>,
    pub profit_margin_before: f64,
    pub profit_margin_after: f64,
    pub chance_of_executing: f64,
    pub timestamp: i64,
    pub exchange: String,
    pub estimated_slippage: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanRequest {
    pub exchanges: Vec<String>,
    pub min_profit: Option<f64>,
    pub collection_duration: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanLog {
    pub timestamp: String,
    pub exchange: String,
    pub message: String,
    pub level: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanSummary {
    pub exchange: String,
    pub pairs_collected: usize,
    pub paths_found: usize,
    pub valid_triangles: usize,
    pub profitable_triangles: usize,
    pub collection_time_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanResponse {
    pub opportunities: Vec<ArbitrageOpportunity>,
    pub summaries: Vec<ScanSummary>,
    pub logs: Vec<ScanLog>,
}

// Order book entry for validation
#[derive(Debug, Clone)]
pub struct OrderBookEntry {
    pub price: f64,
    pub quantity: f64,
}

// Enhanced ticker data with order book snapshot
#[derive(Debug, Clone)]
pub struct TickerData {
    pub market_price: f64,      // Last traded price for quick calc
    pub best_bid: f64,          // Best bid for sell validation
    pub best_ask: f64,          // Best ask for buy validation
    pub bid_depth: Vec<OrderBookEntry>, // Top bid levels
    pub ask_depth: Vec<OrderBookEntry>, // Top ask levels
    pub timestamp: i64,
}

#[derive(Debug, Deserialize)]
struct KuCoinTokenResponse {
    data: KuCoinTokenData,
}

#[derive(Debug, Deserialize)]
struct KuCoinTokenData {
    token: String,
    instanceServers: Vec<KuCoinInstanceServer>,
}

#[derive(Debug, Deserialize)]
struct KuCoinInstanceServer {
    endpoint: String,
    pingInterval: u64,
}

fn parse_symbol(symbol: &str) -> Option<(String, String)> {
    let s = symbol.to_uppercase();

    if s.ends_with("USD1") && s.len() > 4 {
        let base = s[..s.len()-4].to_string();
        if base.len() >= 2 {
            return Some((base, "USD".to_string()));
        }
    }

    if s.ends_with("RLUSD") && s.len() > 5 {
        let base = s[..s.len()-5].to_string();
        if base.len() >= 2 {
            return Some((base, "USD".to_string()));
        }
    }

    let quotes = [
        "FDUSD", "USDT", "BUSD", "USDC", "DAI", "TUSD", "USD",
        "BTC", "ETH", "BNB", "SOL", "XRP", "ADA", "DOGE", "DOT", "LINK",
        "MATIC", "AVAX", "UNI", "ATOM", "ALGO", "FIL", "VET", "THETA",
        "TRY", "EUR", "GBP", "AUD", "BRL", "CAD", "ARS", "RUB", "ZAR",
        "NGN", "UAH", "IDR", "JPY", "KRW", "VND", "MXN", "CHF", "PLN",
    ];

    for q in quotes.iter() {
        if s.ends_with(*q) && s.len() > q.len() {
            let base = s[..s.len() - q.len()].to_string();
            if base.len() >= 2 && base.len() <= 10 && base.chars().all(|c| c.is_ascii_uppercase()) {
                return Some((base, q.to_string()));
            }
        }
    }

    if s.len() > 6 {
        let try4 = s.split_at(s.len() - 4);
        if try4.1.chars().all(|c| c.is_ascii_alphabetic()) {
            let base = try4.0.to_string();
            let quote = try4.1.to_string();
            if base.len() >= 2 && base.len() <= 10 {
                return Some((base, quote));
            }
        }

        let try3 = s.split_at(s.len() - 3);
        if try3.1.chars().all(|c| c.is_ascii_alphabetic()) {
            let base = try3.0.to_string();
            let quote = try3.1.to_string();
            if base.len() >= 2 && base.len() <= 10 {
                return Some((base, quote));
            }
        }
    }

    None
}

fn add_log(logs: &mut Vec<ScanLog>, entry: ScanLog) {
    logs.push(entry);
    if logs.len() > 1000 {
        logs.drain(0..logs.len() - 1000);
    }
}

// ==================== Binance Collector with Order Book ====================

pub struct BinanceCollector {
    collected_data: Arc<Mutex<HashMap<String, TickerData>>>,
    logs: Arc<Mutex<Vec<ScanLog>>>,
}

impl BinanceCollector {
    pub fn new() -> Self {
        Self {
            collected_data: Arc::new(Mutex::new(HashMap::new())),
            logs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn parse_f64(v: Option<&serde_json::Value>) -> Option<f64> {
        v.and_then(|val| {
            val.as_f64()
                .or_else(|| val.as_str().and_then(|s| s.parse::<f64>().ok()))
        })
    }

    pub async fn start_collection(&self, duration_secs: u64) -> ScanSummary {
        let start_time = Instant::now();
        let deadline = start_time + Duration::from_secs(duration_secs);

        let mut data = self.collected_data.lock().await;
        data.clear();
        drop(data);

        {
            let mut logs = self.logs.lock().await;
            logs.clear();
            add_log(&mut logs, ScanLog {
                timestamp: Local::now().format("%H:%M:%S").to_string(),
                exchange: "binance".to_string(),
                message: format!("Starting Binance collection ({}s)", duration_secs),
                level: "info".to_string(),
            });
        }

        let data_clone = self.collected_data.clone();
        let logs_clone = self.logs.clone();
        let ws_url = "wss://stream.binance.com:9443/ws/!ticker@arr";
        let connect_future = connect_async(ws_url);
        let connect_result = timeout(Duration::from_secs(10), connect_future).await;

        match connect_result {
            Ok(Ok((mut ws_stream, _))) => {
                {
                    let mut logs = logs_clone.lock().await;
                    add_log(&mut logs, ScanLog {
                        timestamp: Local::now().format("%H:%M:%S").to_string(),
                        exchange: "binance".to_string(),
                        message: "WebSocket connected".to_string(),
                        level: "success".to_string(),
                    });
                }

                let mut pair_count = 0;
                let mut last_ping = Instant::now();

                while let Ok(Some(msg_result)) = timeout(Duration::from_secs(5), ws_stream.next()).await {
                    if Instant::now() >= deadline { break; }

                    if last_ping.elapsed() > Duration::from_secs(30) {
                        let _ = ws_stream.send(Message::Ping(vec![])).await;
                        last_ping = Instant::now();
                    }

                    match msg_result {
                        Ok(Message::Text(text)) => {
                            if let Ok(ticker_data) = serde_json::from_str::<serde_json::Value>(&text) {
                                match ticker_data {
                                    serde_json::Value::Array(arr) => {
                                        for item in arr {
                                            let symbol = item.get("s").and_then(|v| v.as_str());
                                            let last_price = Self::parse_f64(item.get("c"));
                                            let bid_price = Self::parse_f64(item.get("b"));
                                            let ask_price = Self::parse_f64(item.get("a"));
                                            let bid_qty = Self::parse_f64(item.get("B"));
                                            let ask_qty = Self::parse_f64(item.get("A"));

                                            if let (Some(sym), Some(market), Some(bid), Some(ask)) = (symbol, last_price, bid_price, ask_price) {
                                                if parse_symbol(sym).is_some() && market > 0.0 && bid > 0.0 && ask > 0.0 {
                                                    let mut data = data_clone.lock().await;

                                                    let ticker_data = TickerData {
                                                        market_price: market,
                                                        best_bid: bid,
                                                        best_ask: ask,
                                                        bid_depth: vec![OrderBookEntry { price: bid, quantity: bid_qty.unwrap_or(0.0) }],
                                                        ask_depth: vec![OrderBookEntry { price: ask, quantity: ask_qty.unwrap_or(0.0) }],
                                                        timestamp: Utc::now().timestamp_millis(),
                                                    };

                                                    data.insert(sym.to_string(), ticker_data);
                                                    pair_count += 1;
                                                }
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        Ok(Message::Ping(data)) => { let _ = ws_stream.send(Message::Pong(data)).await; }
                        Ok(Message::Pong(_)) => { last_ping = Instant::now(); }
                        Ok(Message::Close(_)) => break,
                        Err(e) => {
                            let mut logs = logs_clone.lock().await;
                            add_log(&mut logs, ScanLog {
                                timestamp: Local::now().format("%H:%M:%S").to_string(),
                                exchange: "binance".to_string(),
                                message: format!("WebSocket error: {}", e),
                                level: "error".to_string(),
                            });
                            break;
                        }
                        _ => {}
                    }

                    if pair_count > 0 && pair_count % 500 == 0 {
                        let mut logs = logs_clone.lock().await;
                        add_log(&mut logs, ScanLog {
                            timestamp: Local::now().format("%H:%M:%S").to_string(),
                            exchange: "binance".to_string(),
                            message: format!("Collected {} pairs", pair_count),
                            level: "debug".to_string(),
                        });
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }

                {
                    let mut logs = logs_clone.lock().await;
                    add_log(&mut logs, ScanLog {
                        timestamp: Local::now().format("%H:%M:%S").to_string(),
                        exchange: "binance".to_string(),
                        message: format!("Collection complete: {} pairs", pair_count),
                        level: "success".to_string(),
                    });
                }
            }
            Ok(Err(e)) => {
                let mut logs = logs_clone.lock().await;
                add_log(&mut logs, ScanLog {
                    timestamp: Local::now().format("%H:%M:%S").to_string(),
                    exchange: "binance".to_string(),
                    message: format!("Connection failed: {}", e),
                    level: "error".to_string(),
                });
            }
            Err(_) => {
                let mut logs = logs_clone.lock().await;
                add_log(&mut logs, ScanLog {
                    timestamp: Local::now().format("%H:%M:%S").to_string(),
                    exchange: "binance".to_string(),
                    message: "Connection timeout".to_string(),
                    level: "error".to_string(),
                });
            }
        }

        let final_data = self.collected_data.lock().await;
        let pairs_collected = final_data.len();
        drop(final_data);

        ScanSummary {
            exchange: "binance".to_string(),
            pairs_collected,
            paths_found: 0,
            valid_triangles: 0,
            profitable_triangles: 0,
            collection_time_secs: start_time.elapsed().as_secs(),
        }
    }

    pub fn get_data(&self) -> Arc<Mutex<HashMap<String, TickerData>>> {
        self.collected_data.clone()
    }

    pub fn get_logs(&self) -> Arc<Mutex<Vec<ScanLog>>> {
        self.logs.clone()
    }
}


// ==================== Bybit Collector with Order Book ====================

pub struct BybitCollector {
    collected_data: Arc<Mutex<HashMap<String, TickerData>>>,
    logs: Arc<Mutex<Vec<ScanLog>>>,
}

impl BybitCollector {
    pub fn new() -> Self {
        Self {
            collected_data: Arc::new(Mutex::new(HashMap::new())),
            logs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn start_collection(&self, duration_secs: u64) -> ScanSummary {
        let start_time = Instant::now();
        let deadline = start_time + Duration::from_secs(duration_secs);

        let mut data = self.collected_data.lock().await;
        data.clear();
        drop(data);

        {
            let mut logs = self.logs.lock().await;
            logs.clear();
            add_log(&mut logs, ScanLog {
                timestamp: Local::now().format("%H:%M:%S").to_string(),
                exchange: "bybit".to_string(),
                message: format!("Starting Bybit collection ({}s)", duration_secs),
                level: "info".to_string(),
            });
        }

        let data_clone = self.collected_data.clone();
        let logs_clone = self.logs.clone();
        let client = reqwest::Client::new();
        let url = "https://api.bybit.com/v5/market/tickers";
        let mut poll_count = 0;

        while Instant::now() < deadline {
            poll_count += 1;

            match client.get(url).query(&[("category", "spot")]).send().await {
                Ok(response) => {
                    if let Ok(json) = response.json::<serde_json::Value>().await {
                        if let Some(list) = json.get("result").and_then(|r| r.get("list")).and_then(|l| l.as_array()) {
                            let mut new_pairs = 0;

                            for item in list {
                                if let (Some(symbol), Some(last), Some(bid), Some(ask)) = (
                                    item.get("symbol").and_then(|s| s.as_str()),
                                    item.get("lastPrice").and_then(|p| p.as_str()).and_then(|s| s.parse::<f64>().ok()),
                                    item.get("bid1Price").and_then(|b| b.as_str()).and_then(|s| s.parse::<f64>().ok()),
                                    item.get("ask1Price").and_then(|a| a.as_str()).and_then(|s| s.parse::<f64>().ok()),
                                ) {
                                    if parse_symbol(symbol).is_some() && last > 0.0 && bid > 0.0 && ask > 0.0 {
                                        let mut data_map = data_clone.lock().await;

                                        let bid_qty = item.get("bid1Size").and_then(|s| s.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                                        let ask_qty = item.get("ask1Size").and_then(|s| s.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);

                                        if !data_map.contains_key(symbol) {
                                            new_pairs += 1;
                                        }

                                        let ticker_data = TickerData {
                                            market_price: last,
                                            best_bid: bid,
                                            best_ask: ask,
                                            bid_depth: vec![OrderBookEntry { price: bid, quantity: bid_qty }],
                                            ask_depth: vec![OrderBookEntry { price: ask, quantity: ask_qty }],
                                            timestamp: Utc::now().timestamp_millis(),
                                        };

                                        data_map.insert(symbol.to_string(), ticker_data);
                                    }
                                }
                            }

                            let pair_count = data_clone.lock().await.len();

                            if poll_count == 1 || poll_count % 5 == 0 {
                                let mut logs = logs_clone.lock().await;
                                add_log(&mut logs, ScanLog {
                                    timestamp: Local::now().format("%H:%M:%S").to_string(),
                                    exchange: "bybit".to_string(),
                                    message: format!("Poll {}: {} pairs ({} new)", poll_count, pair_count, new_pairs),
                                    level: "debug".to_string(),
                                });
                            }
                        }
                    }
                }
                Err(e) => {
                    let mut logs = logs_clone.lock().await;
                    add_log(&mut logs, ScanLog {
                        timestamp: Local::now().format("%H:%M:%S").to_string(),
                        exchange: "bybit".to_string(),
                        message: format!("API error: {}", e),
                        level: "error".to_string(),
                    });
                }
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }

        let final_count = data_clone.lock().await.len();

        {
            let mut logs = logs_clone.lock().await;
            add_log(&mut logs, ScanLog {
                timestamp: Local::now().format("%H:%M:%S").to_string(),
                exchange: "bybit".to_string(),
                message: format!("Collection complete: {} pairs ({} polls)", final_count, poll_count),
                level: "success".to_string(),
            });
        }

        ScanSummary {
            exchange: "bybit".to_string(),
            pairs_collected: final_count,
            paths_found: 0,
            valid_triangles: 0,
            profitable_triangles: 0,
            collection_time_secs: start_time.elapsed().as_secs(),
        }
    }

    pub fn get_data(&self) -> Arc<Mutex<HashMap<String, TickerData>>> {
        self.collected_data.clone()
    }

    pub fn get_logs(&self) -> Arc<Mutex<Vec<ScanLog>>> {
        self.logs.clone()
    }
}

// ==================== KuCoin Collector with Order Book ====================

pub struct KuCoinCollector {
    collected_data: Arc<Mutex<HashMap<String, TickerData>>>,
    logs: Arc<Mutex<Vec<ScanLog>>>,
}

impl KuCoinCollector {
    pub fn new() -> Self {
        Self {
            collected_data: Arc::new(Mutex::new(HashMap::new())),
            logs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    async fn get_websocket_token(&self) -> Result<(String, String, u64), Box<dyn std::error::Error>> {
        let client = reqwest::Client::new();
        let res = client.post("https://api.kucoin.com/api/v1/bullet-public")
            .send()
            .await?
            .json::<KuCoinTokenResponse>()
            .await?;

        let token = res.data.token;
        let server = res.data.instanceServers.get(0).ok_or("No instance servers available")?;
        let endpoint = server.endpoint.clone();
        let ping_interval = server.pingInterval;

        Ok((token, endpoint, ping_interval))
    }

    pub async fn start_collection(&self, duration_secs: u64) -> ScanSummary {
        let start_time = Instant::now();
        let deadline = start_time + Duration::from_secs(duration_secs);

        let mut data = self.collected_data.lock().await;
        data.clear();
        drop(data);

        {
            let mut logs = self.logs.lock().await;
            logs.clear();
            add_log(&mut logs, ScanLog {
                timestamp: Local::now().format("%H:%M:%S").to_string(),
                exchange: "kucoin".to_string(),
                message: format!("Starting KuCoin collection ({}s)", duration_secs),
                level: "info".to_string(),
            });
        }

        let data_clone = self.collected_data.clone();
        let logs_clone = self.logs.clone();

        let (token, endpoint, ping_interval_ms) = match self.get_websocket_token().await {
            Ok((t, e, p)) => (t, e, p),
            Err(e) => {
                let mut logs = logs_clone.lock().await;
                add_log(&mut logs, ScanLog {
                    timestamp: Local::now().format("%H:%M:%S").to_string(),
                    exchange: "kucoin".to_string(),
                    message: format!("Failed to get WebSocket token: {}", e),
                    level: "error".to_string(),
                });
                return ScanSummary {
                    exchange: "kucoin".to_string(),
                    pairs_collected: 0,
                    paths_found: 0,
                    valid_triangles: 0,
                    profitable_triangles: 0,
                    collection_time_secs: start_time.elapsed().as_secs(),
                };
            }
        };

        let ws_url = format!("{}/?token={}", endpoint, token);
        let connect_future = connect_async(&ws_url);
        let connect_result = timeout(Duration::from_secs(10), connect_future).await;

        match connect_result {
            Ok(Ok((ws_stream, _))) => {
                let (mut write, mut read) = ws_stream.split();
                let mut welcomed = false;
                let welcome_timeout = Instant::now() + Duration::from_secs(5);

                while Instant::now() < welcome_timeout {
                    if let Ok(Some(Ok(Message::Text(text)))) = timeout(Duration::from_secs(1), read.next()).await {
                        if let Ok(msg_data) = serde_json::from_str::<serde_json::Value>(&text) {
                            if msg_data.get("type").and_then(|t| t.as_str()) == Some("welcome") {
                                welcomed = true;
                                break;
                            }
                        }
                    }
                }

                if !welcomed {
                    let mut logs = logs_clone.lock().await;
                    add_log(&mut logs, ScanLog {
                        timestamp: Local::now().format("%H:%M:%S").to_string(),
                        exchange: "kucoin".to_string(),
                        message: "Welcome message not received, continuing anyway".to_string(),
                        level: "warning".to_string(),
                    });
                }

                {
                    let mut logs = logs_clone.lock().await;
                    add_log(&mut logs, ScanLog {
                        timestamp: Local::now().format("%H:%M:%S").to_string(),
                        exchange: "kucoin".to_string(),
                        message: "WebSocket connected".to_string(),
                        level: "success".to_string(),
                    });
                }

                let subscribe_msg = serde_json::json!({
                    "id": "1",
                    "type": "subscribe",
                    "topic": "/market/ticker:all",
                    "response": true
                });

                if let Ok(msg_str) = serde_json::to_string(&subscribe_msg) {
                    if let Err(e) = write.send(Message::Text(msg_str)).await {
                        let mut logs = logs_clone.lock().await;
                        add_log(&mut logs, ScanLog {
                            timestamp: Local::now().format("%H:%M:%S").to_string(),
                            exchange: "kucoin".to_string(),
                            message: format!("Failed to send subscription: {}", e),
                            level: "error".to_string(),
                        });
                    }
                }

                tokio::time::sleep(Duration::from_millis(500)).await;

                let mut pair_count = 0;
                let ping_interval = Duration::from_millis(ping_interval_ms);
                let mut last_ping = Instant::now();
                let mut sample_logged = false;

                while let Ok(Some(msg)) = timeout(Duration::from_secs(5), read.next()).await {
                    if Instant::now() >= deadline { break; }

                    if last_ping.elapsed() >= ping_interval {
                        let ping_msg = serde_json::json!({
                            "id": format!("ping-{}", Utc::now().timestamp()),
                            "type": "ping"
                        });
                        if let Ok(ping_str) = serde_json::to_string(&ping_msg) {
                            let _ = write.send(Message::Text(ping_str)).await;
                        }
                        last_ping = Instant::now();
                    }

                    match msg {
                        Ok(Message::Text(text)) => {
                            if !sample_logged {
                                let mut logs = logs_clone.lock().await;
                                add_log(&mut logs, ScanLog {
                                    timestamp: Local::now().format("%H:%M:%S").to_string(),
                                    exchange: "kucoin".to_string(),
                                    message: format!("Sample message: {}", &text[..text.len().min(200)]),
                                    level: "debug".to_string(),
                                });
                                sample_logged = true;
                            }

                            if let Ok(msg_data) = serde_json::from_str::<serde_json::Value>(&text) {
                                let msg_type = msg_data.get("type").and_then(|t| t.as_str());

                                if msg_type == Some("welcome") || msg_type == Some("pong") || msg_type == Some("ack") {
                                    continue;
                                }

                                if msg_type == Some("message") {
                                    if let Some(data) = msg_data.get("data") {
                                        let symbol = msg_data.get("subject")
                                            .and_then(|s| s.as_str())
                                            .or_else(|| data.get("symbol").and_then(|s| s.as_str()));

                                        let last_price = data.get("price").and_then(|p| p.as_str()).and_then(|s| s.parse::<f64>().ok());
                                        let best_bid = data.get("bestBid").and_then(|b| b.as_str()).and_then(|s| s.parse::<f64>().ok());
                                        let best_ask = data.get("bestAsk").and_then(|a| a.as_str()).and_then(|s| s.parse::<f64>().ok());
                                        let bid_size = data.get("bestBidSize").and_then(|s| s.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                                        let ask_size = data.get("bestAskSize").and_then(|s| s.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);

                                        if let (Some(symbol), Some(last), Some(bid), Some(ask)) = (symbol, last_price, best_bid, best_ask) {
                                            let std_symbol = symbol.replace("-", "");
                                            if parse_symbol(&std_symbol).is_some() && last > 0.0 && bid > 0.0 && ask > 0.0 {
                                                let mut data_map = data_clone.lock().await;

                                                let ticker_data = TickerData {
                                                    market_price: last,
                                                    best_bid: bid,
                                                    best_ask: ask,
                                                    bid_depth: vec![OrderBookEntry { price: bid, quantity: bid_size }],
                                                    ask_depth: vec![OrderBookEntry { price: ask, quantity: ask_size }],
                                                    timestamp: Utc::now().timestamp_millis(),
                                                };

                                                data_map.insert(std_symbol, ticker_data);
                                                pair_count += 1;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Ok(Message::Ping(data)) => { let _ = write.send(Message::Pong(data)).await; }
                        Err(e) => {
                            let mut logs = logs_clone.lock().await;
                            add_log(&mut logs, ScanLog {
                                timestamp: Local::now().format("%H:%M:%S").to_string(),
                                exchange: "kucoin".to_string(),
                                message: format!("WebSocket error: {}", e),
                                level: "error".to_string(),
                            });
                            break;
                        }
                        _ => {}
                    }
                }

                {
                    let mut logs = logs_clone.lock().await;
                    add_log(&mut logs, ScanLog {
                        timestamp: Local::now().format("%H:%M:%S").to_string(),
                        exchange: "kucoin".to_string(),
                        message: format!("Collection complete: {} pairs", pair_count),
                        level: "success".to_string(),
                    });
                }
            }
            Ok(Err(e)) => {
                let mut logs = logs_clone.lock().await;
                add_log(&mut logs, ScanLog {
                    timestamp: Local::now().format("%H:%M:%S").to_string(),
                    exchange: "kucoin".to_string(),
                    message: format!("Connection failed: {}", e),
                    level: "error".to_string(),
                });
            }
            Err(_) => {
                let mut logs = logs_clone.lock().await;
                add_log(&mut logs, ScanLog {
                    timestamp: Local::now().format("%H:%M:%S").to_string(),
                    exchange: "kucoin".to_string(),
                    message: "Connection timeout".to_string(),
                    level: "error".to_string(),
                });
            }
        }

        let final_data = self.collected_data.lock().await;
        let pairs_collected = final_data.len();
        drop(final_data);

        ScanSummary {
            exchange: "kucoin".to_string(),
            pairs_collected,
            paths_found: 0,
            valid_triangles: 0,
            profitable_triangles: 0,
            collection_time_secs: start_time.elapsed().as_secs(),
        }
    }

    pub fn get_data(&self) -> Arc<Mutex<HashMap<String, TickerData>>> {
        self.collected_data.clone()
    }

    pub fn get_logs(&self) -> Arc<Mutex<Vec<ScanLog>>> {
        self.logs.clone()
    }
}


// ==================== Arbitrage Detector with Two-Phase Validation ====================

pub struct ArbitrageDetector {
    binance_collector: BinanceCollector,
    bybit_collector: BybitCollector,
    kucoin_collector: KuCoinCollector,
}

impl ArbitrageDetector {
    pub fn new() -> Self {
        Self {
            binance_collector: BinanceCollector::new(),
            bybit_collector: BybitCollector::new(),
            kucoin_collector: KuCoinCollector::new(),
        }
    }

    // PHASE 1: Build graph using MARKET PRICE for fast triangle finding
    fn build_graph(&self, tickers: &HashMap<String, TickerData>) -> (DiGraph<String, f64>, HashMap<String, NodeIndex>) {
        let mut graph = DiGraph::<String, f64>::new();
        let mut node_indices = HashMap::new();
        let mut currencies = HashSet::new();

        for symbol in tickers.keys() {
            if let Some((base, quote)) = parse_symbol(symbol) {
                currencies.insert(base);
                currencies.insert(quote);
            }
        }

        for currency in currencies {
            let idx = graph.add_node(currency.clone());
            node_indices.insert(currency, idx);
        }

        let mut best_edges: HashMap<(NodeIndex, NodeIndex), f64> = HashMap::new();

        // Use MARKET PRICE for graph building (fast screening)
        for (symbol, data) in tickers {
            if data.market_price <= 0.0 {
                continue;
            }

            if let Some((base, quote)) = parse_symbol(symbol) {
                if let (Some(&base_idx), Some(&quote_idx)) = (node_indices.get(&base), node_indices.get(&quote)) {

                    // Market price rate: 1 base = market_price quote
                    let rate_base_to_quote = data.market_price;
                    let weight1 = -rate_base_to_quote.ln();
                    let key1 = (base_idx, quote_idx);
                    best_edges.entry(key1).and_modify(|e| *e = e.min(weight1)).or_insert(weight1);

                    // Reverse: 1 quote = 1/market_price base
                    let rate_quote_to_base = 1.0 / data.market_price;
                    let weight2 = -rate_quote_to_base.ln();
                    let key2 = (quote_idx, base_idx);
                    best_edges.entry(key2).and_modify(|e| *e = e.min(weight2)).or_insert(weight2);
                }
            }
        }

        for ((from, to), weight) in best_edges {
            graph.add_edge(from, to, weight);
        }

        (graph, node_indices)
    }

    // CORRECTED PHASE 2: Strict order book validation using BID/ASK only
    // Returns (profit, is_executable)
    fn validate_triangle_with_orderbook(&self, path: &[String], tickers: &HashMap<String, TickerData>) -> (f64, bool) {
        if path.len() < 3 || path.first() != path.last() {
            return (-100.0, false);
        }

        let mut amount = 1.0;
        let mut trade_count = 0;
        let mut executable = true;

        for i in 0..path.len() - 1 {
            let from = &path[i];
            let to = &path[i + 1];

            let mut trade_details = None;

            // Find ticker for this trade
            for (symbol, data) in tickers {
                if let Some((base, quote)) = parse_symbol(symbol) {

                    // Case 1: Selling 'from' (base) for 'to' (quote)
                    if &base == from && &quote == to {
                        trade_details = Some(("SELL", symbol.clone(), data));
                        break;
                    }
                    // Case 2: Buying 'to' (base) with 'from' (quote)
                    else if &base == to && &quote == from {
                        trade_details = Some(("BUY", symbol.clone(), data));
                        break;
                    }
                }
            }

            let (action, _symbol, data) = match trade_details {
                Some(d) => d,
                None => return (-100.0, false),
            };

            // Check data freshness
            let age_ms = Utc::now().timestamp_millis() - data.timestamp;
            if age_ms > 5000 { // 5 seconds max
                return (-100.0, false);
            }

            // STRICT VALIDATION: Use actual executable prices
            // SELL = get BID (what buyers pay, lower than market)
            // BUY = pay ASK (what sellers charge, higher than market)
            let execution_price = match action {
                "SELL" => {
                    if data.best_bid <= 0.0 {
                        return (-100.0, false);
                    }

                    // Check spread is reasonable
                    let spread_pct = (data.market_price - data.best_bid) / data.market_price * 100.0;
                    if spread_pct > 1.0 { // Bid more than 1% below market
                        executable = false;
                    }

                    data.best_bid
                }
                "BUY" => {
                    if data.best_ask <= 0.0 {
                        return (-100.0, false);
                    }

                    // Check spread is reasonable
                    let spread_pct = (data.best_ask - data.market_price) / data.market_price * 100.0;
                    if spread_pct > 1.0 { // Ask more than 1% above market
                        executable = false;
                    }

                    data.best_ask
                }
                _ => return (-100.0, false),
            };

            amount *= execution_price;
            trade_count += 1;
        }

        let fee_rate = 0.001_f64;
        let total_fees = (1.0_f64 - fee_rate).powi(trade_count as i32);
        let net_profit = (amount * total_fees - 1.0) * 100.0;

        // Only executable if profit is positive AND spreads were reasonable
        let final_executable = executable && net_profit > 0.0;

        (net_profit, final_executable)
    }

    fn calculate_execution_chance(&self, path: &[String], tickers: &HashMap<String, TickerData>) -> f64 {
        let mut total_score = 0.0;
        let mut trade_count = 0;

        for i in 0..path.len() - 1 {
            let from = &path[i];
            let to = &path[i + 1];

            for (symbol, data) in tickers {
                if let Some((base, quote)) = parse_symbol(symbol) {
                    let matches = (&base == from && &quote == to) || (&base == to && &quote == from);

                    if matches {
                        // Data freshness score
                        let age_ms = Utc::now().timestamp_millis() - data.timestamp;
                        let freshness = if age_ms < 500 { 100.0 } 
                            else if age_ms < 2000 { 90.0 }
                            else if age_ms < 5000 { 70.0 }
                            else if age_ms < 10000 { 40.0 }
                            else { 10.0 };

                        // Price alignment score (market price vs bid/ask)
                        let spread = ((data.best_ask - data.best_bid) / data.market_price * 100.0).abs();
                        let alignment = if spread < 0.1 { 100.0 }
                            else if spread < 0.5 { 90.0 }
                            else if spread < 1.0 { 80.0 }
                            else if spread < 2.0 { 60.0 }
                            else { 40.0 };

                        // Liquidity score
                        let liquidity = if data.bid_depth.iter().any(|e| e.quantity > 1.0) 
                            && data.ask_depth.iter().any(|e| e.quantity > 1.0) { 100.0 }
                            else if data.bid_depth.iter().any(|e| e.quantity > 0.0) 
                                && data.ask_depth.iter().any(|e| e.quantity > 0.0) { 70.0 }
                            else { 30.0 };

                        total_score += freshness * 0.3 + alignment * 0.4 + liquidity * 0.3;
                        trade_count += 1;
                        break;
                    }
                }
            }
        }

        if trade_count == 0 { return 0.0; }

        let avg_score = total_score / trade_count as f64;
        let penalty = match trade_count {
            3 => 5.0,
            4 => 15.0,
            _ => 25.0,
        };

        (avg_score - penalty).clamp(5.0, 95.0)
    }

    fn find_opportunities(&self, graph: &DiGraph<String, f64>, node_indices: &HashMap<String, NodeIndex>, 
                     tickers: &HashMap<String, TickerData>, min_profit: f64) -> (Vec<ArbitrageOpportunity>, usize, usize, usize) {
        let mut opportunities = Vec::new();
        let mut paths_checked = 0;
        let mut valid_triangles = 0;

        let nodes: Vec<NodeIndex> = graph.node_indices()
            .filter(|&n| graph.edges(n).count() >= 1)
            .collect();

        for &a_idx in &nodes {
            let a = &graph[a_idx];
            let a_neighbors: Vec<NodeIndex> = graph.edges(a_idx).map(|e| e.target()).collect();

            for &b_idx in &a_neighbors {
                if b_idx == a_idx { continue; }
                let b = &graph[b_idx];
                let b_neighbors: Vec<NodeIndex> = graph.edges(b_idx).map(|e| e.target()).collect();

                for &c_idx in &b_neighbors {
                    if c_idx == a_idx || c_idx == b_idx { continue; }
                    let c = &graph[c_idx];

                    paths_checked += 1;
                    if graph.find_edge(c_idx, a_idx).is_none() { continue; }

                    valid_triangles += 1;
                    let path = vec![a.clone(), b.clone(), c.clone(), a.clone()];

                    // TWO-PHASE VALIDATION
                    // Phase 1: Graph found this triangle using market price
                    // Phase 2: Validate with order book
                    let (profit, is_executable) = self.validate_triangle_with_orderbook(&path, tickers);

                    if valid_triangles <= 3 {
                        println!("🔍 Triangle {:?}: profit={:.4}%, executable={}", path, profit, is_executable);
                    }

                    // Only return if profitable AND order book validates
                    if profit > min_profit && profit < 50.0 && profit.is_finite() && is_executable {
                        let chance = self.calculate_execution_chance(&path, tickers);
                        let pair_str = path.join(" → ");

                        opportunities.push(ArbitrageOpportunity {
                            pair: pair_str,
                            triangle: path.clone(),
                            profit_margin_before: profit,
                            profit_margin_after: profit * 0.95,
                            chance_of_executing: chance,
                            timestamp: Utc::now().timestamp_millis(),
                            exchange: "unknown".to_string(),
                            estimated_slippage: profit * 0.05,
                        });
                    }
                }
            }
        }

        let mut seen = HashSet::new();
        opportunities.retain(|opp| {
            let mut cycle = opp.triangle.clone();
            cycle.pop();
            let min_pos = cycle.iter().enumerate().min_by_key(|(_, x)| *x).map(|(i, _)| i).unwrap_or(0);
            let mut canonical = cycle[min_pos..].to_vec();
            canonical.extend_from_slice(&cycle[0..min_pos]);
            let key = canonical.join(",");
            if seen.contains(&key) { false } else { seen.insert(key); true }
        });

        opportunities.sort_by(|a, b| b.profit_margin_before.partial_cmp(&a.profit_margin_before).unwrap_or(std::cmp::Ordering::Equal));

        let profitable_count = opportunities.len();
        (opportunities, paths_checked, valid_triangles, profitable_count)
    }

    pub async fn scan_exchange(&self, exchange: &str, min_profit: f64, duration_secs: u64) 
       -> (Vec<ArbitrageOpportunity>, ScanSummary, Vec<ScanLog>) {

       println!("🔍 Scanning {} for {}s", exchange, duration_secs);

       let (summary, data, logs) = match exchange {
           "binance" => {
               let summary = self.binance_collector.start_collection(duration_secs).await;
               (summary, self.binance_collector.get_data(), self.binance_collector.get_logs())
           }
           "bybit" => {
               let summary = self.bybit_collector.start_collection(duration_secs).await;
               (summary, self.bybit_collector.get_data(), self.bybit_collector.get_logs())
           }
           "kucoin" => {
               let summary = self.kucoin_collector.start_collection(duration_secs).await;
               (summary, self.kucoin_collector.get_data(), self.kucoin_collector.get_logs())
           }
           _ => return (Vec::new(), ScanSummary {
               exchange: exchange.to_string(),
               pairs_collected: 0,
               paths_found: 0,
               valid_triangles: 0,
               profitable_triangles: 0,
               collection_time_secs: 0,
           }, Vec::new())
       };

       let tickers = { let data_guard = data.lock().await; data_guard.clone() };
       let scan_logs = { let logs_guard = logs.lock().await; logs_guard.clone() };

       println!("📊 {} pairs collected from {}", tickers.len(), exchange);
       if tickers.is_empty() {
           println!("❌ NO TICKERS COLLECTED - aborting scan");
           return (Vec::new(), summary, scan_logs);
       }

       println!("📋 SAMPLE TICKERS (with market price + order book):");
       for (i, (symbol, data)) in tickers.iter().take(3).enumerate() {
           println!("   {}: {} market={:.4} bid={:.4} ask={:.4}", 
               i, symbol, data.market_price, data.best_bid, data.best_ask);
       }

       // PHASE 1: Build graph with market price
       let (graph, node_indices) = self.build_graph(&tickers);
       println!("📈 Graph: {} nodes, {} edges (built from market price)", graph.node_count(), graph.edge_count());

       if graph.edge_count() == 0 {
           println!("❌ NO EDGES IN GRAPH");
           return (Vec::new(), summary, scan_logs);
       }

       // PHASE 2: Find triangles and validate with order book
       let (opportunities, paths_checked, valid_triangles, profitable) = 
           self.find_opportunities(&graph, &node_indices, &tickers, min_profit);

       println!("📊 RESULTS: {} paths checked, {} valid triangles, {} profitable (order book validated)", 
             paths_checked, valid_triangles, profitable);

       if opportunities.is_empty() {
           println!("⚠️  NO OPPORTUNITIES FOUND");
       } else {
           println!("✅ Found {} validated opportunities", opportunities.len());
       }

       let mut final_summary = summary;
       final_summary.paths_found = paths_checked;
       final_summary.valid_triangles = valid_triangles;
       final_summary.profitable_triangles = profitable;

       let mut final_opportunities = opportunities;
       for opp in &mut final_opportunities {
           opp.exchange = exchange.to_string();
       }

       (final_opportunities, final_summary, scan_logs)
   }

    pub async fn scan_multiple_exchanges(&self, exchanges: Vec<String>, min_profit: f64, duration_secs: u64) 
        -> ScanResponse {

        println!("🚀 Multi-exchange scan started (Two-Phase: Market Price + Order Book)");
        let mut all_opportunities = Vec::new();
        let mut all_summaries = Vec::new();
        let mut all_logs = Vec::new();

        for exchange in exchanges {
            let (opps, summary, logs) = self.scan_exchange(&exchange, min_profit, duration_secs).await;
            all_opportunities.extend(opps);
            all_summaries.push(summary);
            all_logs.extend(logs);
        }

        all_opportunities.sort_by(|a, b| b.profit_margin_before.partial_cmp(&a.profit_margin_before).unwrap_or(std::cmp::Ordering::Equal));

        println!("✅ Scan complete: {} opportunities found", all_opportunities.len());

        ScanResponse {
            opportunities: all_opportunities,
            summaries: all_summaries,
            logs: all_logs,
        }
    }
}

async fn scan_handler(req: web::Json<ScanRequest>) -> impl Responder {
    println!("📨 Scan request: {:?}", req.exchanges);

    if req.exchanges.is_empty() {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "No exchanges selected",
            "opportunities": [],
            "summaries": [],
            "logs": []
        }));
    }

    let valid_exchanges = ["binance", "bybit", "kucoin"];
    for ex in &req.exchanges {
        if !valid_exchanges.contains(&ex.as_str()) {
            return HttpResponse::BadRequest().json(serde_json::json!({
                "error": format!("Invalid exchange: {}. Valid options: binance, bybit, kucoin", ex),
                "opportunities": [],
                "summaries": [],
                "logs": []
            }));
        }
    }

    let min_profit = req.min_profit.unwrap_or(0.3);
    if min_profit < 0.0 || min_profit > 100.0 {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "min_profit must be between 0 and 100",
            "opportunities": [],
            "summaries": [],
            "logs": []
        }));
    }

    let duration = req.collection_duration.unwrap_or(10);
    if duration == 0 || duration > 300 {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "collection_duration must be between 1 and 300 seconds",
            "opportunities": [],
            "summaries": [],
            "logs": []
        }));
    }

    let detector = ArbitrageDetector::new();
    let response = detector.scan_multiple_exchanges(req.exchanges.clone(), min_profit, duration).await;
    HttpResponse::Ok().json(response)
}

async fn health_handler() -> impl Responder {
    HttpResponse::Ok().body("Arbitrage Scanner API is running (Two-Phase Validation)")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let bind_addr = format!("0.0.0.0:{}", port);

    println!("🚀 Server starting on {}", bind_addr);
    println!("📡 Two-Phase Arbitrage Scanner: Market Price → Order Book Validation");
    println!("📡 Exchanges: binance, bybit, kucoin");

    HttpServer::new(|| {
        let cors = Cors::default()
            .allow_any_origin()
            .allow_any_method()
            .allow_any_header()
            .max_age(3600);

        App::new()
            .wrap(cors)
            .route("/health", web::get().to(health_handler))
            .route("/api/scan", web::post().to(scan_handler))
            .service(fs::Files::new("/", "./static").index_file("index.html"))
    })
    .bind(&bind_addr)?
    .run()
    .await
}
