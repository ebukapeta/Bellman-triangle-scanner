// src/main.rs - Backend API only - MARKET PRICE VERSION
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
    pub paths_checked: usize,
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

// KuCoin WebSocket token response
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

// ==================== Symbol Parser ====================

fn parse_symbol(symbol: &str) -> Option<(String, String)> {
    let s = symbol.to_uppercase();

    // Handle special cases
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

    // Standard quote currencies (longest first)
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

    // Fallback parsing
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

// Helper function to add log entry with automatic truncation
fn add_log(logs: &mut Vec<ScanLog>, entry: ScanLog) {
    logs.push(entry);
    if logs.len() > 1000 {
        logs.drain(0..logs.len() - 1000);
    }
}

// ==================== Binance WebSocket Collector ====================

pub struct BinanceCollector {
    collected_data: Arc<Mutex<HashMap<String, (f64, i64)>>>,
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
                    if Instant::now() >= deadline {
                        break;
                    }

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
                                            let price = Self::parse_f64(item.get("c")); // "c" = last price

                                            if let (Some(sym), Some(market_price)) = (symbol, price) {
                                                if parse_symbol(sym).is_some() && market_price > 0.0 {
                                                    let mut data = data_clone.lock().await;
                                                    data.insert(sym.to_string(), (market_price, Utc::now().timestamp_millis()));
                                                    pair_count += 1;
                                                }
                                            }
                                        }
                                    }
                                    serde_json::Value::Object(obj) => {
                                        let symbol = obj.get("s").and_then(|v| v.as_str());
                                        let price = Self::parse_f64(obj.get("c")); // "c" = last price

                                        if let (Some(sym), Some(market_price)) = (symbol, price) {
                                            if parse_symbol(sym).is_some() && market_price > 0.0 {
                                                let mut data = data_clone.lock().await;
                                                data.insert(sym.to_string(), (market_price, Utc::now().timestamp_millis()));
                                                pair_count += 1;
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        Ok(Message::Ping(data)) => {
                            let _ = ws_stream.send(Message::Pong(data)).await;
                        }
                        Ok(Message::Pong(_)) => {
                            last_ping = Instant::now();
                        }
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
            paths_checked: 0,
            valid_triangles: 0,
            profitable_triangles: 0,
            collection_time_secs: start_time.elapsed().as_secs(),
        }
    }

    pub fn get_data(&self) -> Arc<Mutex<HashMap<String, (f64, i64)>>> {
        self.collected_data.clone()
    }

    pub fn get_logs(&self) -> Arc<Mutex<Vec<ScanLog>>> {
        self.logs.clone()
    }
}

// ==================== Bybit REST API Collector ====================

pub struct BybitCollector {
    collected_data: Arc<Mutex<HashMap<String, (f64, i64)>>>,
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

            match client.get(url)
                .query(&[("category", "spot")])
                .send()
                .await 
            {
                Ok(response) => {
                    if let Ok(json) = response.json::<serde_json::Value>().await {
                        if let Some(list) = json.get("result")
                            .and_then(|r| r.get("list"))
                            .and_then(|l| l.as_array()) 
                        {
                            let mut new_pairs = 0;

                            for item in list {
                                if let (Some(symbol), Some(price)) = (
                                    item.get("symbol").and_then(|s| s.as_str()),
                                    item.get("lastPrice").and_then(|p| p.as_str()).and_then(|s| s.parse::<f64>().ok()),
                                ) {
                                    if parse_symbol(symbol).is_some() && price > 0.0 {
                                        let mut data_map = data_clone.lock().await;
                                        if !data_map.contains_key(symbol) {
                                            new_pairs += 1;
                                        }
                                        data_map.insert(symbol.to_string(), (price, Utc::now().timestamp_millis()));
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

        let elapsed = start_time.elapsed().as_secs();

        ScanSummary {
            exchange: "bybit".to_string(),
            pairs_collected: final_count,
            paths_checked: 0,
            valid_triangles: 0,
            profitable_triangles: 0,
            collection_time_secs: elapsed,
        }
    }

    pub fn get_data(&self) -> Arc<Mutex<HashMap<String, (f64, i64)>>> {
        self.collected_data.clone()
    }

    pub fn get_logs(&self) -> Arc<Mutex<Vec<ScanLog>>> {
        self.logs.clone()
    }
}

// ==================== KuCoin WebSocket Collector ====================
pub struct KuCoinCollector {
    collected_data: Arc<Mutex<HashMap<String, (f64, i64)>>>,
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
        let server = res.data.instanceServers.get(0)
            .ok_or("No instance servers available")?;
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
                    paths_checked: 0,
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
                    if Instant::now() >= deadline {
                        break;
                    }

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

                                        let price = data.get("price")
                                            .and_then(|p| p.as_str())
                                            .and_then(|s| s.parse::<f64>().ok());

                                        if let (Some(symbol), Some(market_price)) = (symbol, price) {
                                            let std_symbol = symbol.replace("-", "");
                                            if parse_symbol(&std_symbol).is_some() && market_price > 0.0 {
                                                let mut data_map = data_clone.lock().await;
                                                data_map.insert(std_symbol, (market_price, Utc::now().timestamp_millis()));
                                                pair_count += 1;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Ok(Message::Ping(data)) => {
                            let _ = write.send(Message::Pong(data)).await;
                        }
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
            paths_checked: 0,
            valid_triangles: 0,
            profitable_triangles: 0,
            collection_time_secs: start_time.elapsed().as_secs(),
        }
    }

    pub fn get_data(&self) -> Arc<Mutex<HashMap<String, (f64, i64)>>> {
        self.collected_data.clone()
    }

    pub fn get_logs(&self) -> Arc<Mutex<Vec<ScanLog>>> {
        self.logs.clone()
    }
}

// ==================== Arbitrage Detector ====================

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

    fn build_graph(&self, tickers: &HashMap<String, (f64, i64)>) -> (DiGraph<String, f64>, HashMap<String, NodeIndex>) {
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

        for (symbol, (price, _)) in tickers {
            if *price <= 0.0 {
                continue;
            }

            if let Some((base, quote)) = parse_symbol(symbol) {
                if let (Some(&base_idx), Some(&quote_idx)) = (node_indices.get(&base), node_indices.get(&quote)) {

                    let rate_base_to_quote = *price;
                    let weight1 = -rate_base_to_quote.ln();
                    let key1 = (base_idx, quote_idx);
                    best_edges.entry(key1)
                        .and_modify(|e| *e = e.min(weight1))
                        .or_insert(weight1);

                    let rate_quote_to_base = 1.0 / *price;
                    let weight2 = -rate_quote_to_base.ln();
                    let key2 = (quote_idx, base_idx);
                    best_edges.entry(key2)
                        .and_modify(|e| *e = e.min(weight2))
                        .or_insert(weight2);
                }
            }
        }

        for ((from, to), weight) in best_edges {
            graph.add_edge(from, to, weight);
        }

        (graph, node_indices)
    }

    fn calculate_execution_chance(&self, path: &[String]) -> f64 {
        match path.len() {
            0..=2 => 0.0,
            3 => 85.0,
            4 => 75.0,
            5 => 60.0,
            _ => 50.0,
        }
    }

    fn calculate_real_profit(&self, path: &[String], tickers: &HashMap<String, (f64, i64)>) -> f64 {
        if path.len() < 3 || path.first() != path.last() {
            return -100.0;
        }

        let mut amount = 1.0;
        let mut trade_count = 0;

        for i in 0..path.len() - 1 {
            let from = &path[i];
            let to = &path[i + 1];

            let mut found_rate = None;

            for (symbol, (price, _)) in tickers {
                if let Some((base, quote)) = parse_symbol(symbol) {
                    if &base == from && &quote == to {
                        found_rate = Some(*price);
                        break;
                    } else if &base == to && &quote == from {
                        found_rate = Some(1.0 / *price);
                        break;
                    }
                }
            }

            let rate = match found_rate {
                Some(r) if r > 0.0 => r,
                _ => return -100.0,
            };

            amount *= rate;
            trade_count += 1;
        }

        let fee_rate = 0.001_f64;
        let total_fees = (1.0_f64 - fee_rate).powi(trade_count as i32);
        let net_profit = (amount * total_fees - 1.0) * 100.0;

        net_profit
    }

    fn find_opportunities(&self, graph: &DiGraph<String, f64>, node_indices: &HashMap<String, NodeIndex>, 
                     tickers: &HashMap<String, (f64, i64)>, min_profit: f64) -> (Vec<ArbitrageOpportunity>, usize, usize, usize) {
        let mut opportunities = Vec::new();
        let mut paths_checked = 0;
        let mut valid_triangles = 0;

        let nodes: Vec<NodeIndex> = graph.node_indices()
            .filter(|&n| graph.edges(n).count() >= 1)
            .collect();

        for &a_idx in &nodes {
            let a = &graph[a_idx];

            let a_neighbors: Vec<NodeIndex> = graph.edges(a_idx)
                .map(|e| e.target())
                .collect();

            for &b_idx in &a_neighbors {
                if b_idx == a_idx { continue; }
                let b = &graph[b_idx];

                let b_neighbors: Vec<NodeIndex> = graph.edges(b_idx)
                    .map(|e| e.target())
                    .collect();

                for &c_idx in &b_neighbors {
                    if c_idx == a_idx || c_idx == b_idx { continue; }
                    let c = &graph[c_idx];

                    paths_checked += 1;

                    let c_to_a = graph.find_edge(c_idx, a_idx);
                    if c_to_a.is_none() {
                        continue;
                    }

                    valid_triangles += 1;

                    let path = vec![a.clone(), b.clone(), c.clone(), a.clone()];
                    let profit = self.calculate_real_profit(&path, tickers);

                    if profit > min_profit && profit < 50.0 && profit.is_finite() {
                        let chance = self.calculate_execution_chance(&path);
                        let pair_str = path.join(" → ");

                        opportunities.push(ArbitrageOpportunity {
                            pair: pair_str,
                            triangle: path.clone(),
                            profit_margin_before: profit,
                            profit_margin_after: profit * (1.0 - (path.len() as f64 - 2.0) * 0.05),
                            chance_of_executing: chance,
                            timestamp: Utc::now().timestamp_millis(),
                            exchange: "unknown".to_string(),
                            estimated_slippage: profit * (path.len() as f64 - 2.0) * 0.05,
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
            if seen.contains(&key) {
                false
            } else {
                seen.insert(key);
                true
            }
        });

        opportunities.sort_by(|a, b| b.profit_margin_before.partial_cmp(&a.profit_margin_before).unwrap_or(std::cmp::Ordering::Equal));

        let profitable = opportunities.len();
        (opportunities, paths_checked, valid_triangles, profitable)
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
               paths_checked: 0,
               valid_triangles: 0,
               profitable_triangles: 0,
               collection_time_secs: 0,
           }, Vec::new())
       };

       let tickers = {
           let data_guard = data.lock().await;
           data_guard.clone()
       };

       let scan_logs = {
           let logs_guard = logs.lock().await;
           logs_guard.clone()
       };

       println!("📊 {} pairs collected from {}", tickers.len(), exchange);

       if tickers.is_empty() {
           println!("❌ NO TICKERS COLLECTED - aborting scan");
           return (Vec::new(), summary, scan_logs);
       }

       println!("📋 SAMPLE TICKERS:");
       for (i, (symbol, (price, _ts))) in tickers.iter().take(5).enumerate() {
           println!("   {}: {} price={:.2}", i, symbol, price);
       }

       let (graph, node_indices) = self.build_graph(&tickers);
       println!("📈 Graph: {} nodes, {} edges", graph.node_count(), graph.edge_count());

       if graph.edge_count() == 0 {
           println!("❌ NO EDGES IN GRAPH - cannot find arbitrage");
           return (Vec::new(), summary, scan_logs);
       }

       println!("📋 SAMPLE EDGES:");
       for (i, edge) in graph.edge_indices().take(3).enumerate() {
           let (u, v) = graph.edge_endpoints(edge).unwrap();
           let weight = graph[edge];
           println!("   {}: {} -> {} (weight: {:.6})", i, graph[u], graph[v], weight);
       }

       println!("📋 CURRENCIES ({}):", graph.node_count());
       let currencies: Vec<_> = graph.node_indices().map(|i| graph[i].clone()).collect();
       println!("   {}", currencies.join(", "));

       let (opportunities, paths_checked, valid_triangles, profitable) = 
           self.find_opportunities(&graph, &node_indices, &tickers, min_profit);

       println!("📊 RESULTS: {} paths checked, {} valid triangles, {} profitable", 
             paths_checked, valid_triangles, profitable);

       if opportunities.is_empty() {
           println!("⚠️  NO OPPORTUNITIES FOUND - possible reasons:");
           println!("   1. No arbitrage exists in current market data");
           println!("   2. min_profit ({}) too high", min_profit);
           println!("   3. Cycle detection not working");
           println!("   4. Graph not fully connected (check currencies above)");
       } else {
           println!("✅ Found {} opportunities", opportunities.len());
           for (i, opp) in opportunities.iter().take(3).enumerate() {
               println!("   {}: {} profit={:.2}%", i, opp.pair, opp.profit_margin_before);
           }
       }

       let mut final_summary = summary;
       final_summary.paths_checked = paths_checked;
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

        println!("🚀 Multi-exchange scan started");
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

        println!("✅ Scan complete: {} opportunities", all_opportunities.len());

        ScanResponse {
            opportunities: all_opportunities,
            summaries: all_summaries,
            logs: all_logs,
        }
    }
}

// ==================== API Handlers ====================

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

    let response = detector.scan_multiple_exchanges(
        req.exchanges.clone(), 
        min_profit, 
        duration
    ).await;

    HttpResponse::Ok().json(response)
}

async fn health_handler() -> impl Responder {
    HttpResponse::Ok().body("Arbitrage Scanner API is running")
}

// ==================== Main ====================

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let bind_addr = format!("0.0.0.0:{}", port);

    println!("🚀 Server starting on {}", bind_addr);
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
