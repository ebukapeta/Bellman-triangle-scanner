// src/main.rs - Backend API only
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
use petgraph::algo::bellman_ford;
use petgraph::visit::EdgeRef;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use log::{info, warn, error};

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

// ==================== Binance WebSocket Collector ====================

pub struct BinanceCollector {
    collected_data: Arc<Mutex<HashMap<String, (f64, f64, i64)>>>,
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

        let mut logs = self.logs.lock().await;
        logs.clear();
        logs.push(ScanLog {
            timestamp: Local::now().format("%H:%M:%S").to_string(),
            exchange: "binance".to_string(),
            message: format!("Starting Binance collection ({}s)", duration_secs),
            level: "info".to_string(),
        });
        // Keep last 1000 logs to prevent unbounded growth
        if logs.len() > 1000 {
            logs.drain(0..logs.len() - 1000);
        }
        drop(logs);

        let data_clone = self.collected_data.clone();
        let logs_clone = self.logs.clone();

        let ws_url = "wss://stream.binance.com:9443/ws/!ticker@arr";

        // Add connection timeout
        let connect_future = connect_async(ws_url);
        let connect_result = timeout(Duration::from_secs(10), connect_future).await;

        match connect_result {
            Ok(Ok((mut ws_stream, _))) => {
                logs_clone.lock().await.push(ScanLog {
                    timestamp: Local::now().format("%H:%M:%S").to_string(),
                    exchange: "binance".to_string(),
                    message: "WebSocket connected".to_string(),
                    level: "success".to_string(),
                });

                let mut pair_count = 0;
                let mut last_ping = Instant::now();

                while let Ok(Some(msg_result)) = timeout(Duration::from_secs(5), ws_stream.next()).await {
                    if Instant::now() >= deadline {
                        break;
                    }

                    // Send periodic ping to keep connection alive
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
                                            let bid = Self::parse_f64(item.get("b"));
                                            let ask = Self::parse_f64(item.get("a"));

                                            if let (Some(sym), Some(bid_price), Some(ask_price)) = (symbol, bid, ask) {
                                                if parse_symbol(sym).is_some() {
                                                    let mut data = data_clone.lock().await;
                                                    data.insert(sym.to_string(), (bid_price, ask_price, Utc::now().timestamp_millis()));
                                                    pair_count += 1;
                                                }
                                            }
                                        }
                                    }
                                    serde_json::Value::Object(obj) => {
                                        let symbol = obj.get("s").and_then(|v| v.as_str());
                                        let bid = Self::parse_f64(obj.get("b"));
                                        let ask = Self::parse_f64(obj.get("a"));

                                        if let (Some(sym), Some(bid_price), Some(ask_price)) = (symbol, bid, ask) {
                                            if parse_symbol(sym).is_some() {
                                                let mut data = data_clone.lock().await;
                                                data.insert(sym.to_string(), (bid_price, ask_price, Utc::now().timestamp_millis()));
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
                            last_ping = Instant::now(); // Reset ping timer on pong
                        }
                        Ok(Message::Close(_)) => break,
                        Err(e) => {
                            let mut logs = logs_clone.lock().await;
                            logs.push(ScanLog {
                                timestamp: Local::now().format("%H:%M:%S").to_string(),
                                exchange: "binance".to_string(),
                                message: format!("WebSocket error: {}", e),
                                level: "error".to_string(),
                            });
                            if logs.len() > 1000 {
                                logs.drain(0..logs.len() - 1000);
                            }
                            break;
                        }
                        _ => {}
                    }

                    if pair_count > 0 && pair_count % 500 == 0 {
                        let mut logs = logs_clone.lock().await;
                        logs.push(ScanLog {
                            timestamp: Local::now().format("%H:%M:%S").to_string(),
                            exchange: "binance".to_string(),
                            message: format!("Collected {} pairs", pair_count),
                            level: "debug".to_string(),
                        });
                        if logs.len() > 1000 {
                            logs.drain(0..logs.len() - 1000);
                        }
                    }

                    tokio::time::sleep(Duration::from_millis(10)).await;
                }

                let mut logs = logs_clone.lock().await;
                logs.push(ScanLog {
                    timestamp: Local::now().format("%H:%M:%S").to_string(),
                    exchange: "binance".to_string(),
                    message: format!("Collection complete: {} pairs", pair_count),
                    level: "success".to_string(),
                });
                if logs.len() > 1000 {
                    logs.drain(0..logs.len() - 1000);
                }
            }
            Ok(Err(e)) => {
                let mut logs = logs_clone.lock().await;
                logs.push(ScanLog {
                    timestamp: Local::now().format("%H:%M:%S").to_string(),
                    exchange: "binance".to_string(),
                    message: format!("Connection failed: {}", e),
                    level: "error".to_string(),
                });
                if logs.len() > 1000 {
                    logs.drain(0..logs.len() - 1000);
                }
            }
            Err(_) => {
                let mut logs = logs_clone.lock().await;
                logs.push(ScanLog {
                    timestamp: Local::now().format("%H:%M:%S").to_string(),
                    exchange: "binance".to_string(),
                    message: "Connection timeout".to_string(),
                    level: "error".to_string(),
                });
                if logs.len() > 1000 {
                    logs.drain(0..logs.len() - 1000);
                }
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

    pub fn get_data(&self) -> Arc<Mutex<HashMap<String, (f64, f64, i64)>>> {
        self.collected_data.clone()
    }

    pub fn get_logs(&self) -> Arc<Mutex<Vec<ScanLog>>> {
        self.logs.clone()
    }
}

// ==================== Bybit WebSocket Collector ====================

pub struct BybitCollector {
    collected_data: Arc<Mutex<HashMap<String, (f64, f64, i64)>>>,
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

        // Clear previous data
        let mut data = self.collected_data.lock().await;
        data.clear();
        drop(data);

        let mut logs = self.logs.lock().await;
        logs.clear();
        logs.push(ScanLog {
            timestamp: Local::now().format("%H:%M:%S").to_string(),
            exchange: "bybit".to_string(),
            message: format!("Starting Bybit collection ({}s)", duration_secs),
            level: "info".to_string(),
        });
        if logs.len() > 1000 {
            logs.drain(0..logs.len() - 1000);
        }
        drop(logs);

        let data_clone = self.collected_data.clone();
        let logs_clone = self.logs.clone();

        let ws_url = "wss://stream.bybit.com/v5/public/spot";

        let connect_future = connect_async(ws_url);
        let connect_result = timeout(Duration::from_secs(10), connect_future).await;

        match connect_result {
            Ok(Ok((ws_stream, _))) => {
                let (mut write, mut read) = ws_stream.split();

                // Subscribe to all tickers - Fixed: use tickers.* for all
                let subscribe_msg = serde_json::json!({
                    "op": "subscribe",
                    "args": ["tickers.*"]
                });

                if let Ok(msg_str) = serde_json::to_string(&subscribe_msg) {
                    let _ = write.send(Message::Text(msg_str)).await;
                }

                let mut logs = logs_clone.lock().await;
                logs.push(ScanLog {
                    timestamp: Local::now().format("%H:%M:%S").to_string(),
                    exchange: "bybit".to_string(),
                    message: "WebSocket connected".to_string(),
                    level: "success".to_string(),
                });
                if logs.len() > 1000 {
                    logs.drain(0..logs.len() - 1000);
                }
                drop(logs);

                let mut pair_count = 0;
                let mut last_ping = Instant::now();

                while let Ok(Some(msg)) = timeout(Duration::from_secs(5), read.next()).await {
                    if Instant::now() >= deadline {
                        break;
                    }

                    // Periodic ping
                    if last_ping.elapsed() > Duration::from_secs(30) {
                        let _ = write.send(Message::Ping(vec![])).await;
                        last_ping = Instant::now();
                    }

                    match msg {
                        Ok(Message::Text(text)) => {
                            if let Ok(msg_data) = serde_json::from_str::<serde_json::Value>(&text) {
                                // Bybit sends tickers with topic field starting with "tickers."
                                if let Some(topic) = msg_data.get("topic").and_then(|t| t.as_str()) {
                                    if topic.starts_with("tickers.") {
                                        // The actual data is nested in a "data" field
                                        if let Some(data) = msg_data.get("data") {
                                            if let (Some(symbol), Some(bid), Some(ask)) = (
                                                data.get("symbol").and_then(|s| s.as_str()),
                                                data.get("bid1Price").and_then(|b| b.as_str()).and_then(|s| s.parse::<f64>().ok()),
                                                data.get("ask1Price").and_then(|a| a.as_str()).and_then(|s| s.parse::<f64>().ok()),
                                            ) {
                                                if parse_symbol(symbol).is_some() {
                                                    let mut data_map = data_clone.lock().await;
                                                    data_map.insert(symbol.to_string(), (bid, ask, Utc::now().timestamp_millis()));
                                                    pair_count += 1;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Ok(Message::Ping(data)) => {
                            let _ = write.send(Message::Pong(data)).await;
                            last_ping = Instant::now();
                        }
                        Ok(Message::Pong(_)) => {
                            last_ping = Instant::now();
                        }
                        Err(e) => {
                            let mut logs = logs_clone.lock().await;
                            logs.push(ScanLog {
                                timestamp: Local::now().format("%H:%M:%S").to_string(),
                                exchange: "bybit".to_string(),
                                message: format!("WebSocket error: {}", e),
                                level: "error".to_string(),
                            });
                            if logs.len() > 1000 {
                                logs.drain(0..logs.len() - 1000);
                            }
                            break;
                        }
                        _ => {}
                    }
                }

                let mut logs = logs_clone.lock().await;
                logs.push(ScanLog {
                    timestamp: Local::now().format("%H:%M:%S").to_string(),
                    exchange: "bybit".to_string(),
                    message: format!("Collection complete: {} pairs", pair_count),
                    level: "success".to_string(),
                });
                if logs.len() > 1000 {
                    logs.drain(0..logs.len() - 1000);
                }
            }
            Ok(Err(e)) => {
                let mut logs = logs_clone.lock().await;
                logs.push(ScanLog {
                    timestamp: Local::now().format("%H:%M:%S").to_string(),
                    exchange: "bybit".to_string(),
                    message: format!("Connection failed: {}", e),
                    level: "error".to_string(),
                });
                if logs.len() > 1000 {
                    logs.drain(0..logs.len() - 1000);
                }
            }
            Err(_) => {
                let mut logs = logs_clone.lock().await;
                logs.push(ScanLog {
                    timestamp: Local::now().format("%H:%M:%S").to_string(),
                    exchange: "bybit".to_string(),
                    message: "Connection timeout".to_string(),
                    level: "error".to_string(),
                });
                if logs.len() > 1000 {
                    logs.drain(0..logs.len() - 1000);
                }
            }
        }

        let final_data = self.collected_data.lock().await;
        let pairs_collected = final_data.len();
        drop(final_data);

        ScanSummary {
            exchange: "bybit".to_string(),
            pairs_collected,
            paths_checked: 0,
            valid_triangles: 0,
            profitable_triangles: 0,
            collection_time_secs: start_time.elapsed().as_secs(),
        }
    }

    pub fn get_data(&self) -> Arc<Mutex<HashMap<String, (f64, f64, i64)>>> {
        self.collected_data.clone()
    }

    pub fn get_logs(&self) -> Arc<Mutex<Vec<ScanLog>>> {
        self.logs.clone()
    }
}

// ==================== KuCoin WebSocket Collector ====================
pub struct KuCoinCollector {
    collected_data: Arc<Mutex<HashMap<String, (f64, f64, i64)>>>,
    logs: Arc<Mutex<Vec<ScanLog>>>,
}

impl KuCoinCollector {
    pub fn new() -> Self {
        Self {
            collected_data: Arc::new(Mutex::new(HashMap::new())),
            logs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    async fn get_websocket_token(&self) -> Result<(String, String), Box<dyn std::error::Error>> {
        // KuCoin requires getting a token from REST API first
        let client = reqwest::Client::new();
        let res = client.post("https://api.kucoin.com/api/v1/bullet-public")
            .send()
            .await?
            .json::<KuCoinTokenResponse>()
            .await?;

        let token = res.data.token;
        let endpoint = res.data.instanceServers.get(0)
            .map(|s| s.endpoint.clone())
            .unwrap_or_else(|| "wss://ws-api.kucoin.com/endpoint".to_string());

        Ok((token, endpoint))
    }

    pub async fn start_collection(&self, duration_secs: u64) -> ScanSummary {
        let start_time = Instant::now();
        let deadline = start_time + Duration::from_secs(duration_secs);

        // Clear previous data
        let mut data = self.collected_data.lock().await;
        data.clear();
        drop(data);

        let mut logs = self.logs.lock().await;
        logs.clear();
        logs.push(ScanLog {
            timestamp: Local::now().format("%H:%M:%S").to_string(),
            exchange: "kucoin".to_string(),
            message: format!("Starting KuCoin collection ({}s)", duration_secs),
            level: "info".to_string(),
        });
        if logs.len() > 1000 {
            logs.drain(0..logs.len() - 1000);
        }
        drop(logs);

        let data_clone = self.collected_data.clone();
        let logs_clone = self.logs.clone();

        // Get dynamic WebSocket URL with token
        let (token, endpoint) = match self.get_websocket_token().await {
            Ok((t, e)) => (t, e),
            Err(e) => {
                let mut logs = logs_clone.lock().await;
                logs.push(ScanLog {
                    timestamp: Local::now().format("%H:%M:%S").to_string(),
                    exchange: "kucoin".to_string(),
                    message: format!("Failed to get WebSocket token: {}", e),
                    level: "error".to_string(),
                });
                if logs.len() > 1000 {
                    logs.drain(0..logs.len() - 1000);
                }
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

                let mut logs = logs_clone.lock().await;
                logs.push(ScanLog {
                    timestamp: Local::now().format("%H:%M:%S").to_string(),
                    exchange: "kucoin".to_string(),
                    message: "WebSocket connected".to_string(),
                    level: "success".to_string(),
                });
                if logs.len() > 1000 {
                    logs.drain(0..logs.len() - 1000);
                }
                drop(logs);

                // Subscribe to all tickers
                let subscribe_msg = serde_json::json!({
                    "id": "1",
                    "type": "subscribe",
                    "topic": "/market/ticker:all",
                    "privateChannel": false,
                    "response": true
                });

                if let Ok(msg_str) = serde_json::to_string(&subscribe_msg) {
                    let _ = write.send(Message::Text(msg_str)).await;
                }

                let mut pair_count = 0;
                let mut last_ping = Instant::now();

                while let Ok(Some(msg)) = timeout(Duration::from_secs(5), read.next()).await {
                    if Instant::now() >= deadline {
                        break;
                    }

                    // KuCoin requires ping every 18-20 seconds
                    if last_ping.elapsed() > Duration::from_secs(15) {
                        let ping_msg = serde_json::json!({
                            "id": "ping",
                            "type": "ping"
                        });
                        if let Ok(ping_str) = serde_json::to_string(&ping_msg) {
                            let _ = write.send(Message::Text(ping_str)).await;
                        }
                        last_ping = Instant::now();
                    }

                    match msg {
                        Ok(Message::Text(text)) => {
                            if let Ok(msg_data) = serde_json::from_str::<serde_json::Value>(&text) {
                                // Handle welcome message
                                if msg_data.get("type").and_then(|t| t.as_str()) == Some("welcome") {
                                    continue;
                                }

                                // Handle pong
                                if msg_data.get("type").and_then(|t| t.as_str()) == Some("pong") {
                                    continue;
                                }

                                // KuCoin sends ticker data in messages with type "message"
                                if msg_data.get("type").and_then(|t| t.as_str()) == Some("message") {
                                    if let Some(data) = msg_data.get("data") {
                                        // KuCoin uses hyphens in symbols: BTC-USDT
                                        if let (Some(symbol), Some(best_bid), Some(best_ask)) = (
                                            data.get("symbol").and_then(|s| s.as_str()),
                                            data.get("bestBid").and_then(|b| b.as_str()).and_then(|s| s.parse::<f64>().ok()),
                                            data.get("bestAsk").and_then(|a| a.as_str()).and_then(|s| s.parse::<f64>().ok()),
                                        ) {
                                            // Convert KuCoin's format (BTC-USDT) to standard (BTCUSDT)
                                            let std_symbol = symbol.replace("-", "");
                                            if parse_symbol(&std_symbol).is_some() {
                                                let mut data_map = data_clone.lock().await;
                                                data_map.insert(std_symbol, (best_bid, best_ask, Utc::now().timestamp_millis()));
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
                            logs.push(ScanLog {
                                timestamp: Local::now().format("%H:%M:%S").to_string(),
                                exchange: "kucoin".to_string(),
                                message: format!("WebSocket error: {}", e),
                                level: "error".to_string(),
                            });
                            if logs.len() > 1000 {
                                logs.drain(0..logs.len() - 1000);
                            }
                            break;
                        }
                        _ => {}
                    }
                }

                let mut logs = logs_clone.lock().await;
                logs.push(ScanLog {
                    timestamp: Local::now().format("%H:%M:%S").to_string(),
                    exchange: "kucoin".to_string(),
                    message: format!("Collection complete: {} pairs", pair_count),
                    level: "success".to_string(),
                });
                if logs.len() > 1000 {
                    logs.drain(0..logs.len() - 1000);
                }
            }
            Ok(Err(e)) => {
                let mut logs = logs_clone.lock().await;
                logs.push(ScanLog {
                    timestamp: Local::now().format("%H:%M:%S").to_string(),
                    exchange: "kucoin".to_string(),
                    message: format!("Connection failed: {}", e),
                    level: "error".to_string(),
                });
                if logs.len() > 1000 {
                    logs.drain(0..logs.len() - 1000);
                }
            }
            Err(_) => {
                let mut logs = logs_clone.lock().await;
                logs.push(ScanLog {
                    timestamp: Local::now().format("%H:%M:%S").to_string(),
                    exchange: "kucoin".to_string(),
                    message: "Connection timeout".to_string(),
                    level: "error".to_string(),
                });
                if logs.len() > 1000 {
                    logs.drain(0..logs.len() - 1000);
                }
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

    pub fn get_data(&self) -> Arc<Mutex<HashMap<String, (f64, f64, i64)>>> {
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

    fn build_graph(&self, tickers: &HashMap<String, (f64, f64, i64)>) -> (DiGraph<String, f64>, HashMap<String, NodeIndex>) {
        let mut graph = DiGraph::<String, f64>::new();
        let mut node_indices = HashMap::new();
        let mut currencies = HashSet::new();

        // Collect all currencies first
        for symbol in tickers.keys() {
            if let Some((base, quote)) = parse_symbol(symbol) {
                currencies.insert(base);
                currencies.insert(quote);
            }
        }

        // Add nodes
        for currency in currencies {
            let idx = graph.add_node(currency.clone());
            node_indices.insert(currency, idx);
        }

        // Add edges - only keep best price per direction to avoid duplicates
        let mut best_edges: HashMap<(NodeIndex, NodeIndex), f64> = HashMap::new();

        for (symbol, (bid, ask, _)) in tickers {
            if let Some((base, quote)) = parse_symbol(symbol) {
                if let (Some(&base_idx), Some(&quote_idx)) = (node_indices.get(&base), node_indices.get(&quote)) {
                    // Base -> Quote (selling base for quote): use bid price
                    if *bid > 0.0 {
                        let weight = -bid.ln();
                        let key = (base_idx, quote_idx);
                        if let Some(&existing) = best_edges.get(&key) {
                            if weight < existing {
                                best_edges.insert(key, weight);
                            }
                        } else {
                            best_edges.insert(key, weight);
                        }
                    }
                    // Quote -> Base (buying base with quote): use ask price
                    if *ask > 0.0 && *ask > 0.0 {
                        let weight = -(1.0 / ask).ln();
                        let key = (quote_idx, base_idx);
                        if let Some(&existing) = best_edges.get(&key) {
                            if weight < existing {
                                best_edges.insert(key, weight);
                            }
                        } else {
                            best_edges.insert(key, weight);
                        }
                    }
                }
            }
        }

        // Add best edges to graph
        for ((from, to), weight) in best_edges {
            graph.add_edge(from, to, weight);
        }

        (graph, node_indices)
    }

    fn calculate_execution_chance(&self, path: &[String]) -> f64 {
        // More realistic: longer paths = lower chance due to timing risk
        match path.len() {
            0..=2 => 0.0, // Not a cycle
            3 => 85.0,    // A->B->C->A
            4 => 75.0,    // A->B->C->D->A
            5 => 60.0,    // Longer cycles
            _ => 50.0,    // Very long cycles
        }
    }

    fn calculate_real_profit(&self, path: &[String], tickers: &HashMap<String, (f64, f64, i64)>) -> f64 {
        if path.len() < 3 || path.first() != path.last() {
            return -100.0;
        }

        let mut amount = 1.0;
        let mut trade_count = 0;

        for i in 0..path.len() - 1 {
            let from = &path[i];
            let to = &path[i + 1];

            // Try direct pair: FROMTO (selling FROM for TO)
            let pair = format!("{}{}", from, to);
            if let Some((bid, _, timestamp)) = tickers.get(&pair) {
                // Check if data is fresh (within last 5 seconds)
                let age_ms = Utc::now().timestamp_millis() - timestamp;
                if age_ms > 5000 {
                    return -100.0; // Stale data
                }
                if *bid > 0.0 {
                    amount *= bid;
                    trade_count += 1;
                    continue;
                }
            }

            // Try reverse pair: TOFROM (buying FROM with TO)
            let reverse_pair = format!("{}{}", to, from);
            if let Some((_, ask, timestamp)) = tickers.get(&reverse_pair) {
                let age_ms = Utc::now().timestamp_millis() - timestamp;
                if age_ms > 5000 {
                    return -100.0;
                }
                if *ask > 0.0 {
                    amount *= 1.0 / ask;
                    trade_count += 1;
                    continue;
                }
            }

            return -100.0; // Missing pair
        }

        // Account for trading fees (0.1% per trade typical)
        let fee_rate = 0.001;
        let total_fees = 1.0 - (trade_count as f64 * fee_rate);
        let net_profit = (amount * total_fees - 1.0) * 100.0;

        net_profit
    }

    fn find_opportunities(&self, graph: &DiGraph<String, f64>, node_indices: &HashMap<String, NodeIndex>, 
                         tickers: &HashMap<String, (f64, f64, i64)>, min_profit: f64) -> (Vec<ArbitrageOpportunity>, usize, usize, usize) {
        let mut opportunities = Vec::new();
        let mut paths_checked = 0;
        let mut valid_triangles = 0;

        // Check all nodes as starting points, not just first 30
        let nodes: Vec<_> = graph.node_indices().collect();

        for &start_node in &nodes {
            match bellman_ford(graph, start_node) {
                Ok(paths) => {
                    let distances = paths.distances;
                    let predecessors = paths.predecessors;

                    // Check all edges for negative cycles
                    for edge in graph.edge_indices() {
                        let (u, v) = graph.edge_endpoints(edge).unwrap();
                        let weight = graph[edge];

                        paths_checked += 1;

                        // Relaxation condition for negative cycle detection
                        if distances[u.index()] + weight < distances[v.index()] - 1e-12 {
                            // Try to reconstruct cycle
                            if let Some(cycle) = self.reconstruct_cycle(&predecessors, v, graph) {
                                if cycle.len() >= 3 && cycle.len() <= 6 {
                                    valid_triangles += 1;
                                    let path: Vec<String> = cycle.iter().map(|&idx| graph[idx].clone()).collect();

                                    // Ensure cycle is closed
                                    if path.first() == path.last() {
                                        let profit = self.calculate_real_profit(&path, tickers);

                                        // More realistic profit bounds
                                        if profit > min_profit && profit < 50.0 && profit.is_finite() {
                                            let chance = self.calculate_execution_chance(&path);
                                            let pair = path.join(" → ");

                                            // Estimate slippage based on path length
                                            let slippage = (path.len() as f64 - 2.0) * 0.05; // 5% per hop

                                            opportunities.push(ArbitrageOpportunity {
                                                pair,
                                                triangle: path.clone(),
                                                profit_margin_before: profit,
                                                profit_margin_after: profit * (1.0 - slippage),
                                                chance_of_executing: chance,
                                                timestamp: Utc::now().timestamp_millis(),
                                                exchange: "unknown".to_string(),
                                                estimated_slippage: profit * slippage,
                                            });
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Err(_) => {
                    // Negative cycle detected during initial run
                    // Note: petgraph's bellman_ford returns Err for negative cycles
                }
            }
        }

        // Deduplicate opportunities using canonical form
        let mut seen = HashSet::new();
        opportunities.retain(|opp| {
            let mut cycle = opp.triangle.clone();
            // Rotate to start with smallest element for canonical form
            if let Some(min_pos) = cycle.iter().enumerate().take(cycle.len() - 1).min_by_key(|(_, x)| *x).map(|(i, _)| i) {
                let mut canonical = cycle[min_pos..cycle.len() - 1].to_vec();
                canonical.extend_from_slice(&cycle[0..min_pos]);
                let key = canonical.join(",");
                if seen.contains(&key) {
                    false
                } else {
                    seen.insert(key);
                    true
                }
            } else {
                false
            }
        });

        // Sort by profit descending
        opportunities.sort_by(|a, b| b.profit_margin_before.partial_cmp(&a.profit_margin_before).unwrap_or(std::cmp::Ordering::Equal));

        let profitable = opportunities.len();
        (opportunities, paths_checked, valid_triangles, profitable)
    }

    fn reconstruct_cycle(&self, predecessors: &[Option<NodeIndex>], start: NodeIndex, graph: &DiGraph<String, f64>) -> Option<Vec<NodeIndex>> {
        let mut path = Vec::new();
        let mut visited = HashSet::new();
        let mut current = start;

        // Walk backwards from start
        for _ in 0..predecessors.len() + 1 {
            if visited.contains(&current) {
                // Found cycle, extract it
                let cycle_start = path.iter().position(|&x| x == current).unwrap_or(0);
                let cycle: Vec<_> = path[cycle_start..].to_vec();
                if cycle.len() >= 3 {
                    // Verify it's a cycle
                    if let Some(&first) = cycle.first() {
                        if let Some(&last) = cycle.last() {
                            if first == last {
                                return Some(cycle);
                            }
                        }
                    }
                }
                return None;
            }

            visited.insert(current);
            path.push(current);

            if let Some(prev) = predecessors[current.index()] {
                current = prev;
            } else {
                break;
            }
        }

        None
    }

    pub async fn scan_exchange(&self, exchange: &str, min_profit: f64, duration_secs: u64) 
        -> (Vec<ArbitrageOpportunity>, ScanSummary, Vec<ScanLog>) {

        info!("🔍 Scanning {} for {}s", exchange, duration_secs);

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

        info!("📊 {} pairs collected from {}", tickers.len(), exchange);

        if tickers.is_empty() {
            return (Vec::new(), summary, scan_logs);
        }

        let (graph, node_indices) = self.build_graph(&tickers);
        info!("📈 Graph: {} nodes, {} edges", graph.node_count(), graph.edge_count());

        let (opportunities, paths_checked, valid_triangles, profitable) = 
            self.find_opportunities(&graph, &node_indices, &tickers, min_profit);

        info!("📊 Results: {} paths, {} valid triangles, {} profitable", 
                 paths_checked, valid_triangles, profitable);

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

        info!("🚀 Multi-exchange scan started");
        let mut all_opportunities = Vec::new();
        let mut all_summaries = Vec::new();
        let mut all_logs = Vec::new();

        for exchange in exchanges {
            let (opps, summary, logs) = self.scan_exchange(&exchange, min_profit, duration_secs).await;
            all_opportunities.extend(opps);
            all_summaries.push(summary);
            all_logs.extend(logs);
        }

        // Sort by profit
        all_opportunities.sort_by(|a, b| b.profit_margin_before.partial_cmp(&a.profit_margin_before).unwrap_or(std::cmp::Ordering::Equal));

        info!("✅ Scan complete: {} opportunities", all_opportunities.len());

        ScanResponse {
            opportunities: all_opportunities,
            summaries: all_summaries,
            logs: all_logs,
        }
    }
}

// ==================== API Handlers ====================

async fn scan_handler(req: web::Json<ScanRequest>) -> impl Responder {
    info!("📨 Scan request: {:?}", req.exchanges);

    // Input validation
    if req.exchanges.is_empty() {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "No exchanges selected",
            "opportunities": [],
            "summaries": [],
            "logs": []
        }));
    }

    // Validate exchange names
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

    // Validate min_profit
    let min_profit = req.min_profit.unwrap_or(0.3);
    if min_profit < 0.0 || min_profit > 100.0 {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "min_profit must be between 0 and 100",
            "opportunities": [],
            "summaries": [],
            "logs": []
        }));
    }

    // Validate duration
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
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let bind_addr = format!("0.0.0.0:{}", port);

    info!("🚀 Server starting on {}", bind_addr);
    info!("📡 Exchanges: binance, bybit, kucoin");

    HttpServer::new(|| {
        let cors = Cors::default()
            .allow_any_origin()
            .allow_any_method()
            .allow_any_header()
            .max_age(3600);

        App::new()
            .wrap(cors)  // FIXED: Actually apply CORS middleware
            .route("/health", web::get().to(health_handler))
            .route("/api/scan", web::post().to(scan_handler))
            .service(fs::Files::new("/", "./static").index_file("index.html"))
    })
    .bind(&bind_addr)?
    .run()
    .await
}
