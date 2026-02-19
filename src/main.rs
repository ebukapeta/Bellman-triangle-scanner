// src/main.rs - Backend API only
#![warn(clippy::all)]

use actix_web::{web, App, HttpServer, HttpResponse, Responder};
use actix_cors::Cors;
use actix_files as fs;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant};
use chrono::{Utc, Local};
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::algo::bellman_ford;
use petgraph::algo::find_negative_cycle;
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
    pub profitable_triangles: usize,
    pub collection_time_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanResponse {
    pub opportunities: Vec<ArbitrageOpportunity>,
    pub summaries: Vec<ScanSummary>,
    pub logs: Vec<ScanLog>,
}

// ==================== WebSocket Collectors ====================
pub struct BinanceWebSocketCollector {
    collected_data: Arc<Mutex<HashMap<String, (f64, f64, i64)>>>,
    logs: Arc<Mutex<Vec<ScanLog>>>,
}

impl BinanceWebSocketCollector {
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

    fn parse_symbol(symbol: &str) -> Option<(String, String)> {
       let s = symbol.to_uppercase();
    
    // Common quote currencies (stablecoins and major coins)
       const QUOTES: [&str; 30] = [
           "USDT", "BUSD", "USDC", "FDUSD", "TUSD", "DAI", 
           "BTC", "ETH", "BNB", "SOL", "XRP", "ADA", "DOGE",
           "TRY", "EUR", "GBP", "AUD", "BRL", "CAD", "ARS", 
           "RUB", "ZAR", "NGN", "UAH", "IDR", "JPY", "KRW", 
           "VND", "MXN", "CHF"
       ];

       for q in &QUOTES {
           if s.ends_with(*q) && s.len() > q.len() {
               let base = s[..s.len() - q.len()].to_string();
            // Avoid cases where base is empty or too short
               if !base.is_empty() && base.len() >= 2 {
                   return Some((base, q.to_string()));
               }
           }
       }

    // Fallback: try to split by common patterns
       if s.len() > 6 {
           let try3 = s.split_at(s.len() - 3);
           if try3.1.chars().all(|c| c.is_ascii_alphabetic()) {
               return Some((try3.0.to_string(), try3.1.to_string()));
           }
       }
       if s.len() > 7 {
           let try4 = s.split_at(s.len() - 4);
           if try4.1.chars().all(|c| c.is_ascii_alphabetic()) {
               return Some((try4.0.to_string(), try4.1.to_string()));
           }
       }
       if s.len() > 8 {
           let try5 = s.split_at(s.len() - 5);
           if try5.1.chars().all(|c| c.is_ascii_alphabetic()) {
               return Some((try5.0.to_string(), try5.1.to_string()));
           }
       }
    
       None
    }

    pub async fn start_collection(&self, duration_secs: u64) -> ScanSummary {
        let start_time = Instant::now();
        let deadline = Instant::now() + Duration::from_secs(duration_secs);
        
        // Clear previous data
        let mut data = self.collected_data.lock().await;
        data.clear();
        drop(data);

        let mut logs = self.logs.lock().await;
        logs.clear();
        logs.push(ScanLog {
            timestamp: Local::now().format("%H:%M:%S").to_string(),
            exchange: "binance".to_string(),
            message: format!("Starting Binance WebSocket collection for {} seconds", duration_secs),
            level: "info".to_string(),
        });
        drop(logs);

        let data_clone = self.collected_data.clone();
        let logs_clone = self.logs.clone();

        // Connect to WebSocket
        let ws_url = "wss://stream.binance.com:9443/ws/!ticker@arr";
        
        match connect_async(ws_url).await {
            Ok((mut ws_stream, _)) => {
                logs_clone.lock().await.push(ScanLog {
                    timestamp: Local::now().format("%H:%M:%S").to_string(),
                    exchange: "binance".to_string(),
                    message: "Binance WebSocket connected".to_string(),
                    level: "success".to_string(),
                });

                let mut pair_count = 0;
                let mut last_log_time = Instant::now();

                while let Some(msg) = ws_stream.next().await {
                    if Instant::now() >= deadline {
                        break;
                    }

                    match msg {
                        Ok(m) if m.is_text() => {
                            if let Ok(txt) = m.into_text() {
                                match serde_json::from_str::<serde_json::Value>(&txt) {
                                    Ok(serde_json::Value::Array(arr)) => {
                                        for item in arr {
                                            let symbol = item.get("s").and_then(|v| v.as_str());
                                            let bid = Self::parse_f64(item.get("b"));
                                            let ask = Self::parse_f64(item.get("a"));
                                            
                                            if let (Some(sym), Some(bid_price), Some(ask_price)) = (symbol, bid, ask) {
                                                if let Some((_base, _quote)) = Self::parse_symbol(sym) {
                                                    let mut data = data_clone.lock().await;
                                                    data.insert(sym.to_string(), (bid_price, ask_price, chrono::Utc::now().timestamp_millis()));
                                                    pair_count += 1;
                                                }
                                            }
                                        }
                                    }
                                    Ok(serde_json::Value::Object(obj)) => {
                                        // Single ticker object
                                        let symbol = obj.get("s").and_then(|v| v.as_str());
                                        let bid = Self::parse_f64(obj.get("b"));
                                        let ask = Self::parse_f64(obj.get("a"));
                                        
                                        if let (Some(sym), Some(bid_price), Some(ask_price)) = (symbol, bid, ask) {
                                            if let Some((_base, _quote)) = Self::parse_symbol(sym) {
                                                let mut data = data_clone.lock().await;
                                                data.insert(sym.to_string(), (bid_price, ask_price, chrono::Utc::now().timestamp_millis()));
                                                pair_count += 1;
                                            }
                                        }
                                    }
                                    _ => {
                                        logs_clone.lock().await.push(ScanLog {
                                            timestamp: Local::now().format("%H:%M:%S").to_string(),
                                            exchange: "binance".to_string(),
                                            message: "Received non-object/array message".to_string(),
                                            level: "debug".to_string(),
                                        });
                                    }
                                }
                            }
                        }
                        Ok(_) => {} // Ignore other message types
                        Err(e) => {
                            logs_clone.lock().await.push(ScanLog {
                                timestamp: Local::now().format("%H:%M:%S").to_string(),
                                exchange: "binance".to_string(),
                                message: format!("WebSocket error: {}", e),
                                level: "error".to_string(),
                            });
                            break;
                        }
                    }

                    // Progress logging
                    if pair_count > 0 && (pair_count % 100 == 0 || last_log_time.elapsed() > Duration::from_secs(2)) {
                        logs_clone.lock().await.push(ScanLog {
                            timestamp: Local::now().format("%H:%M:%S").to_string(),
                            exchange: "binance".to_string(),
                            message: format!("Collected {} unique pairs...", pair_count),
                            level: "debug".to_string(),
                        });
                        last_log_time = Instant::now();
                    }

                    // Small delay to prevent CPU overload
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }

                logs_clone.lock().await.push(ScanLog {
                    timestamp: Local::now().format("%H:%M:%S").to_string(),
                    exchange: "binance".to_string(),
                    message: format!("Collection complete. Total unique pairs: {}", pair_count),
                    level: "success".to_string(),
                });
            }
            Err(e) => {
                logs_clone.lock().await.push(ScanLog {
                    timestamp: Local::now().format("%H:%M:%S").to_string(),
                    exchange: "binance".to_string(),
                    message: format!("Connection failed: {}", e),
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

pub struct BybitWebSocketCollector {
    collected_data: Arc<Mutex<HashMap<String, (f64, f64, i64)>>>,
    logs: Arc<Mutex<Vec<ScanLog>>>,
}

impl BybitWebSocketCollector {
    pub fn new() -> Self {
        Self {
            collected_data: Arc::new(Mutex::new(HashMap::new())),
            logs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn start_collection(&self, duration_secs: u64) -> ScanSummary {
        let start_time = Instant::now();
        let end_time = start_time + Duration::from_secs(duration_secs);
        
        // Clear previous data
        let mut data = self.collected_data.lock().await;
        data.clear();
        drop(data);

        let mut logs = self.logs.lock().await;
        logs.clear();
        logs.push(ScanLog {
            timestamp: Local::now().format("%H:%M:%S").to_string(),
            exchange: "bybit".to_string(),
            message: format!("Starting Bybit WebSocket collection for {} seconds", duration_secs),
            level: "info".to_string(),
        });
        drop(logs);

        let data_clone = self.collected_data.clone();
        let logs_clone = self.logs.clone();

        // Connect to WebSocket
        let ws_url = "wss://stream.bybit.com/v5/public/spot";
        
        match connect_async(ws_url).await {
            Ok((ws_stream, _)) => {
                let (mut write, mut read) = ws_stream.split();
                
                // Subscribe to tickers
                let subscribe_msg = serde_json::json!({
                    "op": "subscribe",
                    "args": ["tickers"]
                });
                
                if let Ok(msg_str) = serde_json::to_string(&subscribe_msg) {
                    let _ = write.send(Message::Text(msg_str)).await;
                }

                logs_clone.lock().await.push(ScanLog {
                    timestamp: Local::now().format("%H:%M:%S").to_string(),
                    exchange: "bybit".to_string(),
                    message: "Bybit WebSocket connected and subscribed".to_string(),
                    level: "success".to_string(),
                });

                let mut pair_count = 0;

                // Collect data until end_time
                while Instant::now() < end_time {
                    match tokio::time::timeout(Duration::from_millis(100), read.next()).await {
                        Ok(Some(Ok(Message::Text(text)))) => {
                            if let Ok(ticker_data) = serde_json::from_str::<serde_json::Value>(&text) {
                                if ticker_data["topic"].as_str() == Some("tickers") {
                                    if let Some(data) = ticker_data["data"].as_object() {
                                        if let (Some(symbol), Some(bid), Some(ask)) = (
                                            data.get("symbol").and_then(|s| s.as_str()),
                                            data.get("bid1Price").and_then(|b| b.as_str()).and_then(|s| s.parse::<f64>().ok()),
                                            data.get("ask1Price").and_then(|a| a.as_str()).and_then(|s| s.parse::<f64>().ok()),
                                        ) {
                                            if symbol.ends_with("USDT") {
                                                let mut data_map = data_clone.lock().await;
                                                data_map.insert(symbol.to_string(), (bid, ask, chrono::Utc::now().timestamp_millis()));
                                                pair_count += 1;
                                                
                                                if pair_count % 50 == 0 {
                                                    logs_clone.lock().await.push(ScanLog {
                                                        timestamp: Local::now().format("%H:%M:%S").to_string(),
                                                        exchange: "bybit".to_string(),
                                                        message: format!("Collected {} pairs...", pair_count),
                                                        level: "debug".to_string(),
                                                    });
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Ok(Some(Ok(Message::Ping(data)))) => {
                            let _ = write.send(Message::Pong(data)).await;
                        }
                        Ok(Some(Err(e))) => {
                            logs_clone.lock().await.push(ScanLog {
                                timestamp: Local::now().format("%H:%M:%S").to_string(),
                                exchange: "bybit".to_string(),
                                message: format!("WebSocket error: {}", e),
                                level: "error".to_string(),
                            });
                            break;
                        }
                        _ => {} // Timeout or other messages - continue
                    }
                }

                logs_clone.lock().await.push(ScanLog {
                    timestamp: Local::now().format("%H:%M:%S").to_string(),
                    exchange: "bybit".to_string(),
                    message: format!("Collection complete. Total pairs: {}", pair_count),
                    level: "success".to_string(),
                });
            }
            Err(e) => {
                logs_clone.lock().await.push(ScanLog {
                    timestamp: Local::now().format("%H:%M:%S").to_string(),
                    exchange: "bybit".to_string(),
                    message: format!("Connection failed: {}", e),
                    level: "error".to_string(),
                });
            }
        }

        let final_data = self.collected_data.lock().await;
        let pairs_collected = final_data.len();
        drop(final_data);

        ScanSummary {
            exchange: "bybit".to_string(),
            pairs_collected,
            paths_found: 0,
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

pub struct KuCoinWebSocketCollector {
    collected_data: Arc<Mutex<HashMap<String, (f64, f64, i64)>>>,
    logs: Arc<Mutex<Vec<ScanLog>>>,
}

impl KuCoinWebSocketCollector {
    pub fn new() -> Self {
        Self {
            collected_data: Arc::new(Mutex::new(HashMap::new())),
            logs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn start_collection(&self, duration_secs: u64) -> ScanSummary {
        let start_time = Instant::now();
        let end_time = start_time + Duration::from_secs(duration_secs);
        
        // Clear previous data
        let mut data = self.collected_data.lock().await;
        data.clear();
        drop(data);

        let mut logs = self.logs.lock().await;
        logs.clear();
        logs.push(ScanLog {
            timestamp: Local::now().format("%H:%M:%S").to_string(),
            exchange: "kucoin".to_string(),
            message: format!("Starting KuCoin WebSocket collection for {} seconds", duration_secs),
            level: "info".to_string(),
        });
        drop(logs);

        let data_clone = self.collected_data.clone();
        let logs_clone = self.logs.clone();

        // First get WebSocket token from KuCoin REST API
        let client = reqwest::Client::new();
        let token_url = "https://api.kucoin.com/api/v1/bullet-public";
        
        match client.post(token_url).send().await {
            Ok(resp) => {
                if let Ok(token_data) = resp.json::<serde_json::Value>().await {
                    if let (Some(endpoint), Some(token)) = (
                        token_data["data"]["instanceServers"][0]["endpoint"].as_str(),
                        token_data["data"]["token"].as_str()
                    ) {
                        let ws_url = format!("{}?token={}", endpoint, token);
                        
                        match connect_async(&ws_url).await {
                            Ok((ws_stream, _)) => {
                                let (mut write, mut read) = ws_stream.split();
                                
                                // Subscribe to all tickers
                                let subscribe_msg = serde_json::json!({
                                    "id": 1,
                                    "type": "subscribe",
                                    "topic": "/market/ticker:all",
                                    "privateChannel": false,
                                    "response": true
                                });
                                
                                if let Ok(msg_str) = serde_json::to_string(&subscribe_msg) {
                                    let _ = write.send(Message::Text(msg_str)).await;
                                }

                                logs_clone.lock().await.push(ScanLog {
                                    timestamp: Local::now().format("%H:%M:%S").to_string(),
                                    exchange: "kucoin".to_string(),
                                    message: "KuCoin WebSocket connected and subscribed".to_string(),
                                    level: "success".to_string(),
                                });

                                let mut pair_count = 0;

                                // Collect data until end_time
                                while Instant::now() < end_time {
                                    match tokio::time::timeout(Duration::from_millis(100), read.next()).await {
                                        Ok(Some(Ok(Message::Text(text)))) => {
                                            if let Ok(ticker_data) = serde_json::from_str::<serde_json::Value>(&text) {
                                                if ticker_data["type"].as_str() == Some("message") {
                                                    if let Some(data) = ticker_data["data"].as_object() {
                                                        if let (Some(symbol), Some(bestBid), Some(bestAsk)) = (
                                                            data.get("symbol").and_then(|s| s.as_str()),
                                                            data.get("bestBid").and_then(|b| b.as_str()).and_then(|s| s.parse::<f64>().ok()),
                                                            data.get("bestAsk").and_then(|a| a.as_str()).and_then(|s| s.parse::<f64>().ok()),
                                                        ) {
                                                            if symbol.ends_with("USDT") {
                                                                let mut data_map = data_clone.lock().await;
                                                                data_map.insert(symbol.to_string(), (bestBid, bestAsk, chrono::Utc::now().timestamp_millis()));
                                                                pair_count += 1;
                                                                
                                                                if pair_count % 50 == 0 {
                                                                    logs_clone.lock().await.push(ScanLog {
                                                                        timestamp: Local::now().format("%H:%M:%S").to_string(),
                                                                        exchange: "kucoin".to_string(),
                                                                        message: format!("Collected {} pairs...", pair_count),
                                                                        level: "debug".to_string(),
                                                                    });
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        Ok(Some(Ok(Message::Ping(data)))) => {
                                            let _ = write.send(Message::Pong(data)).await;
                                        }
                                        Ok(Some(Err(e))) => {
                                            logs_clone.lock().await.push(ScanLog {
                                                timestamp: Local::now().format("%H:%M:%S").to_string(),
                                                exchange: "kucoin".to_string(),
                                                message: format!("WebSocket error: {}", e),
                                                level: "error".to_string(),
                                            });
                                            break;
                                        }
                                        _ => {} // Timeout or other messages - continue
                                    }
                                }

                                logs_clone.lock().await.push(ScanLog {
                                    timestamp: Local::now().format("%H:%M:%S").to_string(),
                                    exchange: "kucoin".to_string(),
                                    message: format!("Collection complete. Total pairs: {}", pair_count),
                                    level: "success".to_string(),
                                });
                            }
                            Err(e) => {
                                logs_clone.lock().await.push(ScanLog {
                                    timestamp: Local::now().format("%H:%M:%S").to_string(),
                                    exchange: "kucoin".to_string(),
                                    message: format!("WebSocket connection failed: {}", e),
                                    level: "error".to_string(),
                                });
                            }
                        }
                    } else {
                        logs_clone.lock().await.push(ScanLog {
                            timestamp: Local::now().format("%H:%M:%S").to_string(),
                            exchange: "kucoin".to_string(),
                            message: "Failed to parse WebSocket token".to_string(),
                            level: "error".to_string(),
                        });
                    }
                } else {
                    logs_clone.lock().await.push(ScanLog {
                        timestamp: Local::now().format("%H:%M:%S").to_string(),
                        exchange: "kucoin".to_string(),
                        message: "Failed to parse token response".to_string(),
                        level: "error".to_string(),
                    });
                }
            }
            Err(e) => {
                logs_clone.lock().await.push(ScanLog {
                    timestamp: Local::now().format("%H:%M:%S").to_string(),
                    exchange: "kucoin".to_string(),
                    message: format!("Failed to get WebSocket token: {}", e),
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
    binance_collector: BinanceWebSocketCollector,
    bybit_collector: BybitWebSocketCollector,
    kucoin_collector: KuCoinWebSocketCollector,
}

impl ArbitrageDetector {
    pub fn new() -> Self {
        Self {
            binance_collector: BinanceWebSocketCollector::new(),
            bybit_collector: BybitWebSocketCollector::new(),
            kucoin_collector: KuCoinWebSocketCollector::new(),
        }
    }

    fn parse_symbol(symbol: &str) -> Option<(String, String)> {
        if symbol.ends_with("USDT") {
            let base = symbol.trim_end_matches("USDT").to_string();
            Some((base, "USDT".to_string()))
        } else if symbol.ends_with("BUSD") {
            let base = symbol.trim_end_matches("BUSD").to_string();
            Some((base, "BUSD".to_string()))
        } else if symbol.ends_with("USDC") {
            let base = symbol.trim_end_matches("USDC").to_string();
            Some((base, "USDC".to_string()))
        } else {
            None
        }
    }

    fn build_graph(&self, tickers: &HashMap<String, (f64, f64, i64)>) -> (DiGraph<String, f64>, HashMap<String, NodeIndex>) {
        let mut graph = DiGraph::<String, f64>::new();
        let mut node_indices = HashMap::new();
        let mut currencies = HashSet::new();

        for symbol in tickers.keys() {
            if let Some((base, quote)) = Self::parse_symbol(symbol) {
                currencies.insert(base);
                currencies.insert(quote);
            }
        }

        for currency in currencies {
            let idx = graph.add_node(currency.clone());
            node_indices.insert(currency, idx);
        }

        for (symbol, (bid, ask, _)) in tickers {
            if let Some((base, quote)) = Self::parse_symbol(symbol) {
                if let (Some(&from_idx), Some(&to_idx)) = (node_indices.get(&base), node_indices.get(&quote)) {
                    if *bid > 0.0 {
                        let weight = -bid.ln();
                        graph.add_edge(from_idx, to_idx, weight);
                    }
                    if *ask > 0.0 {
                        let weight = -(1.0 / ask).ln();
                        graph.add_edge(to_idx, from_idx, weight);
                    }
                }
            }
        }

        (graph, node_indices)
    }

    fn calculate_execution_chance(&self, path: &[String], tickers: &HashMap<String, (f64, f64, i64)>) -> f64 {
        let mut score = 0.0;
        let mut count = 0;
        
        for i in 0..path.len() - 1 {
            let symbol = format!("{}{}", path[i], path[i + 1]);
            if let Some((bid, ask, timestamp)) = tickers.get(&symbol) {
                let spread = (ask - bid) / bid;
                let spread_score = 1.0 - (spread / 0.01).min(1.0);
                
                let age = (Utc::now().timestamp_millis() - timestamp) as f64 / 1000.0;
                let recency_score = 1.0 - (age / 5.0).min(1.0);
                
                score += spread_score * 0.6 + recency_score * 0.4;
                count += 1;
            }
        }
        
        if count == 0 { return 50.0; }
        
        let avg_score = score / count as f64;
        50.0 + (avg_score * 45.0)
    }

    // Add this method to ArbitrageDetector
    fn debug_graph(&self, graph: &DiGraph<String, f64>, node_indices: &HashMap<String, NodeIndex>, tickers: &HashMap<String, (f64, f64, i64)>) {
    println!("=== GRAPH DEBUG ===");
    println!("Nodes: {}", graph.node_count());
    println!("Edges: {}", graph.edge_count());
    
    // Show first 20 nodes as sample
    println!("Sample nodes (first 20):");
    let mut node_count = 0;
    for node in graph.node_indices() {
        if node_count >= 20 { break; }
        println!("  {}: {}", node.index(), graph[node]);
        node_count += 1;
    }
    
    // Show all edges for a few currencies to see cycles
    let test_currencies = vec!["BTC", "ETH", "BNB", "USDT"];
    for currency in test_currencies {
        if let Some(&idx) = node_indices.get(currency) {
            println!("Edges involving {}:", currency);
            for edge in graph.edges_directed(idx, petgraph::Direction::Outgoing) {
                let target = graph[edge.target()].clone();
                println!("  {} -> {} : weight = {:.6}", currency, target, *edge.weight());
            }
            for edge in graph.edges_directed(idx, petgraph::Direction::Incoming) {
                let source = graph[edge.source()].clone();
                println!("  {} <- {} : weight = {:.6}", currency, source, *edge.weight());
            }
        } else {
            println!("{} not found in graph nodes", currency);
            // Show what similar nodes exist
            println!("  Looking for similar nodes:");
            for node in graph.node_indices() {
                let name = &graph[node];
                if name.contains("BTC") || name.contains("ETH") || name.contains("BNB") {
                    println!("    Found: {}", name);
                }
            }
        }
    }
    
    // Manually check a few pairs to verify prices
    println!("Sample pair prices:");
    let sample_pairs = vec!["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"];
    for pair in sample_pairs {
        if let Some((bid, ask, ts)) = tickers.get(pair) {
            println!("  {}: bid={:.2}, ask={:.2}, spread={:.4}%", pair, bid, ask, (ask-bid)/bid*100.0);
        }
    }
    
    // Try to find any cycle by brute force for small subset
    println!("Checking for simple 3-currency cycles:");
    let top_nodes: Vec<_> = graph.node_indices().take(10).collect();
    for i in 0..top_nodes.len() {
        for j in 0..top_nodes.len() {
            if i == j { continue; }
            for k in 0..top_nodes.len() {
                if k == i || k == j { continue; }
                
                // Check if cycle exists
                let a = top_nodes[i];
                let b = top_nodes[j];
                let c = top_nodes[k];
                
                let ab = graph.find_edge(a, b);
                let bc = graph.find_edge(b, c);
                let ca = graph.find_edge(c, a);
                
                if let (Some(ab_e), Some(bc_e), Some(ca_e)) = (ab, bc, ca) {
                      let weight_sum = graph[ab_e] + graph[bc_e] + graph[ca_e];
                      let names = (graph[a].clone(), graph[b].clone(), graph[c].clone());
                      println!("  Cycle {}->{}->{}->{}: total weight = {:.6}", names.0, names.1, names.2, names.0, weight_sum);
                      if weight_sum < 0.0 {
                          let profit = (-weight_sum).exp() - 1.0;
                          println!("    🔥 NEGATIVE CYCLE! Profit: {:.4}%", profit * 100.0);
                      }
                  }
              }
          }
      }
    
      println!("=== END DEBUG ===");
    }

    fn find_profitable_triangles(&self, graph: &DiGraph<String, f64>, node_indices: &HashMap<String, NodeIndex>, 
                             tickers: &HashMap<String, (f64, f64, i64)>, min_profit: f64) -> (Vec<ArbitrageOpportunity>, usize, usize) {
       let mut opportunities = Vec::new();
       let mut total_paths_checked = 0;
    
       println!("🔎 Starting Bellman-Ford search...");
       let nodes: Vec<_> = graph.node_indices().collect();
       println!("Total nodes to check: {}", nodes.len());
    
       for (start_idx, &start_node) in nodes.iter().enumerate().take(20) {
           println!("\n--- Running Bellman-Ford from node {}: '{}' ---", start_idx, graph[start_node]);
        
           match bellman_ford(graph, start_node) {
               Ok(paths) => {
                   let distances = paths.distances;
                   let predecessors = paths.predecessors;
                
                // Check for negative cycles
                   for edge in graph.raw_edges() {
                       let u = edge.source();
                       let v = edge.target();
                    
                       if distances[u.index()] + edge.weight < distances[v.index()] - 1e-10 {
                           total_paths_checked += 1;
                        
                           if let Some(cycle_nodes) = self.reconstruct_cycle(&predecessors, v) {
    // Ensure cycle returns to start and has at least 3 nodes
                               if cycle_nodes.len() >= 3 && cycle_nodes.first() == cycle_nodes.last() {
                                  let path: Vec<String> = cycle_nodes.iter().map(|&idx| graph[idx].clone()).collect();
                                  println!("  🔥 Found cycle: {:?}", path);
        
        // Calculate REAL profit
                                  let profit = self.calculate_real_profit(&path, tickers);
        
        // Only include if profit is positive and reasonable
                                  if profit > min_profit && profit < 100.0 {
                                  println!("  ✅ Valid profit: {:.4}%", profit);
            
                                     let chance = self.calculate_execution_chance(&path, tickers);
                                     let pair = path.join(" → ");
            
                                     opportunities.push(ArbitrageOpportunity {
                                        pair,
                                        triangle: path,
                                        profit_margin_before: profit,
                                        profit_margin_after: profit * 0.9,
                                        chance_of_executing: chance,
                                        timestamp: Utc::now().timestamp_millis(),
                                        exchange: "unknown".to_string(),
                                        estimated_slippage: profit * 0.1,
                                     });
                                  } else {
                                     println!("  ❌ Invalid profit: {:.4}%", profit);
                                  }
                               }
                           }
                           break;
                       }
                   }
               }
               Err(_) => {
                   println!("  ⚠️ Negative cycle detected from {}", graph[start_node]);
                
                // Extract cycle using find_negative_cycle
                   if let Some(cycle) = find_negative_cycle(graph, start_node) {
                       let path: Vec<String> = cycle.iter().map(|&idx| graph[idx].clone()).collect();
                    
                    // Verify cycle returns to start
                       if path.len() >= 3 && path.first() == path.last() {
                           println!("  🔥 Extracted cycle: {:?}", path);
                        
                           let profit = self.calculate_real_profit(&path, tickers);
                        
                           if profit > min_profit && profit < 100.0 {
                               let chance = self.calculate_execution_chance(&path, tickers);
                               let pair = path.join(" → ");
                            
                               opportunities.push(ArbitrageOpportunity {
                                   pair,
                                   triangle: path,
                                   profit_margin_before: profit,
                                   profit_margin_after: profit * 0.9,
                                   chance_of_executing: chance,
                                   timestamp: Utc::now().timestamp_millis(),
                                   exchange: "unknown".to_string(),
                                   estimated_slippage: profit * 0.1,
                               });
                           }
                       }
                   }
               }
           }
       }
    
    // Deduplicate
       let mut seen = std::collections::HashSet::new();
       opportunities.retain(|opp| {
           let mut cycle = opp.triangle.clone();
           cycle.sort();
           if seen.contains(&cycle) {
               false
           } else {
               seen.insert(cycle);
               true
           }
       });
    
       let profitable_count = opportunities.len();
       println!("\n📊 Found {} unique opportunities", profitable_count);
       (opportunities, total_paths_checked, profitable_count)
    }
    
    fn reconstruct_cycle(&self, predecessors: &[Option<NodeIndex>], start: NodeIndex) -> Option<Vec<NodeIndex>> {
        let mut cycle = Vec::new();
        let mut visited = HashSet::new();
        let mut current = start;

        while let Some(prev) = predecessors[current.index()] {
            if visited.contains(&current) {
                let cycle_start = current;
                cycle.push(current);
                current = prev;
                
                while current != cycle_start {
                    cycle.push(current);
                    current = predecessors[current.index()]?;
                }
                cycle.push(cycle_start);
                cycle.reverse();
                return Some(cycle);
            }
            
            visited.insert(current);
            cycle.push(current);
            current = prev;
        }

        None
    }

    fn calculate_cycle_profit(&self, path: &[String], tickers: &HashMap<String, (f64, f64, i64)>) -> f64 {
       let mut amount = 1.0;
    
       for i in 0..path.len() - 1 {
           let from = &path[i];
           let to = &path[i + 1];
        
        // Try direct pair
           let direct_pair = format!("{}{}", from, to);
           if let Some((bid, _, _)) = tickers.get(&direct_pair) {
               amount *= bid;
               continue;
           }
        
        // Try reverse pair
           let reverse_pair = format!("{}{}", to, from);
           if let Some((_, ask, _)) = tickers.get(&reverse_pair) {
               amount *= 1.0 / ask;
               continue;
           }
        
        // If no pair found, log warning
           println!("WARNING: No rate found for {} → {}", from, to);
           return 0.0;
       }
    
    // Calculate profit percentage
       let profit = (amount - 1.0) * 100.0;
    
    // Sanity check - if profit is over 1000%, something's wrong
       if profit > 1000.0 {
           println!("WARNING: Suspiciously high profit: {}%", profit);
           println!("Path: {:?}", path);
           println!("Amount multiplier: {}", amount);
       }
    
       profit
    }

    fn calculate_real_profit(&self, path: &[String], tickers: &HashMap<String, (f64, f64, i64)>) -> f64 {
    // Path must return to start (e.g., BTC → ETH → USDT → BTC)
       if path.len() < 3 || path.first() != path.last() {
           return -100.0;
       }
    
       let mut amount = 1.0;
    
       for i in 0..path.len() - 1 {
           let from = &path[i];
           let to = &path[i + 1];
        
        // Try direct pair
           let pair = format!("{}{}", from, to);
           if let Some((bid, ask, _)) = tickers.get(&pair) {
            // For forward direction (base → quote), use bid price
               amount *= bid;
               continue;
           }
        
        // Try reverse pair
           let reverse_pair = format!("{}{}", to, from);
           if let Some((bid, ask, _)) = tickers.get(&reverse_pair) {
            // For reverse direction (quote → base), use 1/ask
               amount *= 1.0 / ask;
               continue;
           }
        
           return -100.0; // Missing rate
       }
    
    // Profit as percentage
       (amount - 1.0) * 100.0
    }

    pub async fn scan_exchange(&self, exchange: &str, min_profit: f64, duration_secs: u64) 
       -> (Vec<ArbitrageOpportunity>, ScanSummary, Vec<ScanLog>) {
    
       println!("🔍 Starting scan for {} ({} seconds)", exchange, duration_secs);
    
       let (summary, data, logs) = match exchange {
           "binance" => {
               println!("📡 Connecting to Binance WebSocket...");
               let summary = self.binance_collector.start_collection(duration_secs).await;
               println!("✅ Binance collected {} pairs", summary.pairs_collected);
               (summary, self.binance_collector.get_data(), self.binance_collector.get_logs())
           }
           "bybit" => {
               println!("📡 Connecting to Bybit WebSocket...");
               let summary = self.bybit_collector.start_collection(duration_secs).await;
               println!("✅ Bybit collected {} pairs", summary.pairs_collected);
               (summary, self.bybit_collector.get_data(), self.bybit_collector.get_logs())
           }
           "kucoin" => {
               println!("📡 Connecting to KuCoin WebSocket...");
               let summary = self.kucoin_collector.start_collection(duration_secs).await;
               println!("✅ KuCoin collected {} pairs", summary.pairs_collected);
               (summary, self.kucoin_collector.get_data(), self.kucoin_collector.get_logs())
            }
            _ => {
                return (Vec::new(), ScanSummary {
                   exchange: exchange.to_string(),
                   pairs_collected: 0,
                   paths_found: 0,
                   profitable_triangles: 0,
                   collection_time_secs: 0,
                }, Vec::new())
            }
        };

        let data_guard = data.lock().await;
        let tickers: HashMap<String, (f64, f64, i64)> = data_guard.clone();
        drop(data_guard);

        let logs_guard = logs.lock().await;
        let scan_logs = logs_guard.clone();
        drop(logs_guard);

        println!("📊 {} collected {} total tickers", exchange, tickers.len());

        if tickers.is_empty() {
           println!("⚠️ No tickers collected for {}", exchange);
           return (Vec::new(), summary, scan_logs);
        }

        println!("🔨 Building currency graph for {}...", exchange);
        let (graph, node_indices) = self.build_graph(&tickers);
        println!("📈 Graph has {} nodes and {} edges", graph.node_count(), graph.edge_count());
        self.debug_graph(&graph, &node_indices, &tickers); //    
    
        println!("🔎 Running Bellman-Ford to find arbitrage opportunities...");
        let (mut opportunities, paths_found, profitable) = self.find_profitable_triangles(&graph, &node_indices, &tickers, min_profit);
    
        println!("📊 Results for {}: {} paths checked, {} profitable triangles found", 
               exchange, paths_found, profitable);

        for opp in &mut opportunities {
            opp.exchange = exchange.to_string();
        }

        let mut final_summary = summary;
        final_summary.paths_found = paths_found;
        final_summary.profitable_triangles = profitable;

        (opportunities, final_summary, scan_logs)
    }

    pub async fn scan_multiple_exchanges(&self, exchanges: Vec<String>, min_profit: f64, duration_secs: u64) 
        -> ScanResponse {
        
        let mut all_opportunities = Vec::new();
        let mut all_summaries = Vec::new();
        let mut all_logs = Vec::new();
        
        for exchange in exchanges {
            let (opps, summary, logs) = self.scan_exchange(&exchange, min_profit, duration_secs).await;
            all_opportunities.extend(opps);
            all_summaries.push(summary);
            all_logs.extend(logs);
        }
        
        all_opportunities.sort_by(|a, b| b.profit_margin_before.partial_cmp(&a.profit_margin_before).unwrap());
        
        ScanResponse {
            opportunities: all_opportunities,
            summaries: all_summaries,
            logs: all_logs,
        }
    }
}

// ==================== API Handlers ====================

async fn scan_handler(req: web::Json<ScanRequest>) -> impl Responder {
    println!("Received scan request for exchanges: {:?}", req.exchanges);
    
    if req.exchanges.is_empty() {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "No exchanges selected",
            "opportunities": [],
            "summaries": [],
            "logs": []
        }));
    }
    
    let detector = ArbitrageDetector::new();
    let min_profit = req.min_profit.unwrap_or(0.3);
    let duration = req.collection_duration.unwrap_or(10);
    
    let response = detector.scan_multiple_exchanges(
        req.exchanges.clone(), 
        min_profit, 
        duration
    ).await;
    
    println!("Scan complete. Found {} opportunities", response.opportunities.len());
    HttpResponse::Ok().json(response)
}

async fn health_handler() -> impl Responder {
    HttpResponse::Ok().body("Arbitrage Scanner API is running with Binance, Bybit, and KuCoin support")
}

// ==================== Main ====================

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();
    
    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let bind_addr = format!("0.0.0.0:{}", port);
    
    println!("Starting arbitrage scanner backend on {}", bind_addr);
    println!("API endpoints: /health, /api/scan (POST)");
    println!("Frontend serving from ./static directory");
    
    HttpServer::new(|| {
    let cors = Cors::default()
        .allow_any_origin()
        .allow_any_method()
        .allow_any_header();
        
    App::new()
        .wrap(cors)
        // API ROUTES FIRST - these need to be before the static files
        .route("/health", web::get().to(health_handler))
        .route("/api/scan", web::post().to(scan_handler))
        // STATIC FILES LAST - this catches everything else
        .service(fs::Files::new("/", "./static").index_file("index.html"))
    })
    .bind(&bind_addr)?
    .run()
    .await
}
