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
        drop(logs);

        let data_clone = self.collected_data.clone();
        let logs_clone = self.logs.clone();

        let ws_url = "wss://stream.binance.com:9443/ws/!ticker@arr";
        
        match connect_async(ws_url).await {
            Ok((mut ws_stream, _)) => {
                logs_clone.lock().await.push(ScanLog {
                    timestamp: Local::now().format("%H:%M:%S").to_string(),
                    exchange: "binance".to_string(),
                    message: "WebSocket connected".to_string(),
                    level: "success".to_string(),
                });

                let mut pair_count = 0;

                while let Some(msg) = ws_stream.next().await {
                    if Instant::now() >= deadline {
                        break;
                    }

                    match msg {
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
                        Ok(Message::Ping(_)) => {}
                        Ok(Message::Pong(_)) => {}
                        Ok(Message::Close(_)) => break,
                        Err(e) => {
                            logs_clone.lock().await.push(ScanLog {
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
                        logs_clone.lock().await.push(ScanLog {
                            timestamp: Local::now().format("%H:%M:%S").to_string(),
                            exchange: "binance".to_string(),
                            message: format!("Collected {} pairs", pair_count),
                            level: "debug".to_string(),
                        });
                    }

                    tokio::time::sleep(Duration::from_millis(10)).await;
                }

                logs_clone.lock().await.push(ScanLog {
                    timestamp: Local::now().format("%H:%M:%S").to_string(),
                    exchange: "binance".to_string(),
                    message: format!("Collection complete: {} pairs", pair_count),
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
        drop(logs);

        let data_clone = self.collected_data.clone();
        let logs_clone = self.logs.clone();

        let ws_url = "wss://stream.bybit.com/v5/public/spot";
        
        match connect_async(ws_url).await {
            Ok((ws_stream, _)) => {
                let (mut write, mut read) = ws_stream.split();
                
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
                    message: "WebSocket connected".to_string(),
                    level: "success".to_string(),
                });

                let mut pair_count = 0;

                while let Some(msg) = read.next().await {
                    if Instant::now() >= deadline {
                        break;
                    }

                    match msg {
                        Ok(Message::Text(text)) => {
                            if let Ok(ticker_data) = serde_json::from_str::<serde_json::Value>(&text) {
                                if ticker_data["topic"].as_str() == Some("tickers") {
                                    if let Some(data) = ticker_data["data"].as_object() {
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
                        Ok(Message::Ping(data)) => {
                            let _ = write.send(Message::Pong(data)).await;
                        }
                        Err(e) => {
                            logs_clone.lock().await.push(ScanLog {
                                timestamp: Local::now().format("%H:%M:%S").to_string(),
                                exchange: "bybit".to_string(),
                                message: format!("WebSocket error: {}", e),
                                level: "error".to_string(),
                            });
                            break;
                        }
                        _ => {}
                    }

                    if pair_count > 0 && pair_count % 500 == 0 {
                        logs_clone.lock().await.push(ScanLog {
                            timestamp: Local::now().format("%H:%M:%S").to_string(),
                            exchange: "bybit".to_string(),
                            message: format!("Collected {} pairs", pair_count),
                            level: "debug".to_string(),
                        });
                    }
                }

                logs_clone.lock().await.push(ScanLog {
                    timestamp: Local::now().format("%H:%M:%S").to_string(),
                    exchange: "bybit".to_string(),
                    message: format!("Collection complete: {} pairs", pair_count),
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
            exchange: "kucoin".to_string(),
            message: format!("Starting KuCoin collection ({}s)", duration_secs),
            level: "info".to_string(),
        });
        drop(logs);

        let data_clone = self.collected_data.clone();
        let logs_clone = self.logs.clone();

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
                                    message: "WebSocket connected".to_string(),
                                    level: "success".to_string(),
                                });

                                let mut pair_count = 0;

                                while let Some(msg) = read.next().await {
                                    if Instant::now() >= deadline {
                                        break;
                                    }

                                    match msg {
                                        Ok(Message::Text(text)) => {
                                            if let Ok(ticker_data) = serde_json::from_str::<serde_json::Value>(&text) {
                                                if ticker_data["type"].as_str() == Some("message") {
                                                    if let Some(data) = ticker_data["data"].as_object() {
                                                        if let (Some(symbol), Some(bestBid), Some(bestAsk)) = (
                                                            data.get("symbol").and_then(|s| s.as_str()),
                                                            data.get("bestBid").and_then(|b| b.as_str()).and_then(|s| s.parse::<f64>().ok()),
                                                            data.get("bestAsk").and_then(|a| a.as_str()).and_then(|s| s.parse::<f64>().ok()),
                                                        ) {
                                                            if parse_symbol(symbol).is_some() {
                                                                let mut data_map = data_clone.lock().await;
                                                                data_map.insert(symbol.to_string(), (bestBid, bestAsk, Utc::now().timestamp_millis()));
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
                                            logs_clone.lock().await.push(ScanLog {
                                                timestamp: Local::now().format("%H:%M:%S").to_string(),
                                                exchange: "kucoin".to_string(),
                                                message: format!("WebSocket error: {}", e),
                                                level: "error".to_string(),
                                            });
                                            break;
                                        }
                                        _ => {}
                                    }

                                    if pair_count > 0 && pair_count % 500 == 0 {
                                        logs_clone.lock().await.push(ScanLog {
                                            timestamp: Local::now().format("%H:%M:%S").to_string(),
                                            exchange: "kucoin".to_string(),
                                            message: format!("Collected {} pairs", pair_count),
                                            level: "debug".to_string(),
                                        });
                                    }
                                }

                                logs_clone.lock().await.push(ScanLog {
                                    timestamp: Local::now().format("%H:%M:%S").to_string(),
                                    exchange: "kucoin".to_string(),
                                    message: format!("Collection complete: {} pairs", pair_count),
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
                    }
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

        for (symbol, (bid, ask, _)) in tickers {
            if let Some((base, quote)) = parse_symbol(symbol) {
                if let (Some(&base_idx), Some(&quote_idx)) = (node_indices.get(&base), node_indices.get(&quote)) {
                    if *bid > 0.0 {
                        let weight = -bid.ln();
                        graph.add_edge(base_idx, quote_idx, weight);
                    }
                    if *ask > 0.0 {
                        let weight = -(1.0 / ask).ln();
                        graph.add_edge(quote_idx, base_idx, weight);
                    }
                }
            }
        }

        (graph, node_indices)
    }

    fn calculate_execution_chance(&self, path: &[String]) -> f64 {
        match path.len() {
            3 => 85.0,
            4 => 75.0,
            _ => 70.0,
        }
    }

    fn calculate_real_profit(&self, path: &[String], tickers: &HashMap<String, (f64, f64, i64)>) -> f64 {
        if path.len() < 3 || path.first() != path.last() {
            return -100.0;
        }
        
        let mut amount = 1.0;
        
        for i in 0..path.len() - 1 {
            let from = &path[i];
            let to = &path[i + 1];
            
            let pair = format!("{}{}", from, to);
            if let Some((bid, _, _)) = tickers.get(&pair) {
                amount *= bid;
                continue;
            }
            
            let reverse_pair = format!("{}{}", to, from);
            if let Some((_, ask, _)) = tickers.get(&reverse_pair) {
                amount *= 1.0 / ask;
                continue;
            }
            
            return -100.0;
        }
        
        (amount - 1.0) * 100.0
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

    fn find_opportunities(&self, graph: &DiGraph<String, f64>, node_indices: &HashMap<String, NodeIndex>, 
                         tickers: &HashMap<String, (f64, f64, i64)>, min_profit: f64) -> (Vec<ArbitrageOpportunity>, usize, usize, usize) {
        let mut opportunities = Vec::new();
        let mut paths_checked = 0;
        let mut valid_triangles = 0;
        
        let nodes: Vec<_> = graph.node_indices().collect();
        
        for &start_node in nodes.iter().take(30) {
            match bellman_ford(graph, start_node) {
                Ok(paths) => {
                    let distances = paths.distances;
                    let predecessors = paths.predecessors;
                    
                    for edge in graph.raw_edges() {
                        let u = edge.source();
                        let v = edge.target();
                        
                        if distances[u.index()] + edge.weight < distances[v.index()] - 1e-10 {
                            paths_checked += 1;
                            
                            if let Some(cycle_nodes) = self.reconstruct_cycle(&predecessors, v) {
                                if cycle_nodes.len() >= 3 && cycle_nodes.first() == cycle_nodes.last() {
                                    valid_triangles += 1;
                                    let path: Vec<String> = cycle_nodes.iter().map(|&idx| graph[idx].clone()).collect();
                                    
                                    let profit = self.calculate_real_profit(&path, tickers);
                                    
                                    if profit > min_profit && profit < 100.0 {
                                        let chance = self.calculate_execution_chance(&path);
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
                            break;
                        }
                    }
                }
                Err(_) => {
                    if let Some(cycle) = find_negative_cycle(graph, start_node) {
                        let path: Vec<String> = cycle.iter().map(|&idx| graph[idx].clone()).collect();
                        
                        if path.len() >= 3 && path.first() == path.last() {
                            paths_checked += 1;
                            valid_triangles += 1;
                            
                            let profit = self.calculate_real_profit(&path, tickers);
                            
                            if profit > min_profit && profit < 100.0 {
                                let chance = self.calculate_execution_chance(&path);
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
        let mut seen = HashSet::new();
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

        let data_guard = data.lock().await;
        let tickers: HashMap<String, (f64, f64, i64)> = data_guard.clone();
        drop(data_guard);

        let logs_guard = logs.lock().await;
        let scan_logs = logs_guard.clone();
        drop(logs_guard);

        println!("📊 {} pairs collected", tickers.len());

        if tickers.is_empty() {
            return (Vec::new(), summary, scan_logs);
        }

        let (graph, node_indices) = self.build_graph(&tickers);
        println!("📈 Graph: {} nodes, {} edges", graph.node_count(), graph.edge_count());

        let (opportunities, paths_checked, valid_triangles, profitable) = 
            self.find_opportunities(&graph, &node_indices, &tickers, min_profit);

        println!("📊 Results: {} paths, {} valid triangles, {} profitable", 
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
        
        all_opportunities.sort_by(|a, b| b.profit_margin_before.partial_cmp(&a.profit_margin_before).unwrap());
        
        println!("✅ Scan complete: {} opportunities", all_opportunities.len());
        
        ScanResponse {
            opportunities: all_opportunities,
            summaries: all_summaries,
            logs: all_logs,
        }
    }
}

/ ==================== API Handlers ====================

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
    
    let detector = ArbitrageDetector::new();
    let min_profit = req.min_profit.unwrap_or(0.3);
    let duration = req.collection_duration.unwrap_or(10);
    
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
    env_logger::init();
    
    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let bind_addr = format!("0.0.0.0:{}", port);
    
    println!("🚀 Server starting on {}", bind_addr);
    println!("📡 Exchanges: binance, bybit, kucoin");
    
    HttpServer::new(|| {
        Cors::default()
            .allow_any_origin()
            .allow_any_method()
            .allow_any_header()
            .max_age(3600);
            
        App::new()
            .route("/health", web::get().to(health_handler))
            .route("/api/scan", web::post().to(scan_handler))
            .service(fs::Files::new("/", "./static").index_file("index.html"))
    })
    .bind(&bind_addr)?
    .run()
    .await
  }
   
