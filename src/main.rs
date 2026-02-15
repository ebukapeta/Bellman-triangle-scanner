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

    pub async fn start_collection(&self, duration_secs: u64) -> ScanSummary {
        let start_time = Instant::now();
        
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

        tokio::spawn(async move {
            let ws_url = "wss://stream.binance.com:9443/ws/!ticker@arr";
            
            match connect_async(ws_url).await {
                Ok((ws_stream, _)) => {
                    let (mut write, mut read) = ws_stream.split();
                    
                    let _ = logs_clone.lock().await.push(ScanLog {
                        timestamp: Local::now().format("%H:%M:%S").to_string(),
                        exchange: "binance".to_string(),
                        message: "Binance WebSocket connected successfully".to_string(),
                        level: "success".to_string(),
                    });

                    let mut pair_count = 0;
                    let start = Instant::now();

                    while start.elapsed().as_secs() < duration_secs {
                        if let Ok(Some(msg)) = tokio::time::timeout(Duration::from_secs(1), read.next()).await {
                            match msg {
                                Ok(Message::Text(text)) => {
                                    if let Ok(ticker_data) = serde_json::from_str::<serde_json::Value>(&text) {
                                        if let Some(symbol) = ticker_data["s"].as_str() {
                                            if symbol.ends_with("USDT") || symbol.ends_with("BUSD") || symbol.ends_with("USDC") {
                                                if let (Some(bid), Some(ask)) = (
                                                    ticker_data["b"].as_str().and_then(|s| s.parse::<f64>().ok()),
                                                    ticker_data["a"].as_str().and_then(|s| s.parse::<f64>().ok()),
                                                ) {
                                                    let mut data = data_clone.lock().await;
                                                    data.insert(symbol.to_string(), (bid, ask, chrono::Utc::now().timestamp_millis()));
                                                    pair_count += 1;
                                                    
                                                    if pair_count % 100 == 0 {
                                                        let _ = logs_clone.lock().await.push(ScanLog {
                                                            timestamp: Local::now().format("%H:%M:%S").to_string(),
                                                            exchange: "binance".to_string(),
                                                            message: format!("Binance collected {} pairs...", pair_count),
                                                            level: "debug".to_string(),
                                                        });
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
                                    let _ = logs_clone.lock().await.push(ScanLog {
                                        timestamp: Local::now().format("%H:%M:%S").to_string(),
                                        exchange: "binance".to_string(),
                                        message: format!("Binance WebSocket error: {}", e),
                                        level: "error".to_string(),
                                    });
                                    break;
                                }
                                _ => {}
                            }
                        }
                    }

                    let _ = write.close().await;
                    
                    let _ = logs_clone.lock().await.push(ScanLog {
                        timestamp: Local::now().format("%H:%M:%S").to_string(),
                        exchange: "binance".to_string(),
                        message: format!("Binance collection complete. Total pairs: {}", pair_count),
                        level: "success".to_string(),
                    });
                }
                Err(e) => {
                    let _ = logs_clone.lock().await.push(ScanLog {
                        timestamp: Local::now().format("%H:%M:%S").to_string(),
                        exchange: "binance".to_string(),
                        message: format!("Binance connection failed: {}", e),
                        level: "error".to_string(),
                    });
                }
            }
        });

        tokio::time::sleep(Duration::from_secs(duration_secs + 2)).await;

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

        tokio::spawn(async move {
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

                    let _ = logs_clone.lock().await.push(ScanLog {
                        timestamp: Local::now().format("%H:%M:%S").to_string(),
                        exchange: "bybit".to_string(),
                        message: "Bybit WebSocket connected and subscribed".to_string(),
                        level: "success".to_string(),
                    });

                    let mut pair_count = 0;
                    let start = Instant::now();

                    while start.elapsed().as_secs() < duration_secs {
                        if let Ok(Some(msg)) = tokio::time::timeout(Duration::from_secs(1), read.next()).await {
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
                                                    if symbol.ends_with("USDT") {
                                                        let mut data_map = data_clone.lock().await;
                                                        data_map.insert(symbol.to_string(), (bid, ask, chrono::Utc::now().timestamp_millis()));
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
                                    let _ = logs_clone.lock().await.push(ScanLog {
                                        timestamp: Local::now().format("%H:%M:%S").to_string(),
                                        exchange: "bybit".to_string(),
                                        message: format!("Bybit WebSocket error: {}", e),
                                        level: "error".to_string(),
                                    });
                                    break;
                                }
                                _ => {}
                            }
                        }
                    }

                    let _ = write.close().await;
                    
                    let _ = logs_clone.lock().await.push(ScanLog {
                        timestamp: Local::now().format("%H:%M:%S").to_string(),
                        exchange: "bybit".to_string(),
                        message: format!("Bybit collection complete. Total pairs: {}", pair_count),
                        level: "success".to_string(),
                    });
                }
                Err(e) => {
                    let _ = logs_clone.lock().await.push(ScanLog {
                        timestamp: Local::now().format("%H:%M:%S").to_string(),
                        exchange: "bybit".to_string(),
                        message: format!("Bybit connection failed: {}", e),
                        level: "error".to_string(),
                    });
                }
            }
        });

        tokio::time::sleep(Duration::from_secs(duration_secs + 2)).await;

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

        tokio::spawn(async move {
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

                                    let _ = logs_clone.lock().await.push(ScanLog {
                                        timestamp: Local::now().format("%H:%M:%S").to_string(),
                                        exchange: "kucoin".to_string(),
                                        message: "KuCoin WebSocket connected and subscribed".to_string(),
                                        level: "success".to_string(),
                                    });

                                    let mut pair_count = 0;
                                    let start = Instant::now();

                                    while start.elapsed().as_secs() < duration_secs {
                                        if let Ok(Some(msg)) = tokio::time::timeout(Duration::from_secs(1), read.next()).await {
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
                                                                    if symbol.ends_with("USDT") {
                                                                        let mut data_map = data_clone.lock().await;
                                                                        data_map.insert(symbol.to_string(), (bestBid, bestAsk, chrono::Utc::now().timestamp_millis()));
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
                                                    let _ = logs_clone.lock().await.push(ScanLog {
                                                        timestamp: Local::now().format("%H:%M:%S").to_string(),
                                                        exchange: "kucoin".to_string(),
                                                        message: format!("KuCoin WebSocket error: {}", e),
                                                        level: "error".to_string(),
                                                    });
                                                    break;
                                                }
                                                _ => {}
                                            }
                                        }
                                    }

                                    let _ = write.close().await;
                                    
                                    let _ = logs_clone.lock().await.push(ScanLog {
                                        timestamp: Local::now().format("%H:%M:%S").to_string(),
                                        exchange: "kucoin".to_string(),
                                        message: format!("KuCoin collection complete. Total pairs: {}", pair_count),
                                        level: "success".to_string(),
                                    });
                                }
                                Err(e) => {
                                    let _ = logs_clone.lock().await.push(ScanLog {
                                        timestamp: Local::now().format("%H:%M:%S").to_string(),
                                        exchange: "kucoin".to_string(),
                                        message: format!("KuCoin WebSocket connection failed: {}", e),
                                        level: "error".to_string(),
                                    });
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    let _ = logs_clone.lock().await.push(ScanLog {
                        timestamp: Local::now().format("%H:%M:%S").to_string(),
                        exchange: "kucoin".to_string(),
                        message: format!("Failed to get KuCoin WebSocket token: {}", e),
                        level: "error".to_string(),
                    });
                }
            }
        });

        tokio::time::sleep(Duration::from_secs(duration_secs + 3)).await;

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

    fn find_profitable_triangles(&self, graph: &DiGraph<String, f64>, node_indices: &HashMap<String, NodeIndex>, 
                                 tickers: &HashMap<String, (f64, f64, i64)>, min_profit: f64) -> (Vec<ArbitrageOpportunity>, usize, usize) {
        let mut opportunities = Vec::new();
        let mut total_paths_checked = 0;
        
        let nodes: Vec<_> = graph.node_indices().collect();
        
        for start_node in nodes.iter().take(20) {
            match bellman_ford(graph, *start_node) {
                Ok(paths) => {
                    let distances = paths.distances;
                    let predecessors = paths.predecessors;
                    
                    for edge in graph.raw_edges() {
                        let u = edge.source();
                        let v = edge.target();
                        
                        if distances[u.index()] + edge.weight < distances[v.index()] - 1e-10 {
                            total_paths_checked += 1;
                            
                            if let Some(cycle) = self.reconstruct_cycle(&predecessors, v) {
                                if cycle.len() == 3 || cycle.len() == 4 {
                                    let path: Vec<String> = cycle.iter().map(|&idx| graph[idx].clone()).collect();
                                    
                                    let profit = self.calculate_cycle_profit(&path, tickers);
                                    
                                    if profit > min_profit {
                                        let chance = self.calculate_execution_chance(&path, tickers);
                                        
                                        if chance > 70.0 {
                                            let pair = path.join(" → ");
                                            
                                            opportunities.push(ArbitrageOpportunity {
                                                pair: pair.clone(),
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
                            break;
                        }
                    }
                }
                Err(_) => continue,
            }
        }
        
        let profitable_count = opportunities.len();
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
            let symbol = format!("{}{}", from, to);
            
            if let Some((bid, _, _)) = tickers.get(&symbol) {
                amount *= bid;
            } else {
                let rev_symbol = format!("{}{}", to, from);
                if let Some((_, ask, _)) = tickers.get(&rev_symbol) {
                    amount *= 1.0 / ask;
                }
            }
        }
        
        (amount - 1.0) * 100.0
    }

    pub async fn scan_exchange(&self, exchange: &str, min_profit: f64, duration_secs: u64) -> (Vec<ArbitrageOpportunity>, ScanSummary, Vec<ScanLog>) {
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

        if tickers.is_empty() {
            return (Vec::new(), summary, scan_logs);
        }

        let (graph, node_indices) = self.build_graph(&tickers);
        let (mut opportunities, paths_found, profitable) = self.find_profitable_triangles(&graph, &node_indices, &tickers, min_profit);

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
            .service(fs::Files::new("/", "./static").index_file("index.html"))
            .route("/health", web::get().to(health_handler))
            .route("/api/scan", web::post().to(scan_handler))
    })
    .bind(&bind_addr)?
    .run()
    .await
 }
