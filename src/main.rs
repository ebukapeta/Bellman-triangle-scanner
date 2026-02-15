// src/main.rs - WebSocket Collection + On-Demand Scanning
#![warn(clippy::all)]

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{RwLock, Mutex};
use tokio::time::{self, Duration, Instant};
use chrono::{Utc, Local};
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::algo::bellman_ford;
use tauri::{Manager, Window, State};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use log::{info, warn, error, debug};

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
pub struct ScanConfig {
    pub min_profit_threshold: f64,
    pub exchanges: Vec<String>,
    pub max_triangle_length: usize,
    pub collection_duration_secs: u64,
}

impl Default for ScanConfig {
    fn default() -> Self {
        Self {
            min_profit_threshold: 0.3,
            exchanges: vec!["binance".to_string(), "bybit".to_string(), "kucoin".to_string()],
            max_triangle_length: 3,
            collection_duration_secs: 10, // 10 seconds of data collection
        }
    }
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
    pub avg_latency_ms: f64,
}

// ==================== WebSocket Collectors ====================

pub struct BinanceWebSocketCollector {
    collected_data: Arc<Mutex<HashMap<String, (f64, f64, i64)>>>, // symbol -> (bid, ask, timestamp)
    is_collecting: Arc<Mutex<bool>>,
    logs: Arc<Mutex<Vec<ScanLog>>>,
}

impl BinanceWebSocketCollector {
    pub fn new() -> Self {
        Self {
            collected_data: Arc::new(Mutex::new(HashMap::new())),
            is_collecting: Arc::new(Mutex::new(false)),
            logs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn start_collection(&self, duration_secs: u64) -> ScanSummary {
        let start_time = Instant::now();
        let mut collect_flag = self.is_collecting.lock().await;
        *collect_flag = true;
        drop(collect_flag);

        // Clear previous data
        let mut data = self.collected_data.lock().await;
        data.clear();
        drop(data);

        let mut logs = self.logs.lock().await;
        logs.clear();
        logs.push(ScanLog {
            timestamp: Local::now().format("%H:%M:%S").to_string(),
            exchange: "binance".to_string(),
            message: format!("Starting WebSocket collection for {} seconds", duration_secs),
            level: "info".to_string(),
        });
        drop(logs);

        let data_clone = self.collected_data.clone();
        let logs_clone = self.logs.clone();
        let is_collecting_clone = self.is_collecting.clone();

        // Spawn WebSocket connection
        let handle = tokio::spawn(async move {
            let ws_url = "wss://stream.binance.com:9443/ws/!ticker@arr";
            
            match connect_async(ws_url).await {
                Ok((ws_stream, _)) => {
                    let (mut write, mut read) = ws_stream.split();
                    
                    logs_clone.lock().await.push(ScanLog {
                        timestamp: Local::now().format("%H:%M:%S").to_string(),
                        exchange: "binance".to_string(),
                        message: "WebSocket connected successfully".to_string(),
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
                                            if symbol.ends_with("USDT") || symbol.ends_with("BUSD") {
                                                if let (Some(bid), Some(ask)) = (
                                                    ticker_data["b"].as_str().and_then(|s| s.parse::<f64>().ok()),
                                                    ticker_data["a"].as_str().and_then(|s| s.parse::<f64>().ok()),
                                                ) {
                                                    let mut data = data_clone.lock().await;
                                                    data.insert(symbol.to_string(), (bid, ask, chrono::Utc::now().timestamp_millis()));
                                                    pair_count += 1;
                                                    
                                                    if pair_count % 100 == 0 {
                                                        logs_clone.lock().await.push(ScanLog {
                                                            timestamp: Local::now().format("%H:%M:%S").to_string(),
                                                            exchange: "binance".to_string(),
                                                            message: format!("Collected {} pairs so far...", pair_count),
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
                                    error!("Binance WebSocket error: {}", e);
                                    break;
                                }
                                _ => {}
                            }
                        }
                    }

                    // Close connection gracefully
                    let _ = write.close().await;
                    
                    logs_clone.lock().await.push(ScanLog {
                        timestamp: Local::now().format("%H:%M:%S").to_string(),
                        exchange: "binance".to_string(),
                        message: format!("Collection complete. Total pairs: {}", pair_count),
                        level: "success".to_string(),
                    });
                }
                Err(e) => {
                    error!("Failed to connect to Binance WebSocket: {}", e);
                    logs_clone.lock().await.push(ScanLog {
                        timestamp: Local::now().format("%H:%M:%S").to_string(),
                        exchange: "binance".to_string(),
                        message: format!("Connection failed: {}", e),
                        level: "error".to_string(),
                    });
                }
            }
        });

        // Wait for collection to complete
        let _ = handle.await;

        let mut collect_flag = self.is_collecting.lock().await;
        *collect_flag = false;

        let final_data = self.collected_data.lock().await;
        let pairs_collected = final_data.len();
        drop(final_data);

        ScanSummary {
            exchange: "binance".to_string(),
            pairs_collected,
            paths_found: 0, // Will be filled after analysis
            profitable_triangles: 0,
            collection_time_secs: start_time.elapsed().as_secs(),
            avg_latency_ms: 50.0, // Placeholder
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
    is_collecting: Arc<Mutex<bool>>,
    logs: Arc<Mutex<Vec<ScanLog>>>,
}

impl BybitWebSocketCollector {
    pub fn new() -> Self {
        Self {
            collected_data: Arc::new(Mutex::new(HashMap::new())),
            is_collecting: Arc::new(Mutex::new(false)),
            logs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn start_collection(&self, duration_secs: u64) -> ScanSummary {
        let start_time = Instant::now();
        let mut collect_flag = self.is_collecting.lock().await;
        *collect_flag = true;
        drop(collect_flag);

        let mut data = self.collected_data.lock().await;
        data.clear();
        drop(data);

        let mut logs = self.logs.lock().await;
        logs.clear();
        logs.push(ScanLog {
            timestamp: Local::now().format("%H:%M:%S").to_string(),
            exchange: "bybit".to_string(),
            message: format!("Starting WebSocket collection for {} seconds", duration_secs),
            level: "info".to_string(),
        });
        drop(logs);

        let data_clone = self.collected_data.clone();
        let logs_clone = self.logs.clone();

        let handle = tokio::spawn(async move {
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
                        message: "WebSocket connected and subscribed".to_string(),
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
                                                    let mut data_map = data_clone.lock().await;
                                                    data_map.insert(symbol.to_string(), (bid, ask, chrono::Utc::now().timestamp_millis()));
                                                    pair_count += 1;
                                                }
                                            }
                                        }
                                    }
                                }
                                Ok(Message::Ping(data)) => {
                                    let _ = write.send(Message::Pong(data)).await;
                                }
                                Err(e) => {
                                    error!("Bybit WebSocket error: {}", e);
                                    break;
                                }
                                _ => {}
                            }
                        }
                    }

                    let _ = write.close().await;
                    
                    logs_clone.lock().await.push(ScanLog {
                        timestamp: Local::now().format("%H:%M:%S").to_string(),
                        exchange: "bybit".to_string(),
                        message: format!("Collection complete. Total pairs: {}", pair_count),
                        level: "success".to_string(),
                    });
                }
                Err(e) => {
                    error!("Failed to connect to Bybit WebSocket: {}", e);
                    logs_clone.lock().await.push(ScanLog {
                        timestamp: Local::now().format("%H:%M:%S").to_string(),
                        exchange: "bybit".to_string(),
                        message: format!("Connection failed: {}", e),
                        level: "error".to_string(),
                    });
                }
            }
        });

        let _ = handle.await;

        let mut collect_flag = self.is_collecting.lock().await;
        *collect_flag = false;

        let final_data = self.collected_data.lock().await;
        let pairs_collected = final_data.len();
        drop(final_data);

        ScanSummary {
            exchange: "bybit".to_string(),
            pairs_collected,
            paths_found: 0,
            profitable_triangles: 0,
            collection_time_secs: start_time.elapsed().as_secs(),
            avg_latency_ms: 50.0,
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
    // Add KuCoin similarly
}

impl ArbitrageDetector {
    pub fn new() -> Self {
        Self {
            binance_collector: BinanceWebSocketCollector::new(),
            bybit_collector: BybitWebSocketCollector::new(),
        }
    }

    fn parse_symbol(symbol: &str) -> Option<(String, String)> {
        if symbol.ends_with("USDT") {
            let base = symbol.trim_end_matches("USDT").to_string();
            Some((base, "USDT".to_string()))
        } else if symbol.ends_with("BUSD") {
            let base = symbol.trim_end_matches("BUSD").to_string();
            Some((base, "BUSD".to_string()))
        } else {
            None
        }
    }

    fn build_graph(&self, tickers: &HashMap<String, (f64, f64, i64)>) -> (DiGraph<String, f64>, HashMap<String, NodeIndex>) {
        let mut graph = DiGraph::<String, f64>::new();
        let mut node_indices = HashMap::new();
        let mut currencies = HashSet::new();

        // Collect all unique currencies
        for symbol in tickers.keys() {
            if let Some((base, quote)) = Self::parse_symbol(symbol) {
                currencies.insert(base);
                currencies.insert(quote);
            }
        }

        // Add nodes
        for currency in currencies {
            let idx = graph.add_node(currency.clone());
            node_indices.insert(currency, idx);
        }

        // Add edges with -log(rate) weights
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
        let mut total_liquidity_score = 0.0;
        
        for i in 0..path.len() - 1 {
            let symbol = format!("{}{}", path[i], path[i + 1]);
            if let Some((bid, ask, timestamp)) = tickers.get(&symbol) {
                // Liquidity score based on bid/ask spread and recency
                let spread = (ask - bid) / bid;
                let spread_score = 1.0 - (spread / 0.01).min(1.0); // 1% spread = 0 score
                
                let age = (Utc::now().timestamp_millis() - timestamp) as f64 / 1000.0;
                let recency_score = 1.0 - (age / 5.0).min(1.0); // 5 seconds old = 0 score
                
                total_liquidity_score += (spread_score * 0.7 + recency_score * 0.3);
            }
        }
        
        let avg_score = total_liquidity_score / (path.len() - 1) as f64;
        50.0 + (avg_score * 45.0) // 50-95% range
    }

    fn find_profitable_triangles(&self, graph: &DiGraph<String, f64>, node_indices: &HashMap<String, NodeIndex>, 
                                 tickers: &HashMap<String, (f64, f64, i64)>, min_profit: f64) -> (Vec<ArbitrageOpportunity>, usize, usize) {
        let mut opportunities = Vec::new();
        let mut total_paths_checked = 0;
        
        let nodes: Vec<_> = graph.node_indices().collect();
        
        for start_node in nodes.iter().take(20) {
            if let Ok((distances, predecessors)) = bellman_ford(graph, *start_node) {
                for edge in graph.raw_edges() {
                    let u = edge.source();
                    let v = edge.target();
                    
                    if distances[u.index()] + edge.weight < distances[v.index()] - 1e-10 {
                        total_paths_checked += 1;
                        
                        if let Some(cycle) = self.reconstruct_cycle(graph, &predecessors, v) {
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
                                            exchange: "binance".to_string(), // Will be updated
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
        }
        
        let profitable_count = opportunities.len();
        (opportunities, total_paths_checked, profitable_count)
    }

    fn reconstruct_cycle(&self, graph: &DiGraph<String, f64>, predecessors: &[Option<NodeIndex>], start: NodeIndex) -> Option<Vec<NodeIndex>> {
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
        let summary = match exchange {
            "binance" => {
                self.binance_collector.start_collection(duration_secs).await
            }
            "bybit" => {
                self.bybit_collector.start_collection(duration_secs).await
            }
            _ => {
                return (Vec::new(), ScanSummary {
                    exchange: exchange.to_string(),
                    pairs_collected: 0,
                    paths_found: 0,
                    profitable_triangles: 0,
                    collection_time_secs: 0,
                    avg_latency_ms: 0.0,
                }, Vec::new())
            }
        };

        let data = match exchange {
            "binance" => self.binance_collector.get_data(),
            "bybit" => self.bybit_collector.get_data(),
            _ => return (Vec::new(), summary, Vec::new()),
        };

        let logs = match exchange {
            "binance" => self.binance_collector.get_logs(),
            "bybit" => self.bybit_collector.get_logs(),
            _ => return (Vec::new(), summary, Vec::new()),
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
        let (opportunities, paths_found, profitable) = self.find_profitable_triangles(&graph, &node_indices, &tickers, min_profit);

        let mut final_summary = summary;
        final_summary.paths_found = paths_found;
        final_summary.profitable_triangles = profitable;

        (opportunities, final_summary, scan_logs)
    }

    pub async fn scan_multiple_exchanges(&self, exchanges: Vec<String>, min_profit: f64, duration_secs: u64) 
        -> (Vec<ArbitrageOpportunity>, Vec<ScanSummary>, Vec<ScanLog>) {
        
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
        
        (all_opportunities, all_summaries, all_logs)
    }
}

// ==================== Tauri State ====================

pub struct AppState {
    detector: Arc<ArbitrageDetector>,
    last_scan_results: Arc<RwLock<Vec<ArbitrageOpportunity>>>,
    last_summaries: Arc<RwLock<Vec<ScanSummary>>>,
    last_logs: Arc<RwLock<Vec<ScanLog>>>,
    config: Mutex<ScanConfig>,
}

// ==================== Tauri Commands ====================

#[tauri::command]
async fn scan_now(
    request: ScanRequest,
    state: tauri::State<'_, AppState>,
) -> Result<(Vec<ArbitrageOpportunity>, Vec<ScanSummary>, Vec<ScanLog>), String> {
    let detector = &state.detector;
    let min_profit = request.min_profit.unwrap_or(0.3);
    let duration = request.collection_duration.unwrap_or(10);
    
    info!("Starting scan on exchanges: {:?} for {} seconds", request.exchanges, duration);
    
    let (opportunities, summaries, logs) = detector
        .scan_multiple_exchanges(request.exchanges, min_profit, duration)
        .await;
    
    // Store results
    let mut last_results = state.last_scan_results.write().await;
    *last_results = opportunities.clone();
    
    let mut last_summaries = state.last_summaries.write().await;
    *last_summaries = summaries.clone();
    
    let mut last_logs = state.last_logs.write().await;
    *last_logs = logs.clone();
    
    info!("Scan complete. Found {} opportunities", opportunities.len());
    
    Ok((opportunities, summaries, logs))
}

#[tauri::command]
async fn get_last_results(state: tauri::State<'_, AppState>) -> Result<Vec<ArbitrageOpportunity>, String> {
    let results = state.last_scan_results.read().await;
    Ok(results.clone())
}

#[tauri::command]
async fn get_last_logs(state: tauri::State<'_, AppState>) -> Result<Vec<ScanLog>, String> {
    let logs = state.last_logs.read().await;
    Ok(logs.clone())
}

#[tauri::command]
async fn update_config(config: ScanConfig, state: tauri::State<'_, AppState>) -> Result<(), String> {
    let mut current_config = state.config.lock().unwrap();
    *current_config = config;
    Ok(())
}

#[tauri::command]
async fn get_config(state: tauri::State<'_, AppState>) -> Result<ScanConfig, String> {
    let config = state.config.lock().unwrap();
    Ok(config.clone())
}

// ==================== Main ====================

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    
    let detector = Arc::new(ArbitrageDetector::new());
    let config = ScanConfig::default();
    
    tauri::Builder::default()
        .manage(AppState {
            detector: Arc::clone(&detector),
            last_scan_results: Arc::new(RwLock::new(Vec::new())),
            last_summaries: Arc::new(RwLock::new(Vec::new())),
            last_logs: Arc::new(RwLock::new(Vec::new())),
            config: Mutex::new(config),
        })
        .invoke_handler(tauri::generate_handler![
            scan_now,
            get_last_results,
            get_last_logs,
            update_config,
            get_config,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
    
    Ok(())
}
```
                             
