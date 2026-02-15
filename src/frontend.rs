// src/frontend.rs - Complete Yew Frontend
use yew::prelude::*;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;
use gloo_net::http::Request;
use serde::{Deserialize, Serialize};
use web_sys::HtmlInputElement;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScanLog {
    pub timestamp: String,
    pub exchange: String,
    pub message: String,
    pub level: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScanSummary {
    pub exchange: String,
    pub pairs_collected: usize,
    pub paths_found: usize,
    pub profitable_triangles: usize,
    pub collection_time_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScanResponse {
    pub opportunities: Vec<ArbitrageOpportunity>,
    pub summaries: Vec<ScanSummary>,
    pub logs: Vec<ScanLog>,
}

#[function_component(App)]
fn app() -> Html {
    let opportunities = use_state(|| Vec::<ArbitrageOpportunity>::new());
    let summaries = use_state(|| Vec::<ScanSummary>::new());
    let logs = use_state(|| Vec::<ScanLog>::new());
    let scanning = use_state(|| false);
    let status = use_state(|| "Ready to scan".to_string());
    
    let binance_ref = use_node_ref();
    let bybit_ref = use_node_ref();
    let kucoin_ref = use_node_ref();
    let profit_ref = use_node_ref();
    let duration_ref = use_node_ref();
    
    let on_scan = {
        let opportunities = opportunities.clone();
        let summaries = summaries.clone();
        let logs = logs.clone();
        let scanning = scanning.clone();
        let status = status.clone();
        let binance_ref = binance_ref.clone();
        let bybit_ref = bybit_ref.clone();
        let kucoin_ref = kucoin_ref.clone();
        let profit_ref = profit_ref.clone();
        let duration_ref = duration_ref.clone();
        
        Callback::from(move |_| {
            let opp_clone = opportunities.clone();
            let sum_clone = summaries.clone();
            let logs_clone = logs.clone();
            let scan_clone = scanning.clone();
            let status_clone = status.clone();
            
            spawn_local(async move {
                scan_clone.set(true);
                status_clone.set("Scanning...".to_string());
                
                // Clear previous results
                opp_clone.set(Vec::new());
                sum_clone.set(Vec::new());
                logs_clone.set(Vec::new());
                
                // Get selected exchanges
                let mut exchanges = Vec::new();
                if let Some(input) = binance_ref.cast::<HtmlInputElement>() {
                    if input.checked() { exchanges.push("binance".to_string()); }
                }
                if let Some(input) = bybit_ref.cast::<HtmlInputElement>() {
                    if input.checked() { exchanges.push("bybit".to_string()); }
                }
                if let Some(input) = kucoin_ref.cast::<HtmlInputElement>() {
                    if input.checked() { exchanges.push("kucoin".to_string()); }
                }
                
                if exchanges.is_empty() {
                    status_clone.set("Please select at least one exchange".to_string());
                    scan_clone.set(false);
                    return;
                }
                
                let min_profit = profit_ref
                    .cast::<HtmlInputElement>()
                    .and_then(|input| input.value().parse::<f64>().ok())
                    .unwrap_or(0.3);
                
                let duration = duration_ref
                    .cast::<HtmlInputElement>()
                    .and_then(|input| input.value().parse::<u64>().ok())
                    .unwrap_or(10);
                
                let request_body = serde_json::json!({
                    "exchanges": exchanges,
                    "min_profit": min_profit,
                    "collection_duration": duration
                });
                
                status_clone.set(format!("Collecting data for {} seconds...", duration));
                
                match Request::post("/api/scan")
                    .header("Content-Type", "application/json")
                    .body(request_body.to_string())
                    .unwrap()
                    .send()
                    .await
                {
                    Ok(response) => {
                        if let Ok(data) = response.json::<ScanResponse>().await {
                            opp_clone.set(data.opportunities);
                            sum_clone.set(data.summaries);
                            logs_clone.set(data.logs);
                            status_clone.set(format!("Found {} opportunities", data.opportunities.len()));
                        } else {
                            status_clone.set("Failed to parse response".to_string());
                        }
                    }
                    Err(e) => {
                        status_clone.set(format!("Error: {}", e));
                    }
                }
                
                scan_clone.set(false);
            });
        })
    };
    
    html! {
        <div class="app-container">
            <header>
                <h1>{ "🪙 Crypto Arbitrage Scanner" }</h1>
                <p>{ "Bellman-Ford Algorithm • Binance • Bybit • KuCoin" }</p>
            </header>
            
            <div class="controls-card">
                <h3>{ "Scan Configuration" }</h3>
                
                <div class="exchange-selector">
                    <label class={if (*binance_ref).clone().cast::<HtmlInputElement>().map_or(false, |i| i.checked()) { "selected" } else { "" }}>
                        <input type="checkbox" ref={binance_ref} checked=true />
                        <span class="exchange-badge binance">{"BINANCE"}</span>
                    </label>
                    <label>
                        <input type="checkbox" ref={bybit_ref} checked=true />
                        <span class="exchange-badge bybit">{"BYBIT"}</span>
                    </label>
                    <label>
                        <input type="checkbox" ref={kucoin_ref} checked=true />
                        <span class="exchange-badge kucoin">{"KUCOIN"}</span>
                    </label>
                </div>
                
                <div class="params">
                    <div class="param">
                        <label>{ "Min Profit %" }</label>
                        <input type="number" ref={profit_ref} value="0.3" step="0.1" min="0.1" />
                    </div>
                    <div class="param">
                        <label>{ "Duration (sec)" }</label>
                        <input type="number" ref={duration_ref} value="10" step="1" min="5" max="30" />
                    </div>
                </div>
                
                <button class="scan-btn" onclick={on_scan} disabled={*scanning}>
                    {if *scanning { 
                        html! { <span class="loading"></span> }
                    } else { 
                        "🚀 Start Scan" 
                    }}
                </button>
                
                <div class="status">
                    <span class={if *scanning { "scanning" } else { "" }}>{ (*status).clone() }</span>
                </div>
            </div>
            
            if !logs.is_empty() {
                <div class="logs-card">
                    <h3>{ "📋 Activity Log" }</h3>
                    <div class="log-container">
                        {for logs.iter().map(|log| {
                            let level_class = format!("log-{}", log.level);
                            html! {
                                <div class={classes!("log-entry", level_class)}>
                                    <span class="log-time">{ &log.timestamp }</span>
                                    <span class="log-exchange">{ &log.exchange }</span>
                                    <span class="log-message">{ &log.message }</span>
                                </div>
                            }
                        })}
                    </div>
                </div>
            }
            
            if !summaries.is_empty() {
                <div class="summaries-card">
                    <h3>{ "📊 Exchange Summaries" }</h3>
                    <div class="summary-grid">
                        {for summaries.iter().map(|s| {
                            html! {
                                <div class="summary-card">
                                    <div class="summary-title">{ s.exchange.to_uppercase() }</div>
                                    <div class="summary-stats">
                                        <div><span class="stat-label">Pairs:</span> <span class="stat-value">{ s.pairs_collected }</span></div>
                                        <div><span class="stat-label">Paths:</span> <span class="stat-value">{ s.paths_found }</span></div>
                                        <div><span class="stat-label">Profitable:</span> <span class="stat-value">{ s.profitable_triangles }</span></div>
                                        <div><span class="stat-label">Time:</span> <span class="stat-value">{ s.collection_time_secs }s</span></div>
                                    </div>
                                </div>
                            }
                        })}
                    </div>
                </div>
            }
            
            if !opportunities.is_empty() {
                <div class="results-card">
                    <h3>{ format!("🎯 Opportunities Found ({})", opportunities.len()) }</h3>
                    <div class="opportunities-list">
                        {for opportunities.iter().map(|opp| {
                            let profit_class = if opp.profit_margin_before > 1.0 { "high" } 
                                else if opp.profit_margin_before > 0.5 { "medium" } 
                                else { "low" };
                            
                            let chance_class = if opp.chance_of_executing > 80.0 { "high" }
                                else if opp.chance_of_executing > 70.0 { "medium" }
                                else { "low" };
                            
                            html! {
                                <div class="opportunity-card">
                                    <div class="opp-header">
                                        <span class={classes!("exchange-badge", opp.exchange.clone())}>{ opp.exchange.to_uppercase() }</span>
                                        <span class={classes!("profit", profit_class)}>{ format!("{:.3}%", opp.profit_margin_before) }</span>
                                    </div>
                                    <div class="opp-path">{ &opp.pair }</div>
                                    <div class="opp-details">
                                        <span>After fees: <strong>{ format!("{:.3}%", opp.profit_margin_after) }</strong></span>
                                        <span class={classes!("chance", chance_class)}>{ format!("{:.1}% chance", opp.chance_of_executing) }</span>
                                        <span>Slippage: { format!("{:.3}%", opp.estimated_slippage) }</span>
                                    </div>
                                </div>
                            }
                        })}
                    </div>
                </div>
            }
        </div>
    }
}

#[wasm_bindgen(start)]
pub async fn run() -> Result<(), JsValue> {
    yew::Renderer::<App>::new().render();
    Ok(())
}
