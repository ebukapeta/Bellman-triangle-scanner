// src/frontend.rs - With Activity Log Panel
use yew::prelude::*;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;
use gloo_net::http::Request;
use serde::{Deserialize, Serialize};
use web_sys::HtmlInputElement;
use std::collections::HashMap;

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
pub struct ScanConfig {
    pub min_profit_threshold: f64,
    pub exchanges: Vec<String>,
    pub max_triangle_length: usize,
    pub collection_duration_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScanRequest {
    pub exchanges: Vec<String>,
    pub min_profit: Option<f64>,
    pub collection_duration: Option<u64>,
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
    pub avg_latency_ms: f64,
}

// ==================== UI Components ====================

#[derive(Properties, PartialEq)]
struct OpportunityTableProps {
    opportunities: Vec<ArbitrageOpportunity>,
    on_view_details: Callback<String>,
}

#[function_component(OpportunityTable)]
fn opportunity_table(props: &OpportunityTableProps) -> Html {
    html! {
        <div class="opportunity-table">
            <table>
                <thead>
                    <tr>
                        <th>{ "Exchange" }</th>
                        <th>{ "Triangle" }</th>
                        <th>{ "Profit (%)" }</th>
                        <th>{ "After Fees" }</th>
                        <th>{ "Chance" }</th>
                        <th>{ "Slippage" }</th>
                        <th>{ "Actions" }</th>
                    </tr>
                </thead>
                <tbody>
                    {props.opportunities.iter().map(|opp| {
                        let profit_class = if opp.profit_margin_before > 1.0 { "high-profit" } 
                            else if opp.profit_margin_before > 0.5 { "medium-profit" } 
                            else { "low-profit" };
                        
                        let chance_class = if opp.chance_of_executing > 80.0 { "high-chance" } 
                            else if opp.chance_of_executing > 70.0 { "medium-chance" } 
                            else { "low-chance" };
                        
                        let on_click = {
                            let pair = opp.pair.clone();
                            let callback = props.on_view_details.clone();
                            Callback::from(move |_| callback.emit(pair.clone()))
                        };
                        
                        html! {
                            <tr>
                                <td class="exchange">{ &opp.exchange }</td>
                                <td class="triangle">{ &opp.pair }</td>
                                <td class={classes!("profit", profit_class)}>
                                    { format!("{:.3}%", opp.profit_margin_before) }
                                </td>
                                <td class="profit">
                                    { format!("{:.3}%", opp.profit_margin_after) }
                                </td>
                                <td class={classes!("chance", chance_class)}>
                                    { format!("{:.1}%", opp.chance_of_executing) }
                                </td>
                                <td>{ format!("{:.3}%", opp.estimated_slippage) }</td>
                                <td>
                                    <button 
                                        class="details-btn"
                                        onclick={on_click}
                                    >
                                        { "View" }
                                    </button>
                                </td>
                            </tr>
                        }
                    }).collect::<Html>()}
                </tbody>
            </table>
        </div>
    }
}

#[derive(Properties, PartialEq)]
struct ActivityLogProps {
    logs: Vec<ScanLog>,
    summaries: Vec<ScanSummary>,
}

#[function_component(ActivityLog)]
fn activity_log(props: &ActivityLogProps) -> Html {
    html! {
        <div class="activity-log">
            <div class="log-header">
                <h3>{ "Scan Activity Log" }</h3>
                <span class="log-count">{ format!("{} events", props.logs.len()) }</span>
            </div>
            
            <div class="summaries">
                {props.summaries.iter().map(|summary| {
                    html! {
                        <div class="summary-card">
                            <div class="summary-title">{ summary.exchange.to_uppercase() }</div>
                            <div class="summary-stats">
                                <div class="stat">
                                    <span class="stat-label">Pairs:</span>
                                    <span class="stat-value">{ summary.pairs_collected }</span>
                                </div>
                                <div class="stat">
                                    <span class="stat-label">Paths:</span>
                                    <span class="stat-value">{ summary.paths_found }</span>
                                </div>
                                <div class="stat">
                                    <span class="stat-label">Profitable:</span>
                                    <span class="stat-value">{ summary.profitable_triangles }</span>
                                </div>
                                <div class="stat">
                                    <span class="stat-label">Time:</span>
                                    <span class="stat-value">{ summary.collection_time_secs }s</span>
                                </div>
                            </div>
                        </div>
                    }
                }).collect::<Html>()}
            </div>
            
            <div class="log-entries">
                {props.logs.iter().map(|log| {
                    let level_class = match log.level.as_str() {
                        "success" => "log-success",
                        "error" => "log-error",
                        "warning" => "log-warning",
                        "debug" => "log-debug",
                        _ => "log-info",
                    };
                    
                    html! {
                        <div class={classes!("log-entry", level_class)}>
                            <span class="log-time">{ &log.timestamp }</span>
                            <span class="log-exchange">{ &log.exchange }</span>
                            <span class="log-message">{ &log.message }</span>
                        </div>
                    }
                }).collect::<Html>()}
            </div>
        </div>
    }
}

#[function_component(ControlPanel)]
fn control_panel() -> Html {
    let config = use_state(|| ScanConfig::default());
    let scanning = use_state(|| false);
    let opportunities = use_state(|| Vec::<ArbitrageOpportunity>::new());
    let logs = use_state(|| Vec::<ScanLog>::new());
    let summaries = use_state(|| Vec::<ScanSummary>::new());
    
    let binance_ref = use_node_ref();
    let bybit_ref = use_node_ref();
    let kucoin_ref = use_node_ref();
    let profit_ref = use_node_ref();
    let duration_ref = use_node_ref();
    
    // Fetch config on mount
    {
        let config = config.clone();
        use_effect_with((), move |_| {
            let config_clone = config.clone();
            spawn_local(async move {
                if let Ok(cfg) = Request::get("/api/config").send().await {
                    if let Ok(cfg) = cfg.json::<ScanConfig>().await {
                        config_clone.set(cfg);
                    }
                }
            });
            || ()
        });
    }
    
    let on_scan = {
        let scanning = scanning.clone();
        let opportunities = opportunities.clone();
        let logs = logs.clone();
        let summaries = summaries.clone();
        let binance_ref = binance_ref.clone();
        let bybit_ref = bybit_ref.clone();
        let kucoin_ref = kucoin_ref.clone();
        let profit_ref = profit_ref.clone();
        let duration_ref = duration_ref.clone();
        
        Callback::from(move |_| {
            let scanning_clone = scanning.clone();
            let opp_clone = opportunities.clone();
            let logs_clone = logs.clone();
            let summaries_clone = summaries.clone();
            
            spawn_local(async move {
                scanning_clone.set(true);
                
                // Clear previous results
                opp_clone.set(Vec::new());
                logs_clone.set(Vec::new());
                summaries_clone.set(Vec::new());
                
                // Get selected exchanges
                let mut exchanges = Vec::new();
                if let Some(input) = binance_ref.cast::<HtmlInputElement>() {
                    if input.checked() {
                        exchanges.push("binance".to_string());
                    }
                }
                if let Some(input) = bybit_ref.cast::<HtmlInputElement>() {
                    if input.checked() {
                        exchanges.push("bybit".to_string());
                    }
                }
                if let Some(input) = kucoin_ref.cast::<HtmlInputElement>() {
                    if input.checked() {
                        exchanges.push("kucoin".to_string());
                    }
                }
                
                let min_profit = profit_ref
                    .cast::<HtmlInputElement>()
                    .and_then(|input| input.value().parse::<f64>().ok())
                    .unwrap_or(0.3);
                
                let duration = duration_ref
                    .cast::<HtmlInputElement>()
                    .and_then(|input| input.value().parse::<u64>().ok())
                    .unwrap_or(10);
                
                let request = ScanRequest {
                    exchanges,
                    min_profit: Some(min_profit),
                    collection_duration: Some(duration),
                };
                
                // Call scan command
                match Request::post("/api/scan")
                    .json(&request)
                    .unwrap()
                    .send()
                    .await 
                {
                    Ok(response) => {
                        if let Ok((results, summs, log_entries)) = response.json::<(Vec<ArbitrageOpportunity>, Vec<ScanSummary>, Vec<ScanLog>)>().await {
                            opp_clone.set(results);
                            summaries_clone.set(summs);
                            logs_clone.set(log_entries);
                        }
                    }
                    Err(e) => {
                        logs_clone.set(vec![ScanLog {
                            timestamp: chrono::Local::now().format("%H:%M:%S").to_string(),
                            exchange: "system".to_string(),
                            message: format!("Scan failed: {}", e),
                            level: "error".to_string(),
                        }]);
                    }
                }
                
                scanning_clone.set(false);
            });
        })
    };
    
    let on_view_details = {
        let opportunities = opportunities.clone();
        Callback::from(move |pair: String| {
            if let Some(opp) = opportunities.iter().find(|o| o.pair == pair) {
                let path_str = opp.triangle.join(" → ");
                let message = format!(
                    "Triangle Path: {}\n\nStep-by-step:\n{}\n\nEstimated Profit: {:.3}%\nExecution Chance: {:.1}%",
                    path_str,
                    opp.triangle.iter().enumerate()
                        .map(|(i, c)| format!("{}. {}", i+1, c))
                        .collect::<Vec<_>>()
                        .join("\n"),
                    opp.profit_margin_before,
                    opp.chance_of_executing
                );
                
                let window = web_sys::window().unwrap();
                window.alert_with_message(&message).unwrap();
            }
        })
    };
    
    html! {
        <div class="control-panel">
            <h2>{ "Triangular Arbitrage Scanner" }</h2>
            
            <div class="scan-controls">
                <div class="exchange-selector">
                    <h3>{ "Exchanges" }</h3>
                    <div class="checkbox-group">
                        <label>
                            <input 
                                ref={binance_ref}
                                type="checkbox" 
                                checked={true}
                            />
                            <span class="exchange-badge binance">{"BINANCE"}</span>
                        </label>
                        <label>
                            <input 
                                ref={bybit_ref}
                                type="checkbox" 
                                checked={true}
                            />
                            <span class="exchange-badge bybit">{"BYBIT"}</span>
                        </label>
                        <label>
                            <input 
                                ref={kucoin_ref}
                                type="checkbox" 
                                checked={true}
                            />
                            <span class="exchange-badge kucoin">{"KUCOIN"}</span>
                        </label>
                    </div>
                </div>
                
                <div class="scan-params">
                    <div class="param-item">
                        <label>{ "Min Profit %" }</label>
                        <input 
                            ref={profit_ref}
                            type="number"
                            step="0.1"
                            min="0.1"
                            value="0.3"
                        />
                    </div>
                    
                    <div class="param-item">
                        <label>{ "Collect for (s)" }</label>
                        <input 
                            ref={duration_ref}
                            type="number"
                            step="1"
                            min="5"
                            max="30"
                            value="10"
                        />
                    </div>
                </div>
                
                <button 
                    class="scan-btn"
                    onclick={on_scan}
                    disabled={*scanning}
                >
                    {if *scanning { 
                        html! { 
                            <>
                                <span class="spinner-small"></span>
                                { " Scanning..." }
                            </>
                        }
                    } else { 
                        html! { "🔍 Scan Now" }
                    }}
                </button>
            </div>
            
            <div class="main-content">
                <div class="results-section">
                    <div class="section-header">
                        <h3>{ format!("Opportunities ({})", opportunities.len()) }</h3>
                        {if !opportunities.is_empty() {
                            html! { <span class="total-profit">
                                { format!("Total Profit: {:.2}%", opportunities.iter().map(|o| o.profit_margin_before).sum::<f64>()) }
                            </span>}
                        } else {
                            html! {}
                        }}
                    </div>
                    
                    if opportunities.is_empty() && !*scanning {
                        <div class="empty-state">
                            <p>{ "No opportunities found. Click Scan Now to start detection." }</p>
                        </div>
                    } else {
                        <OpportunityTable 
                            opportunities={(*opportunities).clone()} 
                            on_view_details={on_view_details}
                        />
                    }
                </div>
                
                <ActivityLog logs={(*logs).clone()} summaries={(*summaries).clone()} />
            </div>
        </div>
    }
}

#[function_component(App)]
fn app() -> Html {
    html! {
        <div class="app">
            <header class="app-header">
                <h1>{ "Crypto Arbitrage Scanner" }</h1>
                <div class="header-badges">
                    <span class="badge">{"WebSocket Collection"}</span>
                    <span class="badge">{"10s Sampling"}</span>
                    <span class="badge">{"Bellman-Ford"}</span>
                </div>
            </header>
            
            <main class="app-main">
                <ControlPanel />
            </main>
            
            <footer class="app-footer">
                <p>{ "⚡ Collects data via WebSocket for 10 seconds • Scans on-demand • No API keys required" }</p>
            </footer>
        </div>
    }
}

// ==================== Styles ====================

const STYLES: &str = r#"
:root {
    --primary: #3b82f6;
    --primary-dark: #2563eb;
    --success: #10b981;
    --warning: #f59e0b;
    --danger: #ef4444;
    --dark: #1f2937;
    --light: #f9fafb;
    --gray: #6b7280;
    --border: #e5e7eb;
    --binance: #f0b90b;
    --bybit: #5c6bc0;
    --kucoin: #0095e5;
}

* {
    margin: 0;
    padding: 0;
    box-sizing: border-box;
}

body {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    min-height: 100vh;
    padding: 20px;
}

.app {
    max-width: 1600px;
    margin: 0 auto;
}

.app-header {
    margin-bottom: 2rem;
    color: white;
}

.app-header h1 {
    font-size: 2.5rem;
    font-weight: 700;
    margin-bottom: 0.5rem;
}

.header-badges {
    display: flex;
    gap: 1rem;
}

.badge {
    background: rgba(255,255,255,0.2);
    padding: 0.3rem 1rem;
    border-radius: 2rem;
    font-size: 0.9rem;
    backdrop-filter: blur(10px);
}

.app-main {
    background: white;
    border-radius: 1rem;
    box-shadow: 0 20px 25px -5px rgba(0,0,0,0.2);
    overflow: hidden;
}

.control-panel {
    padding: 2rem;
}

.scan-controls {
    display: grid;
    grid-template-columns: 2fr 1fr auto;
    gap: 2rem;
    align-items: end;
    margin-bottom: 2rem;
    padding: 1.5rem;
    background: var(--light);
    border-radius: 0.75rem;
    border: 1px solid var(--border);
}

.exchange-selector h3 {
    font-size: 0.9rem;
    font-weight: 600;
    color: var(--gray);
    margin-bottom: 0.75rem;
    text-transform: uppercase;
}

.checkbox-group {
    display: flex;
    gap: 1.5rem;
}

.checkbox-group label {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    cursor: pointer;
}

.exchange-badge {
    padding: 0.4rem 1rem;
    border-radius: 2rem;
    font-size: 0.85rem;
    font-weight: 600;
    color: white;
}

.exchange-badge.binance { background: var(--binance); }
.exchange-badge.bybit { background: var(--bybit); }
.exchange-badge.kucoin { background: var(--kucoin); }

.scan-params {
    display: flex;
    gap: 1rem;
}

.param-item {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
}

.param-item label {
    font-size: 0.85rem;
    font-weight: 600;
    color: var(--gray);
    text-transform: uppercase;
}

.param-item input {
    padding: 0.6rem;
    border: 2px solid var(--border);
    border-radius: 0.5rem;
    font-size: 1rem;
    width: 100px;
}

.scan-btn {
    padding: 0.75rem 2rem;
    background: var(--primary);
    color: white;
    border: none;
    border-radius: 0.5rem;
    font-size: 1rem;
    font-weight: 600;
    cursor: pointer;
    transition: all 0.2s;
    display: flex;
    align-items: center;
    gap: 0.5rem;
}

.scan-btn:hover:not(:disabled) {
    background: var(--primary-dark);
    transform: translateY(-2px);
}

.scan-btn:disabled {
    background: var(--gray);
    cursor: not-allowed;
}

.spinner-small {
    width: 16px;
    height: 16px;
    border: 2px solid rgba(255,255,255,0.3);
    border-top-color: white;
    border-radius: 50%;
    animation: spin 0.8s linear infinite;
    display: inline-block;
}

.main-content {
    display: grid;
    grid-template-columns: 1fr 350px;
    gap: 2rem;
}

.results-section {
    background: white;
    border: 1px solid var(--border);
    border-radius: 0.5rem;
    padding: 1.5rem;
}

.section-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 1rem;
}

.total-profit {
    background: var(--success);
    color: white;
    padding: 0.3rem 1rem;
    border-radius: 2rem;
    font-size: 0.9rem;
    font-weight: 600;
}

.opportunity-table {
    overflow-x: auto;
}

.opportunity-table table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.9rem;
}

.opportunity-table th {
    text-align: left;
    padding: 1rem 0.5rem;
    font-size: 0.8rem;
    font-weight: 600;
    color: var(--gray);
    text-transform: uppercase;
    border-bottom: 2px solid var(--border);
}

.opportunity-table td {
    padding: 1rem 0.5rem;
    border-bottom: 1px solid var(--border);
}

.exchange {
    font-weight: 600;
    text-transform: capitalize;
}

.triangle {
    font-family: monospace;
    font-weight: 500;
}

.profit.high-profit { color: var(--success); font-weight: 700; }
.profit.medium-profit { color: var(--warning); font-weight: 600; }
.profit.low-profit { color: var(--danger); }

.chance.high-chance { color: var(--success); font-weight: 600; }
.chance.medium-chance { color: var(--warning); }
.chance.low-chance { color: var(--danger); }

.details-btn {
    padding: 0.3rem 0.6rem;
    background: var(--light);
    border: 1px solid var(--border);
    border-radius: 0.3rem;
    cursor: pointer;
    font-size: 0.8rem;
}

.details-btn:hover {
    background: var(--primary);
    color: white;
    border-color: var(--primary);
}

.activity-log {
    background: #1a1a1a;
    border-radius: 0.5rem;
    overflow: hidden;
    display: flex;
    flex-direction: column;
    height: 600px;
}

.log-header {
    padding: 1rem;
    background: #2d2d2d;
    color: white;
    display: flex;
    justify-content: space-between;
    align-items: center;
}

.log-header h3 {
    font-size: 1rem;
    font-weight: 600;
}

.log-count {
    background: #404040;
    padding: 0.2rem 0.6rem;
    border-radius: 1rem;
    font-size: 0.8rem;
}

.summaries {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
    gap: 0.5rem;
    padding: 0.5rem;
    background: #262626;
}

.summary-card {
    background: #333;
    padding: 0.8rem;
    border-radius: 0.3rem;
}

.summary-title {
    font-size: 0.8rem;
    font-weight: 600;
    color: #999;
    text-transform: uppercase;
    margin-bottom: 0.5rem;
}

.summary-stats {
    display: grid;
    grid-template-columns: repeat(2, 1fr);
    gap: 0.3rem;
}

.stat {
    display: flex;
    flex-direction: column;
}

.stat-label {
    font-size: 0.7rem;
    color: #666;
}

.stat-value {
    font-size: 1rem;
    font-weight: 700;
    color: white;
}

.log-entries {
    flex: 1;
    overflow-y: auto;
    padding: 0.5rem;
    font-family: 'Monaco', 'Menlo', monospace;
    font-size: 0.85rem;
}

.log-entry {
    padding: 0.4rem 0.6rem;
    margin-bottom: 0.3rem;
    border-radius: 0.3rem;
    display: flex;
    gap: 0.5rem;
    border-left: 3px solid transparent;
}

.log-time {
    color: #666;
    min-width: 60px;
}

.log-exchange {
    color: #999;
    min-width: 70px;
    text-transform: capitalize;
}

.log-message {
    color: #ccc;
}

.log-success {
    border-left-color: var(--success);
    background: rgba(16, 185, 129, 0.1);
}
.log-error {
    border-left-color: var(--danger);
    background: rgba(239, 68, 68, 0.1);
}
.log-warning {
    border-left-color: var(--warning);
    background: rgba(245, 158, 11, 0.1);
}
.log-info {
    border-left-color: var(--primary);
    background: rgba(59, 130, 246, 0.1);
}
.log-debug {
    border-left-color: var(--gray);
    background: rgba(107, 114, 128, 0.1);
}

.empty-state {
    text-align: center;
    padding: 3rem;
    color: var(--gray);
    background: var(--light);
    border-radius: 0.5rem;
}

.app-footer {
    text-align: center;
    margin-top: 2rem;
    color: rgba(255,255,255,0.8);
    font-size: 0.9rem;
}

@media (max-width: 1200px) {
    .main-content {
        grid-template-columns: 1fr;
    }
    
    .scan-controls {
        grid-template-columns: 1fr;
        gap: 1rem;
    }
}
"#;

// ==================== Entry Point ====================

#[wasm_bindgen(start)]
pub async fn run() -> Result<(), JsValue> {
    wasm_logger::init(wasm_logger::Config::default());
    
    let window = web_sys::window().unwrap();
    let document = window.document().unwrap();
    let style = document.create_element("style").unwrap();
    style.set_inner_html(STYLES);
    document.head().unwrap().append_child(&style).unwrap();
    
    yew::Renderer::<App>::new().render();
    
    Ok(())
}
```
