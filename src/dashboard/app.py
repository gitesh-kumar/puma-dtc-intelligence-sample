from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import sqlite3
import pandas as pd
import json
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agent.query_agent import ask

app = FastAPI(title="PUMA DTC Inventory Intelligence")

DB_PATH = "puma_dtc.db"

def get_data(query):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(query, conn)
    conn.close()
    return df

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    health = get_data("SELECT * FROM gold_inventory_health ORDER BY inventory_health_score ASC")
    markdown = get_data("SELECT * FROM gold_markdown_risk ORDER BY estimated_markdown_cost_eur DESC")
    reorder = get_data("SELECT * FROM gold_reorder_signals ORDER BY reorder_priority DESC")
    sell_through = get_data("SELECT * FROM gold_sell_through ORDER BY sell_through_rate DESC")

    total_markdown_risk = markdown["estimated_markdown_cost_eur"].sum()
    critical_divisions = len(health[health["health_label"] == "CRITICAL"])
    avg_health = health["inventory_health_score"].mean()
    total_inventory_value = markdown["inventory_value_eur"].sum()

    health_colors = {
        "HEALTHY": "#22c55e",
        "WATCH": "#f59e0b", 
        "AT RISK": "#f97316",
        "CRITICAL": "#ef4444"
    }

    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PUMA DTC Inventory Intelligence</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ 
            font-family: 'Helvetica Neue', Arial, sans-serif; 
            background: #0a0a0a; 
            color: #ffffff;
            min-height: 100vh;
        }}
        .header {{
            background: linear-gradient(135deg, #1a1a1a, #2d2d2d);
            padding: 24px 40px;
            border-bottom: 2px solid #ff0000;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        .logo {{ 
            font-size: 28px; 
            font-weight: 900; 
            letter-spacing: 4px;
            color: #ffffff;
        }}
        .logo span {{ color: #ff0000; }}
        .subtitle {{ 
            font-size: 13px; 
            color: #888; 
            letter-spacing: 2px;
            text-transform: uppercase;
            margin-top: 4px;
        }}
        .timestamp {{
            font-size: 12px;
            color: #666;
        }}
        .container {{ padding: 32px 40px; }}
        
        .kpi-grid {{
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 16px;
            margin-bottom: 32px;
        }}
        .kpi-card {{
            background: #1a1a1a;
            border: 1px solid #2d2d2d;
            border-radius: 12px;
            padding: 24px;
        }}
        .kpi-label {{
            font-size: 11px;
            color: #666;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 8px;
        }}
        .kpi-value {{
            font-size: 32px;
            font-weight: 700;
            color: #ffffff;
        }}
        .kpi-value.danger {{ color: #ef4444; }}
        .kpi-value.warning {{ color: #f59e0b; }}
        .kpi-value.good {{ color: #22c55e; }}
        .kpi-sub {{
            font-size: 12px;
            color: #555;
            margin-top: 4px;
        }}
        
        .grid-2 {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
            margin-bottom: 24px;
        }}
        .grid-3 {{
            display: grid;
            grid-template-columns: 1fr 1fr 1fr;
            gap: 20px;
            margin-bottom: 24px;
        }}
        .card {{
            background: #1a1a1a;
            border: 1px solid #2d2d2d;
            border-radius: 12px;
            padding: 24px;
        }}
        .card-title {{
            font-size: 13px;
            font-weight: 600;
            color: #888;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 20px;
            display: flex;
            align-items: center;
            gap: 8px;
        }}
        .card-title::before {{
            content: '';
            display: inline-block;
            width: 3px;
            height: 14px;
            background: #ff0000;
            border-radius: 2px;
        }}
        
        .division-row {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 12px 0;
            border-bottom: 1px solid #222;
        }}
        .division-row:last-child {{ border-bottom: none; }}
        .division-name {{
            font-size: 14px;
            font-weight: 600;
            color: #fff;
            width: 100px;
        }}
        .health-bar-container {{
            flex: 1;
            margin: 0 16px;
            height: 6px;
            background: #2d2d2d;
            border-radius: 3px;
            overflow: hidden;
        }}
        .health-bar {{
            height: 100%;
            border-radius: 3px;
            transition: width 0.5s ease;
        }}
        .health-score {{
            font-size: 14px;
            font-weight: 700;
            width: 40px;
            text-align: right;
        }}
        .badge {{
            font-size: 10px;
            font-weight: 700;
            padding: 3px 8px;
            border-radius: 4px;
            margin-left: 8px;
            letter-spacing: 0.5px;
        }}
        .badge-HEALTHY {{ background: #052e16; color: #22c55e; }}
        .badge-WATCH {{ background: #2d1f00; color: #f59e0b; }}
        .badge-AT.RISK {{ background: #2d1200; color: #f97316; }}
        .badge-CRITICAL {{ background: #2d0000; color: #ef4444; }}
        
        .markdown-row {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 10px 0;
            border-bottom: 1px solid #222;
        }}
        .markdown-row:last-child {{ border-bottom: none; }}
        .risk-badge {{
            font-size: 10px;
            font-weight: 700;
            padding: 3px 8px;
            border-radius: 4px;
        }}
        .risk-CRITICAL {{ background: #2d0000; color: #ef4444; }}
        .risk-HIGH {{ background: #2d1200; color: #f97316; }}
        .risk-MEDIUM {{ background: #2d2000; color: #f59e0b; }}
        .risk-LOW {{ background: #052e16; color: #22c55e; }}
        
        .ai-section {{
            background: #1a1a1a;
            border: 1px solid #2d2d2d;
            border-radius: 12px;
            padding: 24px;
            margin-bottom: 24px;
        }}
        .ai-input-row {{
            display: flex;
            gap: 12px;
            margin-top: 16px;
        }}
        .ai-input {{
            flex: 1;
            background: #0d0d0d;
            border: 1px solid #333;
            border-radius: 8px;
            padding: 12px 16px;
            color: #fff;
            font-size: 14px;
            outline: none;
        }}
        .ai-input:focus {{ border-color: #ff0000; }}
        .ai-button {{
            background: #ff0000;
            color: #fff;
            border: none;
            border-radius: 8px;
            padding: 12px 24px;
            font-size: 14px;
            font-weight: 600;
            cursor: pointer;
            letter-spacing: 1px;
        }}
        .ai-button:hover {{ background: #cc0000; }}
        .ai-response {{
            margin-top: 16px;
            padding: 16px;
            background: #0d0d0d;
            border-radius: 8px;
            font-size: 14px;
            color: #ccc;
            line-height: 1.6;
            min-height: 60px;
            border-left: 3px solid #ff0000;
            display: none;
        }}
        .ai-loading {{
            color: #555;
            font-style: italic;
        }}
        
        .quick-questions {{
            display: flex;
            gap: 8px;
            flex-wrap: wrap;
            margin-top: 12px;
        }}
        .quick-q {{
            background: #0d0d0d;
            border: 1px solid #333;
            border-radius: 20px;
            padding: 6px 14px;
            font-size: 12px;
            color: #888;
            cursor: pointer;
        }}
        .quick-q:hover {{ border-color: #ff0000; color: #fff; }}
        
        .chart-container {{
            position: relative;
            height: 220px;
        }}
        
        .weeks-indicator {{
            font-size: 20px;
            font-weight: 700;
        }}
        .weeks-good {{ color: #22c55e; }}
        .weeks-warning {{ color: #f59e0b; }}
        .weeks-danger {{ color: #ef4444; }}
    </style>
</head>
<body>

<div class="header">
    <div>
        <div class="logo">PUMA <span>DTC</span> INTELLIGENCE</div>
        <div class="subtitle">Inventory Analytics Dashboard</div>
    </div>
    <div class="timestamp">Last updated: {pd.Timestamp.now().strftime('%d %b %Y, %H:%M')}</div>
</div>

<div class="container">

    <!-- KPI Row -->
    <div class="kpi-grid">
        <div class="kpi-card">
            <div class="kpi-label">Total Markdown Risk</div>
            <div class="kpi-value danger">€{total_markdown_risk:,.0f}</div>
            <div class="kpi-sub">Across all divisions</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-label">Critical Divisions</div>
            <div class="kpi-value {'danger' if critical_divisions > 0 else 'good'}">{critical_divisions}</div>
            <div class="kpi-sub">Require immediate action</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-label">Avg Health Score</div>
            <div class="kpi-value {'good' if avg_health >= 70 else 'warning' if avg_health >= 50 else 'danger'}">{avg_health:.1f}</div>
            <div class="kpi-sub">Out of 100</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-label">Total Inventory Value</div>
            <div class="kpi-value">€{total_inventory_value:,.0f}</div>
            <div class="kpi-sub">Simulated stock value</div>
        </div>
    </div>

    <!-- Health Scores + Charts -->
    <div class="grid-2">
        <div class="card">
            <div class="card-title">Inventory Health by Division</div>
            {''.join([f"""
            <div class="division-row">
                <div class="division-name">{row.puma_division}</div>
                <div class="health-bar-container">
                    <div class="health-bar" style="width:{row.inventory_health_score}%; background:{health_colors.get(row.health_label, '#888')}"></div>
                </div>
                <div class="health-score" style="color:{health_colors.get(row.health_label, '#888')}">{row.inventory_health_score}</div>
                <span class="badge badge-{row.health_label.replace(' ', '.')}">{row.health_label}</span>
            </div>
            """ for row in health.itertuples()])}
        </div>
        
        <div class="card">
            <div class="card-title">Sell-Through Rate by Division</div>
            <div class="chart-container">
                <canvas id="sellThroughChart"></canvas>
            </div>
        </div>
    </div>

    <!-- Markdown Risk + Reorder -->
    <div class="grid-2">
        <div class="card">
            <div class="card-title">Markdown Risk Exposure</div>
            {''.join([f"""
            <div class="markdown-row">
                <div style="font-size:14px;font-weight:600;width:100px">{row.puma_division}</div>
                <span class="risk-badge risk-{row.markdown_risk}">{row.markdown_risk}</span>
                <div style="font-size:13px;color:#888;margin-left:12px">{row.weeks_of_stock:.1f} weeks stock</div>
                <div style="font-size:14px;font-weight:700;color:#ef4444;margin-left:auto">€{row.estimated_markdown_cost_eur:,.0f}</div>
            </div>
            """ for row in markdown.itertuples()])}
        </div>
        
        <div class="card">
            <div class="card-title">Weeks of Stock Remaining</div>
            <div class="chart-container">
                <canvas id="weeksChart"></canvas>
            </div>
        </div>
    </div>

    <!-- AI Agent -->
    <div class="ai-section">
        <div class="card-title">AI Inventory Analyst</div>
        <div style="font-size:13px;color:#555;margin-bottom:4px">Ask anything about your inventory in plain English</div>
        
        <div class="quick-questions">
            <div class="quick-q" onclick="askQuestion('Which division needs urgent attention?')">Which division needs urgent attention?</div>
            <div class="quick-q" onclick="askQuestion('What is our total markdown exposure?')">Total markdown exposure?</div>
            <div class="quick-q" onclick="askQuestion('Which divisions are performing well?')">Which divisions perform well?</div>
            <div class="quick-q" onclick="askQuestion('What should I focus on this week?')">What to focus on this week?</div>
        </div>
        
        <div class="ai-input-row">
            <input type="text" class="ai-input" id="aiInput" placeholder="e.g. Which division should I reorder first?" 
                   onkeypress="if(event.key==='Enter') askQuestion()"/>
            <button class="ai-button" onclick="askQuestion()">ASK</button>
        </div>
        <div class="ai-response" id="aiResponse"></div>
    </div>

</div>

<script>
    // Sell-through chart
    const stCtx = document.getElementById('sellThroughChart').getContext('2d');
    new Chart(stCtx, {{
        type: 'bar',
        data: {{
            labels: {json.dumps(list(sell_through['puma_division']))},
            datasets: [{{
                label: 'Sell-Through Rate %',
                data: {json.dumps(list(sell_through['sell_through_rate'].round(1)))},
                backgroundColor: {json.dumps([health_colors.get(h, '#888') for h in health.set_index('puma_division').reindex(sell_through['puma_division'])['health_label'].fillna('WATCH')])},
                borderRadius: 6,
            }}]
        }},
        options: {{
            responsive: true,
            maintainAspectRatio: false,
            plugins: {{ legend: {{ display: false }} }},
            scales: {{
                y: {{ 
                    beginAtZero: true, 
                    max: 100,
                    grid: {{ color: '#222' }},
                    ticks: {{ color: '#666', callback: v => v + '%' }}
                }},
                x: {{ grid: {{ display: false }}, ticks: {{ color: '#888' }} }}
            }}
        }}
    }});

    // Weeks of stock chart
    const wCtx = document.getElementById('weeksChart').getContext('2d');
    const weeksData = {json.dumps(list(reorder['weeks_of_stock'].round(1)))};
    const weeksColors = weeksData.map(w => w < 8 ? '#ef4444' : w < 16 ? '#f59e0b' : '#22c55e');
    new Chart(wCtx, {{
        type: 'bar',
        data: {{
            labels: {json.dumps(list(reorder['puma_division']))},
            datasets: [{{
                label: 'Weeks of Stock',
                data: weeksData,
                backgroundColor: weeksColors,
                borderRadius: 6,
            }}]
        }},
        options: {{
            responsive: true,
            maintainAspectRatio: false,
            plugins: {{ legend: {{ display: false }} }},
            scales: {{
                y: {{ 
                    beginAtZero: true,
                    grid: {{ color: '#222' }},
                    ticks: {{ color: '#666', callback: v => v + 'w' }}
                }},
                x: {{ grid: {{ display: false }}, ticks: {{ color: '#888' }} }}
            }}
        }}
    }});

    // AI Agent
    async function askQuestion(question) {{
        const input = document.getElementById('aiInput');
        const response = document.getElementById('aiResponse');
        
        const q = question || input.value.trim();
        if (!q) return;
        
        input.value = q;
        response.style.display = 'block';
        response.innerHTML = '<span class="ai-loading">Analyzing inventory data...</span>';
        
        try {{
            const res = await fetch('/ask', {{
                method: 'POST',
                headers: {{ 'Content-Type': 'application/json' }},
                body: JSON.stringify({{ question: q }})
            }});
            const data = await res.json();
            response.innerHTML = data.answer;
        }} catch(e) {{
            response.innerHTML = 'Error connecting to AI agent.';
        }}
    }}
</script>

</body>
</html>
"""
    return html

@app.post("/ask")
async def ask_endpoint(request: Request):
    body = await request.json()
    question = body.get("question", "")
    answer = ask(question)
    return {"answer": answer}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)