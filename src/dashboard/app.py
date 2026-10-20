from fastapi import FastAPI, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import HTMLResponse
import html as html_lib
import sqlite3
import pandas as pd
import json
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agent.query_agent import ask

app = FastAPI(title="PUMA DTC Inventory Intelligence")

DB_PATH = "puma_dtc.db"

# One status scale for health labels and markdown risk. Colour never stands alone: every status
# is shown with its word and a shape (circle, triangle, diamond, square).
HEALTH_STATUS = {"HEALTHY": "good", "WATCH": "warning", "AT RISK": "serious", "CRITICAL": "critical"}
RISK_STATUS = {"LOW": "good", "MEDIUM": "warning", "HIGH": "serious", "CRITICAL": "critical"}
CHECK_STATUS = {"PASS": "good", "WARN": "warning", "FAIL": "critical"}
REORDER_LINE_WEEKS = 8  # gold_reorder_signals starts signalling below this many weeks of stock


def get_data(query):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def freshness():
    """What the header shows instead of a clock: the newest sale in the data and the day the pipeline last ran.
    The page itself is rendered on every visit, so 'now' would say nothing about the data."""
    try:
        try:  # the demo snapshot has this tiny table instead of Silver
            newest = get_data("SELECT MAX(last_sale) AS d FROM data_period")["d"].iloc[0]
        except Exception:
            newest = get_data("SELECT MAX(t_dat) AS d FROM silver_transactions WHERE quality_flag = 'clean'")["d"].iloc[0]
        ran = get_data("SELECT MAX(generated_at) AS r FROM gold_inventory_health")["r"].iloc[0]
        return (f"Sales data to {pd.Timestamp(newest).strftime('%d %b %Y')}"
                f" &middot; Pipeline run {pd.Timestamp(ran).strftime('%d %b %Y')}")
    except Exception:
        return "Sales data period unknown"


def esc(value):
    return html_lib.escape(str(value))


def chip(label, status):
    return f'<span class="chip chip-{status}">{esc(str(label).capitalize())}</span>'


def check_cell(status):
    """A passing check is plain text; only warnings and failures get a coloured chip."""
    if status == "PASS":
        return '<span class="pass">Pass</span>'
    return chip(status, CHECK_STATUS.get(status, "warning"))


def quality_panel():
    """Collapsible panel with the latest data checks, written by checks.py."""
    try:
        report = get_data("SELECT run_at, layer, check_name, status, observed, expected FROM data_quality_report")
    except Exception:
        return '<div class="dq dq-none">Data checks have not been run yet (python src\\run_pipeline.py)</div>'
    counts = report["status"].value_counts().to_dict()
    passed, warned, failed = counts.get("PASS", 0), counts.get("WARN", 0), counts.get("FAIL", 0)
    level = "fail" if failed else "warn" if warned else "ok"
    order = {"bronze": 0, "silver": 1, "gold": 2}
    report = report.sort_values("layer", key=lambda col: col.map(order), kind="stable")
    rows = "".join(
        f"<tr><td>{esc(r.layer)}</td><td>{check_cell(r.status)}</td>"
        f"<td>{esc(r.check_name)}</td><td>{esc(r.observed)}</td><td>{esc(r.expected)}</td></tr>"
        for r in report.itertuples()
    )
    last_run = esc(report["run_at"].max())
    return f"""
    <details class="dq dq-{level}">
        <summary>Data checks: {passed} passed, {warned} warnings, {failed} failed <span class="dq-when">(last run {last_run})</span></summary>
        <div class="table-scroll">
            <table>
                <tr><th>Layer</th><th>Status</th><th>Check</th><th>Observed</th><th>Expected</th></tr>
                {rows}
            </table>
        </div>
    </details>
    """


def hero_html(health, markdown):
    """The headline is computed from the Gold tables: the division with the lowest health score."""
    worst = health.iloc[0]
    if worst.health_label == "HEALTHY":
        headline = "No division needs attention right now"
        detail = "Every division scores as healthy on simulated stock."
    else:
        headline = f"{worst.puma_division} needs attention first"
        mask = markdown["puma_division"] == worst.puma_division
        cost = float(markdown.loc[mask, "estimated_markdown_cost_eur"].sum())
        total = float(markdown["estimated_markdown_cost_eur"].sum())
        share = f", {cost / total:.0%} of the total," if total > 0 else ""
        detail = (f"Health score {worst.inventory_health_score:.1f}, rated {str(worst.health_label).lower()}. "
                  f"Estimated markdown cost €{cost:,.0f}{share} on simulated stock.")
    return f'<h1 class="hero-title">{esc(headline)}</h1><p class="hero-detail">{esc(detail)}</p>'


def health_rows(health):
    return "".join(
        f'<div class="row row-health">'
        f'<div class="row-name">{esc(r.puma_division)}</div>'
        f'<div class="bar" role="img" aria-label="Health score {r.inventory_health_score:.1f} out of 100">'
        f'<i class="fill-{HEALTH_STATUS.get(r.health_label, "warning")}" style="width:{max(0, min(100, r.inventory_health_score)):.1f}%"></i></div>'
        f'<div class="row-figure">{r.inventory_health_score:.1f}</div>'
        f'{chip(r.health_label, HEALTH_STATUS.get(r.health_label, "warning"))}'
        f"</div>"
        for r in health.itertuples()
    )


def markdown_rows(markdown):
    return "".join(
        f'<div class="row row-markdown">'
        f'<div class="row-name">{esc(r.puma_division)}</div>'
        f'{chip(r.markdown_risk, RISK_STATUS.get(r.markdown_risk, "warning"))}'
        f'<div class="row-weeks">{r.weeks_of_stock:.1f} weeks of stock</div>'
        f'<div class="row-figure">€{r.estimated_markdown_cost_eur:,.0f}</div>'
        f"</div>"
        for r in markdown.itertuples()
    )


def table_view(headers, rows):
    head = "".join(f"<th>{esc(h)}</th>" for h in headers)
    body = "".join("<tr>" + "".join(f"<td>{esc(cell)}</td>" for cell in row) + "</tr>" for row in rows)
    return (f'<details class="table-view"><summary>Show as table</summary>'
            f'<div class="table-scroll"><table class="num"><tr>{head}</tr>{body}</table></div></details>')


CSS = """
:root {
    --ink: #000;
    --paper: #fff;
    --grey-100: #f2f2f2;
    --grey-200: #e2e2e2;
    --grey-500: #6b6b6b;
    --grey-300-on-black: #a8a8a8;
    --brand: #e4002b;
    --good: #0ca30c;
    --warning: #fab219;
    --serious: #ec835a;
    --critical: #d03b3b;
    --display: 'Barlow Condensed', 'Arial Narrow', 'Helvetica Neue', Arial, sans-serif;
    --text: 'Barlow', 'Helvetica Neue', Arial, sans-serif;
    --gutter: clamp(20px, 4vw, 56px);
    color-scheme: light;
}
* { margin: 0; padding: 0; box-sizing: border-box; }
html { -webkit-text-size-adjust: 100%; }
body { font: 400 16px/1.5 var(--text); background: var(--paper); color: var(--ink); }
:focus-visible { outline: 3px solid var(--brand); outline-offset: 3px; }
table { border-collapse: collapse; width: 100%; }
.table-scroll { overflow-x: auto; }

/* ---------- hero ---------- */
.hero { background: var(--ink); color: var(--paper); }
.hero-body { position: relative; overflow: hidden; }
.hero-body::after {
    content: ''; position: absolute; top: 0; bottom: 0; right: -3%; width: 20%;
    background: var(--brand); transform: skewX(-14deg); transform-origin: 100% 0;
    animation: cut 0.8s cubic-bezier(0.2, 0.8, 0.2, 1) both;
}
@keyframes cut { from { transform: translateX(60%) skewX(-14deg); } to { transform: translateX(0) skewX(-14deg); } }
.hero-body > * { position: relative; z-index: 1; }
.topbar {
    display: flex; justify-content: space-between; align-items: baseline; gap: 16px; flex-wrap: wrap;
    padding: 18px var(--gutter); border-bottom: 1px solid rgba(255, 255, 255, 0.25);
}
.brandline { font: 700 22px/1 var(--display); text-transform: uppercase; letter-spacing: 0.04em; }
.updated { font-size: 14px; color: var(--grey-300-on-black); }
.hero-body { padding: clamp(36px, 6vw, 84px) var(--gutter) clamp(32px, 4vw, 56px); }
.hero-title {
    font: 800 clamp(46px, 9vw, 128px)/0.92 var(--display); text-transform: uppercase;
    letter-spacing: -0.005em; max-width: min(64%, 11em); text-wrap: balance;
}
.hero-detail { margin-top: 24px; font-size: clamp(16px, 1.6vw, 20px); max-width: 44ch; color: #e8e8e8; }
.kpis { display: grid; grid-template-columns: repeat(4, 1fr); margin: 0 var(--gutter); border-top: 1px solid rgba(255, 255, 255, 0.25); }
.kpi { padding: 22px 20px 28px; border-left: 1px solid rgba(255, 255, 255, 0.25); }
.kpi:first-child { padding-left: 0; border-left: 0; }
.kpi-label { font-size: 15px; font-weight: 600; }
.kpi-value { font: 700 clamp(38px, 4.6vw, 64px)/1.05 var(--display); font-variant-numeric: proportional-nums; margin: 6px 0 4px; }
.kpi-sub { font-size: 14px; color: var(--grey-300-on-black); }

/* ---------- page body ---------- */
main { max-width: 1360px; margin: 0 auto; padding: 36px var(--gutter) 24px; }
.dq { margin-bottom: 44px; background: var(--grey-100); border-left: 6px solid var(--grey-500); }
.dq-ok { border-left-color: var(--good); }
.dq-warn { border-left-color: var(--warning); }
.dq-fail { border-left-color: var(--critical); }
.dq-none { padding: 16px 20px; font-size: 15px; }
.dq summary { list-style: none; cursor: pointer; padding: 16px 20px; font-size: 15px; font-weight: 600; display: flex; gap: 8px; align-items: baseline; }
.dq summary::-webkit-details-marker { display: none; }
.dq summary::after { content: '+'; margin-left: auto; font: 700 24px/1 var(--display); }
.dq[open] summary::after { content: '\\2212'; }
.dq-when { font-weight: 400; color: var(--grey-500); }
.dq .table-scroll { padding: 0 20px 20px; }
.dq table { font-size: 14px; }
.dq th, .dq td { text-align: left; padding: 8px 12px 8px 0; border-bottom: 1px solid var(--grey-200); vertical-align: top; }
.dq th { font-weight: 600; white-space: nowrap; }

.grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 56px var(--gutter); margin-bottom: 56px; }
.panel { border-top: 4px solid var(--ink); padding-top: 16px; min-width: 0; }
.panel h2 { font: 700 32px/1 var(--display); text-transform: uppercase; letter-spacing: 0.01em; }
.panel .note { color: var(--grey-500); font-size: 15px; margin: 8px 0 20px; max-width: 52ch; }

.row { display: grid; align-items: center; gap: 8px 16px; padding: 14px 0; border-bottom: 1px solid var(--grey-200); }
.row-health { grid-template-columns: minmax(92px, 128px) 1fr 64px 96px; }
.row-markdown { grid-template-columns: minmax(92px, 128px) 96px 1fr auto; }
.row-name { font: 600 22px/1.1 var(--display); text-transform: uppercase; letter-spacing: 0.02em; }
.row-figure { font: 700 30px/1 var(--display); text-align: right; }
.row-weeks { font-size: 15px; color: var(--grey-500); }
.bar { height: 10px; background: var(--grey-200); }
.bar i { display: block; height: 100%; }
.fill-good { background: var(--good); }
.fill-warning { background: var(--warning); }
.fill-serious { background: var(--serious); }
.fill-critical { background: var(--critical); }

.chip { display: inline-flex; align-items: center; gap: 7px; width: fit-content; padding: 5px 9px; font: 700 13px/1 var(--text); white-space: nowrap; color: var(--ink); }
.chip::before { content: ''; width: 9px; height: 9px; background: currentColor; flex: none; }
.chip-good { background: var(--good); }
.chip-good::before { border-radius: 50%; }
.chip-warning { background: var(--warning); }
.chip-warning::before { clip-path: polygon(50% 0, 100% 100%, 0 100%); }
.chip-serious { background: var(--serious); }
.chip-serious::before { clip-path: polygon(50% 0, 100% 50%, 50% 100%, 0 50%); }
.chip-critical { background: var(--critical); color: var(--paper); }

.pass { font-weight: 600; color: #006300; }
.pass::before { content: '\\2713'; margin-right: 6px; }

.chart-box { position: relative; }
.table-view { margin-top: 12px; font-size: 14px; }
.table-view summary { cursor: pointer; font-weight: 600; padding: 6px 0; }
.table-view table { margin-top: 8px; }
.table-view th, .table-view td { text-align: left; padding: 6px 12px 6px 0; border-bottom: 1px solid var(--grey-200); }
.num td, .num th { font-variant-numeric: tabular-nums; }

/* ---------- ask ---------- */
.ask { background: var(--ink); color: var(--paper); padding: clamp(36px, 5vw, 72px) var(--gutter); }
.ask-inner { max-width: 1360px; margin: 0 auto; }
.ask h2 { font: 800 clamp(36px, 5vw, 64px)/0.95 var(--display); text-transform: uppercase; }
.ask .note { margin-top: 12px; color: var(--grey-300-on-black); max-width: 52ch; }
.quick-questions { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 24px; }
.quick-q {
    background: transparent; color: var(--paper); border: 1px solid #fff; padding: 9px 14px;
    font: 500 15px/1.2 var(--text); cursor: pointer; border-radius: 0;
}
.quick-q:hover { background: var(--paper); color: var(--ink); }
.ai-input-row { display: flex; gap: 0; margin-top: 16px; }
.ai-input {
    flex: 1; min-width: 0; background: var(--ink); color: var(--paper); border: 2px solid #fff; border-right: 0;
    padding: 16px 18px; font: 500 18px/1.3 var(--text); border-radius: 0;
}
.ai-input::placeholder { color: #9a9a9a; }
.ai-button {
    background: var(--paper); color: var(--ink); border: 2px solid #fff; padding: 0 clamp(20px, 4vw, 44px);
    font: 700 22px/1 var(--display); text-transform: uppercase; letter-spacing: 0.04em; cursor: pointer; border-radius: 0;
}
.ai-button:hover { background: var(--brand); border-color: var(--brand); color: var(--paper); }
.ai-response { display: none; margin-top: 20px; background: var(--paper); color: var(--ink); padding: 22px 24px; font-size: 17px; line-height: 1.55; max-width: 88ch; }
.verify-badge { margin-top: 16px; font-size: 14px; font-weight: 600; display: flex; gap: 8px; align-items: baseline; }
.verify-badge.ok { color: #006300; }
.verify-badge.ok::before { content: '\\2713'; }
.verify-badge.warn { color: #7a5200; }
.verify-badge.warn::before { content: '!'; font-weight: 800; }
.sql-block { margin-top: 14px; font-size: 14px; }
.sql-block summary { cursor: pointer; font-weight: 600; }
.sql-block pre { white-space: pre-wrap; background: var(--grey-100); padding: 12px; margin: 10px 0; font: 14px/1.45 ui-monospace, Consolas, monospace; color: #111; }
.sql-block table { width: auto; }
.sql-block th, .sql-block td { padding: 5px 14px 5px 0; border-bottom: 1px solid var(--grey-200); text-align: left; font-variant-numeric: tabular-nums; }

.site-footer { max-width: 1360px; margin: 0 auto; padding: 28px var(--gutter) 48px; font-size: 14px; color: var(--grey-500); }

/* ---------- small screens ---------- */
@media (max-width: 960px) {
    .grid-2 { grid-template-columns: 1fr; }
    .kpis { grid-template-columns: 1fr 1fr; }
    .kpi:nth-child(odd) { padding-left: 0; border-left: 0; }
    .kpi:nth-child(n + 3) { border-top: 1px solid rgba(255, 255, 255, 0.25); }
    .hero-body { padding-right: calc(var(--gutter) + 12%); }
    .hero-body::after { width: 10%; right: -2%; bottom: 44%; }  /* beside the headline only, clear of the sentence below */
    .hero-title { max-width: none; }
}
@media (max-width: 560px) {
    .row-health { grid-template-columns: 1fr 56px; }
    .row-health .bar { grid-column: 1 / -1; grid-row: 2; }
    .row-health .chip { grid-column: 1 / -1; }
    .row-markdown { grid-template-columns: auto 1fr; }
    .row-markdown .row-figure { grid-column: 2; grid-row: 1; }
    .row-markdown .chip { grid-column: 1; grid-row: 2; }
    .row-markdown .row-weeks { grid-column: 2; grid-row: 2; text-align: right; }
    .ai-input-row { flex-direction: column; }
    .ai-input { border-right: 2px solid #fff; border-bottom: 0; }
    .ai-button { padding: 14px; }
}
@media (prefers-reduced-motion: reduce) {
    .hero-body::after { animation: none; }
}
"""

# Browser script for the agent box. A plain string (not an f-string), so braces need no doubling.
# It renders the model's text with textContent, never as HTML.
ASK_JS = """
    function renderAnswer(data) {
        const box = document.getElementById('aiResponse');
        box.textContent = '';
        const answer = document.createElement('div');
        answer.textContent = data.answer || data.error || 'No answer.';
        box.appendChild(answer);
        if (data.error && data.answer) {
            const detail = document.createElement('div');
            detail.className = 'verify-badge warn';
            detail.textContent = data.error;
            box.appendChild(detail);
        }
        if (!data.sql) return;

        const badge = document.createElement('div');
        badge.className = 'verify-badge ' + (data.verified ? 'ok' : 'warn');
        badge.textContent = data.verified
            ? 'Numbers checked against the query result'
            : 'Unverified numbers: ' + (data.ungrounded || []).join(', ');
        box.appendChild(badge);

        const details = document.createElement('details');
        details.className = 'sql-block';
        const summary = document.createElement('summary');
        summary.textContent = 'Show SQL and result';
        details.appendChild(summary);
        const pre = document.createElement('pre');
        pre.textContent = data.sql;
        details.appendChild(pre);
        if (data.columns && data.columns.length) {
            const table = document.createElement('table');
            const head = table.insertRow();
            data.columns.forEach(function (c) {
                const th = document.createElement('th');
                th.textContent = c;
                head.appendChild(th);
            });
            data.rows.forEach(function (row) {
                const tr = table.insertRow();
                row.forEach(function (cell) {
                    tr.insertCell().textContent = cell === null ? '' : cell;
                });
            });
            details.appendChild(table);
            if (data.truncated) {
                const note = document.createElement('div');
                note.textContent = 'Showing the first rows only.';
                details.appendChild(note);
            }
        }
        box.appendChild(details);
    }

    async function askQuestion(question) {
        const input = document.getElementById('aiInput');
        const response = document.getElementById('aiResponse');

        const q = question || input.value.trim();
        if (!q) return;

        input.value = q;
        response.style.display = 'block';
        response.textContent = 'Analyzing inventory data...';

        try {
            const res = await fetch('/ask', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ question: q })
            });
            renderAnswer(await res.json());
        } catch (e) {
            response.textContent = 'Error connecting to AI agent.';
        }
    }

    document.querySelectorAll('.quick-q').forEach(function (button) {
        button.addEventListener('click', function () { askQuestion(button.dataset.q); });
    });
    document.getElementById('aiInput').addEventListener('keydown', function (event) {
        if (event.key === 'Enter') askQuestion();
    });
    document.getElementById('aiButton').addEventListener('click', function () { askQuestion(); });
"""

# Charts read their numbers from the JSON block in the page. Without Chart.js (offline) the lists and
# the table views above still show every value.
CHART_JS = """
(function () {
    const dataElement = document.getElementById('dash-data');
    if (!dataElement || typeof Chart === 'undefined') return;
    const d = JSON.parse(dataElement.textContent);
    const reduced = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    const INK = '#000', MUTED = '#6b6b6b', GRID = '#e2e2e2', BRAND = '#e4002b';
    Chart.defaults.font.family = "'Barlow', 'Helvetica Neue', Arial, sans-serif";
    Chart.defaults.color = MUTED;

    // the value at the end of each bar
    const endLabels = {
        id: 'endLabels',
        afterDatasetsDraw(chart, args, opts) {
            const ctx = chart.ctx;
            ctx.save();
            ctx.font = "700 17px 'Barlow Condensed', 'Arial Narrow', sans-serif";
            ctx.fillStyle = INK;
            ctx.textBaseline = 'middle';
            const line = opts.avoid == null ? null : chart.scales.x.getPixelForValue(opts.avoid);
            chart.getDatasetMeta(0).data.forEach(function (bar, i) {
                const text = opts.format(chart.data.datasets[0].data[i]);
                let x = bar.x + 8;
                // a label that would sit on the reference line moves to the right of it
                if (line !== null && x - 4 <= line && line <= x + ctx.measureText(text).width + 4) x = line + 8;
                ctx.fillText(text, x, bar.y);
            });
            ctx.restore();
        }
    };

    // one vertical reference line with its label
    const referenceLine = {
        id: 'referenceLine',
        afterDatasetsDraw(chart, args, opts) {
            if (!opts || opts.value == null) return;
            const x = chart.scales.x.getPixelForValue(opts.value);
            const area = chart.chartArea;
            const ctx = chart.ctx;
            ctx.save();
            ctx.strokeStyle = BRAND;
            ctx.lineWidth = 2;
            ctx.beginPath();
            ctx.moveTo(x, area.top - 6);
            ctx.lineTo(x, area.bottom);
            ctx.stroke();
            ctx.fillStyle = INK;
            ctx.font = "600 13px 'Barlow', sans-serif";
            ctx.textBaseline = 'bottom';
            ctx.fillText(opts.label, x + 6, area.top - 4);
            ctx.restore();
        }
    };

    function barChart(canvasId, labels, values, options) {
        const canvas = document.getElementById(canvasId);
        if (!canvas) return;
        new Chart(canvas.getContext('2d'), {
            type: 'bar',
            data: { labels: labels, datasets: [{ data: values, backgroundColor: INK, borderWidth: 0, barThickness: 16 }] },
            plugins: [endLabels, referenceLine],
            options: {
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                animation: reduced ? false : { duration: 700 },
                layout: { padding: { right: options.padRight, top: options.padTop || 0 } },
                plugins: {
                    legend: { display: false },
                    endLabels: { format: options.format, avoid: options.reference ? options.reference.value : null },
                    referenceLine: options.reference || { value: null },
                    tooltip: {
                        backgroundColor: INK, titleColor: '#fff', bodyColor: '#fff', cornerRadius: 0, padding: 10,
                        displayColors: false,
                        callbacks: { label: function (c) { return options.tooltip(c.parsed.x); } }
                    }
                },
                scales: {
                    x: { beginAtZero: true, max: options.max, grid: { color: GRID, lineWidth: 1 }, border: { display: false },
                         ticks: { color: MUTED, font: { size: 12 }, callback: options.tick } },
                    y: { grid: { display: false }, border: { color: INK, width: 2 },
                         ticks: { color: INK, font: { size: 15, weight: 600 } } }
                }
            }
        });
    }

    barChart('sellThroughChart', d.sellThrough.labels, d.sellThrough.values, {
        max: 100, padRight: 52,
        format: function (v) { return v.toFixed(1) + '%'; },
        tick: function (v) { return v + '%'; },
        tooltip: function (v) { return v.toFixed(1) + '% sell-through'; }
    });
    barChart('weeksChart', d.weeks.labels, d.weeks.values, {
        padRight: 40, padTop: 24,
        format: function (v) { return v.toFixed(1); },
        tick: function (v) { return v + ' wk'; },
        tooltip: function (v) { return v.toFixed(1) + ' weeks of stock'; },
        reference: { value: d.weeks.line, label: 'Reorder starts below ' + d.weeks.line + ' weeks' }
    });
})();
"""


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    health = get_data("SELECT * FROM gold_inventory_health ORDER BY inventory_health_score ASC")
    markdown = get_data("SELECT * FROM gold_markdown_risk ORDER BY estimated_markdown_cost_eur DESC")
    reorder = get_data("SELECT * FROM gold_reorder_signals ORDER BY weeks_of_stock ASC")
    sell_through = get_data("SELECT * FROM gold_sell_through ORDER BY sell_through_rate DESC")

    total_markdown_risk = markdown["estimated_markdown_cost_eur"].sum()
    critical_divisions = len(health[health["health_label"] == "CRITICAL"])
    avg_health = health["inventory_health_score"].mean()
    total_inventory_value = markdown["inventory_value_eur"].sum()

    chart_data = {
        "sellThrough": {"labels": list(sell_through["puma_division"]),
                        "values": [float(v) for v in sell_through["sell_through_rate"].round(1)]},
        "weeks": {"labels": list(reorder["puma_division"]),
                  "values": [float(v) for v in reorder["weeks_of_stock"].round(1)],
                  "line": REORDER_LINE_WEEKS},
    }
    chart_json = json.dumps(chart_data).replace("<", "\\u003c")  # keeps the JSON block from closing the script tag

    sell_label = (f"Bar chart of sell-through rate by division. Highest {sell_through.iloc[0].puma_division}, "
                  f"lowest {sell_through.iloc[-1].puma_division}.")
    weeks_label = (f"Bar chart of weeks of stock by division. Lowest {reorder.iloc[0].puma_division}, "
                   f"highest {reorder.iloc[-1].puma_division}.")
    sell_table = table_view(
        ["Division", "Sell-through", "Units sold, last 3 months", "Simulated stock units"],
        [(r.puma_division, f"{r.sell_through_rate:.1f}%", f"{r.units_last_3m:,}", f"{r.simulated_stock_units:,}")
         for r in sell_through.itertuples()])
    weeks_table = table_view(
        ["Division", "Weeks of stock", "Reorder signal"],
        [(r.puma_division, f"{r.weeks_of_stock:.1f}", r.reorder_signal) for r in reorder.itertuples()])

    updated = freshness()
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="color-scheme" content="light">
    <title>PUMA DTC Inventory Intelligence</title>
    <link rel="icon" href="data:,">
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@600;700;800&family=Barlow:wght@400;500;600;700&display=swap">
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
    <style>{CSS}</style>
</head>
<body>

<header class="hero">
    <div class="topbar">
        <div class="brandline">PUMA DTC Inventory Intelligence</div>
        <div class="updated">{updated}</div>
    </div>
    <div class="hero-body">
        {hero_html(health, markdown)}
    </div>
    <div class="kpis">
        <div class="kpi">
            <div class="kpi-label">Markdown risk</div>
            <div class="kpi-value">€{total_markdown_risk:,.0f}</div>
            <div class="kpi-sub">On simulated stock, all divisions</div>
        </div>
        <div class="kpi">
            <div class="kpi-label">Critical divisions</div>
            <div class="kpi-value">{critical_divisions}</div>
            <div class="kpi-sub">Require immediate action</div>
        </div>
        <div class="kpi">
            <div class="kpi-label">Average health score</div>
            <div class="kpi-value">{avg_health:.1f}</div>
            <div class="kpi-sub">Out of 100</div>
        </div>
        <div class="kpi">
            <div class="kpi-label">Inventory value</div>
            <div class="kpi-value">€{total_inventory_value:,.0f}</div>
            <div class="kpi-sub">Simulated stock value</div>
        </div>
    </div>
</header>

<main>
    {quality_panel()}

    <div class="grid-2">
        <section class="panel">
            <h2>Inventory health</h2>
            <p class="note">Score out of 100, lowest first.</p>
            {health_rows(health)}
        </section>

        <section class="panel">
            <h2>Sell-through</h2>
            <p class="note">Units sold in the last 3 months as a share of units sold plus simulated stock.</p>
            <div class="chart-box" style="height:{len(sell_through) * 46 + 64}px">
                <canvas id="sellThroughChart" role="img" aria-label="{esc(sell_label)}"></canvas>
            </div>
            {sell_table}
        </section>
    </div>

    <div class="grid-2">
        <section class="panel">
            <h2>Markdown exposure</h2>
            <p class="note">Estimated cost of forced discounts on simulated stock, highest first.</p>
            {markdown_rows(markdown)}
        </section>

        <section class="panel">
            <h2>Weeks of stock</h2>
            <p class="note">Simulated stock divided by recent weekly sales. A figure far above the red line means overstock.</p>
            <div class="chart-box" style="height:{len(reorder) * 46 + 88}px">
                <canvas id="weeksChart" role="img" aria-label="{esc(weeks_label)}"></canvas>
            </div>
            {weeks_table}
        </section>
    </div>
</main>

<section class="ask">
    <div class="ask-inner">
        <h2>Ask the inventory analyst</h2>
        <p class="note">Ask in plain English. Every answer shows the SQL it ran and checks its numbers against the result.</p>
        <div class="quick-questions">
            <button type="button" class="quick-q" data-q="Which division needs urgent attention?">Which division needs urgent attention?</button>
            <button type="button" class="quick-q" data-q="What is our total markdown exposure?">Total markdown exposure?</button>
            <button type="button" class="quick-q" data-q="Which divisions are performing well?">Which divisions perform well?</button>
            <button type="button" class="quick-q" data-q="What should I focus on this week?">What to focus on this week?</button>
        </div>
        <div class="ai-input-row">
            <input type="text" class="ai-input" id="aiInput" aria-label="Your question" placeholder="For example: which division should I reorder first?">
            <button type="button" class="ai-button" id="aiButton">Ask</button>
        </div>
        <div class="ai-response" id="aiResponse" aria-live="polite"></div>
    </div>
</section>

<footer class="site-footer">
    Portfolio demo built on the public H&amp;M dataset with simulated stock levels. Not affiliated with, endorsed by or connected to PUMA SE.
</footer>

<script type="application/json" id="dash-data">{chart_json}</script>
<script>
{CHART_JS}
{ASK_JS}
</script>

</body>
</html>
"""
    return html


@app.post("/ask")
async def ask_endpoint(request: Request):
    body = await request.json()
    question = body.get("question", "")
    return await run_in_threadpool(ask, question)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
