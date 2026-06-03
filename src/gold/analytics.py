import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime

DB_PATH = "puma_dtc.db"

def build_gold():
    print(f"[{datetime.now()}] Starting Gold layer - PUMA DTC Inventory Intelligence...")
    
    conn = sqlite3.connect(DB_PATH)

    print("Loading silver data...")
    transactions = pd.read_sql("SELECT * FROM silver_transactions", conn)
    transactions["t_dat"] = pd.to_datetime(transactions["t_dat"])
    transactions["date_key"] = transactions["year"] * 100 + transactions["month"]

    all_date_keys = sorted(transactions["date_key"].unique())
    last_3_keys = all_date_keys[-3:] if len(all_date_keys) >= 3 else all_date_keys
    last_6_keys = all_date_keys[-6:] if len(all_date_keys) >= 6 else all_date_keys

    print(f"Using data from {all_date_keys[0]} to {all_date_keys[-1]}")
    print(f"Last 3 months: {last_3_keys}")

    # ----------------------------------------------------------------
    # GOLD 1: SELL-THROUGH RATE
    # How fast is each category selling relative to its own history?
    # High sell-through = flying off shelves = reorder needed
    # Low sell-through = sitting unsold = markdown risk
    # ----------------------------------------------------------------
    print("\nBuilding sell-through rates...")

    recent_sales = transactions[transactions["date_key"].isin(last_3_keys)]
    historical_sales = transactions[transactions["date_key"].isin(last_6_keys)]

    recent_velocity = recent_sales.groupby("puma_division").agg(
        units_last_3m=("article_id", "count"),
        revenue_last_3m=("price_eur", "sum"),
        unique_products_recent=("article_id", "nunique")
    ).reset_index()

    historical_velocity = historical_sales.groupby("puma_division").agg(
        units_last_6m=("article_id", "count"),
        revenue_last_6m=("price_eur", "sum")
    ).reset_index()

    sell_through = recent_velocity.merge(historical_velocity, on="puma_division", how="left")

    # Monthly velocity comparison — recent 3m vs prior 3m
    sell_through["avg_monthly_recent"] = sell_through["units_last_3m"] / 3
    sell_through["avg_monthly_prior"] = (
        sell_through["units_last_6m"] - sell_through["units_last_3m"]
    ) / 3

    # Sell-through acceleration — are sales speeding up or slowing down?
    sell_through["velocity_change_pct"] = (
        (sell_through["avg_monthly_recent"] - sell_through["avg_monthly_prior"]) /
        sell_through["avg_monthly_prior"].replace(0, np.nan) * 100
    ).round(2)

    # Simulate realistic inventory scenarios based on PUMA's known challenges
    # Training and Swim are overstocked (PUMA's real inventory overhang problem)
    # Accessories and Lifestyle are understocked (high demand, low supply)
    inventory_scenarios = {
        "Running":     1.5,   # Slightly overstocked
        "Training":    3.5,   # Heavily overstocked - mirrors PUMA's real problem
        "Swim":        4.0,   # Critical overstock - seasonal item past peak
        "Basics":      0.8,   # Getting low - needs reorder soon
        "Accessories": 0.4,   # Critical - nearly out of stock
        "Lifestyle":   0.3,   # Critical - nearly out of stock
        "Other":       1.2    # Fine
    }
    
    sell_through["stock_multiplier"] = sell_through["puma_division"].map(
        inventory_scenarios
    ).fillna(1.0)
    
    sell_through["simulated_stock_units"] = (
        sell_through["avg_monthly_recent"] * sell_through["stock_multiplier"] * 4
    ).astype(int)

    # Sell-through rate = units sold in last 3m / (units sold + remaining stock)
    sell_through["total_available"] = (
        sell_through["units_last_3m"] + sell_through["simulated_stock_units"]
    )
    sell_through["sell_through_rate"] = (
        sell_through["units_last_3m"] / sell_through["total_available"] * 100
    ).round(2)

    def sell_through_status(rate):
        if rate >= 75:
            return "EXCELLENT"
        elif rate >= 50:
            return "GOOD"
        elif rate >= 30:
            return "SLOW"
        else:
            return "POOR"

    sell_through["sell_through_status"] = sell_through["sell_through_rate"].apply(sell_through_status)
    sell_through["generated_at"] = datetime.now()

    sell_through.to_sql("gold_sell_through", conn, if_exists="replace", index=False)
    print(f"Sell-through rates: {len(sell_through)} divisions")
    print(sell_through[["puma_division", "sell_through_rate", "sell_through_status", "velocity_change_pct"]])

    # ----------------------------------------------------------------
    # GOLD 2: MARKDOWN RISK
    # Which categories are at risk of forced discounting?
    # Slow sell-through + declining velocity = markdown pressure
    # ----------------------------------------------------------------
    print("\nBuilding markdown risk...")

    markdown = sell_through[["puma_division", "sell_through_rate", "velocity_change_pct",
                              "simulated_stock_units", "avg_monthly_recent"]].copy()

    markdown["weeks_of_stock"] = (
        markdown["simulated_stock_units"] / (markdown["avg_monthly_recent"] / 4.3)
    ).round(1)

    def markdown_risk(row):
        if row["sell_through_rate"] < 30 and row["velocity_change_pct"] < -10:
            return "CRITICAL"
        elif row["sell_through_rate"] < 40 and row["velocity_change_pct"] < 0:
            return "HIGH"
        elif row["sell_through_rate"] < 50:
            return "MEDIUM"
        else:
            return "LOW"

    markdown["markdown_risk"] = markdown.apply(markdown_risk, axis=1)

    # Estimated markdown cost — how much revenue at risk from discounting
    avg_price_by_division = transactions.groupby("puma_division")["price_eur"].mean()
    markdown["avg_price_eur"] = markdown["puma_division"].map(avg_price_by_division)
    markdown["inventory_value_eur"] = (
        markdown["simulated_stock_units"] * markdown["avg_price_eur"]
    ).round(2)

    markdown_discount = {"CRITICAL": 0.35, "HIGH": 0.25, "MEDIUM": 0.15, "LOW": 0.0}
    markdown["estimated_markdown_cost_eur"] = (
        markdown["inventory_value_eur"] *
        markdown["markdown_risk"].map(markdown_discount)
    ).round(2)

    markdown["generated_at"] = datetime.now()
    markdown.to_sql("gold_markdown_risk", conn, if_exists="replace", index=False)
    print(f"Markdown risk: {len(markdown)} divisions")
    print(markdown[["puma_division", "markdown_risk", "weeks_of_stock", "estimated_markdown_cost_eur"]])

    # ----------------------------------------------------------------
    # GOLD 3: REORDER SIGNALS
    # Which categories need replenishment before stockout?
    # Based on weeks of stock remaining vs forecast demand
    # ----------------------------------------------------------------
    print("\nBuilding reorder signals...")

    reorder = markdown[["puma_division", "simulated_stock_units",
                         "avg_monthly_recent", "weeks_of_stock"]].copy()

    seasonal_factors = {
        1: 0.7, 2: 0.75, 3: 0.9, 4: 1.0, 5: 1.1,
        6: 1.2, 7: 1.15, 8: 1.3, 9: 1.1, 10: 1.0,
        11: 1.2, 12: 1.4
    }

    current_month = all_date_keys[-1] % 100
    next_month = current_month + 1 if current_month < 12 else 1
    next_factor = seasonal_factors.get(next_month, 1.0)

    reorder["forecast_next_month_units"] = (
        reorder["avg_monthly_recent"] * next_factor
    ).round(0).astype(int)

    reorder["stock_after_next_month"] = (
        reorder["simulated_stock_units"] - reorder["forecast_next_month_units"]
    )

    def reorder_signal(row):
        if row["weeks_of_stock"] < 4:
            return "URGENT - Order immediately"
        elif row["weeks_of_stock"] < 8:
            return "SOON - Order within 2 weeks"
        elif row["stock_after_next_month"] < row["forecast_next_month_units"]:
            return "PLAN - Review in 30 days"
        else:
            return "OK - Sufficient stock"

    reorder["reorder_signal"] = reorder.apply(reorder_signal, axis=1)
    reorder["reorder_priority"] = reorder["reorder_signal"].map({
        "URGENT - Order immediately": 4,
        "SOON - Order within 2 weeks": 3,
        "PLAN - Review in 30 days": 2,
        "OK - Sufficient stock": 1
    })

    reorder["generated_at"] = datetime.now()
    reorder.to_sql("gold_reorder_signals", conn, if_exists="replace", index=False)
    print(f"Reorder signals: {len(reorder)} divisions")
    print(reorder[["puma_division", "weeks_of_stock", "reorder_signal", "forecast_next_month_units"]])

    # ----------------------------------------------------------------
    # GOLD 4: INVENTORY HEALTH SCORE
    # One number per category summarising overall inventory health
    # 0 = critical problem, 100 = perfect
    # ----------------------------------------------------------------
    print("\nBuilding inventory health scores...")

    health = sell_through[["puma_division", "sell_through_rate", "velocity_change_pct"]].copy()
    health = health.merge(
        markdown[["puma_division", "markdown_risk", "weeks_of_stock"]],
        on="puma_division"
    )
    health = health.merge(
        reorder[["puma_division", "reorder_priority"]],
        on="puma_division"
    )

    # Score components out of 100
    # Sell-through score — higher is better
    health["sell_through_score"] = (health["sell_through_rate"]).clip(0, 100)

    # Velocity score — positive momentum is good
    health["velocity_score"] = (
        50 + health["velocity_change_pct"].clip(-50, 50)
    ).clip(0, 100)

    # Weeks of stock score — 8-16 weeks is ideal
    def weeks_score(weeks):
        if 8 <= weeks <= 16:
            return 100
        elif 4 <= weeks < 8 or 16 < weeks <= 24:
            return 70
        elif 2 <= weeks < 4 or 24 < weeks <= 32:
            return 40
        else:
            return 10
    health["weeks_score"] = health["weeks_of_stock"].apply(weeks_score)

    # Markdown risk score
    markdown_score_map = {"LOW": 100, "MEDIUM": 70, "HIGH": 40, "CRITICAL": 10}
    health["markdown_score"] = health["markdown_risk"].map(markdown_score_map)

    # Weighted overall health score
    health["inventory_health_score"] = (
        health["sell_through_score"] * 0.30 +
        health["velocity_score"] * 0.25 +
        health["weeks_score"] * 0.25 +
        health["markdown_score"] * 0.20
    ).round(1)

    def health_label(score):
        if score >= 80:
            return "HEALTHY"
        elif score >= 60:
            return "WATCH"
        elif score >= 40:
            return "AT RISK"
        else:
            return "CRITICAL"

    health["health_label"] = health["inventory_health_score"].apply(health_label)
    health["generated_at"] = datetime.now()

    health.to_sql("gold_inventory_health", conn, if_exists="replace", index=False)
    print(f"\nInventory health scores:")
    print(health[["puma_division", "inventory_health_score", "health_label"]].sort_values(
        "inventory_health_score", ascending=False
    ))

    conn.close()
    print(f"\n[{datetime.now()}] Gold layer complete.")

if __name__ == "__main__":
    build_gold()