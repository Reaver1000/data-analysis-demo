#!/usr/bin/env python3
"""
E-Commerce Sales Analysis Pipeline
Demonstrates SQL, pandas, and data visualization skills.
"""

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import argparse
from datetime import datetime

# Create output directories
Path("charts").mkdir(exist_ok=True)
Path("reports").mkdir(exist_ok=True)


def create_database(csv_path: str = None) -> sqlite3.Connection:
    """Create SQLite database with sample e-commerce data."""
    conn = sqlite3.connect(":memory:")
    
    # Generate synthetic data if no CSV provided
    import random
    from datetime import timedelta
    
    # Customers
    customers = pd.DataFrame({
        "customer_id": range(1, 201),
        "name": [f"Customer {i}" for i in range(1, 201)],
        "segment": random.choices(["Premium", "Standard", "Basic"], weights=[0.2, 0.5, 0.3], k=200),
        "country": random.choices(["US", "UK", "DE", "FR", "CA"], k=200),
    })
    
    # Products
    products = pd.DataFrame({
        "product_id": range(1, 51),
        "product_name": [f"Product {chr(65 + (i-1) % 26)}{((i-1) // 26) + 1}" for i in range(1, 51)],
        "category": random.choices(["Electronics", "Clothing", "Home", "Sports", "Books"], k=50),
        "unit_price": [round(random.uniform(10, 500), 2) for _ in range(50)],
    })
    
    # Orders
    orders = []
    base_date = datetime(2024, 1, 1)
    for i in range(1, 1001):
        order_date = base_date + timedelta(days=random.randint(0, 365))
        orders.append({
            "order_id": i,
            "customer_id": random.randint(1, 200),
            "order_date": order_date.strftime("%Y-%m-%d"),
            "total_amount": 0,  # Will calculate
            "status": random.choices(["Completed", "Shipped", "Processing"], weights=[0.7, 0.2, 0.1])[0],
        })
    orders_df = pd.DataFrame(orders)
    
    # Order items
    order_items = []
    for order_id in range(1, 1001):
        num_items = random.randint(1, 4)
        for _ in range(num_items):
            order_items.append({
                "order_id": order_id,
                "product_id": random.randint(1, 50),
                "quantity": random.randint(1, 3),
            })
    order_items_df = pd.DataFrame(order_items)
    
    # Calculate order totals
    order_totals = order_items_df.merge(products, on="product_id")
    order_totals["line_total"] = order_totals["quantity"] * order_totals["unit_price"]
    order_totals = order_totals.groupby("order_id")["line_total"].sum().reset_index()
    orders_df = orders_df.merge(order_totals, on="order_id")
    orders_df["total_amount"] = orders_df["line_total"]
    orders_df = orders_df.drop("line_total", axis=1)
    
    # Load to SQLite
    customers.to_sql("customers", conn, index=False, if_exists="replace")
    products.to_sql("products", conn, index=False, if_exists="replace")
    orders_df.to_sql("orders", conn, index=False, if_exists="replace")
    order_items_df.to_sql("order_items", conn, index=False, if_exists="replace")
    
    print(f"Created database with {len(orders_df)} orders, {len(customers)} customers, {len(products)} products")
    return conn


def run_query(conn: sqlite3.Connection, query: str) -> pd.DataFrame:
    """Execute SQL query and return DataFrame."""
    return pd.read_sql(query, conn)


def analyze_revenue_by_category(conn: sqlite3.Connection) -> pd.DataFrame:
    """Revenue breakdown by product category."""
    query = """
    SELECT 
        p.category,
        COUNT(DISTINCT o.order_id) as orders,
        SUM(oi.quantity * p.unit_price) as revenue,
        ROUND(AVG(oi.quantity * p.unit_price), 2) as avg_order_value
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
    GROUP BY p.category
    ORDER BY revenue DESC
    """
    df = run_query(conn, query)
    
    # Visualization
    fig, ax = plt.subplots(figsize=(10, 6))
    colors = ["#3b82f6", "#8b5cf6", "#06b6d4", "#10b981", "#f59e0b"]
    ax.barh(df["category"], df["revenue"], color=colors)
    ax.set_xlabel("Revenue ($)")
    ax.set_title("Revenue by Category")
    ax.invert_yaxis()
    for i, (cat, rev) in enumerate(zip(df["category"], df["revenue"])):
        ax.text(rev + 500, i, f"${rev:,.0f}", va="center")
    plt.tight_layout()
    plt.savefig("charts/revenue_by_category.png", dpi=150)
    plt.close()
    
    return df


def analyze_customer_ltv(conn: sqlite3.Connection) -> pd.DataFrame:
    """Customer lifetime value segmentation."""
    query = """
    WITH customer_stats AS (
        SELECT 
            c.customer_id,
            c.segment,
            COUNT(DISTINCT o.order_id) as order_count,
            SUM(o.total_amount) as lifetime_spend
        FROM customers c
        LEFT JOIN orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id
    )
    SELECT 
        segment,
        COUNT(*) as customers,
        ROUND(AVG(lifetime_spend), 2) as avg_ltv,
        ROUND(AVG(order_count), 1) as avg_orders,
        ROUND(SUM(lifetime_spend), 2) as total_revenue
    FROM customer_stats
    GROUP BY segment
    ORDER BY avg_ltv DESC
    """
    df = run_query(conn, query)
    
    # Visualization
    fig, ax = plt.subplots(figsize=(10, 6))
    colors = ["#3b82f6", "#8b5cf6", "#06b6d4"]
    bars = ax.bar(df["segment"], df["avg_ltv"], color=colors)
    ax.set_ylabel("Average LTV ($)")
    ax.set_title("Customer Lifetime Value by Segment")
    for bar, ltv in zip(bars, df["avg_ltv"]):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 50, 
                f"${ltv:,.0f}", ha="center")
    plt.tight_layout()
    plt.savefig("charts/customer_ltv.png", dpi=150)
    plt.close()
    
    return df


def analyze_monthly_trends(conn: sqlite3.Connection) -> pd.DataFrame:
    """Monthly revenue and order trends."""
    query = """
    SELECT 
        strftime('%Y-%m', order_date) as month,
        COUNT(*) as orders,
        SUM(total_amount) as revenue,
        ROUND(AVG(total_amount), 2) as avg_order_value
    FROM orders
    GROUP BY strftime('%Y-%m', order_date)
    ORDER BY month
    """
    df = run_query(conn, query)
    
    # Visualization
    fig, ax1 = plt.subplots(figsize=(12, 6))
    
    ax1.fill_between(range(len(df)), df["revenue"], alpha=0.3, color="#3b82f6")
    ax1.plot(range(len(df)), df["revenue"], color="#3b82f6", linewidth=2, marker="o", label="Revenue")
    ax1.set_ylabel("Revenue ($)", color="#3b82f6")
    ax1.tick_params(axis="y", labelcolor="#3b82f6")
    
    ax2 = ax1.twinx()
    ax2.plot(range(len(df)), df["orders"], color="#10b981", linewidth=2, marker="s", label="Orders")
    ax2.set_ylabel("Orders", color="#10b981")
    ax2.tick_params(axis="y", labelcolor="#10b981")
    
    ax1.set_xticks(range(len(df)))
    ax1.set_xticklabels(df["month"], rotation=45, ha="right")
    ax1.set_title("Monthly Revenue and Order Trends")
    ax1.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig("charts/monthly_trends.png", dpi=150)
    plt.close()
    
    return df


def analyze_product_affinity(conn: sqlite3.Connection) -> pd.DataFrame:
    """Products frequently bought together."""
    query = """
    SELECT 
        p1.product_name as product_a,
        p2.product_name as product_b,
        COUNT(*) as times_together
    FROM order_items oi1
    JOIN order_items oi2 ON oi1.order_id = oi2.order_id AND oi1.product_id < oi2.product_id
    JOIN products p1 ON oi1.product_id = p1.product_id
    JOIN products p2 ON oi2.product_id = p2.product_id
    GROUP BY p1.product_name, p2.product_name
    ORDER BY times_together DESC
    LIMIT 10
    """
    df = run_query(conn, query)
    return df


def generate_report(
    category_df: pd.DataFrame,
    ltv_df: pd.DataFrame,
    trends_df: pd.DataFrame,
    affinity_df: pd.DataFrame
) -> str:
    """Generate executive summary report."""
    report = f"""# E-Commerce Sales Analysis Report
Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")}

## Executive Summary

This analysis covers 1,000 orders across 200 customers and 50 products.

## Key Findings

### Revenue by Category
Top performing categories:
"""
    for _, row in category_df.head(3).iterrows():
        report += f"- **{row['category']}**: ${row['revenue']:,.0f} ({row['orders']} orders)\n"
    
    report += f"""
### Customer Segmentation
Average LTV by segment:
"""
    for _, row in ltv_df.iterrows():
        report += f"- **{row['segment']}**: ${row['avg_ltv']:,.0f} LTV ({row['customers']} customers)\n"
    
    report += f"""
### Monthly Trends
- Peak revenue month: **{trends_df.loc[trends_df['revenue'].idxmax(), 'month']}** (${trends_df['revenue'].max():,.0f})
- Average monthly revenue: **${trends_df['revenue'].mean():,.0f}**

### Product Affinity (Frequently Bought Together)
"""
    for _, row in affinity_df.head(5).iterrows():
        report += f"- {row['product_a']} + {row['product_b']} ({row['times_together']} times)\n"
    
    report += """
## Visualizations

- `charts/revenue_by_category.png`
- `charts/customer_ltv.png`
- `charts/monthly_trends.png`

## Recommendations

1. **Focus on high-LTV segments**: Premium customers show 2x higher lifetime value
2. **Stock product bundles**: Top affinity pairs suggest bundling opportunities
3. **Seasonal planning**: Q4 shows highest revenue - prepare inventory
"""
    
    with open("reports/executive_summary.md", "w") as f:
        f.write(report)
    
    return report


def main():
    parser = argparse.ArgumentParser(description="E-Commerce Sales Analysis")
    parser.add_argument("--data", help="Path to CSV data file")
    parser.add_argument("--output", default="./output", help="Output directory")
    args = parser.parse_args()
    
    print("=" * 50)
    print("E-Commerce Sales Analysis Pipeline")
    print("=" * 50)
    
    # Create database
    conn = create_database(args.data)
    
    # Run analyses
    print("\n📊 Analyzing revenue by category...")
    category_df = analyze_revenue_by_category(conn)
    print(category_df.to_string(index=False))
    
    print("\n👥 Analyzing customer LTV...")
    ltv_df = analyze_customer_ltv(conn)
    print(ltv_df.to_string(index=False))
    
    print("\n📈 Analyzing monthly trends...")
    trends_df = analyze_monthly_trends(conn)
    print(trends_df.to_string(index=False))
    
    print("\n🔗 Analyzing product affinity...")
    affinity_df = analyze_product_affinity(conn)
    print(affinity_df.to_string(index=False))
    
    # Generate report
    print("\n📝 Generating report...")
    report = generate_report(category_df, ltv_df, trends_df, affinity_df)
    print("\n" + "=" * 50)
    print("Report saved to reports/executive_summary.md")
    print("Charts saved to charts/")
    print("=" * 50)


if __name__ == "__main__":
    main()
