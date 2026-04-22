# E-Commerce Sales Analysis Dashboard

A complete data analysis pipeline demonstrating SQL queries, Python pandas, and data visualization. Shows how to transform raw transaction data into actionable insights.

## What This Demonstrates

| Skill | Implementation |
|-------|----------------|
| **SQL Queries** | Complex joins, aggregations, window functions |
| **Data Cleaning** | Handling nulls, outliers, type conversions |
| **Pandas** | DataFrame operations, groupby, merge |
| **Visualization** | Matplotlib charts, trend analysis |
| **Reporting** | Executive summary generation |

## Dataset

Synthetic e-commerce transaction data (1,000+ orders) including:
- Order details (date, customer, products)
- Customer demographics
- Product categories and pricing
- Shipping and fulfillment data

## Quick Start

```bash
# Install dependencies
pip install pandas matplotlib sqlite3

# Run analysis
python analyze.py

# Output: charts/ and reports/ directories
```

## SQL Queries Included

### 1. Revenue by Category
```sql
SELECT 
    category,
    COUNT(*) as orders,
    SUM(quantity * unit_price) as revenue,
    AVG(quantity * unit_price) as avg_order_value
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
GROUP BY category
ORDER BY revenue DESC;
```

### 2. Customer Lifetime Value
```sql
WITH customer_orders AS (
    SELECT 
        customer_id,
        COUNT(DISTINCT order_id) as order_count,
        SUM(total_amount) as lifetime_spend
    FROM orders
    GROUP BY customer_id
)
SELECT 
    NTILE(4) OVER (ORDER BY lifetime_spend) as quartile,
    AVG(lifetime_spend) as avg_ltv,
    AVG(order_count) as avg_orders
FROM customer_orders
GROUP BY quartile;
```

### 3. Cohort Retention
```sql
WITH first_orders AS (
    SELECT customer_id, MIN(DATE(order_date)) as first_order_date
    FROM orders
    GROUP BY customer_id
),
monthly_activity AS (
    SELECT 
        fo.first_order_date as cohort,
        DATE(o.order_date, 'start of month') as month,
        COUNT(DISTINCT o.customer_id) as active_customers
    FROM first_orders fo
    JOIN orders o ON fo.customer_id = o.customer_id
    GROUP BY fo.first_order_date, DATE(o.order_date, 'start of month')
)
SELECT * FROM monthly_activity
ORDER BY cohort, month;
```

### 4. Product Affinity (Frequently Bought Together)
```sql
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
LIMIT 10;
```

## Analysis Pipeline

```
Raw Data (CSV)
      │
      ▼
┌─────────────────┐
│  SQLite Load    │  ← Create tables, set types
└─────────────────┘
      │
      ▼
┌─────────────────┐
│  SQL Queries    │  ← Aggregations, joins, CTEs
└─────────────────┘
      │
      ▼
┌─────────────────┐
│  Pandas Transform│  ← Clean, feature engineering
└─────────────────┘
      │
      ▼
┌─────────────────┐
│  Visualizations │  ← Charts, heatmaps
└─────────────────┘
      │
      ▼
┌─────────────────┐
│  Report Export  │  ← Markdown + CSV summary
└─────────────────┘
```

## Key Insights Generated

1. **Top Performing Categories**: Which product categories drive revenue
2. **Customer Segmentation**: LTV quartiles and behavior patterns
3. **Seasonality**: Monthly trends and holiday spikes
4. **Product Recommendations**: Frequently bought together items
5. **Churn Risk**: Customers declining in order frequency

## Technologies Used

- **SQLite**: Lightweight SQL database for queries
- **Pandas**: DataFrame operations and transformations
- **Matplotlib**: Static visualizations
- **Python 3.10+**: Core language

## Files

```
data-analysis-demo/
├── analyze.py          # Main analysis script
├── queries/            # SQL query files
│   ├── revenue.sql
│   ├── ltv.sql
│   ├── retention.sql
│   └── affinity.sql
├── data/
│   └── sales_data.csv  # Raw transaction data
├── charts/             # Generated visualizations
│   ├── revenue_trend.png
│   ├── category_breakdown.png
│   └── cohort_heatmap.png
└── reports/
    └── executive_summary.md
```

## Running Locally

```bash
python analyze.py --output ./output

# Options:
#   --data PATH      Custom data path
#   --output PATH    Output directory
#   --format png/svg Chart format
```

## Extending

Add a new query:
1. Create `queries/your_query.sql`
2. Add to `analyze.py`:
```python
def run_your_query(conn):
    with open('queries/your_query.sql') as f:
        return pd.read_sql(f.read(), conn)
```

## Why This Project

For roles requiring data analysis skills, this demonstrates:
- Real SQL (not just SELECT *)
- Data pipeline architecture
- Business insight generation
- Clean, documented code
