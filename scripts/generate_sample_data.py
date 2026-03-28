"""Generate realistic sample data for demo purposes.

Creates an e-commerce dataset that's relatable to any business audience.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

np.random.seed(42)

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "sample")
os.makedirs(DATA_DIR, exist_ok=True)

# --- Customers ---
n_customers = 500
customers = pd.DataFrame({
    "customer_id": range(1, n_customers + 1),
    "name": [f"Customer_{i}" for i in range(1, n_customers + 1)],
    "email": [f"customer_{i}@example.com" for i in range(1, n_customers + 1)],
    "country": np.random.choice(["Mexico", "USA", "Canada", "Brazil", "Colombia", "Spain", "Germany"], n_customers, p=[0.3, 0.25, 0.1, 0.1, 0.1, 0.08, 0.07]),
    "segment": np.random.choice(["Enterprise", "SMB", "Startup", "Individual"], n_customers, p=[0.15, 0.35, 0.3, 0.2]),
    "signup_date": [
        (datetime(2024, 1, 1) + timedelta(days=int(np.random.exponential(180)))).strftime("%Y-%m-%d")
        for _ in range(n_customers)
    ],
})
# Add some nulls for realism
customers.loc[np.random.choice(n_customers, 15, replace=False), "email"] = None
customers.loc[np.random.choice(n_customers, 8, replace=False), "segment"] = None

# --- Products ---
products = pd.DataFrame({
    "product_id": range(1, 21),
    "product_name": [
        "Data Pipeline Setup", "ETL Automation", "Dashboard Builder",
        "AI Chatbot", "Data Warehouse Migration", "API Integration",
        "ML Model Training", "Data Quality Audit", "Real-time Streaming",
        "Cloud Migration", "Security Audit", "Performance Optimization",
        "Custom Report Builder", "Data Lake Setup", "CI/CD Pipeline",
        "Monitoring Setup", "Documentation Service", "Training Workshop",
        "Architecture Review", "Emergency Support"
    ],
    "category": [
        "Data Engineering", "Data Engineering", "Analytics",
        "AI/ML", "Data Engineering", "Integration",
        "AI/ML", "Data Quality", "Data Engineering",
        "Infrastructure", "Security", "Infrastructure",
        "Analytics", "Data Engineering", "DevOps",
        "DevOps", "Consulting", "Consulting",
        "Consulting", "Support"
    ],
    "price_usd": [
        5000, 3500, 2500, 8000, 15000, 4000,
        12000, 3000, 10000, 8000, 5000, 4500,
        3500, 12000, 6000, 3000, 2000, 4000,
        5000, 2000
    ],
})

# --- Orders ---
n_orders = 2000
order_dates = [
    (datetime(2024, 1, 1) + timedelta(days=int(np.random.uniform(0, 450)))).strftime("%Y-%m-%d")
    for _ in range(n_orders)
]
orders = pd.DataFrame({
    "order_id": range(1, n_orders + 1),
    "customer_id": np.random.choice(range(1, n_customers + 1), n_orders),
    "product_id": np.random.choice(range(1, 21), n_orders),
    "order_date": order_dates,
    "quantity": np.random.choice([1, 1, 1, 2, 2, 3], n_orders),
    "status": np.random.choice(
        ["completed", "completed", "completed", "completed", "pending", "cancelled", "refunded"],
        n_orders
    ),
    "discount_pct": np.random.choice([0, 0, 0, 5, 10, 15, 20], n_orders),
})
# Calculate revenue
orders = orders.merge(products[["product_id", "price_usd"]], on="product_id")
orders["total_usd"] = (orders["quantity"] * orders["price_usd"] * (1 - orders["discount_pct"] / 100)).round(2)
orders = orders.drop(columns=["price_usd"])

# Add some nulls
orders.loc[np.random.choice(n_orders, 30, replace=False), "discount_pct"] = None

# --- Save ---
customers.to_csv(os.path.join(DATA_DIR, "customers.csv"), index=False)
products.to_csv(os.path.join(DATA_DIR, "products.csv"), index=False)
orders.to_csv(os.path.join(DATA_DIR, "orders.csv"), index=False)

print(f"Generated sample data in {DATA_DIR}:")
print(f"  customers.csv: {len(customers)} rows")
print(f"  products.csv:  {len(products)} rows")
print(f"  orders.csv:    {len(orders)} rows")
