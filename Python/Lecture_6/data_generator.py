import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# --- Configuration ---
NUM_RECORDS = 5000
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2025, 12, 31)
SKUS = [f'PROD-{i:03}' for i in range(100)]  # 100 Unique SKUs
REGIONS = ['North', 'Central', 'South', 'West', 'East']
WAREHOUSES = ['A', 'B', 'C', 'D']


def generate_transactions(num_records):
    """Generates the Core Transaction Logs (transactions_core.csv)."""

    # Generate random dates within the range
    time_delta = END_DATE - START_DATE
    random_dates = [
        START_DATE + timedelta(days=random.randint(0, time_delta.days))
        for _ in range(num_records)
    ]

    data = {
        'transaction_id': range(10000, 10000 + num_records),
        'item_sku': np.random.choice(SKUS, size=num_records),
        'region': np.random.choice(REGIONS, size=num_records),
        'transaction_date': random_dates,
        'unit_price': np.round(np.random.uniform(10.0, 500.0, num_records), 2),
        'units_sold': np.random.randint(1, 101, num_records),
        'card_number': np.random.randint(1000000000000000, 9000000000000000, num_records),
        'cvv': np.random.randint(100, 999, num_records)
    }

    df = pd.DataFrame(data)
    # Sort by date, which is required for the merge_asof in Phase 2
    df.sort_values(by='transaction_date', inplace=True)
    return df


def generate_inventory(df_transactions):
    """
    Generates the Inventory Snapshots (inventory_updates.json).
    Ensures that snapshot_date is always before transaction_date.
    """

    # Get a list of unique SKUs and their first/last transaction dates
    sku_min_max_dates = df_transactions.groupby('item_sku')['transaction_date'].agg(['min', 'max']).reset_index()

    inventory_data = []

    for sku in SKUS:
        # Get the date range for the current SKU
        sku_info = sku_min_max_dates[sku_min_max_dates['item_sku'] == sku]
        if sku_info.empty:
            continue

        min_date = sku_info['min'].iloc[0]
        max_date = sku_info['max'].iloc[0]

        # Determine the number of snapshots for this SKU (e.g., 5 to 15 snapshots)
        num_snapshots = random.randint(5, 15)

        # Generate random snapshot dates that occur BEFORE the transactions start,
        # but also spread out across the transaction period

        # Create a set of spaced dates between the global start and the SKU's max date
        date_range = pd.date_range(start=START_DATE, end=max_date, periods=num_snapshots)

        for date in date_range:
            # Ensure the snapshot is reasonable (not the exact moment of a sale)
            snapshot_date = date - timedelta(days=random.randint(1, 10))

            inventory_data.append({
                'item_sku': sku,
                'snapshot_date': snapshot_date.date().isoformat(),  # Save as string for JSON
                'current_stock': random.randint(100, 5000),
                'warehouse_id': random.choice(WAREHOUSES)
            })

    df = pd.DataFrame(inventory_data)
    # Sort by date, which is required for the merge_asof in Phase 2
    df.sort_values(by='snapshot_date', inplace=True)
    return df


# --- Execution ---

# 1. Generate Transactions CSV
df_tx = generate_transactions(NUM_RECORDS)
df_tx.to_csv('transactions_core.csv', index=False)
print(f"Generated {len(df_tx)} records in 'transactions_core.csv'")

# 2. Generate Inventory JSON
df_inv = generate_inventory(df_tx)
# Convert date objects back to string format for the JSON output requirement
df_inv['snapshot_date'] = df_inv['snapshot_date'].astype(str)
df_inv.to_json('inventory_updates.json', orient='records', indent=4)
print(f"Generated {len(df_inv)} records in 'inventory_updates.json'")
