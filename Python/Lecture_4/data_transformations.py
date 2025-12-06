import pandas as pd
import numpy as np
import time

# --- Configuration ---
INPUT_CSV = 'large_customer_orders.csv'
OUTPUT_CSV = 'transformed_orders_summary.csv'


# --- Transformation Functions ---

def load_data(file_path):
    """Loads the CSV data into a Pandas DataFrame."""
    print(f"🔄 Loading data from {file_path}...")
    # Use appropriate dtypes to save memory, especially for large datasets
    dtype_config = {
        'Order_ID': str,
        'Customer_ID': str,
        'Order_Date': str,  # Read as string, convert later
        'Product_Category': 'category',
        'Product_Name': str,
        'Quantity': 'int16',
        'Price_USD': 'float32',
        'Shipping_City': 'category',
        'Payment_Method': 'category'
    }

    # Read the data, optimizing for memory
    df = pd.read_csv(file_path, dtype=dtype_config)

    # Convert 'Order_Date' to datetime objects
    df['Order_Date'] = pd.to_datetime(df['Order_Date'])

    print(f"✅ Data loaded. Shape: {df.shape}")
    return df


def transform_data(df):
    """Performs various data cleaning and feature engineering steps."""
    print("🛠️ Starting data transformation and feature engineering...")

    # 1. Data Cleaning: Handle potential missing/invalid data (though Faker data is clean)
    # This is a good practice for real-world data
    df.dropna(inplace=True)

    # 2. Feature Engineering: Calculate Total Revenue per Order
    # Total_Revenue = Quantity * Price_USD
    df['Total_Revenue'] = df['Quantity'] * df['Price_USD']
    print("   -> Calculated 'Total_Revenue'.")

    # 3. Feature Engineering: Extract Temporal Features
    df['Order_Year'] = df['Order_Date'].dt.year.astype('int16')
    df['Order_Month'] = df['Order_Date'].dt.month.astype('int8')
    df['Order_DayOfWeek'] = df['Order_Date'].dt.day_name().astype('category')
    print("   -> Extracted temporal features (Year, Month, DayOfWeek).")

    # 4. Data Validation: Flag high-value orders (e.g., revenue > $1000)
    df['Is_High_Value'] = np.where(df['Total_Revenue'] > 1000, True, False)
    print("   -> Created 'Is_High_Value' flag.")

    # 5. Data Aggregation: Summarize orders by Customer and Category
    # This reduces the 1M rows into a more manageable summary for analysis

    summary_df = df.groupby(['Customer_ID', 'Product_Category']).agg(
        Total_Orders=('Order_ID', 'count'),
        Total_Revenue=('Total_Revenue', 'sum'),
        Avg_Price=('Price_USD', 'mean'),
        Min_Order_Date=('Order_Date', 'min'),
        Max_Order_Date=('Order_Date', 'max')
    ).reset_index()

    # Rename columns for clarity
    summary_df.columns = [
        'Customer_ID', 'Product_Category', 'Total_Orders',
        'Total_Revenue_Category', 'Avg_Price_Per_Unit',
        'First_Order_Date', 'Last_Order_Date'
    ]

    print(f"✅ Transformation complete. Summary DataFrame shape: {summary_df.shape}")
    return summary_df


def save_data(df, file_path):
    """Saves the transformed DataFrame to a new CSV file."""
    print(f"💾 Saving transformed data to {file_path}...")
    # Using 'gzip' compression is highly recommended for large CSV files
    df.to_csv(file_path, index=False)
    print("✅ Data saved successfully!")


# --- Main Execution Block ---

if __name__ == "__main__":
    start_time = time.time()

    try:
        # Step 1: Load Data
        df_raw = load_data(INPUT_CSV)

        # Step 2: Transform Data
        df_transformed = transform_data(df_raw)

        # Step 3: Save Data
        save_data(df_transformed, OUTPUT_CSV)

        end_time = time.time()
        total_time = end_time - start_time
        print(f"\n--- Process Complete ---")
        print(f"Total execution time: **{total_time:.2f} seconds**")

    except FileNotFoundError:
        print(f"\n❌ Error: The input file **{INPUT_CSV}** was not found.")
        print("Please ensure you ran the previous data generation script first.")
    except Exception as e:
        print(f"\n❌ An unexpected error occurred: {e}")