import csv
from faker import Faker
import random
import time

# --- Configuration ---
NUM_RECORDS = 1000001  # Set to 1 million
CSV_FILE = 'large_customer_orders.csv'
fake = Faker('en_US')
# Set the batch size for writing (improves efficiency)
BATCH_SIZE = 50000

# Define the CSV column headers
fieldnames = [
    'Order_ID',
    'Customer_ID',
    'Order_Date',
    'Product_Category',
    'Product_Name',
    'Quantity',
    'Price_USD',
    'Shipping_City',
    'Payment_Method'
]

# --- Helper Functions ---

# Pre-define lists for faster random selection
PRODUCT_CATEGORIES = ['Electronics', 'Books', 'Clothing', 'Home Goods', 'Groceries']
PAYMENT_METHODS = ['Credit Card', 'PayPal', 'Bank Transfer', 'Cryptocurrency']
CUSTOMER_IDS = [f'CUST{i}' for i in range(10000, 11000)]  # 1000 unique customers


def create_fake_record(record_id):
    """Generates a dictionary containing fake order data."""

    category = random.choice(PRODUCT_CATEGORIES)

    # Simple logic for product names based on category
    if category == 'Electronics':
        product_name = random.choice( ['Laptop Pro', 'Smartphone X', 'Wireless Earbuds', 'Smart Watch'] )
    elif category == 'Books':
        product_name = random.choice(['Sci-Fi Novel', 'History Textbook', 'Self-Help Guide', 'Poetry Collection'])
    elif category == 'Clothing':
        product_name = random.choice(['T-Shirt', 'Jeans', 'Jacket', 'Sneakers'])
    else:
        product_name = fake.word().capitalize() + ' Item'  # Default generic product

    quantity = random.randint(1, 5)

    return {
        'Order_ID': f"ORD{10000000 + record_id}",
        'Customer_ID': random.choice(CUSTOMER_IDS),
        'Order_Date': fake.date_between(start_date='-2y', end_date='today').strftime('%Y-%m-%d'),
        'Product_Category': category,
        'Product_Name': product_name,
        'Quantity': quantity,
        'Price_USD': round(random.uniform(5.00, 500.00), 2),
        'Shipping_City': fake.city(),
        'Payment_Method': random.choice(PAYMENT_METHODS)
    }


# csv -> comma separated values

# --- Main Writing Block ---

start_time = time.time()
print(f"🚀 Starting to generate and write {NUM_RECORDS} records to {CSV_FILE}...")

# w -> write
# r -> read

try:
    with open(CSV_FILE, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        # Generate data in batches to keep memory usage low and increase speed
        data_to_write = []
        for i in range(1, NUM_RECORDS):
            record = create_fake_record(i)
            data_to_write.append(record)

            if i % BATCH_SIZE == 0:
                writer.writerows(data_to_write)
                data_to_write = []
                # Print progress update
                print(f"   -> Wrote {i:,} records...")

        # Write any remaining records
        if len(data_to_write) > 0:
            writer.writerows(data_to_write)

    end_time = time.time()
    total_time = end_time - start_time

    print(f"\n✅ Success! Total time taken: {total_time:.2f} seconds.")
    print(f"The large dataset has been saved to **{CSV_FILE}**.")

except IOError:
    print(f"❌ Error: Could not write to file {CSV_FILE}. Check permissions.")
