import pandas as pd
from faker import Faker
import pyarrow as pa

import pyarrow.parquet as pq

fake = Faker()

def generate_data(num_rows=1000000):
    data = []
    for i in range(num_rows):
        data.append({
            'id': i + 1,
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'age': fake.random_int(min=18, max=80),
            'city': fake.city()
        })
    return pd.DataFrame(data)

if __name__ == "__main__":
    df = generate_data()
    
    # Write to CSV
    df.to_csv('data.csv', index=False)
    
    # Write to JSON
    df.to_json('data.json', orient='records')
    
    # Write to Parquet
    table = pa.Table.from_pandas(df)
    pq.write_table(table, 'data.parquet')