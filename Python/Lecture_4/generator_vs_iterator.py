import csv

def csv_generator(file_path):
    """Generates one row at a time."""
    with open(file_path, 'r', newline='') as f:
        reader = csv.reader(f)
        header = next(reader)  # Skip header
        for row in reader:
            yield row  # Generates a row and pauses until next call


# Using the generator:
for row_data in csv_generator('large_customer_orders.csv'):
    # Process the row data here
    print(row_data)


def read_file_manually(file_path):
    """
    Reads a file using 'try...except...finally' to ensure the file is closed.
    """
    f = None  # Initialize file handle
    try:
        # File is opened here
        f = open(file_path, 'r', encoding='utf-8')

        # Read the entire content (less memory efficient for large files)
        content = f.read()
        print(f"Successfully read the file. First 100 characters:\n{content[:100]}...")

    except FileNotFoundError:
        print(f"\n Error: File **{file_path}** not found.")
    except Exception as e:
        print(f"\n An unexpected error occurred: {e}")

    finally:
        # The 'finally' block guarantees the file is closed, regardless of errors
        if f:  # Check if the file object was actually created
            f.close()
            print("\nFile successfully closed in the finally block.")

# open() ..... close()

# --- Example ---
print("\n--- Traditional Approach Example ---")
read_file_manually('large_customer_orders.csv')

def csv_list_loader(file_path):
    """Loads ALL rows into a list."""
    data = []
    with open(file_path, 'r', newline='') as f:
        reader = csv.reader(f)
        next(reader) # Skip header
        for row in reader:
            data.append(row) # Storing every row in the 'data' list
    return data

# Using the list:
all_data = csv_list_loader('large_customer_orders.csv') # HUGE memory spike here
for row_data in all_data:
    # Process the row data here
    print(row_data)