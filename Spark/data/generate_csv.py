import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

OUTPUT_FILE = Path(__file__).parent / "employees.csv"
NUM_ROWS = 100_000

DEPARTMENTS = ["Engineering", "Marketing", "Sales", "HR", "Finance", "Operations", "Legal", "Product"]
CITIES = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego"]
STATUSES = ["active", "inactive", "on_leave"]

FIRST_NAMES = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
               "William", "Barbara", "David", "Susan", "Richard", "Jessica", "Joseph", "Sarah",
               "Thomas", "Karen", "Charles", "Lisa", "Daniel", "Nancy", "Matthew", "Betty"]

LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
              "Wilson", "Taylor", "Anderson", "Thomas", "Jackson", "White", "Harris", "Martin",
              "Thompson", "Young", "Robinson", "Lewis", "Walker", "Hall", "Allen", "King"]


def random_date(start_year=2015, end_year=2024):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    return (start + timedelta(days=random.randint(0, (end - start).days))).strftime("%Y-%m-%d")


def generate_row(employee_id):
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    return {
        "employee_id": employee_id,
        "first_name": first,
        "last_name": last,
        "email": f"{first.lower()}.{last.lower()}{employee_id}@example.com",
        "department": random.choice(DEPARTMENTS),
        "city": random.choice(CITIES),
        "salary": round(random.uniform(40_000, 150_000), 2),
        "hire_date": random_date(),
        "status": random.choice(STATUSES),
        "years_experience": random.randint(0, 30),
    }


def main():
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["employee_id", "first_name", "last_name", "email",
                  "department", "city", "salary", "hire_date", "status", "years_experience"]

    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(1, NUM_ROWS + 1):
            writer.writerow(generate_row(i))

    print(f"Generated {NUM_ROWS:,} rows -> {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
