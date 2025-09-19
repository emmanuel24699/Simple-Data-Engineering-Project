# Filename: scripts/generate_data.py
import os
import random
from datetime import datetime

import pandas as pd
from faker import Faker

# Initialize Faker
fake = Faker()

# Define data parameters
REGIONS = [
    'Ashanti', 'Brong-Ahafo', 'Volta', 'Eastern', 'Western', 'Central',
    'Greater Accra', 'Northern', 'Upper East', 'Upper West',
    'Western North', 'Oti', 'Bono East', 'Ahafo', 'Savannah', 'North East'
]
BEAN_TYPES = ['Forastero', 'Criollo', 'Trinitario']
NUM_FILES = 10
MIN_RECORDS = 1000
MAX_RECORDS = 10000


def generate_cocoa_data(num_records: int) -> pd.DataFrame:
    """Generates a DataFrame of synthetic cocoa shipment data."""

    print(f"Generating {num_records} records...")

    data = []
    for _ in range(num_records):
        shipment_date = fake.date_time_between(start_date='-2y', end_date='now')
        record = {
            'shipment_id': fake.uuid4(),
            'timestamp': shipment_date.isoformat(),
            'farm_id': f"FARM-{random.randint(100, 200)}",
            'region': random.choice(REGIONS),
            'bean_type': random.choice(BEAN_TYPES),
            'quality_score': round(random.uniform(7.5, 9.8), 2),
            'shipment_weight_kg': random.randint(500, 5000),
            'temperature_celsius': round(random.uniform(18.0, 25.0), 1)
            if random.random() > 0.1 else None,
        }
        data.append(record)

    df = pd.DataFrame(data)
    print("Data generation complete.")
    return df


def generate_and_save_multiple_files(
        num_files: int = NUM_FILES
) -> list[str]:
    """Generates and saves multiple CSV files with a random number of records each."""

    # Use a temporary directory that Airflow can access
    output_dir = "/tmp"

    print(f"Generating {num_files} files...")

    file_paths = []
    for i in range(1, num_files + 1):
        num_records = random.randint(MIN_RECORDS, MAX_RECORDS)
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = os.path.join(output_dir, f"cocoa_shipments_{timestamp_str}_{i}.csv")

        cocoa_df = generate_cocoa_data(num_records)
        cocoa_df.to_csv(file_path, index=False)

        print(f"Successfully generated and saved file {i} to '{file_path}'")
        file_paths.append(file_path)

    return file_paths


if __name__ == "__main__":
    generate_and_save_multiple_files(num_files=3)