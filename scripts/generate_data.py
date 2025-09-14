# Filename: scripts/generate_data.py
import pandas as pd
from faker import Faker
import random
from datetime import datetime

# Initialize Faker
fake = Faker()

# Define data parameters
REGIONS = [
    'Ashanti', 'Brong-Ahafo', 'Volta', 'Eastern', 'Western', 'Central',
    'Greater Accra', 'Northern', 'Upper East', 'Upper West',
    'Western North', 'Oti', 'Bono East', 'Ahafo', 'Savannah', 'North East'
]
BEAN_TYPES = ['Forastero', 'Criollo', 'Trinitario']
NUM_RECORDS = 500

def generate_cocoa_data(num_records: int = NUM_RECORDS) -> pd.DataFrame:
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
            'temperature_celsius': round(random.uniform(18.0, 25.0), 1) if random.random() > 0.1 else None, # Introduce some missing data
        }
        data.append(record)
        
    df = pd.DataFrame(data)
    print("Data generation complete.")
    return df

if __name__ == "__main__":
    import os
    if not os.path.exists('data'):
        os.makedirs('data')

    # Generate data and save to CSV
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = f"data/cocoa_shipments_{timestamp_str}.csv"
    
    cocoa_df = generate_cocoa_data()
    cocoa_df.to_csv(file_path, index=False)
    
    print(f"Successfully generated and saved data to '{file_path}'")