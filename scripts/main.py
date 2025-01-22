import pandas as pd
import random
import numpy as np

# Seed for reproducibility
random.seed(42)
np.random.seed(42)

# Define parameters
num_rows = 1_000_000
locations = ["Nairobi", "Mombasa", "Kisumu", "Nakuru", "Eldoret", "Thika", "Naivasha", "Machakos", "Garissa", "Meru"]
causes = ["High Cost of Living", "Election Results", "Corruption", "Police Brutality", "Unemployment", 
          "Land Grievances", "Wages Disputes", "Fuel Prices", "Healthcare Strikes", "Climate Change"]
outcomes = ["Peaceful", "Arrests", "Violent Clashes"]
organizers = ["Civil Society", "Political Party", "Union", "Community Leaders", "Youth Movement"]

# Generate data
data = {
    "Date": pd.date_range(start="2010-01-01", end="2025-12-31", periods=num_rows).strftime("%Y-%m-%d"),
    "Location": np.random.choice(locations, size=num_rows),
    "Cause": np.random.choice(causes, size=num_rows),
    "Participants": np.random.randint(50, 100_001, size=num_rows),
    "Outcome": np.random.choice(outcomes, size=num_rows),
    "Organizer": np.random.choice(organizers, size=num_rows)
}

# Create DataFrame
maandamano_df = pd.DataFrame(data)

# Save to CSV
output_path = "maandamano_kenya_dataset.csv"
maandamano_df.to_csv(output_path, index=False)

print(f"Dataset saved to {output_path}")

