import pandas as pd
import numpy as np

# Create a DataFrame with 100000 rows and 5 columns
# with random data
df = pd.DataFrame({
    # Integer values between 1 and 100
    'Column1': np.random.randint(1, 100, 100000),
    'Column2': np.random.rand(100000),  # Random float values between 0 and 1
    'Column3': [f'Value_{i}' for i in range(1, 100001)],  # String values
    # Random dates in 2023
    'Column4': pd.to_datetime(np.random.randint(1672531200, 1704067200, 100000), unit='s'),
    'Column5': np.random.choice(['A', 'B', 'C'], 100000)  # Categorical values
})

# Save the DataFrame to a CSV file
df.to_csv('test_data.csv', index=False)
