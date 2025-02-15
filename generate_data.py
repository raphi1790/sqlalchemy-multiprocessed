import pandas as pd
import numpy as np

num_rows = 1000000
# with random data
df = pd.DataFrame({
    # Integer values between 1 and 100
    'Column1': np.random.randint(1, 100, num_rows),
    'Column2': np.random.rand(num_rows),  # Random float values between 0 and 1
    'Column3': [f'Value_{i}' for i in range(1, num_rows+1)],  # String values
    # Random dates in 2023
    'Column4': pd.to_datetime(np.random.randint(1672531200, 1704067200, num_rows), unit='s'),
    # Categorical values
    'Column5': np.random.choice(['A', 'B', 'C'], num_rows),
    'Column6': np.random.rand(num_rows),
    'Column7': np.random.rand(num_rows),
    'Column8': np.random.rand(num_rows),
    'Column9': np.random.rand(num_rows),
    'Column10': np.random.rand(num_rows),
})

# Save the DataFrame to a CSV file
df.to_csv('test_data.csv', index=False)
