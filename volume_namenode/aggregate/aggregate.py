import pandas as pd

csv_path = '..\csv_cleaned\dataset_sismique_cleaned.csv'

df = pd.read_csv(csv_path)

df['secousse_x'] = df['secousse_x'] == 'True'
df['secousse_y'] = df['secousse_y'].fillna('False') == 'True'

df['date'] = pd.to_datetime(df['date'])

# Aggregate par heure
hourly_data = df.resample('h', on='date').agg({
    'magnitude_x': ['mean', 'max', 'min', 'count'],  # Moyenne, max, min, et count magnitude_x
    'tension entre plaque_x': 'mean',  # Moyenne tension plaque_x
    'magnitude_y': ['mean', 'max', 'min', 'count'],  # Moyenne, max, min, et count magnitude_y
    'tension entre plaque_y': 'mean',  # Moyenne tension plaque_y
    'secousse_x': 'sum',  # Total secousses_x
    'secousse_y': 'sum',  # Total secousses_y
})

# Rename columns
hourly_data.columns = [
    'Magnitude X Moyenne', 'Magnitude X Max', 'Magnitude X Min', 'Nombre d\'Événements X',
    'Tension entre Plaque X Moyenne',
    'Magnitude Y Moyenne', 'Magnitude Y Max', 'Magnitude Y Min', 'Nombre d\'Événements Y',
    'Tension entre Plaque Y Moyenne',
    'Total Secousses X', 'Total Secousses Y'
]

# Save to CSV
hourly_data.to_csv('aggregated_hourly_data.csv')

print(hourly_data.head())
