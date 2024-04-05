import pandas as pd

def load_analyze_data(csv_path):
    # Data prep
    df = pd.read_csv(csv_path)
    df['date'] = pd.to_datetime(df['date'])

    # Calc amp
    df['amplitude_x'] = df['magnitude_x'].max() - df['magnitude_x'].min()
    df['amplitude_y'] = df['magnitude_y'].max() - df['magnitude_y'].min()

    # Group by and calc max magnitude
    df_grouped_x = df.groupby(['ville', pd.Grouper(key='date', freq='H')])['magnitude_x'].agg(['min', 'max']).reset_index()
    df_grouped_y = df.groupby(['ville', pd.Grouper(key='date', freq='H')])['magnitude_y'].agg(['min', 'max']).reset_index()
    df_grouped_x['amplitude_max_x'] = df_grouped_x['max'] - df_grouped_x['min']
    df_grouped_y['amplitude_max_y'] = df_grouped_y['max'] - df_grouped_y['min']

    # Amp treshold
    SEUIL_IMPORTANCE_X = 0.5
    SEUIL_IMPORTANCE_Y = 0.5

    # Filter
    heures_importants_x = df_grouped_x[df_grouped_x['amplitude_max_x'] > SEUIL_IMPORTANCE_X]
    heures_importants_y = df_grouped_y[df_grouped_y['amplitude_max_y'] > SEUIL_IMPORTANCE_Y]

    return heures_importants_x, heures_importants_y

# Cleaned CSV path
csv_path = '../csv_cleaned/dataset_sismique_cleaned.csv'
heures_importants_x, heures_importants_y = load_analyze_data(csv_path)

# Results
print("Heures et villes avec activité sismique significative pour x :")
print(heures_importants_x[['ville', 'date', 'amplitude_max_x']])
print("\nHeures et villes avec activité sismique significative pour y :")
print(heures_importants_y[['ville', 'date', 'amplitude_max_y']])