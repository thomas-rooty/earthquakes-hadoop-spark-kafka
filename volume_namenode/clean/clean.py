import pandas as pd
import numpy as np

df_sismique = pd.read_csv("../dataset_sismique.csv")
df_villes = pd.read_csv("../dataset_sismique_villes.csv")

# Suppression des lignes où la secousse est False et la magnitude est 0
df_sismique_clean = df_sismique[(df_sismique["secousse"] == True) | (df_sismique["magnitude"] > 0)]
df_villes_clean = df_villes[(df_villes["secousse"] == True) | (df_villes["magnitude"] > 0)]

# Correction des valeurs aberrantes pour la tension entre plaque
# .loc est utilisé pour accéder à un groupe de lignes et de colonnes par leurs labels ou une condition booléenne
# Ici, ":" signifie "toutes les lignes", ce qui indique que l'opération doit être appliquée à chaque ligne du DataFrame
# np.where est utilisé pour remplacer les valeurs de "tension entre plaque" supérieures au SEUIL_TENSION par SEUIL_TENSION lui-même
SEUIL_TENSION = 10
df_villes_clean.loc[:, "tension entre plaque"] = np.where(df_villes_clean["tension entre plaque"] > SEUIL_TENSION, SEUIL_TENSION, df_villes_clean["tension entre plaque"])

# Fusion des datasets
df_merged = pd.merge(df_sismique_clean, df_villes_clean, on="date", how="outer")

# Sauvegarde des données nettoyées
df_merged.to_csv("../csv_cleaned/dataset_sismique_cleaned.csv", index=False)

print("Le nettoyage des données est terminé.")
