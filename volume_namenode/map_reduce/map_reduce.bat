@echo off

set CSV_PATH="..\csv_cleaned\dataset_sismique_cleaned.csv"

type %CSV_PATH% | python map.py | sort | python reduce.py

pause
