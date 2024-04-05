#!/bin/bash

CSV_PATH="../csv_cleaned/dataset_sismique_cleaned.csv"

cat $CSV_PATH | python3 map.py | sort | python3 reduce.py
