import sys
import csv
from datetime import datetime


def map_function():
    for line in sys.stdin:
        try:
            fields = list(csv.reader([line]))[0]
            date = datetime.strptime(fields[0], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
            city = fields[4]
            magnitude_x = fields[2]
            tension_x = fields[3]
            magnitude_y = fields[6] if fields[6] != '' else 'None'
            tension_y = fields[7] if fields[7] != '' else 'None'

            # Format : Ville\tDate,Magnitude_X,Tension_X,Magnitude_Y,Tension_Y
            print(f"{city}\t{date},{magnitude_x},{tension_x},{magnitude_y},{tension_y}")
        except Exception as e:
            continue


if __name__ == "__main__":
    map_function()
