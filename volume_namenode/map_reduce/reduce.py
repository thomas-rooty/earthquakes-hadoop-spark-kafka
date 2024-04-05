import sys


def reduce_function():
    current_city = None
    events = []

    for line in sys.stdin:
        try:
            city, data = line.strip().split('\t', 1)
            if current_city is None:
                current_city = city

            if current_city != city:
                # Previous city
                analyze_events(current_city, events)
                current_city = city
                events = [data]
            else:
                events.append(data)
        except ValueError:
            # Ignore errors
            continue

    # Last city
    if current_city:
        analyze_events(current_city, events)


def analyze_events(city, events):
    # Results
    print(f"{city}: {len(events)} événements")


if __name__ == "__main__":
    reduce_function()
