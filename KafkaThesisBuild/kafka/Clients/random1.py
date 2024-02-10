import random

# Bounding box for the area in Greece
min_lat, max_lat = 38.8561, 40.4167
min_lon, max_lon = 21.1804, 22.6182

coordinates = []
for _ in range(20):
    # lat = round(random.uniform(min_lat, max_lat), 4)
    lat = 40.2938
    lon = round(random.uniform(min_lon, max_lon), 4)
    coordinates.append((lat, lon))

for lat, lon in coordinates:
    print(f"({lat}, {lon}),")