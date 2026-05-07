# Data Profiling Results

## `data/raw/stops.txt`

- Rows: `1488`
- Columns: `stop_id, stop_name, stop_lat, stop_lon, location_type, parent_station`

### Null Counts

| Column | Nulls |
|---|---:|
| `stop_id` | 0 |
| `stop_name` | 0 |
| `stop_lat` | 0 |
| `stop_lon` | 0 |
| `location_type` | 992 |
| `parent_station` | 496 |

### Numeric Ranges

| Column | Min | Max |
|---|---:|---:|
| `location_type` | 1.0 | 1.0 |
| `stop_lat` | 40.512764 | 40.903125 |
| `stop_lon` | -74.251961 | -73.755405 |

### Sample Rows

- `{'stop_id': '101', 'stop_name': 'Van Cortlandt Park-242 St', 'stop_lat': '40.889248', 'stop_lon': '-73.898583', 'location_type': '1', 'parent_station': ''}`
- `{'stop_id': '101N', 'stop_name': 'Van Cortlandt Park-242 St', 'stop_lat': '40.889248', 'stop_lon': '-73.898583', 'location_type': '', 'parent_station': '101'}`
- `{'stop_id': '101S', 'stop_name': 'Van Cortlandt Park-242 St', 'stop_lat': '40.889248', 'stop_lon': '-73.898583', 'location_type': '', 'parent_station': '101'}`
- `{'stop_id': '103', 'stop_name': '238 St', 'stop_lat': '40.884667', 'stop_lon': '-73.900870', 'location_type': '1', 'parent_station': ''}`
- `{'stop_id': '103N', 'stop_name': '238 St', 'stop_lat': '40.884667', 'stop_lon': '-73.900870', 'location_type': '', 'parent_station': '103'}`

## `data/raw/MTA_Stations.csv`

- Rows: `496`
- Columns: `GTFS Stop ID, Station ID, Complex ID, Division, Line, Stop Name, Borough, CBD, Daytime Routes, Structure, GTFS Latitude, GTFS Longitude, North Direction Label, South Direction Label, ADA, ADA Northbound, ADA Southbound, ADA Notes, Georeference`

### Null Counts

| Column | Nulls |
|---|---:|
| `GTFS Stop ID` | 0 |
| `Station ID` | 0 |
| `Complex ID` | 0 |
| `Division` | 0 |
| `Line` | 0 |
| `Stop Name` | 0 |
| `Borough` | 0 |
| `CBD` | 0 |
| `Daytime Routes` | 0 |
| `Structure` | 0 |
| `GTFS Latitude` | 0 |
| `GTFS Longitude` | 0 |
| `North Direction Label` | 0 |
| `South Direction Label` | 0 |
| `ADA` | 0 |
| `ADA Northbound` | 0 |
| `ADA Southbound` | 0 |
| `ADA Notes` | 487 |
| `Georeference` | 0 |

### Numeric Ranges

| Column | Min | Max |
|---|---:|---:|
| `ADA` | 0.0 | 2.0 |
| `ADA Northbound` | 0.0 | 1.0 |
| `ADA Southbound` | 0.0 | 1.0 |
| `Complex ID` | 1.0 | 636.0 |
| `GTFS Latitude` | 40.512764 | 40.903125 |
| `GTFS Longitude` | -74.251961 | -73.755405 |
| `Station ID` | 1.0 | 523.0 |

### Sample Rows

- `{'GTFS Stop ID': '127', 'Station ID': '317', 'Complex ID': '611', 'Division': 'IRT', 'Line': 'Broadway - 7Av', 'Stop Name': 'Times Sq-42 St', 'Borough': 'M', 'CBD': 'true', 'Daytime Routes': '1 2 3', 'Structure': 'Subway', 'GTFS Latitude': '40.75529', 'GTFS Longitude': '-73.987495', 'North Direction Label': 'Uptown', 'South Direction Label': 'Downtown', 'ADA': '1', 'ADA Northbound': '1', 'ADA Southbound': '1', 'ADA Notes': '', 'Georeference': 'POINT (-73.987495 40.75529)'}`
- `{'GTFS Stop ID': 'S17', 'Station ID': '515', 'Complex ID': '515', 'Division': 'SIR', 'Line': 'Staten Island', 'Stop Name': 'Annadale', 'Borough': 'SI', 'CBD': 'false', 'Daytime Routes': 'SIR', 'Structure': 'Open Cut', 'GTFS Latitude': '40.54046', 'GTFS Longitude': '-74.178217', 'North Direction Label': 'Ferry', 'South Direction Label': 'South Shore', 'ADA': '0', 'ADA Northbound': '0', 'ADA Southbound': '0', 'ADA Notes': '', 'Georeference': 'POINT (-74.178217 40.54046)'}`
- `{'GTFS Stop ID': 'S01', 'Station ID': '139', 'Complex ID': '627', 'Division': 'BMT', 'Line': 'Franklin Shuttle', 'Stop Name': 'Franklin Av', 'Borough': 'Bk', 'CBD': 'false', 'Daytime Routes': 'S', 'Structure': 'Elevated', 'GTFS Latitude': '40.680596', 'GTFS Longitude': '-73.955827', 'North Direction Label': 'Last Stop', 'South Direction Label': 'Prospect Park', 'ADA': '1', 'ADA Northbound': '1', 'ADA Southbound': '1', 'ADA Notes': '', 'Georeference': 'POINT (-73.955827 40.680596)'}`
- `{'GTFS Stop ID': '254', 'Station ID': '349', 'Complex ID': '349', 'Division': 'IRT', 'Line': 'Eastern Pky', 'Stop Name': 'Junius St', 'Borough': 'Bk', 'CBD': 'false', 'Daytime Routes': '3', 'Structure': 'Elevated', 'GTFS Latitude': '40.663515', 'GTFS Longitude': '-73.902447', 'North Direction Label': 'Manhattan', 'South Direction Label': 'New Lots', 'ADA': '0', 'ADA Northbound': '0', 'ADA Southbound': '0', 'ADA Notes': '', 'Georeference': 'POINT (-73.902447 40.663515)'}`
- `{'GTFS Stop ID': 'M01', 'Station ID': '108', 'Complex ID': '108', 'Division': 'BMT', 'Line': 'Myrtle Av', 'Stop Name': 'Middle Village-Metropolitan Av', 'Borough': 'Q', 'CBD': 'false', 'Daytime Routes': 'M', 'Structure': 'Elevated', 'GTFS Latitude': '40.711396', 'GTFS Longitude': '-73.889601', 'North Direction Label': 'Inbound', 'South Direction Label': 'Last Stop', 'ADA': '1', 'ADA Northbound': '1', 'ADA Southbound': '1', 'ADA Notes': '', 'Georeference': 'POINT (-73.889601 40.711396)'}`

## `data/raw/MTA_Hourly_Ridership_Beginning_2025.csv`

- Rows: `37097502`
- Columns: `transit_timestamp, transit_mode, station_complex_id, station_complex, borough, payment_method, fare_class_category, ridership, transfers, latitude, longitude, Georeference`

### Null Counts

| Column | Nulls |
|---|---:|
| `transit_timestamp` | 0 |
| `transit_mode` | 0 |
| `station_complex_id` | 0 |
| `station_complex` | 0 |
| `borough` | 0 |
| `payment_method` | 0 |
| `fare_class_category` | 0 |
| `ridership` | 0 |
| `transfers` | 0 |
| `latitude` | 0 |
| `longitude` | 0 |
| `Georeference` | 0 |

### Numeric Ranges

| Column | Min | Max |
|---|---:|---:|
| `latitude` | 40.576126 | 40.903126 |
| `longitude` | -74.07484 | -73.7554 |

### Sample Rows

- `{'transit_timestamp': '05/23/2025 12:00:00 AM', 'transit_mode': 'subway', 'station_complex_id': '100', 'station_complex': 'Hewes St (M,J)', 'borough': 'Brooklyn', 'payment_method': 'omny', 'fare_class_category': 'OMNY - Full Fare', 'ridership': '12', 'transfers': '0', 'latitude': '40.70687', 'longitude': '-73.95343', 'Georeference': 'POINT (-73.95343 40.70687)'}`
- `{'transit_timestamp': '05/23/2025 12:00:00 AM', 'transit_mode': 'subway', 'station_complex_id': '135', 'station_complex': 'Livonia Av (L)', 'borough': 'Brooklyn', 'payment_method': 'metrocard', 'fare_class_category': 'Metrocard - Full Fare', 'ridership': '1', 'transfers': '1', 'latitude': '40.66404', 'longitude': '-73.90057', 'Georeference': 'POINT (-73.90057 40.66404)'}`
- `{'transit_timestamp': '05/23/2025 12:00:00 AM', 'transit_mode': 'subway', 'station_complex_id': '138', 'station_complex': 'Canarsie-Rockaway Pkwy (L)', 'borough': 'Brooklyn', 'payment_method': 'metrocard', 'fare_class_category': 'Metrocard - Full Fare', 'ridership': '7', 'transfers': '1', 'latitude': '40.646652', 'longitude': '-73.90185', 'Georeference': 'POINT (-73.90185 40.646652)'}`
- `{'transit_timestamp': '05/23/2025 12:00:00 AM', 'transit_mode': 'subway', 'station_complex_id': '152', 'station_complex': '135 St (C,B)', 'borough': 'Manhattan', 'payment_method': 'omny', 'fare_class_category': 'OMNY - Students', 'ridership': '3', 'transfers': '0', 'latitude': '40.817894', 'longitude': '-73.94765', 'Georeference': 'POINT (-73.94765 40.817894)'}`
- `{'transit_timestamp': '05/23/2025 12:00:00 AM', 'transit_mode': 'subway', 'station_complex_id': '17', 'station_complex': 'Prince St (R,W)', 'borough': 'Manhattan', 'payment_method': 'metrocard', 'fare_class_category': 'Metrocard - Full Fare', 'ridership': '5', 'transfers': '0', 'latitude': '40.72433', 'longitude': '-73.9977', 'Georeference': 'POINT (-73.9977 40.72433)'}`

## `data/raw/noaa_weather.csv`

- Rows: `477`
- Columns: `DATE, PRCP, SNOW, TMAX, TMIN`

### Null Counts

| Column | Nulls |
|---|---:|
| `DATE` | 0 |
| `PRCP` | 0 |
| `SNOW` | 0 |
| `TMAX` | 0 |
| `TMIN` | 0 |

### Numeric Ranges

| Column | Min | Max |
|---|---:|---:|
| `PRCP` | 0.0 | 2.64 |
| `SNOW` | 0.0 | 13.6 |
| `TMAX` | 17.5 | 100.5 |
| `TMIN` | 3.0 | 81.0 |

### Sample Rows

- `{'DATE': '2025-01-01', 'PRCP': '0.0', 'SNOW': '0.0', 'TMAX': '52.0', 'TMIN': '40.0'}`
- `{'DATE': '2025-01-02', 'PRCP': '0.0', 'SNOW': '0.0', 'TMAX': '44.5', 'TMIN': '34.0'}`
- `{'DATE': '2025-01-03', 'PRCP': '0.0', 'SNOW': '0.0', 'TMAX': '40.0', 'TMIN': '32.0'}`
- `{'DATE': '2025-01-04', 'PRCP': '0.0', 'SNOW': '0.0', 'TMAX': '35.0', 'TMIN': '29.0'}`
- `{'DATE': '2025-01-05', 'PRCP': '0.0', 'SNOW': '0.0', 'TMAX': '35.0', 'TMIN': '28.5'}`
