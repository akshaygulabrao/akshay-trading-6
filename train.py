import os,sqlite3
import pandas as pd
import matplotlib.pyplot as plt

# sensors looks like this. It contains observations of 2 types: automatic metar data every 5 minutes and precise data every hour. There are 7 stations: KNYC,KMDW,KMIA,KAUS,KPHL,KDEN,KLAX
#                id                       inserted_at station          observation_time  air_temp  relative_humidity  dew_point  wind_speed
# 0               1         2025-08-04T00:26:05+00:00    KLAX  2025-08-03T17:15:00-0700     71.60              68.81      60.76       14.96
# 1               6         2025-08-04T00:33:34+00:00    KLAX  2025-08-03T17:25:00-0700     69.80              73.15      60.76       13.81
# 2             242         2025-08-04T00:38:39+00:00    KLAX  2025-08-03T17:30:00-0700     69.80              73.15      60.76       13.81
# 3             274         2025-08-04T00:39:18+00:00    KNYC  2025-08-03T19:51:00-0400     75.02              41.46      49.98        0.00
# 4             275         2025-08-04T00:39:18+00:00    KMDW  2025-08-03T19:30:00-0500     75.20              47.06      53.58        9.21
# ...           ...                               ...     ...                       ...       ...                ...        ...         ...
# 40830  1104107472  2025-08-25T11:49:27.937282+00:00    KMDW  2025-08-25T06:40:00-0500     55.40              76.68      48.13        6.91
# 40831  1104109837  2025-08-25T11:49:27.940279+00:00    KMIA  2025-08-25T07:35:00-0400     78.80              88.78      75.20        0.00
# 40832  1104109838  2025-08-25T11:49:27.940280+00:00    KMIA  2025-08-25T07:40:00-0400     78.80              94.25      77.00        0.00
# 40833  1104112470  2025-08-25T11:49:27.943422+00:00    KPHL  2025-08-25T07:35:00-0400     73.40              83.25      67.98        4.60
# 40834  1104112471  2025-08-25T11:49:27.943423+00:00    KPHL  2025-08-25T07:40:00-0400     73.40              83.25      67.98        3.45


# forecasts look like this, which returns a 48 hour forecast every hour. The stations are identical to the sensors
#                              inserted_at  idx station           observation_time  air_temp dew_point  relative_humidity  wind_speed
# 0              2025-08-04T06:00:00+00:00    0    KNYC  2025-08-04T02:00:00-04:00      71.0      52.0               55.0         1.0
# 1              2025-08-04T06:00:00+00:00    1    KNYC  2025-08-04T03:00:00-04:00      70.0      53.0               57.0         1.0
# 2              2025-08-04T06:00:00+00:00    2    KNYC  2025-08-04T04:00:00-04:00      69.0      55.0               61.0         1.0
# 3              2025-08-04T06:00:00+00:00    3    KNYC  2025-08-04T05:00:00-04:00      68.0      54.0               61.0         1.0
# 4              2025-08-04T06:00:00+00:00    4    KNYC  2025-08-04T06:00:00-04:00      68.0      55.0               63.0         1.0
# ...                                  ...  ...     ...                        ...       ...       ...                ...         ...
# 138187  2025-08-25T11:10:12.153664+00:00   43    KPHL   2025-08-27T02:00:00-0400      63.0      52.0               67.0         6.0
# 138188  2025-08-25T11:10:12.153664+00:00   44    KPHL   2025-08-27T03:00:00-0400      62.0      52.0               70.0         6.0
# 138189  2025-08-25T11:10:12.153664+00:00   45    KPHL   2025-08-27T04:00:00-0400      61.0      52.0               72.0         6.0
# 138190  2025-08-25T11:10:12.153664+00:00   46    KPHL   2025-08-27T05:00:00-0400      60.0      52.0               75.0         6.0
# 138191  2025-08-25T11:10:12.153664+00:00   47    KPHL   2025-08-27T06:00:00-0400      60.0      52.0               75.0         6.0

# you need to predict the next 48 hours of sensor data WITH CONFIDENCE INTERVALS

conn = sqlite3.connect(os.getenv("WEATHER_DB_PATH"))
sensor = pd.read_sql("select * from weather;",conn)
conn.close()

conn = sqlite3.connect(os.getenv("FORECAST_DB_PATH"))
forecast = pd.read_sql("select * from forecast;", conn)
conn.close()


STATIONS = ['KNYC','KMDW','KMIA','KAUS','KPHL','KDEN','KLAX']
VARS = ['air_temp', 'dew_point', 'relative_humidity', 'wind_speed']
N_STATIONS = len(STATIONS)
N_VARS = len(VARS)

sensor['observation_time'] = pd.to_datetime(sensor['observation_time'],utc=True,format="ISO8601")
sensor['inserted_at'] = pd.to_datetime(sensor['inserted_at'],utc=True, format="ISO8601")

forecast['observation_time'] = pd.to_datetime(sensor['observation_time'],utc=True,format="ISO8601")
forecast['inserted_at'] = pd.to_datetime(sensor['inserted_at'],utc=True,format="ISO8601")

print(sensor)

print(forecast)

sensor = sensor.set_index("observation_time").groupby("station")[VARS].resample('H').mean().reset_index()

forecast = forecast.set_index(["inserted_at","idx"])

print(sensor)
print(forecast)