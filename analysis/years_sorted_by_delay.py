from google.cloud import bigquery
import pandas as pd

client = bigquery.Client(project='flight-data-pipeline', location='EU')

query = """
SELECT year, AVG(arr_delay/arr_flights) as avg_delay
FROM flight-data-pipeline.flights_dataset.flights
group by year
order by avg_delay desc
"""

df = client.query(query).to_dataframe()

print(df)

#     year  avg_delay
# 0   2023  15.022613
# 1   2022  13.519376
# 2   2019  13.067128
# 3   2018  12.466164
# 4   2014  12.079051
# 5   2017  11.741596
# 6   2021  10.858307
# 7   2015  10.850292
# 8   2016  10.435473
# 9   2013   9.936982
# 10  2020   5.685047