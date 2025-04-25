from google.cloud import bigquery
import pandas as pd

client = bigquery.Client(project='flight-data-pipeline', location='EU')

query = """
SELECT carrier_name, AVG(arr_delay/arr_flights) as avg_delay
FROM flight-data-pipeline.flights_dataset.flights
group by carrier_name
order by avg_delay desc
limit 5
"""

df = client.query(query).to_dataframe()

print(df.head())


#                carrier_name  avg_delay
# 0           JetBlue Airways  15.635230
# 1    Frontier Airlines Inc.  14.642440
# 2        Mesa Airlines Inc.  13.944452
# 3             Allegiant Air  13.657190
# 4  ExpressJet Airlines Inc.  13.645334