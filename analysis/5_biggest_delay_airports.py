from google.cloud import bigquery
import pandas as pd

client = bigquery.Client(project='flight-data-pipeline', location='EU')

query = """
SELECT airport_name, AVG(arr_delay/arr_flights) as avg_delay
FROM flight-data-pipeline.flights_dataset.flights
group by airport_name
order by avg_delay desc
limit 5
"""

df = client.query(query).to_dataframe()

print(df.head())

#                                         airport_name  avg_delay
# 0            Williamsport, PA: Williamsport Regional  60.178195
# 1  Youngstown/Warren, OH: Youngstown-Warren Regional  59.500000
# 2  Bullhead City, AZ: Laughlin/Bullhead Internati...  26.529019
# 3                         Wilmington, DE: New Castle  23.957327
# 4                        Topeka, KS: Topeka Regional  23.325192