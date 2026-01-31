from sqlalchemy import create_engine
import pandas as pd
from tqdm.auto import tqdm

dtype ={
    "VendorID":"Int64",
    "passenger_count":"Int64",
    "trip_distance":"float64",
    "RatecodeID":"Int64",
    "store_and_fwd_flag":"string",
    "PULocation":"Int64",
    "DOLocation":"Int64",
    "payment_type":"Int64",
    "fare_amount":"float64",
    "extra":"float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}
parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


def run():
    
    month = '01'
    year = '2021'
    baseurl = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
    taxicolor ='yellow/'
    endpoint = f'yellow_tripdata_{year}-{month}.csv.gz'
    url = baseurl+taxicolor+endpoint
    
    engine= create_engine('postgresql://root:admin@pgdb:5432/ny_taxi')

    chunksize = 100000        
    target_table = f'yellow_taxi_data_{year}_{month}'

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize,
    )

    first = True

    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists='replace'
            )
            first = False

        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists='append'
        )

if __name__ == '__main__':
    run()