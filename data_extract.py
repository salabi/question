import requests
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

endpoint = "https://api.open-meteo.com/v1/forecast?latitude=51.5085&longitude=-0.1257&hourly=temperature_2m,rain,showers,visibility&past_days=31"

def fectch_data (endpoint : str) -> str:
    response = requests.get(endpoint)
    response.raise_for_status() # check for success
    data = response.json()
    return data

def sum_daily_totals(df_data : pd.DataFrame) -> pd.DataFrame:
    '''
    takes in dataframe and aggregate
    '''
    daily_totals = df_data.groupby('date').agg({
        'temperature_2m': 'sum',
        'rain': 'sum',
        'showers': 'sum',
        'visibility': 'sum'
    }).reset_index()
    return daily_totals


if __name__ == '__main__':

    data = fectch_data(endpoint)

    # transform to DataFrame
    df_data = pd.DataFrame(data['hourly'])


    df_data['latitude'] = data['latitude']
    df_data['longitude'] = data['longitude']
    df_data['generationtime_ms'] = data['generationtime_ms']
    df_data['utc_offset_seconds'] = data['utc_offset_seconds']
    df_data['timezone'] = data['timezone']
    df_data['timezone_abbreviation'] = data['timezone_abbreviation']
    df_data['elevation'] = data['elevation']

    #print(df_data)

    # Group by date
    df_data['time'] = pd.to_datetime(df_data['time'])
    df_data['date'] = df_data['time'].dt.date

    x = sum_daily_totals(df_data)


    print(x)

    table = pa.Table.from_pandas(x)
    pq.write_table(table, 'daily_totalss.parquet')
    # print(table)
