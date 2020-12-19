import os
import json
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

raw_data_path = f"{os.getcwd()}/raw_data"

def read_file(path):
    try:
        data = []
        with open(path) as f:
            for line in f:
                data.append(json.loads(line))
        return pd.DataFrame.from_dict(data)
    except Exception as e:
        print(e)
        raise Exception(f"Failed to read file from path {path}. Error - {e}")

def df_handler(df, original_filename, _sorted=False):
    try:
        # aggerate by keyword 
        df['dt'] = pd.to_datetime(df['dt'])
        df_grouped = df.groupby(['keyword', 'dt'])\
            .agg(
                avg_cpc=('cpc', np.mean),
                avg_searches_monthly_volume=('monthly_volume', np.mean)
                )
        if _sorted: df_grouped = df_grouped.sort_values(by=['avg_searches_monthly_volume', 'avg_cpc'], ascending=False)
        # save as parquet
        table = pa.Table.from_pandas(df_grouped)
        parq_filename = original_filename.replace('_json', '')
        pq.write_table(table, f"{os.getcwd()}/output/{parq_filename}.parquet")
    except Exception as e:
        print(e)

if __name__ == "__main__":
    try:
        if not os.path.exists(f"{os.getcwd()}/output"): os.mkdir(f"{os.getcwd()}/output")
        all_raw_files = os.listdir(raw_data_path)
        for raw_file in all_raw_files:
            raw_df = read_file(f"{raw_data_path}/{raw_file}")
            df_handler(raw_df, raw_file)
            print(f"Finished file {raw_file}.")
        print("Done!")
    except Exception as e:
        print(e)