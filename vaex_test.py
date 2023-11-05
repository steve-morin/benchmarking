import polars as pl
import pandas as pd
import vaex
import time

import time
import gc
import tqdm
scale = 10000000
repeats = 100
# Generate sample dataframes
df1 = pd.DataFrame({'ID': range(scale), 'Value1': range(scale)})
df2 = pd.DataFrame({'ID': range(int(scale * 0.5), int(scale * 1.5)), 'Value2': range(scale)})

df1.to_csv('df1.csv')
df2.to_csv('df2.csv')

vaex_times = []
for i in tqdm.trange(repeats):
    df1 = vaex.read_csv('df1.csv', index_col=0)
    df2 = vaex.read_csv('df2.csv', index_col=0)

    # Benchmark merging
    start_time = time.time()
    merged_df = df1.join(df2, on='ID', how='inner')
    merge_time = time.time() - start_time

    # Benchmark selecting
    start_time = time.time()
    selected_df = merged_df[merged_df['Value1'] > 50000]
    select_time = time.time() - start_time

    # Benchmark statistics calculation
    start_time = time.time()
    stats = merged_df.describe()
    stats_time = time.time() - start_time

    # Save dataframes to CSV
    start_time = time.time()
    merged_df.export('merged_df.csv') #uses pandas write_csv backend, so nothing to expect
    selected_df.export('selected_df.csv')
    stats.to_csv('vaex_stats.csv') 
    csv_write_time = time.time() - start_time


    # Save dataframes to Parquet
    start_time = time.time()
    merged_df.export('merged_df.parquet')
    selected_df.export('selected_df.parquet')
    stats.to_csv('stats.parquet')
    parquet_write_time = time.time() - start_time

    
    vaex_times.append([merge_time, select_time, stats_time, csv_write_time, parquet_write_time])
    del df1
    del df2
    gc.collect()
    
    # write to csv

# write to csv
df = pd.DataFrame(vaex_times, columns=['merge_time', 'select_time', 'stats_time', 'csv_write_time', 'parquet_write_time'])
df.to_csv('vaex_times.csv')
    