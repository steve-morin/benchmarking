import polars as pl
import pandas as pd

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

polars_times = []
for i in tqdm.trange(repeats):
    df1 = pl.read_csv('df1.csv')
    df2 = pl.read_csv('df2.csv')

    # Benchmark merging
    start_time = time.time()
    merged_df = df1.join(df2, on='ID', how='inner')
    merge_time = time.time() - start_time

    # Benchmark selecting
    start_time = time.time()
    selected_df = merged_df.filter(pl.col('Value1') > 50000)
    select_time = time.time() - start_time

    # Benchmark statistics calculation
    start_time = time.time()
    stats = merged_df.describe()
    stats_time = time.time() - start_time

    # Save dataframes to CSV
    start_time = time.time()
    merged_df.write_csv('merged_df.csv')
    selected_df.write_csv('selected_df.csv')
    stats.write_csv('polars_stats.csv')
    csv_write_time = time.time() - start_time

    # Save dataframes to Parquet
    start_time = time.time()
    merged_df.write_parquet('merged_df.parquet')
    selected_df.write_parquet('selected_df.parquet')
    stats.write_parquet('stats.parquet')
    parquet_write_time = time.time() - start_time

    polars_times.append([merge_time, select_time, stats_time, csv_write_time, parquet_write_time])
    del df1
    del df2
    gc.collect()
    
# write to csv
df = pd.DataFrame(polars_times, columns=['merge_time', 'select_time', 'stats_time', 'csv_write_time', 'parquet_write_time'])
df.to_csv('polars_times.csv')
