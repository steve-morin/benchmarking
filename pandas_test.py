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


pandas_times = []
for i in tqdm.trange(repeats):
    local_times = []
    df1 = pd.read_csv('df1.csv')
    df2 = pd.read_csv('df2.csv')

    # Benchmark merging
    start_time = time.time()
    merged_df = pd.merge(df1, df2, on='ID', how='inner')
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
    merged_df.to_csv('merged_df.csv', index=False)
    selected_df.to_csv('selected_df.csv', index=False)
    stats.to_csv('pandas_stats.csv')
    csv_write_time = time.time() - start_time


    # Save dataframes to Parquet
    start_time = time.time()
    merged_df.to_parquet('merged_df.parquet', index=False)
    selected_df.to_parquet('selected_df.parquet', index=False)
    stats.to_parquet('stats.parquet')
    parquet_write_time = time.time() - start_time

    pandas_times.append([merge_time, select_time, stats_time, csv_write_time, parquet_write_time])
    del df1
    del df2
    gc.collect()

    
# write to csv
df = pd.DataFrame(pandas_times, columns=['merge_time', 'select_time', 'stats_time', 'csv_write_time', 'parquet_write_time'])
df.to_csv('pandas_times.csv')
    
