import pandas as pd
import numpy as np 
import matplotlib.pyplot as plt
# open csv file

# set width of bar 
barWidth = 0.25
fig = plt.subplots(figsize =(12, 8)) 

# Read in data
pandas_df = pd.read_csv('pandas_times.csv', index_col=0)
pandas_df_2 = pandas_df[["merge_time", "select_time", "stats_time", "csv_write_time", "parquet_write_time"]].mean()

# Read in data
vaex_df = pd.read_csv('vaex_times.csv', index_col=0)
vaex_df_2 = vaex_df[["merge_time", "select_time", "stats_time", "csv_write_time", "parquet_write_time"]].mean()

# Read in data
polars_df = pd.read_csv('polars_times.csv', index_col=0)
polars_df_2 = polars_df[["merge_time", "select_time", "stats_time", "csv_write_time", "parquet_write_time"]].mean()

frames = [pandas_df_2, vaex_df_2, polars_df_2]

result = pd.concat(frames, axis=1, sort=False)
result.columns = ['pandas', 'vaex', 'polars']

# Set position of bar on X axis 
br1 = np.arange(len(result['pandas'])) 
br2 = [x + barWidth for x in br1] 
br3 = [x + barWidth for x in br2] 

plt.bar(br1, result['pandas'], color ='r', width = barWidth, 
		edgecolor ='grey', label ='pandas') 

plt.bar(br2, result['vaex'], color ='g', width = barWidth, 
		edgecolor ='grey', label ='vaex') 

plt.bar(br3, result['polars'], color ='b', width = barWidth, 
		edgecolor ='grey', label ='polars') 

# Adding Xticks 
plt.xlabel('Function', fontweight ='bold', fontsize = 15) 
plt.ylabel('Time(seconds)', fontweight ='bold', fontsize = 15) 

plt.xticks([r + barWidth for r in range(len(result['pandas']))], 
		['merge', 'select', 'stats', 'csv_write', 'parquet_write'])

plt.title('Benchmarking of Pandas, Vaex and Polars')
plt.legend()
plt.show() 

print('done')