import pandas as pd
import pyarrow.parquet as pq
import os


def write_parquet_file(fname, target_fname):
    ''' Convert csv files to parquet format.
    Arguments:
        None
    '''
    df = pd.read_csv(fname)
    df.to_parquet(target_fname,
                engine='pyarrow',
                compression='snappy',
                partition_cols=['ForecastSiteCode']
                )
    
# Write parquet files
# write_parquet_file('C:/Users/ksriv/viki/weather.20160201.csv', 'C:/Users/ksriv/viki/pq/weather_parquet1')
# write_parquet_file('C:/Users/ksriv/viki/weather.20160301.csv', 'C:/Users/ksriv/viki/pq/weather_parquet2')
    
#Source directory
directory = 'C:/Users/ksriv/viki/pq'
# max_temp = defaultdict(list)
max_temp = {}

#Navigate through each directory
for x in os.walk(directory):
    df = pq.read_table(source=x[0],
                       columns=['ObservationDate', 
                                'ScreenTemperature',
                                'Region']).to_pandas()
    if not bool(max_temp): # Check if max_temp is empty
        max_temp = (df.iloc[df['ScreenTemperature'].idxmax()]).to_dict() # if empty, locate and log max
    else: #Extract max temperature from each parquet file and update max-log
        s = df.iloc[df['ScreenTemperature'].idxmax()]
        if max_temp['ScreenTemperature'] < s['ScreenTemperature']:
           max_temp['ScreenTemperature'] = s['ScreenTemperature']
           max_temp['ObservationDate'] = s['ObservationDate']
           max_temp['Region'] = s['Region']
      
# Print result
print('Date of the Hottestday : ',max_temp['ObservationDate'])
print('Temperature of the Hottest Date :', max_temp['ScreenTemperature'])
print('Region of the Hottest Day', max_temp['Region'])
