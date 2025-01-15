import pandas as pd
import gzip
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, min as spark_min, when

spark = SparkSession.builder.appName("ColdStreaks").getOrCreate()

def cold_streaks():
    folder_path = './source/dados/'

    dataframes = []
    list_of_data = []

    columns = ['DATE','DTG', 'LOCATION', 'NAME', 'LATITUDE', 'LONGITUDE', 
            'ALTITUDE', 'U_BOOL_10', 'T_DRYB_10', 'TN_10CM_PAST_6H_10', 
            'T_DEWP_10', 'T_DEWP_SEA_10', 'T_DRYB_SEA_10', 'TN_DRYB_10', 
            'T_WETB_10', 'TX_DRYB_10', 'U_10', 'U_SEA_10']
    count = 0 

    # Iterate over all files in the folder
    for filename in os.listdir(folder_path):
        if count > 1:
            break
        if filename.endswith('.gz'):
            file_path = os.path.join(folder_path, filename)
            with gzip.open(file_path, 'rt') as f:
                try:
                    list_of_data = f.read().splitlines()
                    data = list_of_data[21:]
                    data = [x.split(' ') for x in data]
                    data = [[value for value in sublist if value != ''] for sublist in data]
                    data = [sublist[:18] for sublist in data]
                    data = [sublist + [None] * (18 - len(sublist)) if len(sublist) < 18 else sublist for sublist in data]
                    dataframes.append(pd.DataFrame(data, columns=columns))
                    
                    f.close()
                    count += 1
                    
                except Exception as e:
                    raise Exception(e)
                
    combined_df = pd.concat(dataframes, ignore_index=True)

    spark_df = spark.createDataFrame(combined_df)

    spark_df = spark_df.withColumn('DATE', to_date(col('DATE')))
    spark_df = spark_df.withColumn('TN_DRYB_10', col('TN_DRYB_10').cast('float'))
    spark_df = spark_df.withColumn('T_DRYB_10', col('T_DRYB_10').cast('float'))
    spark_df = spark_df.withColumn('TN_DRYB_10',
                                   when(col('TN_DRYB_10').isNull(), col('T_DRYB_10')).otherwise(col('TN_DRYB_10')))

    grouped_df = spark_df.groupBy('DATE').agg(spark_min('TN_DRYB_10').alias('TN_DRYB_10')).orderBy('DATE')

    grouped_df = grouped_df.withColumn('TN_DRYB_10', (col('TN_DRYB_10') - 32) * 5.0/9.0)

    grouped_df = grouped_df.collect()

    current_streak = []
    total_streaks = []
    low_temp_days = 0
    start_temperature = 3
    low_temperature = 0
    min_days = 5
    min_low_temp_days = 3

    for row in grouped_df:
        temp = row['TN_DRYB_10']
        date = row['DATE']
                    
        if float(temp) <= start_temperature:
            data = (date, temp)
            current_streak.append(data)
            if float(temp) <= low_temperature:
                low_temp_days += 1
        else:
            low_temp_days = 0
            current_streak = []
        if len(current_streak) >= min_days and low_temp_days >= min_low_temp_days:
            total_streaks.append(current_streak)
            current_streak = []

    print(f'Total cold streaks: {len(total_streaks)}')
    
if __name__ == '__main__':
    cold_streaks()
