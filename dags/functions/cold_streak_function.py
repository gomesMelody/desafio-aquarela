import pandas as pd
import gzip
import os

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
        if count > 10:
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

    combined_df['DATE'] = pd.to_datetime(combined_df['DATE'])
    combined_df['TN_DRYB_10'] = pd.to_numeric(combined_df['TN_DRYB_10'], errors='coerce')
    combined_df['T_DRYB_10'] = pd.to_numeric(combined_df['T_DRYB_10'], errors='coerce')
    combined_df.loc[combined_df['TN_DRYB_10'] == 'nan', 'TN_DRYB_10'] = combined_df['T_DRYB_10']

    combined_df = combined_df.sort_values(by='DATE')
    grouped_df = combined_df.groupby(combined_df['DATE'])['TN_DRYB_10'].min().reset_index()

    grouped_df['TN_DRYB_10'] = (grouped_df['TN_DRYB_10'] - 32) * 5.0/9.0

    current_streak = []
    total_streaks = []
    low_temp_days = 0
    start_temperature = 3
    low_temperature = 0
    min_days = 5
    min_low_temp_days = 3


    for i in range(len(grouped_df)):
        temp = grouped_df.iloc[i]['TN_DRYB_10']
        date = grouped_df.iloc[i]['DATE']
                    
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
