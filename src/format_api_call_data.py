import pandas as pd
import numpy as np
import os


def format_api_call_data(data_folder='', data_file=''):
    ''' 

    if a data_folder is given:
        INPUT: data_foler is a directory with files created by api_call.py: 
            data_folder = 'data/'

        OUTPUT1:  a LIST of formatted api data dumps as panda dataframes 
        OUTPUT2:  A formatted version of the dataframe will also be saved 
            in same directory as passed in as 'file_name_formatted.csv'

    if data_file is given:
        INPUT:  data_file is the path to just one api_call data dump:
            data_file = 'some_csv.csv'

        OUTPUT:  Pandas DataFrame one formatted dataframe
        OUTPUT2:  A formatted version of the dataframe will also be saved 
            in same directory as passed in as 'file_name_formatted.csv'


    '''

    if len(data_folder) > 2:
        print "Loading data file: \n"
        cwd = data_folder
        cwd_files = os.listdir(cwd)
        print "Processing following documents \n \t"
        print cwd_files
        data_files = []

        for i in cwd_files:
            if i[-3:] == 'csv':
                data_files.append(cwd + i)

        lst_of_dfs = []
        
        for p in data_files:

            # Skip the files that have _formatted in the title. 
            if '_formatted' in p:
                continue

            df = pd.read_csv(p)
            x = df.columns

            if 'timestamp' in x:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df = df.set_index('timestamp')

                # If the polarity is above 0.2, label it 'positive'
                df['positive'] = np.where(df['polarity'] >= 0.2, 1, 0)
                
                # If the polarity is below -0.2, label it 'negative'
                df['negative'] = np.where(df['polarity'] <= -0.2, 1, 0)

                # pos_neg is a column that is a string repersenation of its polarity
                # each cell value will either be 'positive', 'negative', or 'neutral'
                df['pos_neg'] = 'neutral'
                pos = df['polarity'] >=  0.2
                neg = df['polarity'] <= -0.2
                df['pos_neg'][pos] = 'positive'
                df['pos_neg'][neg] = 'negative'

                df['neutral'] = np.where(df['pos_neg'] == 'neutral', 1, 0)
                df.to_csv( p[:-4] + '_formatted.csv', sep=',')
                
                lst_of_dfs.append(df)
            
        return lst_of_dfs

    if len(data_file) > 2:
        df = pd.read_csv(data_file)
        x = df.columns
        if 'timestamp' in x:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.set_index('timestamp')

            # If the polarity is above 0.2, label it 'positive'
            df['positive'] = np.where(df['polarity'] >= 0.2, 1, 0)
            
            # If the polarity is below -0.2, label it 'negative'
            df['negative'] = np.where(df['polarity'] <= -0.2, 1, 0)

            # pos_neg is a column that is a string repersenation of its polarity
            # each cell value will either be 'positive', 'negative', or 'neutral'
            df['pos_neg'] = 'neutral'

            pos = df['polarity'] >=  0.2
            neg = df['polarity'] <= -0.2
            df['pos_neg'][pos] = 'positive'
            df['pos_neg'][neg] = 'negative'

            # If the pos_neg col is 'neutral', put a 1, in the neutral column.
            df['neutral'] = np.where(df['pos_neg'] == 'neutral', 1, 0)

            # Save the file
            df.to_csv( data_file[:-4] + '_formatted.csv', sep=',')
        return df

x = format_api_call_data(data_file='data/api_call_output.csv')
