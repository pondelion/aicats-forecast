import os
from datetime import date
from glob import glob

import pandas as pd


class DataSet:

    def __init__(self):
        self._data_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            '../../../data/wa',
            'whale_alert'
        )
        self._df_filelist = self._load_filelist()
        print(self._df_filelist)
        self._latest_file_date = self._df_filelist['date'].iloc[-1]
        self._df_data_without_today, self._df_data_today = self._load_files_to_df(self._df_filelist)

    def _load_filelist(self):
        csv_files = glob(os.path.join(self._data_dir, '*.csv'), recursive=True)
        df_whale_files = pd.DataFrame({
            'file': csv_files,
            'date': [date(int(file.split('/')[-1].strip('.csv')[:4]), int(file.split('/')[-1].strip('.csv')[4:6]), int(file.split('/')[-1].strip('.csv')[6:])) for file in csv_files]
        })
        df_whale_files.sort_values(by='date', inplace=True)
        df_whale_files.reset_index(inplace=True, drop=True)
        return df_whale_files

    def _load_files_to_df(self, df_filelist):
        df_files_without_today = df_filelist[df_filelist['date'] != date.today()]['file']
        print(f'len(df_files_without_today)) : {len(df_files_without_today)}')
        file_today = df_filelist[df_filelist['date'] == date.today()]['file'].iloc[0]
        print(f'file_today : {file_today}')
        df_data_without_today = pd.concat([pd.read_csv(f) for f in df_files_without_today])
        df_data_without_today['datetime_jst'] = pd.to_datetime(df_data_without_today['datetime'].map(lambda x: x.replace('+09:00', '')))
        df_data_without_today.set_index('datetime_jst', inplace=True)
        df_data_without_today.sort_index(inplace=True)
        df_data_today = pd.read_csv(file_today)
        df_data_today['datetime_jst'] = pd.to_datetime(df_data_today['datetime'].map(lambda x: x.replace('+09:00', '')))
        df_data_today.set_index('datetime_jst', inplace=True)
        df_data_today.sort_index(inplace=True)
        return df_data_without_today, df_data_today

    def _load_todays_file_to_df(self, file_today):
        df_data_today = pd.read_csv(file_today)
        df_data_today['datetime_jst'] = pd.to_datetime(df_data_today['datetime'].map(lambda x: x.replace('+09:00', '')))
        df_data_today.set_index('datetime_jst', inplace=True)
        df_data_today.sort_index(inplace=True)
        return df_data_today

    @property
    def df(self):
        if date.today() != self._latest_file_date:
            # need updation of filelist and whole data
            print('updating filelist and whole data')
            self._df_filelist = self._load_filelist()
            self._latest_file_date = self._df_filelist['date'].iloc[-1]
            assert date.today() == self._latest_file_date
            self._df_data_without_today, self._df_data_today = self._load_files_to_df(self._df_filelist)
        else:
            # need updation of today's data
            print('updating todays data')
            file_today = self._df_filelist[self._df_filelist['date'] == date.today()]['file'].iloc[0]
            self._df_data_today = self._load_todays_file_to_df(file_today)
        df_whole_data = pd.concat([self._df_data_without_today, self._df_data_today])
        df_whole_data.sort_index(inplace=True)
        df_whole_data.drop_duplicates(subset='id', keep='last', inplace=True)
        return df_whole_data
