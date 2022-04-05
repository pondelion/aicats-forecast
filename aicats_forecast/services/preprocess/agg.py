from datetime import timedelta
from time import sleep
from threading import Thread

import schedule
import pandas as pd


class ConstantTimeIntervalAggService:

    def __init__(
        self,
        symbol: str,
        csv_file_repo,
        agg_time_interval: timedelta = timedelta(minutes=5),
        process_schedule_interval: timedelta = timedelta(minutes=5),
    ):
        self._symbol = symbol
        self._agg_time_interval = agg_time_interval
        self._process_schedule_interval = process_schedule_interval
        self._csv_repo = csv_file_repo

    def start_agg_daemon(self) -> None:
        Thread(
            target=self._start_agg_daemon,
        ).start()

    def _start_agg_daemon(self) -> None:
        schedule.every(self._process_schedule_interval .total_seconds()).seconds.do(self._agg)
        print(f'scheduled agg task every {self._process_schedule_interval .total_seconds()} seconds')
        self._service_running = True
        while self._service_running:
            schedule.run_pending()
            sleep(1)

    def stop_agg_daemon(self) -> None:
        self._service_running = False

    def oneshot_agg(self) -> None:
        self._agg()

    def _agg(self):
        print('start agg task')
        self._df = self._load_csvs_to_df()

        df_buy = self._df[self._df['side']=='BUY']
        df_sell = self._df[self._df['side']=='SELL']

        df_buy.drop_duplicates('timestamp', inplace=True)
        df_sell.drop_duplicates('timestamp', inplace=True)

        df_buy.set_index('timestamp', inplace=True)
        df_buy.sort_index(inplace=True)

        df_sell.set_index('timestamp', inplace=True)
        df_sell.sort_index(inplace=True)

        df_buy_resample = df_buy.resample(
            f'{int(self._agg_time_interval.total_seconds())}s'
        ).mean().interpolate()
        df_sell_resample = df_sell.resample(
            f'{int(self._agg_time_interval.total_seconds())}s'
        ).mean().interpolate()

        df_buy_size_minmax_resample = df_buy[['size']].resample(
            f'{int(self._agg_time_interval.total_seconds())}s'
        ).agg(['min', 'max']).interpolate()
        df_buy_size_minmax_resample.columns = ['_'.join(cols) for cols in df_buy_size_minmax_resample.columns]
        df_sell_size_minmax_resample = df_sell[['size']].resample(
            f'{int(self._agg_time_interval.total_seconds())}s'
        ).agg(['min', 'max']).interpolate()
        df_sell_size_minmax_resample.columns = ['_'.join(cols) for cols in df_sell_size_minmax_resample.columns]

        df_buy_resample = df_buy_resample.join(
            df_buy_size_minmax_resample, how='inner',
        )
        df_sell_resample = df_sell_resample.join(
            df_sell_size_minmax_resample, how='inner',
        )

        df_resample = df_buy_resample.join(
            df_sell_resample, how='inner', lsuffix='_buy', rsuffix='_sell'
        )
        self._save_agg_df(df_resample)
        print('done agg task')

    def _load_csvs_to_df(self) -> pd.DataFrame:
        df = pd.read_csv(self._csv_repo.filepath)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df

    def _save_agg_df(self, df: pd.DataFrame) -> None:
        print(df.head())
