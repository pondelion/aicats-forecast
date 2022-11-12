from datetime import  date, datetime, timedelta, timezone
import time
import threading
import os

import requests
import pandas as pd
import numpy as np

from .daily_csv_file import DailyCSVFile


JST = timezone(timedelta(hours=+9))
DEFAULT_API_KEY = os.environ['WHALE_ALERT_API_KEY']


class WACrawler:

    def __init__(self, api_key: str = DEFAULT_API_KEY):
        self._crawling = False
        self._INTERVAL_SEC = 15
        self._fail_cnt = 0
        self._URL = 'https://api.whale-alert.io/v1/transactions'
        self._API_KEY = api_key
        self._TIMEOUT_SEC = 5*60  # 5min

    def _json2df(self, json_data):
        df = pd.DataFrame(json_data['transactions'])
        df['datetime'] = df['timestamp'].map(lambda x: datetime.fromtimestamp(x, JST))
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['from_address'] = df['from'].map(lambda x: x.get('address', np.nan))
        df['from_owner'] = df['from'].map(lambda x: x.get('owner', np.nan))
        df['from_owner_type'] = df['from'].map(lambda x: x.get('owner_type', np.nan))
        df['to_address'] = df['to'].map(lambda x: x.get('address', np.nan))
        df['to_owner'] = df['to'].map(lambda x: x.get('owner', np.nan))
        df['to_owner_type'] = df['to'].map(lambda x: x.get('owner_type', np.nan))
        df.drop(columns=['from', 'to'], inplace=True)
        return df

    def _get_columns(self):
        params = {
            'api_key': self._API_KEY,
            'min_value': 500000,
            'start': int(datetime.now().timestamp())-10*60,
        }
        res = requests.get(self._URL, params=params)
        print(res)
        print(list(res.json().keys()))
        print(res.json())
        if res.status_code != 200:
            print(f'get_column failed, {res}')
            raise Exception(f'{res}')
        df = self._json2df(res.json())
        return df.columns.tolist()

    def start_crawl(self):
        threading.Thread(
            target=self._crawl_worker
        ).start()

    def stop_crawl(self):
        self._crawling = False

    def _crawl_worker(self):
        print(f'{datetime.now()} start crawling whale alert')
        self._crawling = True
        cursor = None
        columns = self._get_columns()
        csv_file = DailyCSVFile(dir_tag='whale_alert', columns=columns)
        while self._crawling:
            if cursor is None:
                params = {
                    'api_key': self._API_KEY,
                    'min_value': 500000,
                    'start': int(datetime.now().timestamp())-3500,
                }
            else:
                params = {
                    'api_key': self._API_KEY,
                    'min_value': 500000,
                    'cursor': cursor,
                }
            try:
                res = requests.get(self._URL, params=params, timeout=self._TIMEOUT_SEC)
                data = res.json()
                df = self._json2df(data)
                csv_file.append(df)
                cursor = data['cursor']
                print(f'[{datetime.now()}] len(df)={len(df)}, len(csv_file)={len(csv_file)}')
                fail_cnt = 0
            except Exception as e:
                print(e)
                print(res)
                print(data)
                fail_cnt += 1
                if fail_cnt % 100 == 0:
                    print(f'[WARNING] whale alert crawling keep failing.\n {res} : {data}')
                if 'maximum transaction history is' in data.get('message', ''):
                    cursor = None
                time.sleep(60)
            time.sleep(self._INTERVAL_SEC)
        print('end crawling whale alert')

    @property
    def is_crawling(self) -> bool:
        return self._crawling
