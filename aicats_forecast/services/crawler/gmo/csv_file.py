from datetime import datetime, timezone
from typing import Optional
import threading
import os
import warnings
warnings.filterwarnings("ignore")

import pandas as pd


class GMOTradeCsvFile:

    def __init__(
        self,
        symbol: str,
        flush_interval: Optional[int] = 100,
        save_dir: Optional[str] = None,
    ):
        self._COL_NAMES = ['timestamp', 'symbol', 'side', 'price', 'size', 'timestamp_saved']
        self._symbol = symbol
        if save_dir is not None:
            self._save_dir = save_dir
        else:
            self._save_dir = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                '../../../../data/gmo/trade',
                self._symbol
            )
        if not self._save_dir.lower().startswith('s3://'):
            os.makedirs(self._save_dir, exist_ok=True)
        self._filepath = self.get_filename()
        self._df = self._load_or_create(self._filepath)
        self._lock = threading.Lock()
        self._flush_interval = flush_interval

    def get_filename(self) -> str:
        return os.path.join(
            self._save_dir,
            f'{datetime.now(tz=timezone.utc).strftime("%Y%m%d")}.csv.gzip'
        )

    def _load_or_create(self, filepath: str) -> pd.DataFrame:
        if os.path.exists(filepath):
            return pd.read_csv(filepath, compression='gzip')
        else:
            return pd.DataFrame(columns=self._COL_NAMES)

    def append(
        self,
        timestamp: str,
        price: int,
        size: float,
        side: str,
        symbol: str,
        timestamp_saved: str,
        flush: Optional[bool] = True
    ) -> None:
        with self._lock:
            data = pd.Series([timestamp, symbol, side, price, size, timestamp_saved], index=self._COL_NAMES)
            if self._filepath == self.get_filename():
                self._df = self._df.append(data, ignore_index=True)
                if flush and len(self._df) % self._flush_interval == 0:
                    self.flush_file()
            else:
                # 次の日になってファイル名が変わった
                if len(self._df):
                    # 前の日のファイルパスでデータ保存
                    self.flush_file()
                self._filepath = self.get_filename()  # 今日のファイルパスに変更
                self._df = self._load_or_create(self._filepath)
                self._df = self._df.append(data, ignore_index=True)
                if flush and len(self._df) % self._flush_interval == 0:
                    self.flush_file()

    def flush_file(self):
        self._df = self._df.drop_duplicates(keep='first')
        self._df.to_csv(self._filepath, index=False, compression='gzip')

    @property
    def filepath(self) -> str:
        # return self._filepath
        return self.get_filename()
