import os
from datetime import datetime, timezone
from typing import List

import pandas as pd


class DailyCSVFile:

    def __init__(self, dir_tag: str, columns: List[str]):
        self._columns = columns
        self._dir_tag = dir_tag
        self._save_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            '../../../../data/wa',
            self._dir_tag
        )
        os.makedirs(self._save_dir, exist_ok=True)
        self._filepath = self.get_filename()
        self._df_data = self._load_or_create(self._filepath)

    def get_filename(self):
        return os.path.join(
            self._save_dir,
            f'{datetime.now(tz=timezone.utc).strftime("%Y%m%d")}.csv.gzip'
        )

    def _load_or_create(self, filepath: str) -> pd.DataFrame:
        if os.path.exists(filepath):
            return pd.read_csv(filepath, compression='gzip')
        else:
            return pd.DataFrame(columns=self._columns)

    def append(self, data: pd.DataFrame):
        if self._filepath != self.get_filename():
            self._filepath = self.get_filename()
            self._df_data = self._load_or_create(self._filepath)
        self._df_data = pd.concat([self._df_data, data]).drop_duplicates(keep='first')
        self._df_data.to_csv(self._filepath, index=False, compression='gzip')

    def __len__(self):
        return len(self._df_data)
