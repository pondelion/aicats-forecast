from datetime import datetime, timedelta
import pickle
import os
import warnings
warnings.simplefilter('ignore')

import pandas as pd
from fin_app_models.feature.creation.single_ts import create_single_ts_features

from .dataset import DataSet


class FeatureExtractor:

    def __init__(
        self,
    ):
        self._dataset = DataSet()
        self._use_symbols = ['btc', 'usdt', 'eth', 'usdc', 'xrp']
        self._feats_base_cols = ['amount_btc', 'amount_usdt', 'amount_eth', 'amount_usdc', 'amount_xrp']
        self._nan_check_cols = [
            f'trans_amount_{sym}_amount_{sym}' for sym in self._use_symbols
        ]
        base_dir = os.path.dirname(os.path.abspath(__file__))
        self._x_scaler = pickle.load(
            open(os.path.join(base_dir, 'x_scaler.pkl'), 'rb')
        )
        self._feat_names = pickle.load(
            open(os.path.join(base_dir, 'feat_names.pkl'), 'rb')
        )
        self._feat_time_scale = 5

    def extract(self):
        df = self._dataset.df
        now = datetime.now()
        df_trans_5min = pd.DataFrame(index=pd.date_range(now - timedelta(days=2), now, freq='5T').floor('5T'))
        base_datetime = df_trans_5min.index[-2]
        min_datetime = df_trans_5min.index.min()
        print(base_datetime)
        for symbol in self._use_symbols:
            df_trans_symbol_5min = df[df['symbol']==symbol][['amount']].resample('5T').sum()
            print(f'{symbol} : {df_trans_symbol_5min.index.min()} : {df_trans_symbol_5min.index.max()} : {len(df_trans_symbol_5min)}')
            assert df_trans_symbol_5min.index.min() < min_datetime
            df_trans_5min = pd.merge(
                df_trans_5min, df_trans_symbol_5min.rename(columns={'amount': f'amount_{symbol}'})[[f'amount_{symbol}']],
                # how='inner',
                how='left',
                left_index=True, right_index=True,
                # suffixes=['', f'_{symbol}'],
            )

        feat_dfs = []
        scale = self._feat_time_scale
        for col in self._feats_base_cols:
            df_ = create_single_ts_features(
                df_trans_5min[col],
                macd_fastperiod=scale*12,
                macd_slowperiod=scale*26,
                macd_signalperiod=scale*9,
                bb_periods=[scale*7, scale*20, scale*30, scale*60],
                basic_stats_period=scale*14,
                atr_period=scale*14,
                return_lags=[scale*1, scale*3, scale*7, scale*10, scale*20, scale*30, scale*60],
                col_name_prefix=f'trans_{col}',
                include_deviation=False,
            )
            feat_dfs.append(df_)

        df_feats = pd.concat(
            feat_dfs,
            axis=1
        )

        # df_feats.dropna(inplace=True)

        if all(df_feats.loc[base_datetime, self._nan_check_cols].isnull()):
            # 切り捨て後最新10分前~5分前の間に全シンボルのデータが１つも無しのためエラーの可能性高い
            raise NoDataFoundInLatest5Min()

        df_feats = df_feats.clip(-100000000000000, 100000000000000)

        feats_scaled = self._x_scaler.transform(df_feats[self._feat_names])
        df_feats_scaled = pd.DataFrame(
            data=feats_scaled,
            columns=df_feats.columns,
            index=df_feats.index
        )

        return df_feats_scaled.loc[base_datetime].to_frame().T.fillna(0)[self._feat_names]


class NoDataFoundInLatest5Min(Exception):
    pass
