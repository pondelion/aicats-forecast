from typing import List

import pandas as pd
from fin_app_models.feature.creation.single_ts import create_single_ts_features
from fin_app_models.feature.selection.random_selection import random_feat_select


def create_generic_ts_features(
    df_buy: pd.DataFrame,
    df_sell: pd.DataFrame,
    feat_bases: List[str] = ['price_buy', 'price_sell'],  # ['price_buy', 'price_sell', 'spread', 'size_buy', 'size_sell']
    scale: int = 10,
    include_deviation: bool = False,
) -> pd.DataFrame:
    feat_dfs = []
    if 'price_buy' in feat_bases:
        df_feats_buy = create_single_ts_features(
            df_buy['price_buy'],
            macd_fastperiod=scale*12,
            macd_slowperiod=scale*26,
            macd_signalperiod=scale*9,
            bb_periods=[scale*7, scale*20, scale*30, scale*60],
            basic_stats_period=scale*14,
            atr_period=scale*14,
            return_lags=[scale*1, scale*3, scale*7, scale*10, scale*20, scale*30, scale*60],
            col_name_prefix='buy',
            include_deviation=include_deviation,
        )
        feat_dfs.append(df_feats_buy)
    if 'price_sell' in feat_bases:
        df_feats_sell = create_single_ts_features(
            df_sell['price_sell'],
            macd_fastperiod=scale*12,
            macd_slowperiod=scale*26,
            macd_signalperiod=scale*9,
            bb_periods=[scale*7, scale*20, scale*30, scale*60],
            basic_stats_period=scale*14,
            atr_period=scale*14,
            return_lags=[scale*1, scale*3, scale*7, scale*10, scale*20, scale*30, scale*60],
            col_name_prefix='sell',
            include_deviation=include_deviation,
        )
        feat_dfs.append(df_feats_sell)
    if 'spread' in feat_bases:
        df_feats_spread = create_single_ts_features(
            df_sell['price_sell']-df_buy['price_buy'],
            macd_fastperiod=scale*12,
            macd_slowperiod=scale*26,
            macd_signalperiod=scale*9,
            bb_periods=[scale*7, scale*20, scale*30, scale*60],
            basic_stats_period=scale*14,
            atr_period=scale*14,
            return_lags=[scale*1, scale*3, scale*7, scale*10, scale*20, scale*30, scale*60],
            col_name_prefix='spread',
        )
        feat_dfs.append(df_feats_spread)
    df_feats = pd.concat(feat_dfs, axis=1)

    df_feats.dropna(inplace=True)
    df_feats = df_feats.clip(-100000000000000, 100000000000000)

    return df_feats
