from datetime import datetime, timedelta
from threading import Thread
from time import sleep
import traceback
import uuid
import functools

import schedule
from aicats_forecast.bots.m0001.preprocess import FeatureExtractor
from aicats_forecast.bots.m0001.model import Model
from aicats_forecast.bots.m0001.postprocess import PostProcess
from aicats_forecast.crawler.gmo_client import GMOClient

from ...utils import Logger


class SimulatedBot:

    def __init__(
        self,
        initial_cash: int = 500000,
        thresh: float = 0.2,
    ):
        self._extractor = FeatureExtractor()
        self._model = Model()
        self._post_process = PostProcess()
        self._cash = initial_cash
        self._thresh = thresh
        self._gmo_client = GMOClient()
        self._btc = 0.0
        self._commission = 0
        self._bot_running = False
        self._last_portfolio = ''

    def start_bot_daemon(self) -> None:
        Thread(
            target=self._start_bot_daemon,
        ).start()

    def _start_bot_daemon(self) -> None:
        schedule.every(5).minutes.do(self.take_action)
        print(f'scheduled bot task every 5 minutes')
        self._bot_running = True
        while self._bot_running:
            schedule.run_pending()
            sleep(1)

    def stop_bot_daemon(self):
        self._bot_running = False

    def take_action(self):
        try:
            df_feat = self._extractor.extract()
            pred = self._model.predict(df_feat)
            pred_value = pred[0]
            Logger.d('SimulatedBot.take_action', f'pred : {pred}')
            pred_return = self._post_process.post_process(pred.reshape(1, -1))
            Logger.d('SimulatedBot.take_action', f'pred_return : {pred_return}')
            BASE_SIZE_SCALE = 0.1
            if pred_value >= self._thresh:
                self.buy(size=BASE_SIZE_SCALE*pred_return)
            elif pred_value <= -self._thresh:
                self.sell(size=BASE_SIZE_SCALE*abs(pred_return))
            else:
                Logger.d('SimulatedBot.take_action', f'pred value = {pred_value}, doing nothing')
            self.dump_current_portfolio()
        except Exception as e:
            Logger.d('SimulatedBot.take_action', str(e))
            Logger.d('SimulatedBot3.take_action', traceback.format_exc())
            Logger.d('SimulatedBot.take_action', self._last_portfolio)

    def buy(self, size: float):
        ticker = self._gmo_client.ticker()
        Logger.d('SimulatedBot.buy', ticker)
        ask = int(ticker['ask'])
        if ask*size > self._cash:
            Logger.d('SimulatedBot.buy', 'not enough cash to buy')
            return
        self._btc += size
        self._cash -= ask*size
        Logger.d('SimulatedBot.buy', f'bought btc : size={size}')

    def sell(self, size: float):
        size = min(size, self._btc)
        if size == 0:
            Logger.d('SimulatedBot.sell', 'not enough BTC to sell')
            return
        ticker = self._gmo_client.ticker()
        Logger.d('SimulatedBot.sell', ticker)
        bid = int(ticker['bid'])
        self._btc -= size
        self._cash += bid*size
        Logger.d('SimulatedBot.sell', f'sold btc : size={size}')

    def total_evaluated_cash(self):
        ticker = self._gmo_client.ticker()
        Logger.d('SimulatedBot.total_evaluated_cash', ticker)
        bid = int(ticker['bid'])
        return self._cash + bid*self._btc

    def dump_current_portfolio(self):
        self._last_portfolio = f'[PORTFOLIO] cash : {self._cash}, btc : {self._btc}, total_evaluated_cash : {self.total_evaluated_cash()}'
        Logger.d('SimulatedBot.dump_current_portfolio', self._last_portfolio)


class SimulatedBot2:

    def __init__(
        self,
        initial_cash: int = 500000,
        thresh: float = 0.2,
    ):
        self._extractor = FeatureExtractor()
        self._model = Model()
        self._post_process = PostProcess()
        self._cash = initial_cash
        self._thresh = thresh
        self._gmo_client = GMOClient()
        self._btc = 0.0
        self._commission = 0
        self._bot_running = False
        self._long_positions = {}
        self._short_positions = {}
        self._MAX_POSITIONS = 5
        self._last_portfolio = ''

    def start_bot_daemon(self) -> None:
        Thread(
            target=self._start_bot_daemon,
        ).start()

    def _start_bot_daemon(self) -> None:
        schedule.every(5).minutes.do(self.take_action)
        print(f'scheduled bot task every 5 minutes')
        self._bot_running = True
        while self._bot_running:
            schedule.run_pending()
            sleep(1)

    def stop_bot_daemon(self):
        self._bot_running = False

    def take_action(self):
        try:
            df_feat = self._extractor.extract()
            pred = self._model.predict(df_feat)
            pred_value = pred[0]
            Logger.d('SimulatedBot2.take_action', f'pred : {pred}')
            pred_return = self._post_process.post_process(pred.reshape(1, -1))
            Logger.d('SimulatedBot2.take_action', f'pred_return : {pred_return}')
            BASE_SIZE_SCALE = 0.1
            if pred_value >= self._thresh:
                if len(self._long_positions) >= self._MAX_POSITIONS:
                    Logger.d('SimulatedBot2.take_action', f'reached max long positions, doing nothing')
                else:
                    size = self.buy(size=BASE_SIZE_SCALE*pred_return)
                    if size:
                        position_id = uuid.uuid4()
                        self._long_positions[position_id] = {'size': size}
                        # schedule closing task
                        target_dt = df_feat.index[0] + timedelta(hours=48)
                        schedule.every(
                            int((target_dt-datetime.now()).total_seconds())
                        ).seconds.do(
                            functools.partial(self.close_long, position_id)
                        )
                        Logger.d('SimulatedBot2.take_action', f'scheduled long position closing task at {target_dt}')
            elif pred_value <= -self._thresh:
                if len(self._short_positions) >= self._MAX_POSITIONS:
                    Logger.d('SimulatedBot2.take_action', f'reached short long positions, doing nothing')
                else:
                    size = self.sell(size=BASE_SIZE_SCALE*abs(pred_return))
                    if size:
                        position_id = uuid.uuid4()
                        self._short_positions[position_id] = {'size': size}
                        # schedule closing task
                        target_dt = df_feat.index[0] + timedelta(hours=48)
                        schedule.every(
                            int((target_dt-datetime.now()).total_seconds())
                        ).seconds.do(
                            functools.partial(self.close_short, position_id)
                        )
                        Logger.d('SimulatedBot2.take_action', f'scheduled short position closing task at {target_dt}')
            else:
                Logger.d('SimulatedBot2.take_action', f'pred value = {pred_value}, doing nothing')
            self.dump_current_portfolio()
        except Exception as e:
            Logger.d('SimulatedBot2.take_action', str(e))
            Logger.d('SimulatedBot3.take_action', traceback.format_exc())
            Logger.d('SimulatedBot2.take_action', self._last_portfolio)

    def buy(self, size: float):
        ticker = self._gmo_client.ticker()
        Logger.d('SimulatedBot2.buy', ticker)
        ask = int(ticker['ask'])
        if ask*size > self._cash:
            Logger.d('SimulatedBot2.buy', 'not enough cash to buy')
            return None
        self._btc += size
        self._cash -= ask*size
        Logger.d('SimulatedBot2.buy', f'bought btc : size={size}')
        return size

    def sell(self, size: float):
        size = min(size, self._btc)
        if size == 0:
            Logger.d('SimulatedBot2.sell', 'not enough BTC to sell')
            return None
        ticker = self._gmo_client.ticker()
        Logger.d('SimulatedBot2.sell', ticker)
        bid = int(ticker['bid'])
        self._btc -= size
        self._cash += bid*size
        Logger.d('SimulatedBot2.sell', f'sold btc : size={size}')
        return size

    def close_long(self, position_id: int):
        size = self._long_positions[position_id]['size']
        self.sell(size=size)
        self.dump_current_portfolio()
        del self._long_positions[position_id]
        return schedule.CancelJob

    def close_short(self, position_id: int):
        size = self._short_positions[position_id]['size']
        self.buy(size=size)
        self.dump_current_portfolio()
        del self._short_positions[position_id]
        return schedule.CancelJob

    def total_evaluated_cash(self):
        ticker = self._gmo_client.ticker()
        Logger.d('SimulatedBot2.total_evaluated_cash', ticker)
        bid = int(ticker['bid'])
        return self._cash + bid*self._btc

    def dump_current_portfolio(self):
        self._last_portfolio = f'[PORTFOLIO] cash : {self._cash}, btc : {self._btc}, total_evaluated_cash : {self.total_evaluated_cash()}'
        Logger.d('SimulatedBot2.dump_current_portfolio', self._last_portfolio)


class SimulatedBot3:

    def __init__(
        self,
        initial_cash: int = 500000,
        initial_btc: float = 0.0,
        thresh: float = 0.2,
    ):
        self._extractor = FeatureExtractor()
        self._model = Model()
        self._post_process = PostProcess()
        self._cash = initial_cash
        self._thresh = thresh
        self._gmo_client = GMOClient()
        self._btc = initial_btc
        self._commission = 0
        self._bot_running = False
        self._long_positions = {}
        self._short_positions = {}
        self._MAX_POSITIONS = 8
        self._last_portfolio = ''
        self._n_pred_up = 0
        self._n_pred_down = 0

    def start_bot_daemon(self) -> None:
        Thread(
            target=self._start_bot_daemon,
        ).start()

    def _start_bot_daemon(self) -> None:
        schedule.every(5).minutes.do(self.take_action)
        print(f'scheduled bot task every 5 minutes')
        self._bot_running = True
        while self._bot_running:
            schedule.run_pending()
            sleep(1)

    def stop_bot_daemon(self):
        self._bot_running = False

    def take_action(self):
        try:
            df_feat = self._extractor.extract()
            pred = self._model.predict(df_feat)
            pred_value = pred[0]
            Logger.d('SimulatedBot3.take_action', f'pred : {pred}')
            pred_return = self._post_process.post_process(pred.reshape(1, -1))
            Logger.d('SimulatedBot3.take_action', f'pred_return : {pred_return}')
            BASE_SIZE_SCALE = 0.1
            if pred_value >= self._thresh:
                self._n_pred_up += 1
                self._n_pred_down = 0
                if len(self._long_positions) >= self._MAX_POSITIONS:
                    Logger.d('SimulatedBot3.take_action', f'reached max long positions, doing nothing')
                elif self._n_pred_up >= 6:
                    Logger.d('SimulatedBot3.take_action', f'{self._n_pred_up} successive up prediction')
                    size = self.buy(size=BASE_SIZE_SCALE*pred_return)
                    if size:
                        position_id = uuid.uuid4()
                        self._long_positions[position_id] = {'size': size}
                        # schedule closing task
                        target_dt = df_feat.index[0] + timedelta(hours=48)
                        schedule.every(
                            int((target_dt-datetime.now()).total_seconds())
                        ).seconds.do(
                            functools.partial(self.close_long, position_id)
                        )
                        Logger.d('SimulatedBot3.take_action', f'scheduled long position closing task at {target_dt}')
                        Logger.d('SimulatedBot3.take_action', f'current short positions : {self._short_positions}')
                        self._n_pred_up = 0
                else:
                    Logger.d('SimulatedBot3.take_action', f'n_pred_up = {self._n_pred_up}, doing nothing')
            elif pred_value <= -self._thresh:
                self._n_pred_down += 1
                self._n_pred_up = 0
                if len(self._short_positions) >= self._MAX_POSITIONS:
                    Logger.d('SimulatedBot3.take_action', f'reached short long positions, doing nothing')
                elif self._n_pred_down >= 6:
                    Logger.d('SimulatedBot3.take_action', f'{self._n_pred_down} successive down prediction')
                    size = self.sell(size=BASE_SIZE_SCALE*abs(pred_return))
                    if size:
                        position_id = uuid.uuid4()
                        self._short_positions[position_id] = {'size': size}
                        # schedule closing task
                        target_dt = df_feat.index[0] + timedelta(hours=48)
                        schedule.every(
                            int((target_dt-datetime.now()).total_seconds())
                        ).seconds.do(
                            functools.partial(self.close_short, position_id)
                        )
                        Logger.d('SimulatedBot3.take_action', f'scheduled short position closing task at {target_dt}')
                        Logger.d('SimulatedBot3.take_action', f'current long positions : {self._long_positions}')
                        self._n_pred_down = 0
                else:
                    Logger.d('SimulatedBot3.take_action', f'n_pred_dwon = {self._n_pred_down}, doing nothing')
            else:
                self._n_pred_up = 0
                self._n_pred_down = 0
                Logger.d('SimulatedBot3.take_action', f'pred value = {pred_value}, doing nothing')
            self.dump_current_portfolio()
        except Exception as e:
            Logger.d('SimulatedBot3.take_action', str(e))
            Logger.d('SimulatedBot3.take_action', traceback.format_exc())
            Logger.d('SimulatedBot3.take_action', self._last_portfolio)

    def buy(self, size: float):
        ticker = self._gmo_client.ticker()
        Logger.d('SimulatedBot3.buy', ticker)
        ask = int(ticker['ask'])
        if ask*size > self._cash:
            Logger.d('SimulatedBot3.buy', 'not enough cash to buy')
            return None
        self._btc += size
        self._cash -= ask*size
        Logger.d('SimulatedBot3.buy', f'bought btc : size={size}')
        return size

    def sell(self, size: float):
        size = min(size, self._btc)
        if size == 0:
            Logger.d('SimulatedBot3.sell', 'not enough BTC to sell')
            return None
        ticker = self._gmo_client.ticker()
        Logger.d('SimulatedBot3.sell', ticker)
        bid = int(ticker['bid'])
        self._btc -= size
        self._cash += bid*size
        Logger.d('SimulatedBot3.sell', f'sold btc : size={size}')
        return size

    def close_long(self, position_id: int):
        try:
            size = self._long_positions[position_id]['size']
            self.sell(size=size)
            self.dump_current_portfolio()
            del self._long_positions[position_id]
        except Exception as e:
            print(e)
        return schedule.CancelJob

    def close_short(self, position_id: int):
        try:
            size = self._short_positions[position_id]['size']
            self.buy(size=size)
            self.dump_current_portfolio()
            del self._short_positions[position_id]
        except Exception as e:
            print(e)
        return schedule.CancelJob

    def total_evaluated_cash(self):
        ticker = self._gmo_client.ticker()
        Logger.d('SimulatedBot3.total_evaluated_cash', ticker)
        bid = int(ticker['bid'])
        return self._cash + bid*self._btc

    def dump_current_portfolio(self):
        self._last_portfolio = f'[PORTFOLIO] cash : {self._cash}, btc : {self._btc}, total_evaluated_cash : {self.total_evaluated_cash()}'
        Logger.d('SimulatedBot3.dump_current_portfolio', self._last_portfolio)
