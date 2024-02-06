import json
from datetime import datetime, timezone, timedelta
import threading
import websocket

from .csv_file import GMOTradeCsvFile
from ....utils.print import print_red


class WSTradeAPIClient(object):

    def __init__(
        self,
        repo,
        symbol: str = 'BTC',
    ):
        """_summary_

        Args:
            symbol (str, optional): Any of the following.
                BTC ETH BCH LTC XRP XEM XLM XYM MONA BAT QTUM BTC_JPY ETH_JPY BCH_JPY LTC_JPY XRP_JPY.
                Defaults to 'BTC'.
        """
        self._url = 'wss://api.coin.z.com/ws/public/v1'
        # self._csv_repo = GMOTradeCsvFile(symbol)
        self._csv_repo = repo  # TODO
        self._symbol = symbol
        self._crawling = False
        self._fail_cnt = 0

        self.ws = websocket.WebSocketApp(
            self._url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )
        websocket.enableTrace(True)

    def start_crawl(self) -> None:
        self._crawling = True
        threading.Thread(
            target=self._run
        ).start()

    def stop_crawl(self) -> None:
        try:
            self.ws.close()
        finally:
            self._crawling = False

    def _run(self):
        try:
            self.ws.run_forever()
        finally:
            self._crawling = False

    def _on_message(self, ws, message):
        print_red(f'[{datetime.now()}] on_message : {message}')
        data = json.loads(message)
        #print(json.dumps(data, indent=2))
        del data['channel']
        data['timestamp_saved'] = str(datetime.now(tz=timezone.utc))
        self._csv_repo.append(
            **data
        )
        self._fail_cnt = 0

    def _on_error(self, ws, error):
        print_red(f'on_error : {str(error)}')
        try:
            self._csv_repo.flush_file()
        except Exception as e:
            print(e)
        self.stop_crawl()
        self._fail_cnt += 1
        # self._crawling = False
        # raise error

    def _on_close(self, ws, close_status_code, close_msg):
        print_red(f'on_close : {close_status_code} : {close_msg}')
        try:
            self._csv_repo.flush_file()
        except Exception as e:
            print(e)
        # self._crawling = False
        self.stop_crawl()
        # raise ConnectionClosed()
        raise ConnectionClosed()

    def _on_open(self, ws):
        print_red('on_open')
        message = {
            "command": "subscribe",
            "channel": "trades",
            "symbol": self._symbol
        }
        ws.send(json.dumps(message))

    @property
    def is_crawling(self) -> bool:
        return self._crawling

    def is_maintanance_time(self) -> bool:
        JST = timezone(timedelta(hours=+9), 'JST')
        now_dt = datetime.now(JST)
        if (
            (now_dt.weekday() == 5)
            and
            ((now_dt.hour >= 9) and (now_dt.hour < 11))
        ):
            return True
        else:
            return False


class ConnectionClosed(Exception):
    pass
