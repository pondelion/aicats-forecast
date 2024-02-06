import time
from datetime import datetime, timedelta, timezone
import sys

from aicats_forecast.services.crawler.gmo import (
    WSTradeAPIClient,
    GMOTradeCsvFile,
    ConnectionClosed,
)
from aicats_forecast.utils.print import print_red


JST = timezone(timedelta(hours=+9), 'JST')
repo = GMOTradeCsvFile(symbol='BTC_JPY')
crawler = WSTradeAPIClient(symbol='BTC_JPY', repo=repo)


def crawl(crawler):
    crawler.start_crawl()
    while crawler.is_crawling:
        time.sleep(1)
        if crawler.is_maintanance_time():
            print_red(f'[{datetime.now(JST)}] entered maintenance time.. stop crawling')
            return


while True:
    try:
        crawl(crawler=crawler)
    except ConnectionClosed:
        print_red('ConnectionClosed, witing 30 sec')
        crawler = WSTradeAPIClient(symbol='BTC_JPY', repo=repo)
        time.sleep(30)
    except KeyboardInterrupt:
        print_red('keyboard interrupted')
        crawler.stop_crawl()
        time.sleep(2)
        sys.exit()
    except Exception as e:
        print_red(f'unable to handle the error {e}, exiting')
        sys.exit()
    now_dt = datetime.now(JST)
    if (
        (now_dt.weekday() == 5)
        and
        ((now_dt.hour >= 9) and (now_dt.hour < 11))
    ):
        wait_sec = (datetime(now_dt.year, now_dt.month, now_dt.day, 11, 3, 0, tzinfo=JST) - now_dt).total_seconds()
        wait_dt = now_dt + timedelta(seconds=wait_sec)
        print_red(f'maintenance time...waining till {wait_dt} ({wait_sec} secs)...')
    else:
        wait_sec = 15 * 60
        print_red(f'unknown error_occured...waining 15 minutes...')
    time.sleep(wait_sec)
    print_red('recreating client..')
    crawler = WSTradeAPIClient(symbol='BTC_JPY', repo=repo)
