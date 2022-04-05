import time

from aicats_forecast.services.crawler.gmo import (
    WSTradeAPIClient,
    GMOTradeCsvFile
)


repo = GMOTradeCsvFile(symbol='BTC_JPY')
crawler = WSTradeAPIClient(symbol='BTC_JPY', repo=repo)


try:
    crawler.start_crawl()
    while crawler.is_crawling:
        time.sleep(1)
except KeyboardInterrupt:
    print('keyboard interrupted')
    crawler.stop_crawl()
