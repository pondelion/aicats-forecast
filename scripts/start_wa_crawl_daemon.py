import time
import sys

from aicats_forecast.services.crawler.wa import (
    WACrawler, FailureExceededMaxRetries
)


crawler = WACrawler()

while True:
    try:
        crawler.start_crawl()
        while crawler.is_crawling:
            time.sleep(1)
    except FailureExceededMaxRetries:
        crawler = WACrawler()
        time.sleep(30)
    except KeyboardInterrupt:
        print('keyboard interrupted')
        crawler.stop_crawl()
        sys.exit()
