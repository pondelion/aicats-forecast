import time

from aicats_forecast.services.crawler.wa import (
    WACrawler
)


crawler = WACrawler()

try:
    crawler.start_crawl()
    while crawler.is_crawling:
        time.sleep(1)
except KeyboardInterrupt:
    print('keyboard interrupted')
    crawler.stop_crawl()
