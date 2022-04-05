from datetime import timedelta
import time

from aicats_forecast.services.preprocess.agg import (
    ConstantTimeIntervalAggService,
)
from aicats_forecast.services.crawler.gmo import (
    GMOTradeCsvFile,
)


repo = GMOTradeCsvFile(symbol='BTC_JPY')
agg_service = ConstantTimeIntervalAggService(
    symbol='BTC_JPY',
    csv_file_repo=repo,
    process_schedule_interval=timedelta(minutes=1),
)
agg_service.start_agg_daemon()
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print('keyboard interrupted')
    agg_service.stop_agg_daemon()
