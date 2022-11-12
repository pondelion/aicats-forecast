import time

from aicats_forecast.bots.m0001.simulated_bot import SimulatedBot2


sb = SimulatedBot2()
# sb.take_action()

sb.start_bot_daemon()
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print('keyboard interrupted')
    sb.stop_bot_daemon()
