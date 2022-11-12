import time

import schedule


def oneshot_task():
    print('oneshot_task')
    return schedule.CancelJob


def task():
    print('task')


schedule.every(5).seconds.do(task)
schedule.every(13).seconds.do(oneshot_task)

while True:
    schedule.run_pending()
    time.sleep(1)