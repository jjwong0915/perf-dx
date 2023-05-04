import datetime
import psutil
import sched
import sys
import time

process = psutil.Process(int(sys.argv[1]))
scheduler = sched.scheduler(time.time, time.sleep)

def print_metric():
    scheduler.enter(1, 0, print_metric)
    print(datetime.datetime.now().isoformat(), process.cpu_percent(), process.memory_info().vms)

process.cpu_percent()
scheduler.enter(0, 0, print_metric)
scheduler.run()

