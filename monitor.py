import os, time
import subprocess
from apscheduler.schedulers.blocking import BlockingScheduler
from execute import stock
from execute import sales

SCRIPT_TO_EXECUTE = "C:\\Users\\abhi\\Desktop\\code\\smc-db\\execute.py"

def execute_script():
    try:
        subprocess.run(["pythonw", SCRIPT_TO_EXECUTE], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error executing script: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

scheduler = BlockingScheduler()
scheduler.add_job(stock, 'interval', minutes=15)
scheduler.add_job(sales(1), 'interval', minutes=15)
scheduler.add_job(stock(14), 'interval', minutes=240)
scheduler.start()