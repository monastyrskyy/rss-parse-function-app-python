import logging
import azure.functions as func

app = func.FunctionApp()

@app.schedule(schedule="0 0 3 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def rss_refresh_daily(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Start of Parallel dev in python.')