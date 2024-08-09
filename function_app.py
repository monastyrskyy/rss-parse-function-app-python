import logging
import azure.functions as func

app = func.FunctionApp()

@app.schedule(schedule="0 0 3 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def rss_refresh_daily(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Start of Parallel dev in python. Hello.')

@app.timer_trigger(schedule="0 2/20 * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def mp3_download(myTimer: func.TimerRequest) -> None:
    
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Second func, just straight up in line?')

@app.timer_trigger(schedule="0 0 5 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def reading_in_rss_and_writing_to_sql(myTimer: func.TimerRequest) -> None:
    
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')