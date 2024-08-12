# Register this blueprint by adding the following line of code 
# to your entry point file.  
# app.register_functions(blueprint) 
# 
# Please refer to https://aka.ms/azure-functions-python-blueprints

import azure.functions as func
import logging

blueprint = func.Blueprint()


@blueprint.timer_trigger(schedule="0 0 5 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def reading_in_rss_and_writing_to_sql(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')