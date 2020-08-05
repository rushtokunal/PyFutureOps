import time
import os
import sys
import logging
from datetime import datetime
import requests
import pandas as pd
import concurrent.futures
from google.cloud import spanner
import pyfutureops.database.db_tables as db
import pyfutureops.database.db_operations as db_ops
import pyfutureops.thread_ops as thread_ops
requests.packages.urllib3.disable_warnings() 

#google credentials must be exported

# Get environment variables
instance_id = os.environ['INSTANCE_NAME']
database_id = os.environ['DATABASE_NAME']

logging_level = os.environ.get('LOGLEVEL', 'INFO').upper() #if not defined then set to INFO

if logging_level in ['DEBUG','INFO','WARNING','ERROR','CRITICAL']:
    logging.basicConfig(stream=sys.stdout,format='%(asctime)s - %(levelname)s - %(message)s', level=logging_level)
    logging.info("Logging level set by env variable : {}".format(logging_level))
    logging.debug("Logging level set by env variable : {}".format(logging_level))
else:
    logging.basicConfig(stream=sys.stdout,format='%(asctime)s - %(levelname)s - %(message)s', level='INFO')
    logging.info("LOGLEVEL not set in env variable or incorrect value passed")
    logging.info("Defaulting LOGLEVEL to : {}".format('INFO'))

pgm='ace-dar-on-order-load-rms-to-bq'

#the function which is threaded
def get_wiki_page_existence(wiki_page_url, timeout=10):
    response = requests.get(url=wiki_page_url, timeout=timeout,verify=False)
    time.sleep(3)
    page_status = "unknown"
    if response.status_code == 200:
        page_status = "exists"
    elif response.status_code == 404:
        page_status = "does not exist"

    return wiki_page_url
wiki_page_urls = ["https://en.wikipedia.org/wiki/" + str(i) for i in range(10)]

if __name__ == '__main__':
    try:
        #create batch tables if they do not exist already
        db.check_if_tables_already_exist(instance_id,database_id)

        logging.info("Running threaded:")
        threaded_start = time.time()
        #initializing dataframes for each changed elements
        df_defn = pd.DataFrame(columns=['RESTART_NAME',
                                        'THREAD_ID',
                                        'THREAD_VAL',
                                        'START_TIME',
                                        'PROGRAM_NAME',
                                        'PROGRAM_STATUS',
                                        'RESTART_FLAG',
                                        'RESTART_TIME',
                                        'FINISH_TIME',
                                        'ERR_MESSAGE'])
        start_time=datetime.now()
        #we are multithreading here based on the URL, this can be our departments or locations
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            threads = []
            for idx,url in enumerate(wiki_page_urls):
                threads.append([executor.submit(get_wiki_page_existence, wiki_page_url=url),url])
                thread_str=str(threads[idx][0])
                thread_id=thread_str[11:22] #get the hex value of the thread
                dict_defn = {'RESTART_NAME':pgm,
                             'THREAD_ID':thread_id,
                             'THREAD_VAL':url,
                             'START_TIME':start_time,
                             'PROGRAM_NAME':pgm,
                             'PROGRAM_STATUS':'Ready For Start',
                             'RESTART_FLAG':'Y',
                             'RESTART_TIME':None,
                             'FINISH_TIME':None,
                             'ERR_MESSAGE':None}
                df_defn = df_defn.append(dict_defn, ignore_index=True)

            logging.info(threads)
            logging.info(len(threads))
            logging.debug(df_defn)

            table_name='BATCH_FUTURE_PROGRAM_STATUS'
            db_ops.upsert_cluster_db(df_defn,table_name)
            
            for future in concurrent.futures.as_completed([row[0] for row in threads]):
                # logging.info(future)
                try:
                    future.arg=pgm #pass program name
                    future.add_done_callback(thread_ops.done)
                except Exception as exc:
                    logging.info('%r generated an exception: %s' % (url, exc))
            #check for all completed
            for future in concurrent.futures.wait([row[0] for row in threads],timeout=None, return_when='ALL_COMPLETED'):
                logging.info('all completed '+str(future))
            executor.shutdown()
        logging.info("Threaded time: {}".format(time.time() - threaded_start))
    except Exception as exc:
        logging.info('generated an exception in main: %s',exc)