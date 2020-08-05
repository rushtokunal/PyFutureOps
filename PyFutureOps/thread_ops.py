
import pyfutureops.database.db_operations as db_ops
from datetime import datetime
import pandas as pd
import pyfutureops.config
import logging

table_name='BATCH_FUTURE_PROGRAM_STATUS'
def done(fn):
    finish_time=datetime.now()
    thread_str=str(fn)
    thread_id=thread_str[11:22]
    program_name=fn.arg
    logging.info('testing '+thread_id+'~'+program_name)
    result = fn.result() #thread value
    df_defn = pd.DataFrame(columns=['RESTART_NAME',
                                    'THREAD_ID',
                                    'PROGRAM_NAME',
                                    'PROGRAM_STATUS',
                                    'RESTART_FLAG',
                                    'RESTART_TIME',
                                    'FINISH_TIME',
                                    'ERR_MESSAGE'])
    if fn.cancelled():
        dict_defn = {'RESTART_NAME':program_name,
                     'THREAD_ID':thread_id,
                     'PROGRAM_NAME':program_name,
                     'PROGRAM_STATUS':'cancelled',
                     'RESTART_FLAG':'Y',
                     'RESTART_TIME':None,
                     'FINISH_TIME':finish_time,
                     'ERR_MESSAGE':None}
        df_defn = df_defn.append(dict_defn, ignore_index=True)
        db_ops.upsert_cluster_db(df_defn,table_name)
        logging.info('{}: canceled'.format(fn.arg))
    elif fn.done():
        error = fn.exception()
        if error:
            dict_defn = {'RESTART_NAME':program_name,
                         'THREAD_ID':thread_id,
                         'PROGRAM_NAME':program_name,
                         'PROGRAM_STATUS':'aborted',
                         'RESTART_FLAG':'Y',
                         'RESTART_TIME':None,
                         'FINISH_TIME':finish_time,
                         'ERR_MESSAGE':error}
            df_defn = df_defn.append(dict_defn, ignore_index=True)
            db_ops.upsert_cluster_db(df_defn,table_name)
            logging.info('{}: error returned: {}'.format(fn.arg, error))
        else:
            # result = fn.result()
            dict_defn = {'RESTART_NAME':program_name,
                         'THREAD_ID':thread_id,
                         'PROGRAM_NAME':program_name,
                         'PROGRAM_STATUS':'finished',
                         'RESTART_FLAG':'Y',
                         'RESTART_TIME':None,
                         'FINISH_TIME':finish_time,
                         'ERR_MESSAGE':None}
            df_defn = df_defn.append(dict_defn, ignore_index=True)
            db_ops.upsert_cluster_db(df_defn,table_name)
            logging.debug(df_defn)
            logging.info('{}: value returned: {}'.format(fn.arg, result))
