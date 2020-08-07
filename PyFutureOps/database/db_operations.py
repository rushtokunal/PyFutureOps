import os
from google.cloud import spanner
from google.cloud.spanner_v1 import param_types
from google.cloud.spanner import Client
from google.cloud.spanner import TransactionPingingPool
import pyfutureops.database.db_tables as dbt
import pyfutureops.config
import logging

# Get environment variables
INSTANCE_NAME = os.environ['INSTANCE_NAME']
DATABASE_NAME = os.environ['DATABASE_NAME']
PROJECT_ID = os.environ['PROJECT_ID_SPANNER']

client = spanner.Client(project=PROJECT_ID)
instance = client.instance(INSTANCE_NAME)
pool = TransactionPingingPool(size=10, default_timeout=5, ping_interval=300)
database = instance.database(DATABASE_NAME)

import threading

#Set up a background thread to ping the pool's session, keeping them from becoming stale, 
#and ensuring that each session has a new transaction started before it is used:
background = threading.Thread(target=pool.ping, name='ping-pool')
background.daemon = True
background.start()

def upsert_cluster_db(df_defn,table_name):
    
    columns = df_defn.columns
    values = df_defn.values.tolist()
    
    def unit_of_work(transaction):
        transaction.insert_or_update(
            table=table_name,
            columns=columns,
            values=values)
    try:        
        database.run_in_transaction(unit_of_work)
    except Exception as exc:    
        logging.error("Exception occured while upserting in : {} with exception {}".format(table_name,exc))
    else:
        logging.info("upserted data to  : {}".format(table_name))

def preprocess(pgm):
    try:
        logging.info(pgm)
        dbt.check_if_tables_already_exist(INSTANCE_NAME,DATABASE_NAME,PROJECT_ID)
        #if previously successful threads are present then move them to history table
        def execute_spannel_transaction_dml(transaction):
            insert_dml = """ INSERT INTO batch_future_program_hist
                  (restart_name,
                   thread_id, 
	               thread_val, 
	               start_time, 
	               program_name, 
                   program_status, 
	               restart_time, 
	               finish_time,
                   last_upd_tmst)
                  SELECT bfps.restart_name,
                         bfps.thread_id,
                         bfps.thread_val,
                         bfps.start_time,
                         bfps.program_name,
                         bfps.program_status,
                         bfps.restart_time,
                         bfps.finish_time,
                         CURRENT_TIMESTAMP()
                    FROM batch_future_program_status bfps
                   WHERE bfps.program_name = @program_name
                     AND bfps.program_status ='finished'
                     AND NOT EXISTS( SELECT 1
                                       FROM batch_future_program_status bf
                                      WHERE bf.program_name=bfps.program_name
                                        AND bf.program_status <>'finished'
                     )"""

            delete_dml = """ DELETE from batch_future_program_status
                         WHERE program_name
                         IN(
                            SELECT program_name
                              FROM batch_future_program_status bfps
                             WHERE program_name = @program_name
                               AND program_status ='finished'
                               AND NOT EXISTS( SELECT 1
                                                 FROM batch_future_program_status bf
                                                WHERE bf.program_name=bfps.program_name
                                                  AND bf.program_status <>'finished')
                        )"""

            row_ct = transaction.execute_update(
                        insert_dml,
                        params={"program_name": pgm},
                        param_types={"program_name": spanner.param_types.STRING}
                        )
            logging.info("{} previous successfully finished threads for program {} inserted into table batch_future_program_hist".format(row_ct,pgm))
            row_ct = transaction.execute_update(
                        delete_dml,
                        params={"program_name": pgm},
                        param_types={"program_name": spanner.param_types.STRING}
                        )
            logging.info("{} previous successfully finished threads deleted for program {} from into table batch_future_program_status".format(row_ct,pgm))
        database.run_in_transaction(execute_spannel_transaction_dml)
    except Exception as exc:    
        logging.error("{} Exception occured while preprocessing in pyfutureops {}".format(pgm,exc))
    