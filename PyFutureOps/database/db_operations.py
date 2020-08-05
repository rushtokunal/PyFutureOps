import os
from google.cloud import spanner
from google.cloud.spanner_v1 import param_types
from google.cloud.spanner import Client
from google.cloud.spanner import TransactionPingingPool
import pyfutureops.config
import logging

# Get environment variables
INSTANCE_NAME = os.environ['INSTANCE_NAME']
DATABASE_NAME = os.environ['DATABASE_NAME']
# INSTANCE_NAME = '<Spanner Instance Name>'
# DATABASE_NAME = '<Spanner Database Name>'

client = Client()
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
