#!/usr/bin/env python
"""This file creates spanner tables for storing multiprocessing and multithreading bathes
"""
import argparse
import base64
import datetime
from google.cloud import spanner
from google.cloud.spanner_v1.proto import type_pb2
import pyfutureops.config
import logging
import os

# Get environment variables
INSTANCE_NAME = os.environ['INSTANCE_NAME']
DATABASE_NAME = os.environ['DATABASE_NAME']
PROJECT_ID = os.environ['PROJECT_ID_SPANNER']

client = spanner.Client(project=PROJECT_ID)
instance = client.instance(INSTANCE_NAME)
database = instance.database(DATABASE_NAME)

master_tables=['BATCH_FUTURE_CONTROL',
               'BATCH_FUTURE_BOOKMARK',
               'BATCH_FUTURE_PROGRAM_STATUS',
               'BATCH_FUTURE_PROGRAM_HIST'
               ]
#ddl statements, putting in the same file as unable to read for .sql files
BATCH_FUTURE_BOOKMARK="""CREATE TABLE BATCH_FUTURE_BOOKMARK
   (RESTART_NAME STRING(1024),
    THREAD_ID STRING(1024), 
	THREAD_VAL STRING(1024), 
	BOOKMARK_STRING STRING(1024), 
	APPLICATION_IMAGE STRING(1024), 
	OUT_FILE_STRING STRING(1024), 
	NON_FATAL_ERR_FLAG STRING(1024), 
	NUM_COMMITS INT64, 
	AVG_TIME_BTWN_COMMITS INT64
   ) PRIMARY KEY (RESTART_NAME,THREAD_ID,THREAD_VAL)"""
BATCH_FUTURE_CONTROL="""CREATE TABLE BATCH_FUTURE_CONTROL
        (   PROGRAM_NAME STRING(1024), 
            PROGRAM_DESC STRING(1024), 
            DRIVER_NAME STRING(1024), 
            NUM_THREADS INT64, 
            UPDATE_ALLOWED STRING(1024), 
            PROCESS_FLAG STRING(1), 
            COMMIT_MAX_CTR INT64, 
            LOCK_WAIT_TIME INT64, 
            RETRY_MAX_CTR INT64
        ) PRIMARY KEY (PROGRAM_NAME,DRIVER_NAME)"""
BATCH_FUTURE_PROGRAM_STATUS="""CREATE TABLE BATCH_FUTURE_PROGRAM_STATUS
   (RESTART_NAME STRING(1024),
    THREAD_ID STRING(1024), 
	THREAD_VAL STRING(1024), 
	START_TIME TIMESTAMP, 
	PROGRAM_NAME STRING(1024), 
	PROGRAM_STATUS STRING(1024), 
	RESTART_FLAG STRING(1), 
	RESTART_TIME TIMESTAMP, 
	FINISH_TIME TIMESTAMP, 
	CURRENT_PID INT64, 
	CURRENT_OPERATOR_ID STRING(1024), 
	ERR_MESSAGE STRING(1024)
   ) PRIMARY KEY (RESTART_NAME,THREAD_ID)"""
BATCH_FUTURE_PROGRAM_HIST="""CREATE TABLE BATCH_FUTURE_PROGRAM_HIST
   (RESTART_NAME STRING(1024), 
    THREAD_ID STRING(1024), 
	THREAD_VAL STRING(1024), 
	START_TIME TIMESTAMP, 
	PROGRAM_NAME STRING(1024), 
    PROGRAM_STATUS STRING(1024),
	NUM_THREADS INT64, 
	COMMIT_MAX_CTR INT64, 
	RESTART_TIME TIMESTAMP, 
	FINISH_TIME TIMESTAMP,
    LAST_UPD_TMST TIMESTAMP,
	SUCCESS_FLAG STRING(1024), 
	NON_FATAL_ERR_FLAG STRING(1024), 
	NUM_COMMITS INT64, 
	AVG_TIME_BTWN_COMMITS INT64,
	COMMITS INT64
   )PRIMARY KEY (RESTART_NAME,THREAD_ID,THREAD_VAL)
   """
# [START spanner_query_data_with_new_column]
def check_if_tables_already_exist(instance_id, database_id, proj_id):
    with database.snapshot() as snapshot:
        tables=[]
        results = snapshot.execute_sql(
            """SELECT upper(table_name)
                 FROM information_schema.tables
                WHERE table_name IN('BATCH_FUTURE_CONTROL',
                                    'BATCH_FUTURE_BOOKMARK',
                                    'BATCH_FUTURE_PROGRAM_STATUS',
                                    'BATCH_FUTURE_PROGRAM_HIST'
                                    )"""
        )

        for row in results:
            #get the value of each element of the result list
            tables.append(row[0])
        if len(tables)>0:
            logging.info('existing batch tables in the database are')
            logging.info(tables)
            #create if any tables are missing
            missing_tables=list(set(master_tables)-set(tables))
            if missing_tables:
                logging.info('missing batch tables in the database are')
                logging.info(missing_tables)
                #create the mssing tables
                for tab_name in missing_tables:
                    logging.info(tab_name)
                    create_batch_tables(instance_id, database_id, tab_name)
            else:
                logging.info('all batch tables are present in the database')
        else:
            """all tables are missing so create all of them"""
            logging.info("all batch future tables are missing so we are going to create all of them")
            for tab_name in master_tables:
                    logging.info('Starting to create table {}'.format(tab_name))
                    create_batch_tables(instance_id, database_id, tab_name)

class MyStr(str):
    """ Special string subclass to override the default representation method
        which puts single quotes around the result.
    """
    def __repr__(self):
        return super(MyStr, self).__repr__().strip("'")
# [START spanner_create_database]
def create_batch_tables(instance_id, database_id, table_name):
    """Creates a database and tables for sample data."""
    # logging.info(table_name.lower())
    # ddl="test"
    # if table_name:
    #     logging.info('inside the if vlock')
    #     file_name=table_name.lower()+'.sql'
    #     logging.info(file_name)
    #     fd = open(file_name, 'r', encoding='utf-8')     # Open and read the file as a single buffer
    #     ddl = fd.read().replace('\n',' ')
    #     fd.close()
    #     logging.info(repr(ddl))
    #     ddl = ddl.replace("'", "")
    #     ddl=MyStr(ddl)
    #     logging.info(repr(ddl))
    if table_name=='BATCH_FUTURE_BOOKMARK':
        operation = database.update_ddl([BATCH_FUTURE_BOOKMARK])
        logging.info('Waiting for operation to complete...')
        operation.result(120)
        logging.info('Created table {} in database {} on instance {}'.format(
        table_name,database_id, instance_id))
    elif table_name=='BATCH_FUTURE_CONTROL':
        operation = database.update_ddl([BATCH_FUTURE_CONTROL])
        logging.info('Waiting for operation to complete...')
        operation.result(120)
        logging.info('Created table {} in database {} on instance {}'.format(
        table_name,database_id, instance_id))
    elif table_name=='BATCH_FUTURE_PROGRAM_STATUS':
        operation = database.update_ddl([BATCH_FUTURE_PROGRAM_STATUS])
        logging.info('Waiting for operation to complete...')
        operation.result(120)
        logging.info('Created table {} in database {} on instance {}'.format(
        table_name,database_id, instance_id))
    elif table_name=='BATCH_FUTURE_PROGRAM_HIST':
        operation = database.update_ddl([BATCH_FUTURE_PROGRAM_HIST])
        logging.info('Waiting for operation to complete...')
        operation.result(120)
        logging.info('Created table {} in database {} on instance {}'.format(
        table_name,database_id, instance_id))
    else:
        logging.info('invalid table name')
# [END spanner_create_database]
