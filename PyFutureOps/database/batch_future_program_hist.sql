"""CREATE TABLE BATCH_FUTURE_PROGRAM_HIST
   (RESTART_NAME STRING(1024), 
    THREAD_ID STRING(1024), 
	THREAD_VAL STRING(1024), 
	START_TIME DATE, 
	PROGRAM_NAME STRING(1024), 
    PROGRAM_STATUS STRING(1024), 
	NUM_THREADS INT64, 
	COMMIT_MAX_CTR INT64, 
	RESTART_TIME DATE, 
	FINISH_TIME DATE, 
    LAST_UPD_TMST DATE,
	SUCCESS_FLAG STRING(1024), 
	NON_FATAL_ERR_FLAG STRING(1024), 
	NUM_COMMITS INT64, 
	AVG_TIME_BTWN_COMMITS INT64,
	COMMITS INT64
   )
   """