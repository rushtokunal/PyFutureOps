# PyFutureOps
Restart recovery for concurrent futures

Coded in Python 3.7.5. Implement restart/recovery in python concurrent futures

Steps to follow

1) All the application logic is in the multithread_wiki.py file, other folder multi_ops has bootstrap DB logic which can be ignored at this moment
2) export INSTANCE_NAME=<instance_name>
   export DATABASE_NAME=<database_name>
   export PROJECT_ID_SPANNER=<spanner project id>
3) ignore the file concurrent-futures.py
4) Spanner r/w and DDL permission are needed
5) while executing the script it creates 3 tables by default to track batches

    BATCH_RESTART_BOOKMARK (to track threads failures where intra thread chunking is done)
    BATCH_RESTART_CONTROL (pre set up of batches, would not be used mostly)
    BATCH_RESTART_PROGRAM_STATUS (this tracks the entire processing, if thread fails they are captured here, so you can re process them without having to start from the beginning)
    BATCH_FUTURE_PROGRAM_HIST previos successfull runs are moved from status table

TO DO:
1)Implement sample for multiprocessing

Steps to build
python setup.py sdist
python -m twine upload dist/pyfutureops-0.x.tar.gz
