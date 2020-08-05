import os
import sys
import logging

logging_level = os.environ.get('LOGLEVEL', 'INFO').upper() #if not defined then set to INFO

if logging_level in ['DEBUG','INFO','WARNING','ERROR','CRITICAL']:
    logging.basicConfig(stream=sys.stdout,format='%(asctime)s - %(levelname)s - %(message)s', level=logging_level)
    logging.info("Logging level set by env variable : {}".format(logging_level))
    logging.debug("Logging level set by env variable : {}".format(logging_level))
else:
    logging.basicConfig(stream=sys.stdout,format='%(asctime)s - %(levelname)s - %(message)s', level='INFO')
    logging.info("LOGLEVEL not set in env variable or incorrect value passed")
    logging.info("Defaulting LOGLEVEL to : {}".format('INFO'))