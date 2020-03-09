#!/usr/bin/env python3

from src.api.rest_api import initialize_api
from src.utils.db_utils import mongo_ping, redis_ping
from logging.handlers import RotatingFileHandler
from config import *
import logging, sys

logger = logging.getLogger('plasma-core')
file_handler = handler = RotatingFileHandler(LOG_PATH, maxBytes=10000, backupCount=3)
stdout_handler = logging.StreamHandler(sys.stdout)
handlers = [stdout_handler, file_handler]
format_string = '%(asctime)s | %(levelname)7s | %(message)s'
date_format = '%m/%d/%Y %I:%M:%S %p'
logging.basicConfig(handlers=handlers, level=logging.DEBUG, format=format_string, datefmt=date_format)



def pre_init_check():
    status = {True:'connected',False:'failed to connect'}
    mongo_connected = mongo_ping(MONGODB_CONNECTION_URI)
    redis_connected = redis_ping(REDIS_HOST,REDIS_PORT)
    print('> Mongo status : '+status[mongo_connected])
    print('> Redis status : '+status[redis_connected])
    if mongo_connected & redis_connected:
        print('> Initializing Plasma Daemon')
        output = True
    else:
        output = False
    return output


if __name__ == '__main__':
    if pre_init_check():
        initialize_api(REST_API_HOST, REST_API_PORT)
    else:
        print('> Failed to start plasma-core')
