#!/usr/bin/env python3

from src.api.rest_api import initialize_api
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


def redis_ping():
    return True


def mongo_ping():
    return True


def pre_init_check():
    if (redis_ping() and mongo_ping()):
        return True
    else:
        return False


if __name__ == '__main__':
    if pre_init_check():
        initialize_api(REST_API_HOST, REST_API_PORT)
    else:
        print('Failed to start plasma-core')
