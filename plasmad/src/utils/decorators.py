#!/usr/bin/env python3

from functools import wraps
from logging import getLogger

logger = getLogger('decorators')

def log_exceptions(f):
    @wraps(f)
    def wrapped_function(*args, **kwargs):
        function_name = f.__name__
        try:
            logger.info('Executing | %s' % function_name)
            return f(*args, **kwargs)
        except Exception as e:
            logger.error('Exception | %s | %s |' % (function_name, str(e)))
    return wrapped_function



def redis_operation(f):
    @wraps(f)
    def wrapped_function(*args, **kwargs):
        function_name = f.__name__
        try:
            client = redis.Redis()
            return f(client, *args, **kwargs)
        except Exception as e:
            logger.error('Redis Exception | %s | %s |' %(function_name, str(e)))
    return wrapped_function


