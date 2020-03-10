#!/usr/bin/env python

import config
import sys
import os
import logging
from src.utils.decorators import *
import json

logger = logging.getLogger('WXE')

def create_execution_job(args):
    if config.EXECUTION_CONTEXT == 'process':
        #output = serial_workflow_executor()
        pass
    elif config.EXECUTION_CONTEXT == 'celery':
        pass
        #output = celery_workflow_executor.delay(args)
    return output


@redis_operation
def run_workflow(client,execution_job):
    try:
        execution_string = json.dumps(execution_job)
        client.publish('execution_queue',execution_string)
        return True
    except Exception as e:
        logger.error('Failed to insert execution job in queue : '+str(e))
        return False


@redis_operation
def stop_workflow(client, execution_id):
    try:
        client.publish(execution_id,'stop')
        return True
    except Exception as e:
        logger.error('Failed to stop execution job in : '+str(e))
        return False
   

