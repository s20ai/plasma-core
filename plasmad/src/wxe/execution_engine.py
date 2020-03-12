#!/usr/bin/env python

import config
import sys
import os
import logging
from src.utils.decorators import *
from src.utils.db_utils import get_plasma_db
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
        client.publish('execution_queue',json.dumps(execution_job))
        db = get_plasma_db()
        db.workflows.find_one_and_update(
                {'workflow-id':execution_job['workflow-id']},
                {'$set':{'status':1,'execution-id':execution_job['execution-id']}}
        )
        return True
    except Exception as e:
        logger.error('Failed to insert execution job in queue : '+str(e))
        return False


@redis_operation
def stop_workflow(client, execution_id):
    try:
        client.publish(execution_id,'stop')
        db = get_plasma_db()
        execution = db.executions.find_one({"execution-id":execution_id})
        db.workflows.find_one_and_update(
                {'workflow-id':execution['workflow-id']},
                {'$set':{'status':4}}
        )
        return True
    except Exception as e:
        logger.error('Failed to stop execution job  : '+str(e))
        return False
   

