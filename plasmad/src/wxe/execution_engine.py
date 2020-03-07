#!/usr/bin/env python

import config
import sys,os
from src.utils.rejson import *
from src.utils.decorators import *
import json

def create_execution_job(args):
    if config.EXECUTION_CONTEXT == 'process':
        #output = serial_workflow_executor()
        pass
    elif config.EXECUTION_CONTEXT == 'celery':
        pass
        #output = celery_workflow_executor.delay(args)
    return output


def run_workflow(project_id,workflow_id):
    status = insert_in_job_queue(project_id,workflow_id,'run')
    return status


def stop_workflow(args):
    status = insert_in_job_queue(project_id,workflow_id,'stop')
    return status


@redis_operation
def insert_in_job_queue(client,project_id,workflow_id,operation):
    job = {}
    job['project-id'] = project_id
    job['workflow-id'] = workflow_id
    job['operation'] = operation
    client.publish('workflow_execution_queue',json.dumps(job))
    workflow = get_json('workflow',workflow_id)
    workflow['status'] = 'Queued'
    update_json('workflow',workflow_id,workflow)
    return True
