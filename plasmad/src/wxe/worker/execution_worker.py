#!/usr/bin/env python

import logging
from utils import *
from workflow import Workflow
from time import sleep, time
import redis
import yaml
import os
import sys
import pip
import importlib
from logging.handlers import RotatingFileHandler
import json
import requests
from time import sleep

logger = logging.getLogger(' Worker ')
project_config = {}


def setup_logger(execution_id, log_path):
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logger = logging.getLogger(execution_id+'_executor')
    log_file = log_path+execution_id+'.log'
    file_handler = handler = RotatingFileHandler(log_file)
    stdout_handler = logging.StreamHandler(sys.stdout)
    handlers = [stdout_handler, file_handler]
    format_string = '%(asctime)s | %(levelname)7s | %(message)s'
    date_format = '%m/%d/%Y %I:%M:%S %p'
    logging.basicConfig(handlers=handlers, level=logging.INFO,
                        format=format_string, datefmt=date_format)


def component_loader(component_name, component_path):
    spec = importlib.util.spec_from_file_location(
        component_name, component_path)
    component = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(component)
    return component


def setup_workflow_requirements(workflow):
    workflow_name = workflow.name
    logger.info('Generating workflow requirements')
    try:
        components = list(workflow.workflow['workflow'].keys())
        requirements_list = []
        for component in components:
            requirements_path = project_config['paths']['components_path'] + \
                component+'/requirements.txt'
            with open(requirements_path, 'r') as requirements_file:
                requirements = requirements_file.read().split('\n')
                requirements = [x for x in requirements if x]
                requirements_list += requirements
        logger.info('Installing workflow requirements')
        for each in requirements_list:
            pip.main(['install',each])
    except Exception as e:
        logger.error('failed to generate requirements file')
        logger.error(e)
        exit(1)


def update_output_variables(step, output_dict):
    output_keys = list(output_dict.keys())
    updated_parameters = {}
    for key, value in step['parameters'].items():
        if value in output_keys:
            updated_parameters[key] = output_dict[value]
        else:
            updated_parameters[key] = value
    step['parameters'] = updated_parameters
    return step


def execute_step(step):
    try:
        logger.info('Executing step :'+step['component'])
        component_name = step['component']
        component_path = project_config['paths']['components_path'] + \
            component_name+'/component.py'
        component = component_loader(component_name, component_path)
        logging.getLogger(component_name).setLevel(logging.ERROR)
        output = component.main(step)
        return output
    except Exception as e:
        logger.error('failed to execute step : %s' % step['component'])
        logger.error(e)


def execute_workflow(workflow_steps):
    logger.info('Executing workflow')
    try:
        output_dict = {}
        for step in workflow_steps:
            step = update_output_variables(step, output_dict)
            output = execute_step(step)
            if type(output) is dict:
                output_dict.update(output)
        return True
    except Exception as e:
        logger.error('exception : '+str(e))
        return False


def validate_components(workflow, project_paths):
    logger.info('Validating components')
    components_path = project_paths['components_path']
    local_components = os.listdir(components_path)
    workflow_components = list(workflow.workflow['workflow'].keys())
    missing_components = set(workflow_components) - set(local_components)
    if(len(missing_components) == 0):
        return True
    else:
        logger.error('component not found : ' + ', '.join(missing_components))
        return False


def validate_job(execution_job):
    # Change status codes to preocessing
    workflow_id = execution_job['workflow-id']
    execution_id = execution_job['execution-id']
    update_status(workflow_id, execution_id, 2)
    try:
        # Validate plasma project path
        logger.info('Validating Plasma Project path')
        project_path = execution_job['project-path']
        config_file = json.load(open(project_path+'/.plasma.json'))
        execution_job['project-paths'] = config_file['paths']
        # Load and validate workflow
        logger.info('Validating workflow')
        workflow_path = execution_job['project-paths']['workflows_path'] + \
            execution_job['workflow-name']
        workflow = Workflow(workflow_path)
        workflow_valid = workflow.validate()
        # validate components
        components_valid = validate_components(
            workflow,
            execution_job['project-paths']
        )
        return workflow
    except Exception as e:
        logger.error('Failed to validate job :'+str(e))
        update_status(workflow_id, execution_id, -1)
        return False


def run(execution_job):
    global project_config
    workflow_id = execution_job['workflow-id']
    execution_id = execution_job['execution-id']
    workflow = validate_job(execution_job)
    if workflow:
        project_config['paths'] = workflow['project-paths']
        update_status(workflow_id, execution_id, 3)
        setup_logger(execution_id, execution_job['project-paths']['log_path'])
        setup_workflow_requirements(workflow)
        execution_successful = execute_workflow(workflow.steps)
        if execution_successful:
            update_status(workflow_id, execution_id, 5)
        else:
            update_status(workflow_id, execution_id, -1)
    else:
        update_status(workflow_id, execution_id, -1)
    timestamp = str(time())
    update_execution_job(execution_id, {"finished-at": timestamp})


if __name__ == '__main__':
    try:
        logger = logging.getLogger('execution_worker')
        stdout_handler = logging.StreamHandler(sys.stdout)
        handlers = [stdout_handler]
        format_string = '%(asctime)s | %(levelname)7s | %(message)s'
        date_format = '%m/%d/%Y %I:%M:%S %p'
        logging.basicConfig(handlers=handlers, level=logging.INFO,
                            format=format_string, datefmt=date_format)
        client = redis.Redis()
        subscriber = client.pubsub()
        subscriber.subscribe('execution_queue')
        while True:
            message = subscriber.get_message()
            if message:
                if message['type'] == 'message':
                    logger.info('Received Job : Starting Execution')
                    execution_job = json.loads(message['data'])
                    run(execution_job)
            else:
                sleep(2)
    except KeyboardInterrupt:
        logger.error('Keyboard Interrupt')
