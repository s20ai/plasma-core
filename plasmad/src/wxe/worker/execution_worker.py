#!/usr/bin/env python

import logging
from utils import *
from workflow import Workflow
from time import sleep,time
import redis
import yaml
import os
import venv
import sys
import subprocess
import json
import requests
from time import sleep

logger = logging.getLogger(' Worker ')


def setup_loggers(execution_id, log_path):
    logger = logging.getLogger(execution_id+'_executor')
    log_file = log_path+'/'+execution_id+'.log'
    file_handler = handler = RotatingFileHandler(log_file)
    stdout_handler = logging.StreamHandler(sys.stdout)
    handlers = [stdout_handler, file_handler]
    format_string = '%(asctime)s | %(levelname)7s | %(message)s'
    date_format = '%m/%d/%Y %I:%M:%S %p'
    logging.basicConfig(handlers=handlers, level=logging.DEBUG,
                        format=format_string, datefmt=date_format)



def component_loader(component_name, component_path):
    spec = importlib.util.spec_from_file_location(
        component_name, component_path)
    component = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(component)
    return component


def verify_components(workflow):
    logger.info('verifying components')
    components_path = project_config['paths']['components_path']
    local_components = os.listdir(components_path)
    workflow_components = list(workflow.workflow['workflow'].keys())
    missing_components = set(workflow_components) - set(local_components)
    if(len(missing_components) == 0):
        return True
    else:
        logger.error('unable to execute workflow')
        logger.error('component not found : ' + ', '.join(missing_components))
        return False


def generate_workflow_requirements(workflow):
    workflow_name = workflow.name
    logger.info('generating workflow requirements')
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
        workflow_requirements_path = project_config['paths']['data_path'] + \
            workflow_name+'.requirements'
        with open(workflow_requirements_path, 'w') as file:
            requirements_string = '\n'.join(requirements_list)
            file.write(requirements_string)
        return workflow_requirements_path
    except Exception as e:
        logger.error('failed to generate requirements file')
        logger.error(e)
        exit(1)


def setup_virtual_environment(requirements_path, workflow_name):
    try:
        logger.info('setting up virtual environment')
        venv_path = project_config['paths']['data_path']+workflow_name+'_venv'
        venv.create(venv_path)
        logger.info('activating virtual environment')
        output = os.system('bash '+venv_path+'/bin/activate')
        logger.info('installing dependencies')
        output = subprocess.check_output(
            ['pip3', 'install', '-r', requirements_path])
        return True
    except Exception as e:
        logger.error("unable to setup virtual environment")
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
        logger.info('executing step :'+step['component'])
        component_name = step['component']
        component_path = project_config['paths']['components_path'] + \
            component_name+'/component.py'
        component = component_loader(component_name, component_path)
        logging.getLogger(component_name).setLevel(logging.ERROR)
        output = component.main(step)
        return output
    except Exception as e:
        logger.error('failed to execute step : %s > %s' %
                     (step['component'], step['operation']))
        logger.error(e)


def execute_workflow(workflow_steps):
    logger.info('executing workflow')
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
        exit(1)


def verify_components(workflow,project_paths):
    logger.info('verifying components')
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
        project_path = execution_job['project-path']
        config_file = json.load(open(project_path+'/.plasma.json'))
        execution_job['project-paths'] = config_file['paths']
        # Load and validate workflow
        workflow_path = execution_job['project-path']['workflows_path'] + \
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
    workflow_id = execution_job['workflow-id']
    execution_id = execution_job['execution-id']
    workflow = validate_job(execution_job)
    if workflow:
        update_status(workflow_id, execution_id, 3)
        execution_successful = execute_workflow(workflow)
        if execution_successful:
            update_status(workflow_id, execution_id, 5)
        else:
            update_status(workflow_id, execution_id, -1)
        update_execution_job(execution_id, {"finished-at":timestamp})
    else:
        update_status(workflow_id, execution_id, -1)
    timestamp = str(time())
    update_execution_job(execution_id, {"finished-at":timestamp})

if __name__ == '__main__':
    try:
        client = redis.Redis()
        subscriber = client.pubsub()
        subscriber.subscribe('execution_queue')
        while True:
            message = subscriber.get_message()
            if message:
                if message['type'] == 'message':
                    execution_job = json.loads(message['data'])
                    run(execution_job)
            else:
                sleep(2)
    except KeyboardInterrupt:
        logger.error('Keyboard Interrupt')
