#!/usr/bin/env python

import logging
from workflow import Workflow
from time import sleep
import redis
import yaml
import os
import venv
import sys
import subprocess
import json
import requests
from time import sleep


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





def celery_workflow_executor(args):
    global project_config
    workflow_name = args['workflow-name']
    project_config = args['config']
    workflow_path = project_config['paths']['workflows_path'] + workflow_name
    workflow = Workflow(workflow_path)
    workflow_valid = workflow.validate()
    if workflow_valid:
        components = verify_components(workflow)
        if not components:
            logger.error('components declared in workflow are missing')
            exit(1)
        #requirements = generate_workflow_requirements(workflow)
        #virtual_environment = setup_virtual_environment(requirements, workflow_name)
        state = execute_workflow(workflow.steps)
        if state is True:
            logger.info('workflow executed')
        else:
            logger.info('failed to execute workflow')
    else:
        logger.error('invalid workflow')
        exit(1)


# resolve details of workflow and project
# check paths
# validate components 
# validate workflows
# setup environment
# execute workflow
#################################################################################################################################################

def update_execution_job(execution_id, update):
    url = 'http://0.0.0.0:8196/api/execution/'+execution_id
    json = {'update':update}
    response = requests.put(url,json=json)
    if response.status_code == 204:
        output = True
    else:
        output = False
    return output

def update_workflow(workflow_id, update):
    url = 'http://0.0.0.0:8196/api/workflow/'+workflow_id
    json = {'update':update}
    response = requests.put(url,json=json)
    if response.status_code == 204:
        output = True
    else:
        output = False
    return output


def update_status(workflow_id,execution_id,status_code):
    update_workflow(
            workflow_id, 
            {"status":status_code}
    )
    update_execution_job(
            execution_id, 
            {"status":status_code}
    )



def validate_job(execution_job):
    update_status(
            execution_job['workflow-id'],
            execution_job['execution-id'],
            2
    )



if __name__ == '__main__':
    client = redis.Redis()
    subscriber = client.pubsub()
    subscriber.subscribe('execution_queue')
    while True:
        message = subscriber.get_message()
        if message:
            if message['type'] == 'message':
                execution_job = json.loads(message['data'])
                valid = validate_job(execution_job)
        else:
            sleep(2)
