#!/usr/bin/python3

import requests


def update_execution_job(execution_id, update):
    url = 'http://0.0.0.0:8196/api/execution/'+execution_id
    json = {'update': update}
    response = requests.put(url, json=json)
    if response.status_code == 204:
        output = True
    else:
        output = False
    return output


def update_workflow(workflow_id, update):
    url = 'http://0.0.0.0:8196/api/workflow/'+workflow_id
    json = {'update': update}
    response = requests.put(url, json=json)
    if response.status_code == 204:
        output = True
    else:
        output = False
    return output


def update_status(workflow_id, execution_id, status_code):
    update_workflow(
        workflow_id,
        {"status": status_code}
    )
    update_execution_job(
        execution_id,
        {"status": status_code}
    )
