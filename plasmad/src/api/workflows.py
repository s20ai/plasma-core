#!/usr/bin/env python3

import werkzeug
# Werkzeug 1.0.0 breaks flask_restplus, need to keep this line in 
# till flask restplus patches this
werkzeug.cached_property = werkzeug.utils.cached_property
from flask_restplus import Namespace, Resource, fields, reqparse, marshal
from xxhash import xxh64_hexdigest
from src.utils.api_utils import *
from src.utils.decorators import *
from src.utils.db_utils import *
from src.api.executions import execution_pass
from src.wxe.execution_engine import run_workflow, stop_workflow
from time import time

api = Namespace('workflow', description='routes for workflow management')


workflow = api.model('Workflow', {
    'project-id': fields.String(required=True, description='Project id'),
    'workflow-id': fields.String(required=True, description='Workflow id'),
    'workflow-name': fields.String(required=True, description='Workflow name'),
    'status': fields.String(required=True, description='Workflow status'),
    'schedule': fields.Integer(required=True, description='Workflow schedule'),
    'execution-id': fields.String(required=True,description='Execution id')
})


@api.route('')
class WorkflowList(Resource):
    @api.doc('List all workflows')
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('project-id', type=str, location='args')
        args = parser.parse_args()
        if args['project-id']:
            query = {'project-id':args['project-id']}
        else:
            query = {}
        db = get_plasma_db()
        workflow_collection = db.get_collection('workflows')
        workflows = []
        for item in workflow_collection.find(query):
            workflows.append(marshal(item, workflow))
        response = generate_response(200, workflows)
        return response



@api.route('/<workflow_id>')
@api.param('workflow_id','Workflow ID')
class Workflows(Resource):
    @api.doc('Get a workflow')
    def get(self, workflow_id):
        db = get_plasma_db()
        workflow_collection = db.get_collection('workflows')
        result = workflow_collection.find_one({'workflow-id': workflow_id})
        if result:
            response_data = marshal(result, workflow)
            response = generate_response(200, response_data)
        else:
            response = generate_response(404)
        return response

    @api.doc('Create a new workflow')
    def post(self, workflow_id):
        parser = reqparse.RequestParser()
        parser.add_argument('workflow-name', type=str, required=True, help='workflow name')
        parser.add_argument('project-id', type=str, required=True, help='workflow path')
        args = parser.parse_args()
        args['workflow-id'] = xxh64_hexdigest(args['workflow-name'])
        args['status'] = 'Created'
        args['schedule'] = None
        args['execution'] = None
        db = get_plasma_db()
        workflow_collection = db.get_collection('workflows')
        workflow_collection.insert(dict(args))
        update_project_statistics(args['project-id'])
        response = generate_response(201)
        return response

    @api.doc('Update an existing workflow')
    def put(self, workflow_id):
        parser = reqparse.RequestParser()
        parser.add_argument('update', type=dict, required=True,help='values which need to be updated')
        args = parser.parse_args()
        db = get_plasma_db()
        workflow_collection = db.get_collection('workflows')
        updated_resource = workflow_collection.find_one_and_update(
            {"workflow-id": workflow_id},
            {"$set": dict(args)}
        )
        if updated_resource:
            response = generate_response(204)
        else:
            response = generate_response(404)
        return response

    @api.doc('Delete a workflow')
    def delete(self, workflow_id):
        db = get_plasma_db()
        workflow_collection = db.get_collection('workflows')
        workflow = workflow_collection.find_one({"workflow-id":workflow_id})
        deleted_workflow = workflow_collection.delete_one({"workflow-id": workflow_id})
        if deleted_workflow.deleted_count == 1:
            update_project_statistics(workflow['project-id'])
            response = generate_response(200)
        else:
            response = generate_response(404)
        return response


@api.route('/<workflow_id>/run')
@api.param('workflow_id','Workflow ID')
class WorkflowStart(Resource):
    @api.doc('Execute a workflow')
    def post(self, workflow_id):
        db = get_plasma_db()
        execution_pass = {}
        execution_pass['workflow-id'] = workflow_id
        execution_pass['status'] = 'Queued'
        execution_pass['started-at'] = time()
        execution_pass['finished-at'] = None
        execution_pass['execution-id'] = xxh64_hexdigest(workflow_id+str(time()))
        execution_collection = db.get_collection('executions')
        workflow_collection = db.get_collection('workflows')
        workflow_collection.find_one_and_update(
                {'workflow-id': workflow_id},
                {'$set':{'status':'Executing','execution-id':execution_pass['execution-id']}})
        execution_collection.insert(execution_pass)
        run_workflow(execution_pass)
        response = generate_response(200)
        return response


@api.route('/<workflow_id>/stop')
@api.param('workflow_id','Workflow ID')
class WorkflowStop(Resource):
    @api.doc('Stop a workflow')
    def post(self, workflow_id):
        db = get_plasma_db()
        workflow_collection = db.get_collection('workflows')
        workflow = workflow_collection.find_one_and_update(
                {'workflow-id': workflow_id},
                {'$set':{'status':'Stopped'}})
        workflow = workflow_collection.find_one({'workflow-id': workflow_id})
        execution_id = workflow['execution-id']
        stop_workflow(execution_pass)
        response = generate_response(200)
        return response
