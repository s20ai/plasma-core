#!/usr/bin/env python3

import werkzeug
# Werkzeug 1.0.0 breaks flask_restplus, need to keep this line in 
# till flask restplus patches this
werkzeug.cached_property = werkzeug.utils.cached_property
from flask_restplus import Namespace, Resource, fields, reqparse
from xxhash import xxh64_hexdigest
from src.utils.api_utils import *
from src.utils.decorators import *
from src.api.executions import execution_pass

api = Namespace('workflow', description='routes for workflow management')


workflow = api.model('Workflow', {
    'project-id': fields.String(required=True, description='Project id'),
    'workflow-id': fields.String(required=True, description='Workflow id'),
    'workflow-name': fields.String(required=True, description='Workflow name'),
    'status': fields.Integer(required=True, description='Workflow status'),
    'schedule': fields.Integer(required=True, description='Workflow schedule'),
    'execution': fields.Nested(execution_pass,required=True,description='Execution Detail')
})


@api.route('')
class WorkflowList(Resource):
    @api.doc('List all workflows')
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('project-id', type=str, location='args')
        args = parser.parse_args()
        db = get_plasma_db()
        workflow_collection = db.get_collection('workflows')
        workflows = []
        for item in workflow_collection.find({'project-id':args['project-id']}):
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
        deleted_workflow = workflow_collection.delete_one({"workflow-id": workflow_id})
        if deleted_workflow.deleted_count == 1:
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
        workflow_collection = db.get_collection('workflows')
        result = workflow_collection.find_one({'workflow-id': workflow_id})
        if result:
            response_data = marshal(result, workflow)
            response = generate_response(200, response_data)
        else:
            response = generate_response(404)
        return response
    ### create new execution_pass


@api.route('/<workflow_id>/stop')
@api.param('workflow_id','Workflow ID')
class WorkflowStop(Resource):
    @api.doc('Stop a workflow')
    def post(self, workflow_id):
        db = get_plasma_db()
        workflow_collection = db.get_collection('workflows')
        result = workflow_collection.find_one({'workflow-id': workflow_id})
        if result:
            response_data = marshal(result, workflow)
            response = generate_response(200, response_data)
        else:
            response = generate_response(404)
        return response
    ## stop_execution_pass
