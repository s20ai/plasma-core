#!/usr/bin/env python3

import werkzeug
# Werkzeug 1.0.0 breaks flask_restplus, need to keep this line in
# till flask restplus patches this
werkzeug.cached_property = werkzeug.utils.cached_property
from xxhash import xxh32_intdigest
from flask_restplus import Namespace, Resource, fields, marshal, reqparse
from src.utils.api_utils import *
from src.utils.decorators import *
from src.utils.db_utils import *
from time import time

api = Namespace('model', description='routes for model management')

model = api.model('Project', {
    'project-id': fields.String(required=True, description = 'Project ID'),
    'workflow-id': fields.String(required=True, description = 'Workflow ID'),
    'model-id': fields.String(required=True, description = 'Model Id'),
    'model-name': fields.String(required=True, description = 'Model Name'),
    'model-path': fields.String(required = True, description = 'Model file path'),
    'type': fields.String(required=True, description = 'Model Type'),
    'framework': fields.Integer(required=True, description = 'Model Framework'),
    'metrics': fields.Integer(required=True, description = 'Model Metrics'),
    'created-at': fields.String(require=True, description = 'Model generation timestamp')
})


@api.route('')
class ProjectList(Resource):
    @api.doc('List all models')
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('project-id', type=str, location='args')
        args = parser.parse_args()
        if args['project-id']:
            query = {'project-id':args['project-id']}
        else:
            query = {}
        db = get_plasma_db()
        model_collection = db.get_collection('models')
        models = []
        for item in model_collection.find(query):
            models.append(marshal(item, model))
        response = generate_response(200, models)
        return response


@api.route('/<model_id>')
@api.param('model_id', 'Model ID')
class Project(Resource):
    @api.doc('Get a model')
    def get(self, model_id):
        db = get_plasma_db()
        model_collection = db.get_collection('models')
        result = model_collection.find_one({'model-id': model_id})
        if result:
            response_data = marshal(result, model)
            response = generate_response(200, response_data)
        else:
            response = generate_response(404)
        return response

    @api.doc('Create a new model')
    def post(self, model_id):
        parser = reqparse.RequestParser()
        parser.add_argument('project-id', type=str, required=True, help='project-id')
        parser.add_argument('workflow-id', type=str, required=True, help='workflow-id')
        parser.add_argument('model-name', type=str, required=True, help='model name')
        parser.add_argument('model-path', type=str, required=True, help='model path')
        parser.add_argument('type', type=str, default='undefined', help='model type')
        parser.add_argument('framework', type=str, default='undefined', help='model framework')
        parser.add_argument('metrics', type=str, default='undefined', help='model framework')
        args = parser.parse_args()
        model_id = xxh32_intdigest(args['model-name'])
        args['model-id'] = model_id
        args['created-at'] = str(time())
        db = get_plasma_db()
        model_collection = db.get_collection('models')
        model_collection.insert(dict(args))
        update_project_statistics(args['project-id'])
        response_data = {'model-id':model_id}
        response = generate_response(201,response_data)
        return response

    @api.doc('Update an existing model')
    def put(self, model_id):
        parser = reqparse.RequestParser()
        parser.add_argument('update', type=dict, required=True,help='values which need to be updated')
        args = parser.parse_args()
        db = get_plasma_db()
        model_collection = db.get_collection('models')
        model = model_collection.find({'model-id':model_id})
        updated_resource = model_collection.find_one_and_update(
            {"model-id": model_id},
            {"$set": dict(args)}
        )
        update_project_statistics(model['model-id'])
        if updated_resource:
            response = generate_response(204)
        else:
            response = generate_response(404)
        return response

    @api.doc('Delete a model')
    def delete(self, model_id):
        db = get_plasma_db()
        model_collection = db.get_collection('models') 
        model = model_collection.find({'model-id':model_id})
        deleted_model = model_collection.delete_one({"model-id": model_id})
        if deleted_model.deleted_count == 1:
            update_project_statistics(model['project-id'])
            response = generate_response(200)
        else:
            response = generate_response(404)
        return response
