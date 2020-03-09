#!/usr/bin/env python3

import werkzeug
# Werkzeug 1.0.0 breaks flask_restplus, need to keep this line in
# till flask restplus patches this
werkzeug.cached_property = werkzeug.utils.cached_property
from xxhash import xxh32_intdigest
from flask_restplus import Namespace, Resource, fields, marshal, reqparse
from src.utils.api_utils import *
from src.utils.decorators import *
from src.utils.db_utils import get_plasma_db

api = Namespace('project', description='routes for project management')

project = api.model('Project', {
    'project-id': fields.String(required=True, description='Project id'),
    'project-name': fields.String(required=True, description='Project name'),
    'project-path': fields.String(required=True, description='Project path'),
    'workflows': fields.Integer(required=True, description='Number of workflows'),
    'models': fields.Integer(required=True, description='Number of models')
})


@api.route('')
class ProjectList(Resource):
    @api.doc('List all projects')
    def get(self):
        db = get_plasma_db()
        project_collection = db.get_collection('projects')
        projects = []
        for item in project_collection.find():
            projects.append(marshal(item, project))
        response = generate_response(200, projects)
        return response


@api.route('/<project_id>')
@api.param('project_id', 'Project ID')
class Project(Resource):
    @api.doc('Get a project')
    def get(self, project_id):
        db = get_plasma_db()
        project_collection = db.get_collection('projects')
        result = project_collection.find_one({'project-id': project_id})
        if result:
            response_data = marshal(result, project)
            response = generate_response(200, response_data)
        else:
            response = generate_response(404)
        return response

    @api.doc('Create a new project')
    def post(self, project_id):
        parser = reqparse.RequestParser()
        parser.add_argument('project-name', type=str, required=True, help='project name')
        parser.add_argument('project-path', type=str, required=True, help='project path')
        parser.add_argument('workflows', type=int, default=0, help='number of connected workfows')
        parser.add_argument('models', type=int, default=0, help='number of connected models')
        args = parser.parse_args()
        project_id = xxh32_intdigest(args['project-name'])
        args['project-id'] = project_id
        db = get_plasma_db()
        project_collection = db.get_collection('projects')
        project_collection.insert(dict(args))
        response_data = {'project-id':project_id}
        response = generate_response(201,response_data)
        return response

    @api.doc('Update an existing project')
    def put(self, project_id):
        parser = reqparse.RequestParser()
        parser.add_argument('update', type=dict, required=True,help='values which need to be updated')
        args = parser.parse_args()
        db = get_plasma_db()
        project_collection = db.get_collection('projects')
        updated_resource = project_collection.find_one_and_update(
            {"project-id": project_id},
            {"$set": dict(args)}
        )
        if updated_resource:
            response = generate_response(204)
        else:
            response = generate_response(404)
        return response

    @api.doc('Delete a project')
    def delete(self, project_id):
        db = get_plasma_db()
        project_collection = db.get_collection('projects')
        deleted_project = project_collection.delete_one({"project-id": project_id})
        if deleted_project.deleted_count == 1:
            response = generate_response(200)
        else:
            response = generate_response(404)
        return response
