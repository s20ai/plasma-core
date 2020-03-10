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

api = Namespace('settings', description='routes for managing plasma settings')

settings = api.model('Settings', {
    'automatic-crash-reporting': fields.Integer(required=True, description='Enable automatic crash reporting'),
    'plasma-auth-key': fields.String(required=True, description='Plasma authentication key'),
})


@api.route('')
class Settings(Resource):
    @api.doc('Get plasma settings')
    def get(self):
        db = get_plasma_db()
        settings_collection = db.get_collection('settings')
        result = settings_collection.find_one({'settings-type': 'general'})
        if result:
            response_data = marshal(result, settings)
            response = generate_response(200, response_data)
        else:
            response = generate_response(404)
        return response

    @api.doc('Update settings')
    def put(self):
        parser = reqparse.RequestParser()
        parser.add_argument('update', type=dict, required=True,
                            help='values which need to be updated')
        args = parser.parse_args()
        db = get_plasma_db()
        settings_collection = db.get_collection('settings')
        updated_resource = project_collection.find_one_and_update(
            {"settings-type": 'general'},
            {"$set": dict(args)['update']}
        )
        if updated_resource:
            response = generate_response(204)
        else:
            response = generate_response(404)
        return response
