#!/usr/bin/env python

import werkzeug
# Werkzeug 1.0.0 breaks flask_restplus, need to keep this line in
# till flask restplus patches this
werkzeug.cached_property = werkzeug.utils.cached_property

from flask import Flask, request, jsonify
from flask_restplus import Api
from flask_cors import CORS
from src.api.projects import api as project_routes
from src.api.workflows import api as workflow_routes
from src.api.executions import api as execution_routes
import logging
import sys

# Initialize logger
logger = logging.getLogger('plasma-api')

# Initialize flask app
app = Flask(__name__)
api = Api(app=app, prefix='/api')

# Attach namespaces to api
api.add_namespace(project_routes,path='/project')
api.add_namespace(workflow_routes,path='/workflow')
api.add_namespace(execution_routes,path='/execution')
api.add_namespace(model_routes,path='/model')
#api.add_namespace(component_routes,path='/component')
#api.init_app(app)

# enable CORS
CORS(app)


def initialize_api(api_host,api_port):
    app.run(host=api_host,port=api_port)
