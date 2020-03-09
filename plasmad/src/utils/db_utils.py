#!/usr/bin/env python3

from redis import Redis
from redis.exceptions import ConnectionError
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from logging import getLogger

logger = getLogger('db_utils')


def mongo_ping(connection_uri):
    client = MongoClient(
            connection_uri,
            serverSelectionTimeoutMS=5000
    )
    try:
        info = client.server_info()
        return True
    except ServerSelectionTimeoutError:
        logger.error('Failed to connect to mongo : timeout')
        return False
    except Exception as e:
        logger.error('Failed to connect to mongo : '+str(e))
        return False

def redis_ping(redis_host, redis_port):
    client = Redis(host=redis_host, port=redis_port)
    try:
        status = client.ping()
        return True
    except ConnectionError:
        logger.error('Failed to connect to redis : connection error')
        return False
    except exception as e:
        logger.error('Failed to connect to redis : '+str(e))
        return False

def get_plasma_db():
    client = MongoClient()
    db = client.get_database('plasma')
    return db


def update_project_statistics(project_id):
    db = get_plasma_db()
    model_count = db.models.count({"project-id":project_id})
    workflow_count = db.workflows.count({"project-id":project_id})
    db.projects.find_one_and_update(
        {"project-id":project_id},
        {"$set":{
            "workflows":workflow_count,
            "models":model_count
    }})
    return True

