# ---------------------------------------------------------------
#                           Imports
# ---------------------------------------------------------------

# General libraries
import os
import io
import json
import pickle
import base64
import pandas as pd

from datetime import date
from urllib.parse import urlparse

# Logging Library imports
import logging
import logging.config

# Cloud object storage library imports
import ibm_boto3
from ibm_botocore.client import Config, ClientError

# SQL Library imports
import mysql.connector
from mysql.connector import Error
from mysql.connector import pooling
from mysql.connector.connection import MySQLConnection

# Mongo-DB Library imports
import pymongo
from pymongo import MongoClient

# Flask library imports
from flask_cors import CORS
from flask import Flask, jsonify, request, make_response

#-------------------------------------------------------------------
#                        Global Variables              
#-------------------------------------------------------------------

# TODO: Remove this variable
dst_table = "actual_sales_2"

NOSQL_DB_NAME = os.getenv('TARGET_NOSQL_DB')
ACTUAL_SALES_TABLE = os.getenv('ACTUAL_SALES')

#-------------------------------------------------------------------
#                        Database credentials              
#-------------------------------------------------------------------

if 'VCAP_SERVICES' in os.environ:
    vcap = json.loads(os.getenv('VCAP_SERVICES'))
    print('Found VCAP_SERVICES')

    if 'cloud-object-storage' in vcap:
        s3Credential = vcap['cloud-object-storage'][0]['credentials']
        COS_ENDPOINT = 'https://s3.us-south.cloud-object-storage.appdomain.cloud'
        COS_API_KEY_ID = s3Credential['apikey']
        COS_AUTH_ENDPOINT = "https://iam.cloud.ibm.com/identity/token"
        COS_RESOURCE_CRN = s3Credential['resource_instance_id']

    if 'compose-for-mysql' in vcap:
        mysqlCreds = urlparse(vcap['compose-for-mysql'][0]['credentials']['uri'])
        mysqlUser = mysqlCreds.username
        mysqlPassword = mysqlCreds.password
        mysqlUrl = mysqlCreds.hostname
        mysqlPort = mysqlCreds.port
        mysqlDb = mysqlCreds.path[1:]
        mysqlSsl_ca = base64.b64decode(vcap['compose-for-mysql'][0]['credentials']['ca_certificate_base64'])
    
    if 'databases-for-mongodb' in vcap:
        mongoCreds = vcap["databases-for-mongodb"][0]["credentials"]
        mongo_composed = mongoCreds["connection"]["mongodb"]["composed"][0]

elif os.path.isfile('vcap_services.json'):
    with open('vcap_services.json') as f:
        vcap = json.load(f)
        print('Found local VCAP_SERVICES')

        # Cloud object storage
        if 'cloud-object-storage' in vcap:
            s3Credential = vcap['cloud-object-storage'][0]['credentials']
            COS_API_KEY_ID = s3Credential['apikey']
            COS_RESOURCE_CRN = s3Credential['resource_instance_id']
            COS_ENDPOINT = 'https://s3.us-south.cloud-object-storage.appdomain.cloud'
            COS_AUTH_ENDPOINT = "https://iam.cloud.ibm.com/identity/token"
            COS_CONFIG = Config(signature_version='oauth')

        # SQL database
        if 'compose-for-mysql' in vcap:
            mysqlCreds = urlparse(vcap['compose-for-mysql'][0]['credentials']['uri'])
            mysqlUser = mysqlCreds.username
            mysqlPassword = mysqlCreds.password
            mysqlUrl = mysqlCreds.hostname
            mysqlPort = mysqlCreds.port
            mysqlDb = mysqlCreds.path[1:]
            mysqlSsl_ca = base64.b64decode(vcap['compose-for-mysql'][0]['credentials']['ca_certificate_base64'])

        if 'databases-for-mongodb' in vcap:
            mongoCreds = vcap["databases-for-mongodb"][0]["credentials"]
            mongo_composed = mongoCreds["connection"]["mongodb"]["composed"][0]

# ---------------------------------------------------------------
#                               Utils
# ---------------------------------------------------------------

def jsonify_cos(name, file):

    ext = os.path.splitext(name)[-1]
    
    if ext == ".xlsx":
        item = pd.read_excel(file, encoding='utf-8')

    if ext == ".csv":
        item = pd.read_csv(file)

    return item