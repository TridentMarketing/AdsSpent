from pymongo import MongoClient
from bson import ObjectId
import pyodbc
from ftplib import FTP
from elasticsearch import Elasticsearch, helpers

def mongodb_connection(clint_connection_str,db_name):
    try:
        client = MongoClient(clint_connection_str)
        db = client[db_name]
        return db
    except Exception as e:
        print("Error:",e)
        return None

def get_dw_conn(server,database,username,password):
    try:
        WAREHOUSE_CONN_STRING = 'DRIVER={ODBC Driver 17 for SQL Server};' \
        'SERVER=' + '{};'.format(server) + \
        'DATABASE=' + '{};'.format(database) + \
        'UID=' + '{};'.format(username) + \
        'PWD=' + '{};'.format(password)
        return pyodbc.connect(WAREHOUSE_CONN_STRING)
    except Exception as e:
        print("Error:",e)
        return None

def get_ftp_connection(ftp_path,ftp_username,ftp_password):
    try:
        ftp_conn = FTP(ftp_path,ftp_username,ftp_password)
        return ftp_conn
    except Exception as e:
        print("Error:",e)
        return None

def elasticsearch_connection(connection_string):
    try:
        elasticsearch_conn = Elasticsearch([connection_string])
        return elasticsearch_conn

    except:
        error_msg = "Error occured while connecting Elasticsearch client"
        print(error_msg)
        return None