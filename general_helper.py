import sys 
import json
import time
import pyodbc
import logging
import petl as etl
import pandas as pd
from datetime import datetime
from requests import get,post
from elasticsearch import Elasticsearch, helpers

from load_env_var import *
from connection import * 

def encode_agent_name(name):
    try:
#         return name.encode('ascii', errors='ignore').strip()
        return name.decode('utf-8').strip().replace('?', '')
    except Exception as e:
        print(e)
        return str(name)

def get_campaign_manager_id_from_adname(ad_name):
    try:
        if "Copy" in ad_name:
            ad_name = int(ad_name.split(" ")[-3])
            return ad_name
        else:
            ad_name = int(ad_name.split(" ")[-1])
            return ad_name
    except:
        print('ERROR while fetching campaign manager id from ad name', ad_name)
        return 0

def check_existing_files(participant,filename):
    dw_conn = get_dw_conn(server,database,username,password)
    existing_files = pd.read_sql("""
    SELECT *
    FROM [Warehouse01].[dbo].[fact_fbCostFilesDev]
    where 
    Participant in ('{}') 
    AND 
    filename in ('{}')
    """.format(participant,filename), dw_conn)
    existing_files = list(existing_files.filename.unique())
    if filename in existing_files:
        return True
    else:
        return False 
    
def insert_into_fbCostFile(filename,PARTICIPANT):
    dw_conn = get_dw_conn(server,database,username,password)
    cursor = dw_conn.cursor()
    query = """
    INSERT INTO Fact_FbCostFilesDev VALUES('{}', '{}', '{}')
    """.format(filename, PARTICIPANT, str(datetime.now())[:10])
    cursor.execute(query)
    return dw_conn.commit()

def getTradbTags(collection,campaignManagerId):
    try:
        tags_json={}
        for return_doc in collection.find({'campaignManagerId':campaignManagerId}):
            tags_json["campaignType"]=return_doc['contactType']
            tags_json["tag_mongoId"]=str(return_doc['_id'])
            tags_json["campaignManagerId"]=return_doc['campaignManagerId']
            tags_json["contactType"]=return_doc['contactType']
            tags_json["name"]=return_doc['name']
            tags_json["promotion"]=return_doc['promotion']
            tags_json["dnis"]=return_doc['dnis']
            tags_json["resort"]=str(return_doc['resort'])
            tags_json["isActive"]=return_doc['isActive']
            return tags_json
    except:
        return None

def calculate_spent(totalAdSpent,percent):
    try:
        if (totalAdSpent != 0.0):
            charges = totalAdSpent * percent
            return charges
        else:
            return 0
    except Exception as e:
        print("Error:",e)

def mongodb_many_to_many_insert(collection, docs_bulk):
    try:
        if len(docs_bulk) > 0:
            x = collection.insert_many(docs_bulk)
            response = {"acknowledged": x.acknowledged,
                        "inserted_records": len(x.inserted_ids),
                        "records": len(docs_bulk)}
        else:
            response = {"acknowledged": False,
                        "inserted_records": 0,
                        "records": len(docs_bulk)}

    except:
        print("Error While Dumping Data into MongoDb")
        response=None

    return(response)

def adSpentElasticIndexing(es, adSpentDataBulk,fbAdSpentIndex,fbAdSpentDoctype):
    try:
        response = helpers.bulk(es, adSpentDataBulk,
                                index=fbAdSpentIndex,
                                doc_type=fbAdSpentDoctype)

        print ("\nActions RESPONSE Dumped Docs:", response[0])
        return response[0]

    except Exception as err:
        error_msg = "Elasticsearch index() ERROR:"
        print(error_msg, err)

        return None
