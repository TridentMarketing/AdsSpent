import sys 
import json
import time
import pyodbc
import logging
import petl as etl
import pandas as pd
from datetime import datetime
from requests import get,post

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
            tags_json["resort"]=return_doc['resort']
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
            responce = {"acknowledged": x.acknowledged,
                        "inserted_records": len(x.inserted_ids),
                        "records": len(docs_bulk)}
        else:
            responce = {"acknowledged": False,
                        "inserted_records": 0,
                        "records": len(docs_bulk)}

    except:
        print("Error While Dumping Data into MongoDb")

    return(responce)
