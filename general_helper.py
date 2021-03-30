import logging
import petl as etl
import pandas as pd
from datetime import datetime
from requests import get,post
from dateutil import parser
import json

from .load_env_var import *
from .connection import * 

# Connection to Mongo Tradb live and Dev
tradbDev_conn = mongodb_connection(tradbDev_str,tradbDev_db)
tradbLive_conn = mongodb_connection(tradbLive_str,tradbLive_db)
print(tradbDev_conn, "\n" ,tradbLive_conn)

# Connection to  Elasticsearch
es=elasticsearch_connection(elasticsearch_conn_str)
print(es)
# Connection to MsSql warehouse
dw_conn = get_dw_conn(server,database,username,password)
cursor = dw_conn.cursor()
print(cursor)

tagsCollection=tradbLive_conn["tags"]
adsSpentCollection = tradbDev_conn["spentData"]
campaignsCollection = tradbLive_conn["campaigns"]
resortCollection = tradbLive_conn["resorts"]

# Custom function for getting campaign manager ids from account_name and ad_name.
def get_campaign_manager_id(account_name,ad_name):
    try:
        # Get campaign manager ids for slicebread ad account on the basis of account_name.
        if account_name == "Wally World SlicedBread": # TRO
            return int(5)
        elif account_name == "GettysburgBattlefieldResort":# TRG
            return int(6)
        elif account_name == "BLR Slicedbread":
            return int(77)
        elif account_name == "LSR Slicedbread":
            return int(87)
        elif account_name == "NSL Slicedbread":
            return int(45)
        elif account_name == "RFR Slicedbread":
            return int(47)
        elif account_name == "TNC Slicedbread":
            return int(88)
        
        # Special Handling for getting campaign manager ids for visibility and capital campaigns ad account from ad_names.
        if " - Copy" in ad_name:
            ad_name = ad_name.replace(" - Copy","")
            return int(ad_name.split('-')[-1].split('_')[-1])
        else:
            return int(ad_name.split('-')[-1].split('_')[-1])
    except:
        print ('ERROR while fetching campaign manager id from ad name')
        return int(0)
    
# Custom function for getting ad_account_ids w.r.t account_name
def get_ad_account_id(account_name):
    try:
        account_name = str(account_name)
        if account_name == "TRA Visibility Media Ad Account":
            return int(1276483386035120)
        elif account_name == "Capital Campaign":
            return int(285985605614201)
        elif account_name == "TNC Slicedbread":
            return int(414609479482979)
        elif account_name == "BLR Slicedbread":
            return int(435828907286515)
        elif account_name == "NSL Slicedbread":
            return int(448927332330936)
        elif account_name == "RFR Slicedbread":
            return int(321408348781798)
        elif account_name == "LSR Slicedbread":
            return int(1008801122784494)
        elif account_name == "GettysburgBattlefieldResort": # TRG
            return int(1654237951318967)
        elif account_name == "Wally World SlicedBread": # TRO
            return int(392394034937382)
        else:
            return int(0)
    except Exception as e:
        print(str(e))
        return int(0)

def get_program_id(account_name):
    try:
        # Get program Id  (Affiliates) for all ad_accounts
        account_name = str(account_name)
        if account_name == "TRA Visibility Media Ad Account":
            return int(1000000195)
        elif account_name == "Capital Campaign":
            return int(1000000195)
        elif account_name == "TNC Slicedbread":
            return int(1000000195)
        elif account_name == "BLR Slicedbread":
            return int(1000000195)
        elif account_name == "NSL Slicedbread":
            return int(1000000195)
        elif account_name == "RFR Slicedbread":
            return int(1000000195)
        elif account_name == "LSR Slicedbread":
            return int(1000000195)
        elif account_name == "GettysburgBattlefieldResort": # TRG
            return int(1000000195)
        elif account_name == "Wally World SlicedBread": # TRO
            return int(1000000195)
        else:
            return int(0)
    except Exception as e:
        print(str(e))
        return int(0)

def get_participant_id(account_name):
    try:
        account_name = str(account_name)
        # Get participant_id for particiapnt Visibility Media
        if account_name == "TRA Visibility Media Ad Account":
            return int(21388)
        # Get participant id for participant MI
        elif account_name == "Capital Campaign":
            return int(20269)
        # Get participant id for participant Slicebread
        elif account_name == "TNC Slicedbread":
            return int(18050)
        elif account_name == "BLR Slicedbread":
            return int(18050)
        elif account_name == "NSL Slicedbread":
            return int(18050)
        elif account_name == "RFR Slicedbread":
            return int(18050)
        elif account_name == "LSR Slicedbread":
            return int(18050)
        elif account_name == "GettysburgBattlefieldResort": # TRG
            return int(18050)
        elif account_name == "Wally World SlicedBread": # TRO
            return int(18050)
        else:
            return int(0)
    except Exception as e:
        print(str(e)) 
        return int(0)
      
def encode_agent_name(name):
    try:
#         return name.encode('ascii', errors='ignore').strip()
        return name.decode('utf-8').strip().replace('?', '')
    except Exception as e:
        return ""
#         return str(name)

# def check_on_date_and_ad_account_id(dw_conn, date, ad_account_id):
#     df = pd.read_sql("""
#     select * 
#     FROM [Warehouse01_old].[dbo].[Fact_fbcampaigndev]
#     where DateCreated = '{}' and ad_account_id = {}
#     """.format(date,ad_account_id),dw_conn)
#     if len(df) > 0:
#         return False
#     else:
#         return True

def check_on_date_and_ad_account_id(es, date, ad_account_id):
    try:
        query = {
        "query": { 
                "bool": { 
                  "must": [
                    { "match": {"adAccountId": ad_account_id}}
                  ],
                  "filter": [ 

                    { "range": { "dateCreated": { "gte": date,"lte":date}}}
                  ]
                }
              }
            }
        results = es.search(index = "fb-ad-spent-test", body=query)
        hits = results["hits"]["total"]['value']
        if hits > 0:
            return False
        else:
            return True
    except Exception as err:
        print(err)
    
def insert_into_fbCostFile(filename,PARTICIPANT):
    dw_conn = get_dw_conn(server,database,username,password)
    cursor = dw_conn.cursor()
    query = """
    INSERT INTO Fact_FbCostFilesDev VALUES('{}', '{}', '{}')
    """.format(filename, PARTICIPANT, str(datetime.now())[:10])
    cursor.execute(query)
    return dw_conn.commit()

def getTradbResortInfo(mongo_id):
    try:
        resortinfo = resortCollection.find_one({"_id":ObjectId(mongo_id)})
        resortName = resortinfo['resortName']
        return str(resortName)
    except:
        return ""

def getTradbTagsId(collection,campaignManagerId):
    tags_container=[]
    try:
        for return_doc in collection.find({'campaignManagerId':str(campaignManagerId)}):
            tag={}
            tag['tags']=return_doc['_id']
            tags_container.append(tag)
        return tags_container
    except:
        return tags_container
    
def getTradbTagsInfo(collection,tagsId):
    tags_json={}
    try:
        return_doc = collection.find_one({'_id':ObjectId(tagsId)})
        tags_json["campaignType"]=return_doc['contactType']
        tags_json["tag_mongoId"]=str(return_doc['_id'])
        tags_json["campaignManagerId"]=return_doc['campaignManagerId']
        tags_json["contactType"]=return_doc['contactType']
        tags_json["name"]=return_doc['name']
        tags_json["promotion"]=return_doc['promotion']
        tags_json["dnis"]=return_doc['dnis']
        tags_json["resort"]=getTradbResortInfo(str(return_doc['resort']))
        tags_json["isActive"]=return_doc['isActive']
        tags_json
        return tags_json
    except:
        return tags_json
    
def getTradbCampaignId(collection, tagId):
    try:
        return_doc = collection.find_one({"tags":{'$in' : [ObjectId(tagId)]}})
        return_doc
        return return_doc['_id']
    except :
        return None

def getTradbCampaignInfo(collection,campaignId):
    campaign_json={}
    try:
        return_doc = collection.find_one({"_id":ObjectId(campaignId)})
        campaign_json["participant"]=str(return_doc['participant'])
        campaign_json["program"]=str(return_doc['program'])
        return campaign_json
    except:
        return campaign_json
    
def calculate_spent_by_percent(totalAdSpent,percent):
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
            print(x)
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

def datetime_parsing(String_date):
    try:
        d = parser.parse(String_date).date()
        return(str(d))
    except:
        print("Error while parsing datetime")
        
def get_fb_leads(fb_resp):
    fb_lead=[]
    for i in range(0,len(fb_resp)):
        haveactiontype=False
        if 'actions' in fb_resp[i].keys():        
            for actions in fb_resp[i]['actions']:
                if actions['action_type'] == 'lead':
                    haveactiontype=True
                    fb_lead.append(str(actions['value']))

        if haveactiontype==False:
            fb_lead.append("0")
    return fb_lead

def get_serviceCharges_payload(fbData,serviceCharge):
    v=fbData
    serviceChargesJson={}
    try:
        serviceChargesJson["campaignManagerId"]=v.CampaignManagerId
        serviceChargesJson["LeadCorpWeek"]=v.CorpWeek
        serviceChargesJson["dateCreated"]=v.DateCreated
        serviceChargesJson["spent"]= float(serviceCharge) 
        serviceChargesJson["type"]= "spend"
        serviceChargesJson["subtype"]="service_charges" 
        serviceChargesJson["contactAttempts"]=getTradbTagsId(tagsCollection,int(v.CampaignManagerId))

        if len(serviceChargesJson["contactAttempts"]) > 0:
            t = serviceChargesJson["contactAttempts"]
            serviceChargesJson["campaign"]=getTradbCampaignId(campaignsCollection,str(str(t[0]["tags"])))
        else:
            serviceChargesJson["campaign"]=None
    except:
        print("Error while getting service charges payload")
    return serviceChargesJson

def get_marketingSpent_payload(fbData):
    v=fbData
    marketingSpendJson={}
    try:
        marketingSpendJson["campaignManagerId"]=v.CampaignManagerId
        marketingSpendJson["adId"]=v.adid
        marketingSpendJson["adName"]=v.AdName
        marketingSpendJson["adsetid"]=v.adsetid
        marketingSpendJson["adSetName"]=v.AdSetName
        marketingSpendJson["clicks"]=v.clicks
        marketingSpendJson["LeadCorpWeek"]=v.CorpWeek
        marketingSpendJson["dateCreated"]=v.DateCreated
        marketingSpendJson["fbCampaignId"]=v.CampaignId
        marketingSpendJson["impressions"]=v.impressions
        marketingSpendJson["spent"]=v.spent
        marketingSpendJson["subtype"]="marketing_spend"
        marketingSpendJson["type"]="spend"
        marketingSpendJson["leads"]=v.leads
        marketingSpendJson["adAccountId"]=v.ad_account_id
        marketingSpendJson["contactAttempts"]=getTradbTagsId(tagsCollection,int(v.CampaignManagerId)) 
        
        if len(marketingSpendJson["contactAttempts"]) > 0:
            t = marketingSpendJson["contactAttempts"]
            marketingSpendJson["campaign"]=getTradbCampaignId(campaignsCollection,str(str(t[0]["tags"])))
        else:
            marketingSpendJson["campaign"]=None
    except:
        print("Error while getting marketing spent payload")
    return marketingSpendJson

def generate_allParticipants_ads_spent(fb_costs):
    # FB Account Ids by Participants
    slicebread=[1654237951318967,414609479482979,1008801122784494,
    435828907286515,448927332330936,321408348781798,392394034937382] #List of Slicebread FB_Account_Ids
    visibility=[1276483386035120] #List of Vsisbility FB Account Id
    capital_campaign=[285985605614201] #List of MI FB Account Ids
    elasticIndexBulk=[]
    mongodbCollectionBulk=[]
    
    if fb_costs['ad_account_id'].iloc[0] in capital_campaign:
        mi_Campaign_list=[]
        active_campaigns = len(fb_costs.CampaignManagerId.unique())
        day_charge=257.1428571429 
        mi_service_charge = day_charge / active_campaigns
        print("MI Service Charges = {}%".format(mi_service_charge))

    for v in fb_costs.itertuples():
        serviceChargesJson={}
        if fb_costs['ad_account_id'].iloc[0] in slicebread:
            charges=0.25
            calculatedServChar = calculate_spent_by_percent(v.spent,charges)
            serviceChargesJson = get_serviceCharges_payload(v,calculatedServChar)
            mongodbCollectionBulk.append(serviceChargesJson)
        elif fb_costs['ad_account_id'].iloc[0] in visibility:
            charges=0.20
            calculatedServChar = calculate_spent_by_percent(v.spent,charges)
            serviceChargesJson = get_serviceCharges_payload(v,calculatedServChar)
            mongodbCollectionBulk.append(serviceChargesJson)
        elif fb_costs['ad_account_id'].iloc[0] in capital_campaign:
            if v.CampaignManagerId not in mi_Campaign_list:
                calculatedServChar = mi_service_charge
                serviceChargesJson = get_serviceCharges_payload(v,calculatedServChar)
                mi_Campaign_list.append(v.CampaignManagerId)
                mongodbCollectionBulk.append(serviceChargesJson)
       
        marketingSpendJson=get_marketingSpent_payload(v)
        mongodbCollectionBulk.append(marketingSpendJson)
        
    return(mongodbCollectionBulk)

# Service Charges for Good Sam are 25% of ad spent value = 0.25 * spent    
def generate_goodsam_ads_spent(fb_costs):
    elasticIndexBulk=[]
    mongodbCollectionBulk=[]
    charges=0.25
    print("Good Sam Service Charges = 25%")

    for v in fb_costs.itertuples():
                
        serviceChargesJson={}
        calculatedServChar = calculate_spent_by_percent(v.spent,charges)
        serviceChargesJson = get_serviceCharges_payload(v,calculatedServChar)
        mongodbCollectionBulk.append(serviceChargesJson)
        
        marketingSpendJson=get_marketingSpent_payload(v)
        mongodbCollectionBulk.append(marketingSpendJson)
        
    return(mongodbCollectionBulk)

def check_existing_files(db_conn,participant,filename):
    dw_conn = db_conn
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

# Business Details

#  414609479482979 : Slicedbread TNC 
# 1654237951318967 : Daily Slicebread/ Gettysburg battle field/TRG
# 1008801122784494 : Slicedbread LSR 
#  435828907286515 : Slicedbread BLR
#  448927332330936 : Slicedbread NSL
#  321408348781798 : Slicedbread RFR 
#  392394034937382 : Slicedbread TRO / Wallyworld slicebread 
#  285985605614201 = Capital Campaign (MI)
# 1276483386035120 = Visibility
# 2754621351247660 = RedRover

# ACCOUNT_ID = ['1276483386035120','285985605614201',
# '414609479482979','1654237951318967','1008801122784494',
# '435828907286515','448927332330936','392394034937382','321408348781798']