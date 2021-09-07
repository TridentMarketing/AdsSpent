import pandas as pd
from datetime import datetime
import requests
from dateutil import parser
import json
import sys
import os
import logging
import petl as etl
from pprint import pprint
from connection import * 

corp_weeks = json.load(open('corp_weeks.json'))

def request_to_access_token(BASE_URL,APP_ID,APP_SECRETE,LONG_LIVED_ACCESS_TOKEN):
    try:
        resp = requests.get(BASE_URL + \
                            "/oauth/access_token?"+"grant_type=fb_exchange_token"+"&client_id="+ APP_ID+ \
                            "&client_secret="+ APP_SECRETE+ \
                            "&fb_exchange_token="+ LONG_LIVED_ACCESS_TOKEN)
        if resp != None:
            resp = resp.json()
            RESP_ACCESS_TOKEN = resp['access_token']
            RESP_TOKEN_TYPE = resp['token_type']
            return RESP_ACCESS_TOKEN,RESP_TOKEN_TYPE
        else:
            print("Failed To Process Request to Access Token")
        
    except Exception as exp:
        print("Exception",exp)
        
def fb_Graph_api_data_request(BASE_URL,
                              RESP_ACCESS_TOKEN,
                              RESP_TOKEN_TYPE,
                              DATE,FB_ACCOUNTID):
            
    since="'since': '{}'".format(DATE)
    until = "'until': '{}'".format(DATE)
    DATE_RANGE= "{"+since+","+until+"}"
    LIMIT = '100000'
    GRAPH_API_VERSION = "v11.0"

    # Get request to fb on ad_level to get desired fields from response.
    resp = requests.get(BASE_URL + "/"+ GRAPH_API_VERSION +
                        "/act_" + str(FB_ACCOUNTID) + \
                        "/insights?pretty=True&level=ad" \
                        "&fields=campaign_name,campaign_id,ad_name,adset_id,ad_id,reach,adset_name,inline_post_engagement," \
                        "impressions,frequency,spend,clicks,cpc,account_id," \
                        "account_name,cost_per_action_type,actions&time_range="+ DATE_RANGE+\
                        "&level=ad&limit="+ LIMIT+ \
                        "&access_token="+RESP_ACCESS_TOKEN+ \
                        "&token_type="+RESP_TOKEN_TYPE)
    return resp

def get_payload(fb_resp):
    try:
        for fbr in fb_resp:
            int_details = ['reach', 'impressions', 'inline_post_engagement','clicks']
            float_details = ['frequency', 'spend', 'cpc', 'inline_post_engagement', 'cost_per_action_type']
            str_detaisl = ['resort']
            for i in int_details:
                if i not in fbr.keys():
                    fbr[i] = 0
            for i in float_details:
                if i not in fbr.keys():
                    fbr[i] = 0.0
            for i in str_detaisl:
                if i not in fbr.keys():
                    fbr[i] = ''
                
        return fb_resp
    except:
        return []
    
def renaming_dataframe_columns(df):
    df = df.rename(columns={
    'Campaign ID': 'CampaignId', 'campaign_id': 'CampaignId', 'Ad Set ID': 'adsetid',
    'Ad set ID': 'adsetid', 'adset_id': 'adsetid','Ad ID': 'adid', 'ad_id': 'adid' ,'Ad Set Name': 'AdSetName',
    'Ad set name': 'AdSetName', 'adset_name': 'AdSetName' , 'Frequency':'frequency',
    'Ad Name': 'AdName', 'Ad name': 'AdName', 'ad_name': 'AdName', 'Reach': 'social_reach', 'reach': 'social_reach',
    'Impressions': 'impressions', 'impressions': 'impressions', 
    'Amount Spent (USD)': 'spent', 'Amount spent (USD)': 'spent', 'spend':'spent',
    'CPC (All)': 'cpc', 'CPC (all)': 'cpc','Page Likes': 'page_likes', 
    'Post Engagement': 'post_engagement', 'inline_post_engagement':'post_engagement' ,
    'Post engagement': 'post_engagement','Video Plays': 'video_views',
    'Video plays': 'video_views', 'Cost per Lead': 'cost_per_lead_lp',
    'cost_per_lead_action': 'cost_per_lead_lp', 'Page likes': 'page_likes', 
    'Leads (Form)': 'leads_form', 'Leads (form)': 'leads_form', 'Clicks (All)': 'clicks', 'Clicks (all)': 'clicks', 
    'Campaign Name': 'CampaignName', 'Campaign name': 'CampaignName',
    'campaign_name': 'CampaignName','cost_per_action_type': 'cost_per_lead_action'})
    return df

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

def get_tag_by_fbCampaignId(db,fbCampaignId):
    resp = db.tags.find_one({"fbCampaignIds":{"$in":[fbCampaignId]}})
    if resp != None:
        return resp
    else:
        msg=("High Alert ! Ad Spent Service, Facebook CampaignId Don't Exist in Tradb Tags Collection "+ str(fbCampaignId))
        print(msg)
        
def get_tradb_campaignInfo_by_tagid(db,tagId):
    try:
        return_doc = db.campaigns.find_one({"tags":{'$in' : [ObjectId(tagId)]}},
                                           {"participant":1,"program":1,"_id":1})
        if return_doc != None:
            campaign_json={}
            campaign_json["campaignId"]=str(return_doc['_id'])
            campaign_json["participant"]=str(return_doc['participant'])
            campaign_json["program"]=str(return_doc['program'])
            return campaign_json
        else: 
            return {}
    except:
        return {}
    
def account_fbcampaignids_existence_status(db,accountId,fbCampaignIds):
    campaignInfo_json={}
    missing_fbCampaignIds=[]
    flag=True
    for x in fbCampaignIds:
        r = get_tag_by_fbCampaignId(db,str(x))
        if r != None:
            cmpinfo_resp = get_tradb_campaignInfo_by_tagid(db,str(r["_id"]))
            r["campaignId"] = cmpinfo_resp["campaignId"]
            r["program"] = cmpinfo_resp["program"]
            r["participant"] = cmpinfo_resp["participant"] 
            campaignInfo_json[str(x)] = r
        else:
            flag=False
            miss_cmpids_json={}
            miss_cmpids_json["AccountId"] = accountId
            miss_cmpids_json["AccountName"] = get_accountName_by_accountId(str(accountId))
            miss_cmpids_json["FbCampaignId"] = x 
            missing_fbCampaignIds.append(miss_cmpids_json)
            
    if flag == True:
        return flag,campaignInfo_json
    else:
        return flag,missing_fbCampaignIds   
    
def get_accountName_by_accountId(accountId):
    accountId = str(accountId)
    payload={"1276483386035120":"TRA Visibility Media Ad Account",
             "285985605614201":"Capital Campaign",
             "503236307794730":"Marketing Informatics",
             "414609479482979":"TNC Slicedbread",
             "435828907286515":"BLR Slicedbread",
             "448927332330936":"NSL Slicedbread",
             "321408348781798":"RFR Slicedbread",
             "1008801122784494":"LSR Slicedbread",
             "1654237951318967":"TRG Slicedbread",
             "392394034937382":"TRO Slicedbread"}
    resp = payload.get(accountId,"")
    return resp

def get_accountId_by_accountName(accountName):
    payload={"TRA Visibility Media Ad Account":"1276483386035120",
             "Capital Campaign":"285985605614201",
             "Marketing Informatics":"503236307794730",
             "TNC Slicedbread":"414609479482979",
             "BLR Slicedbread":"435828907286515",
             "NSL Slicedbread":"448927332330936",
             "RFR Slicedbread":"321408348781798",
             "LSR Slicedbread":"1008801122784494",
             "GettysburgBattlefieldResort":"1654237951318967",
             "Wally World SlicedBread":"392394034937382"}
    resp = payload.get(accountName,0)
    return resp

def get_programId_by_accountName(accountName):
    payload={"TRA Visibility Media Ad Account":"1000000195",
             "Capital Campaign":"1000000195",
             "Marketing Informatics":"1000000195",
             "TNC Slicedbread":"1000000195",
             "BLR Slicedbread":"1000000195",
             "NSL Slicedbread":"1000000195",
             "RFR Slicedbread":"1000000195",
             "LSR Slicedbread":"1000000195",
             "GettysburgBattlefieldResort":"1000000195",
             "Wally World SlicedBread":"1000000195"}
    resp = payload.get(accountName,0)
    return resp

def get_participantId_by_accountName(accountName):
    payload={"TRA Visibility Media Ad Account":"21388",
             "Capital Campaign":"20269",
             "Marketing Informatics":"20269",
             "TNC Slicedbread":"18050",
             "BLR Slicedbread":"18050",
             "NSL Slicedbread":"18050",
             "RFR Slicedbread":"18050",
             "LSR Slicedbread":"18050",
             "GettysburgBattlefieldResort":"18050",
             "Wally World SlicedBread":"18050"}
    resp = payload.get(accountName,0)
    return resp
    
def encode_agent_name(name):
    try:
        return name.decode('utf-8').strip().replace('?', '')
    except Exception as e:
        return ""

def extract_campaign_manager_id(fbCampaignId,payload):
    try:      
        payload = payload.get(str(fbCampaignId),"")
        campaignManagerId = str(payload.get("campaignManagerId",0))
        return int(campaignManagerId)
        
    except:
        print ('ERROR while fetching campaign manager id from ad name')
        return int(0)
    
def extract_tradb_tagId(fbCampaignId,payload):
    try:      
        payload = payload.get(str(fbCampaignId),{})
        _tagId = payload.get("_id",None)
        return _tagId
    except:
        print ('ERROR while Extracting TraDb TagId from Campaigns Info Payload')
        return  ""
    
def extract_tradb_campaignId(fbCampaignId,payload):
    try:      
        payload = payload.get(str(fbCampaignId),{})
        _campaignId = payload.get("campaignId",None)
        return _campaignId
    except:
        print ('ERROR while Extracting TraDb campaignId from Campaigns Info Payload')
        return ""
    
def extract_tags_required_info(fbCampaignId,payload):
    tags_json={}
    try:
        payload = payload.get(str(fbCampaignId),{})
        tags_json["description"]=payload.get("description","")
        tags_json["campaignType"]=str(payload.get("campaignType",""))
        tags_json["tag_mongoId"]=str(payload.get("_id",""))
        tags_json["campaignManagerId"]=str(payload.get("campaignManagerId",""))
        tags_json["contactType"]=str(payload.get("contactType",""))
        tags_json["name"]=str(payload.get("name",""))
        tags_json["promotion"]=str(payload.get("promotion",""))
        tags_json["dnis"]=str(payload.get("dnis",""))
        tags_json["isActive"]=payload.get("isActive","")
        tags_json["isRV"]=payload.get("isRV","")
        tags_json["metaState"]=str(payload.get("metaState",""))
        tags_json["listSource"]=payload.get("listSource","")
        tags_json["dropDate"]=str(payload.get("dropDate",""))
        tags_json["dealerSource"]=str(payload.get("dealerSource",""))
        tags_json["source"]=str(payload.get("source",""))
        tags_json["channel"]=str(payload.get("channel",""))
        tags_json["website"]=str(payload.get("website",""))
        tags_json["medium"]=str(payload.get("medium",""))
        tags_json["vendor"]=str(payload.get("vendor",""))
        tags_json["corpWeekStart"]=payload.get("corpWeekStart","")
        tags_json["resort"]=str(getTradbResortInfo(tradb,str(payload.get("resort",""))))

        return tags_json
    except:
        return tags_json     
    
def extract_campaigns_required_info(fbCampaignId,payload):
    tags_json={}
    try:
        payload = payload.get(str(fbCampaignId),{})
        tags_json["program"] = payload.get("program")
        tags_json["participant"] = payload.get("participant")
        return tags_json
    except:
        return tags_json 
     

def check_on_date_and_ad_account_id(es,indexName,date,ad_account_id):
    try:
        query = {
            "query": { 
                "bool": { 
                  "must": [
                      {"match": {"adAccountId": ad_account_id}},
                      {"range": {"dateCreated": {"gte": datetime_parsing(str(date)),
                                                   "lte":datetime_parsing(str(date))}}}
                  ]
                }
            }
        }
        results = es.search(index = indexName , body=query)
        hits = results["hits"]["total"]['value']
        if hits > 0:
            return False
        else:
            return True
    except Exception as err:
        print(err)  

def getTradbResortInfo(db,mongo_id):
    try:
        resortinfo = db.resorts.find_one({"_id":ObjectId(mongo_id)})
        resortName = resortinfo['resortName']
        return str(resortName)
    except:
        return ""
        
def calculate_spent_by_percent(totalAdSpent,percent):
    try:
        if (totalAdSpent != 0.0):
            charges = totalAdSpent * percent
            return charges
        else:
            return 0
    except Exception as e:
        print("Error:",e)

def adSpentElasticIndexing(es, adSpentDataBulk,indexName):
    try:
        response = helpers.bulk(es, adSpentDataBulk,
                                index=indexName,
                                doc_type='_doc')

        print ("\nActions RESPONSE Dumped Docs:", response[0])
        return response[0]

    except Exception as err:
        error_msg = "Elasticsearch index() ERROR:"
        print(error_msg, err)

        return None
    
def get_adSetname_socialInbox(data):
    request_url="https://socialinbox.apps.travelresorts.com/api/getBulkAdSetInfos"
    headers = {'content-type': 'application/json'}
    
    payload={}
    for index in range(0, len(data), 50):
        chunk_ids = data[index:index+50]
        d={"ids": chunk_ids}
        resp = requests.post(url=request_url, data=json.dumps(d),headers=headers)
        if resp.status_code == 200:
            resp_json = resp.json()
            for x in resp_json:
                adSetName = str(x['name'])
                payload[str(x["id"])] = adSetName
        else:
            print("Don't get AdSetName")
            
    return payload

def get_serviceCharges_payload(fbData,serviceCharge,):
    v=fbData
    serviceChargesJson={}
    try:
        serviceChargesJson["campaignManagerId"]=v.CampaignManagerId
        serviceChargesJson["adid"]=v.adid   
        serviceChargesJson["adName"]=v.AdName
        serviceChargesJson["adSetid"]=v.adsetid
        serviceChargesJson["adSetName"]= v.AdSetName
        serviceChargesJson["clicks"]=v.clicks
        serviceChargesJson["LeadCorpWeek"]=v.CorpWeek
        serviceChargesJson["dateCreated"]=v.DateCreated
        serviceChargesJson["fbCampaignId"]=v.CampaignId
        serviceChargesJson["impressions"]=v.impressions
        serviceChargesJson["spent"]= float(serviceCharge) 
        serviceChargesJson["type"]= "spend"
        serviceChargesJson["subtype"]="service_charges" 
        serviceChargesJson["leads"]=v.leads
        serviceChargesJson["adAccountId"]=v.ad_account_id
        serviceChargesJson["contactAttempts"]= [{"tags" : ObjectId(v.traDb_tagId)}]
        serviceChargesJson["cpc"]=v.cpc
        serviceChargesJson["frequency"]=v.frequency
        serviceChargesJson["cost_per_lead_lp"]=v.cost_per_lead_lp
        serviceChargesJson["social_reach"]=v.social_reach
        serviceChargesJson["post_engagement"]=v.post_engagement
        serviceChargesJson["campaignName"]=v.CampaignName
        serviceChargesJson["participantid"]=v.dim_participantid
        serviceChargesJson["ProgramId"]=v.dimProgramId
        serviceChargesJson["campaign"]=ObjectId(v.traDb_camapignId)

    except:
        print("Error while getting service charges payload")
    return serviceChargesJson

def get_marketingSpent_payload(fbData):
    v=fbData
    marketingSpendJson={}
    try:
        marketingSpendJson["campaignManagerId"]=v.CampaignManagerId
        marketingSpendJson["adid"]=v.adid
        marketingSpendJson["adName"]=v.AdName
        marketingSpendJson["adSetid"]=v.adsetid
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
        marketingSpendJson["contactAttempts"]= [{"tags" : ObjectId(v.traDb_tagId)}]
        marketingSpendJson["cpc"]=v.cpc
        marketingSpendJson["frequency"]=v.frequency
        marketingSpendJson["cost_per_lead_lp"]=v.cost_per_lead_lp
        marketingSpendJson["social_reach"]=v.social_reach
        marketingSpendJson["post_engagement"]=v.post_engagement
        marketingSpendJson["campaignName"]=v.CampaignName
        marketingSpendJson["participantid"]=v.dim_participantid
        marketingSpendJson["ProgramId"]=v.dimProgramId
        marketingSpendJson["campaign"]=ObjectId(v.traDb_camapignId)
    except:
        print("Error while getting marketing spent payload")
       
    return marketingSpendJson

def generate_allParticipants_ads_spent(fb_costs):
    
    # FB Account Ids by Participants
    slicebread=[1654237951318967,414609479482979,1008801122784494,
    435828907286515,448927332330936,321408348781798,392394034937382] #List of Slicebread FB_Account_Ids
    visibility=[1276483386035120] #List of Vsisbility FB Account Id
    capital_campaign=[285985605614201,503236307794730] #List of MI FB Account Ids
    
    elasticIndexBulk=[]
    mongodbCollectionBulk=[]

    for v in fb_costs.itertuples():
        serviceChargesJson={}
        if fb_costs['ad_account_id'].iloc[0] in slicebread:
            charges=0.25
            calculatedServChar = calculate_spent_by_percent(v.spent,charges)
            serviceChargesJson = get_serviceCharges_payload(v,calculatedServChar)
            mongodbCollectionBulk.append(serviceChargesJson)
        elif fb_costs['ad_account_id'].iloc[0] in visibility:
            charges=0.15
            calculatedServChar = calculate_spent_by_percent(v.spent,charges)
            serviceChargesJson = get_serviceCharges_payload(v,calculatedServChar)
            mongodbCollectionBulk.append(serviceChargesJson)
        elif fb_costs['ad_account_id'].iloc[0] in capital_campaign:
            charges=0.20
            calculatedServChar = calculate_spent_by_percent(v.spent,charges)
            serviceChargesJson = get_serviceCharges_payload(v,calculatedServChar)
            mongodbCollectionBulk.append(serviceChargesJson)
            
       
        marketingSpendJson=get_marketingSpent_payload(v)
        mongodbCollectionBulk.append(marketingSpendJson)
        
    return(mongodbCollectionBulk)

def traindex_traDb_check(db,mongo_id):
    r = {}
    r = db.spentData.find_one({"_id":ObjectId(mongo_id)})
    if str(r.get("_id","")) != "":
        return True
    else:
        return False

def hit_teams_channel_alert(msg):
    try:
        teamsConnector.text(msg)
        return teamsConnector.send()
    except:
        print("Error While sending alert To MSTeams Channel") 
    return
  
def index_fb_campaign_missing_tags_details(es,df,indexName):
    bulk=[]
    for x in df.itertuples():
        payload={}
        payload["FbAccountId"] = str(x.FbAccountId)
        payload["FbAccountName"] = str(x.FbAccountName)
        payload["FbCampaignId"] = str(x.FbCampaignId)
        payload["AdId"] = str(x.AdId)
        payload["AdSetId"] = str(x.AdSetId)
        payload["TraDbTagStatus"] = "Missing"
        payload["TraDbFlowId"] = ""
        payload["e_timestamp"] = datetime.now().isoformat()
        
        status = check_fbcampaigns_with_missing_tags_match(es,
                                                           indexName,
                                                           payload["FbAccountId"],
                                                           payload["FbCampaignId"],
                                                           payload["AdId"],
                                                           payload["TraDbTagStatus"])

        if status == False:
            bulk.append(payload)
        
    try:
        response = helpers.bulk(es,
                                bulk,
                                index=indexName,
                                doc_type="_doc")

        print ("\nActions RESPONSE Dumped Docs:", response[0])
        return response[0]

    except Exception as err:
        error_msg = "Elasticsearch index() ERROR:"
        print(error_msg, err)

        return None

def check_fbcampaigns_with_missing_tags_match(es,
                                              indexName,
                                              fbAccountId,
                                              fbCampaignId,
                                              adId,
                                              traDbTagStatus):
    try:
        query = {
            "query": { 
                "bool": { 
                  "must": [
                      {
                          "term": {"FbAccountId": int(fbAccountId) }
                      },
                      {
                          "term": {"FbCampaignId.keyword": str(fbCampaignId)}
                      },
                      {
                          "term": {"AdId.keyword": str(adId)}
                      },
                      {
                          "term": {"TraDbTagStatus.keyword": traDbTagStatus}
                      }
                  ]
                }
            }
        }
        results = es.search(index = indexName , body=query)
        hits = results["hits"]["total"]['value']
        if hits > 0:
            return True
        else:
            return False
    except Exception as err:
        print(err) 
        
def sum_of_spent_by_participants():
    payload={
        "SLICEBREAD":{"Ingestion_Status":"Successful",
                      "Ingested_Spent":0,
                      "Missing_Spent":0},
        
        "MARKETING INFORMATICS":{"Ingestion_Status":"Successful",
                                 "Ingested_Spent":0,
                                 "Missing_Spent":0},
        
        "VISIBILITY MEDIA":{"Ingestion_Status":"Successful",
                            "Ingested_Spent":0,
                            "Missing_Spent":0}
    }
    
    return payload
    
def fb_data_pre_processing(fb_resp,campaignsInfo_payload):
    print('Number of Records Fetched from facebook Graph API : ' + str(len(fb_resp)))
    REQUIRED_COLUMNS = ['CampaignName', 'CampaignId', 'adsetid', 'adid',
                        'AdSetName', 'AdName','social_reach', 'impressions', 'frequency',
                        'spent', 'cpc', 'post_engagement','cost_per_lead_lp', 'clicks', 'CorpWeekId',
                        'dim_dateid','account_name','date_stop','CorpWeek','leads']
    
    fb_costs = pd.DataFrame(fb_resp)
    fb_lead = get_fb_leads(fb_resp)
    fb_costs['leads'] = fb_lead

    try:
        del fb_costs['cost_per_action_type']
        container = []
        for i in range(0,len(fb_resp)): 
            cpat_values = '0.0'
            try:
                for cpat in fb_resp[i]['cost_per_action_type']:
                    if cpat['action_type'] == 'lead':
                        cpat_values = str(cpat['value'])          
                container.append(cpat_values)
            except:
                container.append('0.0')
        fb_costs['cost_per_lead_action'] = container
    except:
        pass

    fb_costs = renaming_dataframe_columns(fb_costs)
    fb_costs['date_stop'] = pd.to_datetime(fb_costs['date_stop'])
    fb_costs['CorpWeek'] = fb_costs[['date_stop']] \
        .apply(lambda x: corp_weeks.get(str(x[0])[:10], 0), axis=1)
    Dim_CorpWeek = pd.read_sql("""SELECT * FROM Dim_CorpWeek""", dataWarehouse)
    Dim_Date = pd.read_sql("""SELECT * from Dim_Date""", dataWarehouse)
    Dim_CorpWeek.CorpWeek = Dim_CorpWeek.CorpWeek.apply(int)
    fb_costs = pd.merge(left=fb_costs, right=Dim_CorpWeek, how='left', on='CorpWeek')
    fb_costs['date'] = fb_costs[['date_stop']].apply(lambda x: str(x[0])[:10], axis=1)
    Dim_Date.date = Dim_Date.date.apply(str)
    fb_costs = pd.merge(left=fb_costs, right=Dim_Date, on='date', how='left')
    fb_costs = fb_costs.rename(columns={'DateId': 'dim_dateid'})
    fb_costs = fb_costs[REQUIRED_COLUMNS]
    fb_costs['dim_participantid'] = fb_costs['account_name'].apply(
        lambda x: get_participantId_by_accountName(x))
    fb_costs['dimProgramId'] = fb_costs['account_name'].apply(
        lambda x: get_programId_by_accountName(x))
    fb_costs['ad_account_id'] = fb_costs['account_name'].apply(
        lambda x: get_accountId_by_accountName(x))
    fb_costs['CampaignManagerId'] = fb_costs['CampaignId'].apply(
        lambda x: extract_campaign_manager_id(x,campaignsInfo_payload))
    fb_costs['resort'] = ''
    for i in ['CampaignName', 'adsetid', 'adid', 'AdSetName', 'AdName', 'resort']:
        fb_costs[i] = fb_costs[i].fillna('') 

    for i in ['social_reach', 'impressions', 'CampaignManagerId', 'post_engagement',
             'clicks', 'CorpWeekId', 'dim_dateid', 'dim_participantid', 'dimProgramId',
             'ad_account_id','CorpWeek','leads']:
        fb_costs[i] = fb_costs[i].fillna(0).replace('', 0).apply(int)

    for i in ['frequency', 'spent', 'cpc', 'post_engagement', 'cost_per_lead_lp']:
        fb_costs[i] = fb_costs[i].fillna(0.0).replace('', 0.0).apply(float)

    # decoding ascii's
    fb_costs['AdSetName'] = fb_costs[['AdSetName']] \
        .apply(lambda x: encode_agent_name(x[0]).title(), axis=1)

    # delete account_name from dataframe
    del fb_costs['account_name']
    fb_costs.rename(columns=({"date_stop": "DateCreated"}),inplace=True)
    fb_costs["traDb_tagId"] = fb_costs['CampaignId'].apply(
        lambda x: extract_tradb_tagId(x,campaignsInfo_payload))
    fb_costs["traDb_camapignId"] = fb_costs['CampaignId'].apply(
        lambda x: extract_tradb_campaignId(x,campaignsInfo_payload))

    return fb_costs

def final_exe_push_changes(db,
                           es,
                           adSpentIndex,
                           df,
                           campaignsInfo_payload):
    spent = sum(df.spent)
    dateCreated = df['DateCreated'].iloc[0]
    ad_account_id = df['ad_account_id'].iloc[0]
    status = check_on_date_and_ad_account_id(es,adSpentIndex,dateCreated,ad_account_id)
    if status == True:
        try:
            print("Date Report: {} | Facebook Spent: {}").format(dateCreated,spent)
            payloadBulk = generate_allParticipants_ads_spent(df)
            adsetid_list=[]
            for x in payloadBulk:
                adsetid =  str(x['adSetid'])
                if adsetid not in adsetid_list:
                    adsetid_list.append(adsetid)

            resp = get_adSetname_socialInbox(adsetid_list)
            
            marketing_charges=0
            service_charges=0
            for x in payloadBulk:
                x['adSetName'] = resp[str(x['adSetid'])]
                if x['subtype'] == "marketing_spend":
                    marketing_charges = marketing_charges + x['spent']
                elif x["subtype"] == "service_charges":
                    service_charges = service_charges + x['spent']
            print("Total Marketing Spent after Completing Transformation Process",marketing_charges)
            print("Total Service Charges after Completing Transformation Process",service_charges)

            elasticBulk=[]
            for v in payloadBulk:
                elastic_dateCreated = v['dateCreated']
                v['dateCreated'] = datetime_parsing(str(elastic_dateCreated))
                elastic_dateCreated = elastic_dateCreated.isoformat() + 'Z'
                elastic_dateCreated = elastic_dateCreated.replace("Z", "-05:00")

                try:
                    x = db.spentData.insert(v)
                except Exception as e:
                    print("Error While Dumping Doc in MongoDb" , e)
                    print(v)

                if traindex_traDb_check(db,x) == True:
                    mongoid = str(v['_id'])
                    del v['_id']

                    v['mongo_id'] = mongoid
                    v['dateCreated'] = elastic_dateCreated
                    v['campaign'] = extract_campaigns_required_info(v["fbCampaignId"],campaignsInfo_payload)
                    for contA in v['contactAttempts']:
                        contA['tags']=extract_tags_required_info(v["fbCampaignId"],campaignsInfo_payload)

                    elasticBulk.append(v)
                else:
                    print("Error While Dumping Doc in TraDb MongoDb")

            if len(elasticBulk) > 0:
                print("Elastic Ingestion Container Length",len(elasticBulk))
                adSpentElasticIndexing(es, elasticBulk, adSpentIndex)
                return elasticBulk
                
        except Exception as e:
            print(str(e))

    else:
        print("Records has already been Dumped,"+
              "ad account id: {} | spent: {} | date: {} \n").format(ad_account_id,
                                                                    spent,
                                                                    dateCreated)