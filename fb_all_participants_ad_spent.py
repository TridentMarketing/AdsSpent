# Contributors: Muhammad Tahir and Shah Fahad, Hassan Mehmood
# Date last Modified: 25. March, 2021
# Descrition: This notebook will insert Slicedbread,Visibility,Capital Campaign (MI) FB Costs to Warehouse.

import pandas as pd
from datetime import datetime
import time
import requests
from requests import get,post
import sys
import os
import json
from facepy import GraphAPI
from facepy.utils import get_extended_access_token
import petl as etl
import pprint

from .general_helper import *

# Attaching Corp Week Id using Dim_CorpWeek table in warehouse
dw_conn = get_dw_conn(server,database,username,password)
# Converting datetime object to Corp Week Id
corp_weeks = json.load(open('corp_weeks.json'))

LIMIT = '100000'
GRAPH_API_VERSION = "v9.0"
# List of All FB Account Ids
ACCOUNT_ID = [1654237951318967,414609479482979,
              1008801122784494,435828907286515,
              448927332330936,321408348781798,392394034937382,
              1276483386035120, 
              285985605614201]
try:
    long_lived_access_token, expires_at = get_extended_access_token(access_token, app_id, app_secrete)
except Exception as e:
    print(str(e))

try:
    for acc in ACCOUNT_ID:
        # Get request to fb to get access_token and token_type from response.
        resp = requests.get(base_url +"/oauth/access_token?"+ \
                            "grant_type=fb_exchange_token"+ \
                            "&client_id="+app_id+ \
                            "&client_secret="+app_secrete+ \
                            "&fb_exchange_token="+long_lived_access_token)

        resp = resp.json()
        resp_access_token = resp['access_token']
        resp_token_type = resp['token_type']
        
        # Get request to fb on ad_level to get desired fields from response.
        fb_resp = requests.get(base_url + "/"+ GRAPH_API_VERSION + "/act_" + str(acc) + \
                               "/insights?pretty=True&level=ad" \
                               "&fields=campaign_name,campaign_id,ad_name,adset_id,ad_id,reach,adset_name,inline_post_engagement," \
                               "impressions,frequency,spend,clicks,cpc,account_id,account_name,cost_per_action_type,actions&time_range={'since':'2021-03-22','until':'2021-03-22'}" \
                               "&level=ad&limit="+LIMIT+ \
                               "&access_token="+resp_access_token+"&token_type="+resp_token_type)

        fb_resp = fb_resp.json()['data']
        fb_costs = pd.DataFrame(fb_resp)
        fb_lead = get_fb_leads(fb_resp)
        fb_costs['leads'] = fb_lead

        try:
            # Special handling for getting cost_per_action_type(action_type:lead) from nested response if found
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

        print(len(fb_costs), 'Number of Rows.')
        if len(fb_costs) == 0:
            print('0 Rows found, Nothing to insert in warehouse \n')
        else:
            fb_costs = fb_costs.rename(columns={
                        'Campaign ID': 'CampaignId', 'campaign_id': 'CampaignId', 'Ad Set ID': 'adsetid',
                        'Ad set ID': 'adsetid', 'adset_id': 'adsetid','Ad ID': 'adid', 'ad_id': 'adid' ,'Ad Set Name': 'AdSetName',
                        'Ad set name': 'AdSetName', 'adset_name': 'AdSetName' , 'Frequency':'frequency',
                        'Ad Name': 'AdName', 'Ad name': 'AdName', 'ad_name': 'AdName', 'Reach': 'social_reach', 'reach': 'social_reach',
                        'Impressions': 'impressions', 'impressions': 'impressions', 
                        'Amount Spent (USD)': 'spent', 'Amount spent (USD)': 'spent', 'spend':'spent',
                        'CPC (All)': 'cpc', 'CPC (all)': 'cpc','Page Likes': 'page_likes', 
                        'Post Engagement': 'post_engagement', 'inline_post_engagement':'post_engagement' , 'Post engagement': 'post_engagement',
                        'Video Plays': 'video_views','Video plays': 'video_views', 'Cost per Lead': 'cost_per_lead_lp',
                        'cost_per_lead_action': 'cost_per_lead_lp', 'Page likes': 'page_likes', 
                        'Leads (Form)': 'leads_form', 'Leads (form)': 'leads_form', 'Clicks (All)': 'clicks', 'Clicks (all)': 'clicks', 
                        'Campaign Name': 'CampaignName', 'Campaign name': 'CampaignName', 'campaign_name': 'CampaignName'})

            # Converting date string into datetime object
            fb_costs['date_stop'] = pd.to_datetime(fb_costs['date_stop'])
            fb_costs['CorpWeek'] = fb_costs[['date_stop']] \
                .apply(lambda x: corp_weeks.get(str(x[0])[:10], 0), axis=1)
            
            Dim_CorpWeek = pd.read_sql("""
            SELECT * FROM Dim_CorpWeek""", dw_conn)
            Dim_Date = pd.read_sql("""
            SELECT * from Dim_Date""", dw_conn)

            Dim_CorpWeek.CorpWeek = Dim_CorpWeek.CorpWeek.apply(int)
            fb_costs = pd.merge(left=fb_costs, right=Dim_CorpWeek, how='left', on='CorpWeek')
            fb_costs['date'] = fb_costs[['date_stop']].apply(lambda x: str(x[0])[:10], axis=1)
            Dim_Date.date = Dim_Date.date.apply(str)
            fb_costs = pd.merge(left=fb_costs, right=Dim_Date, on='date', how='left')
            fb_costs = fb_costs.rename(columns={'DateId': 'dim_dateid'})

            REQUIRED_COLUMNS = ['CampaignName', 'CampaignId', 'adsetid', 'adid',
                                'AdSetName', 'AdName','social_reach', 'impressions', 'frequency',
                                'spent', 'cpc', 'post_engagement','cost_per_lead_lp', 'clicks', 'CorpWeekId',
                                'dim_dateid','account_name','date_stop','CorpWeek','leads'] #(page_likes,video_views)

            fb_costs = fb_costs[REQUIRED_COLUMNS]
            fb_costs['dim_participantid'] = fb_costs[['account_name']]  \
                    .apply(lambda x: get_participant_id(x[0]), axis=1)

            fb_costs['dimProgramId'] = fb_costs[['account_name']]  \
                    .apply(lambda x: get_program_id(x[0]), axis=1)

            fb_costs['CampaignManagerId'] = fb_costs \
                    .apply(lambda x: get_campaign_manager_id(x['account_name'], x['AdName']), axis=1)

            fb_costs['ad_account_id'] = fb_costs[['account_name']]  \
                    .apply(lambda x: get_ad_account_id(x[0]), axis=1)

            fb_costs['resort'] = ''
            for i in ['CampaignName', 'adsetid', 'adid', 'AdSetName', 'AdName', 'resort']:
                fb_costs[i] = fb_costs[i].fillna('') 

            for i in ['social_reach', 'impressions', 'CampaignManagerId', 'post_engagement',
                     'clicks', 'CorpWeekId', 'dim_dateid', 'dim_participantid', 'dimProgramId',
                     'ad_account_id','CorpWeek','leads']: #(video_views,page_likes)
                fb_costs[i] = fb_costs[i].fillna(0).replace('', 0).apply(int)

            for i in ['frequency', 'spent', 'cpc', 'post_engagement', 'cost_per_lead_lp']: #(video_views)
                fb_costs[i] = fb_costs[i].fillna(0.0).replace('', 0.0).apply(float)

            # decoding ascii's
            fb_costs['AdSetName'] = fb_costs[['AdSetName']] \
                .apply(lambda x: encode_agent_name(x[0]).title(), axis=1)

            # total spent 
            spent = sum(fb_costs.spent)
            # delete account_name from dataframe
            del fb_costs['account_name']
            fb_costs.rename(columns=({"date_stop": "DateCreated"}),inplace=True)
            DateCreated = fb_costs['DateCreated'].iloc[0]
            ad_account_id = fb_costs['ad_account_id'].iloc[0]
            
            # Check on date and ad_account_id so we dont duplicate ad spent cost for a day.
            try:
                status = check_on_date_and_ad_account_id(es,DateCreated,ad_account_id)
                if status == True:
                    print("pushing data for ad account id: {} | date: {} | {}").format(ad_account_id,DateCreated,spent)
                    payloadBulk = generate_allParticipants_ads_spent(fb_costs)
                    # Insert in MongoDb and Elasticsearch
                    mongodb_many_to_many_insert(adsSpentCollection,payloadBulk)
                    elasticBulk=[]
                    for v in payloadBulk:
                        mongoid = str(v['_id'])
                        del v['_id']
                        v['mongo_id'] = mongoid
                        v['campaign'] = getTradbCampaignInfo(campaignsCollection,str(v['campaign']))
                        v['dateCreated'] = v['dateCreated'].isoformat()
                        for contA in v['contactAttempts']:
                            contA['tags']=getTradbTagsInfo(tagsCollection,str(contA['tags']))
                        elasticBulk.append(json.dumps(v))
                    adSpentElasticIndexing(es, payloadBulk, FbAdSpentIndex, FbAdSpentDoctype)
                    
                else:
                    print("Record already has been Dumped, ad account id: {} | spent: {} | date: {} \n").format(ad_account_id,spent,DateCreated)
            except Exception as e:
                print(str(e))
except Exception as e:
    print(e)