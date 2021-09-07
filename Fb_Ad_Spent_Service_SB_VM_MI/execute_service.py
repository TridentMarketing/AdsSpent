import pandas as pd
from facepy import GraphAPI
from facepy.utils import get_extended_access_token
from jinja2 import Environment, select_autoescape, FileSystemLoader

from generals import *

env = Environment(loader=FileSystemLoader(
searchpath="templates"), autoescape=select_autoescape(['html', 'xml']))
missing_cmpg_template = env.get_template('teams_channel_alert_ad_spent_missing_campaign.html')

def template_message(template,date,
                     fbCampaignId=None,
                     flowId=None,
                     adAccount=None):
    body_html = template.render(date_trigger=date,
                                fbCampaignId=fbCampaignId,
                                flowId=flowId,
                                adAccount=adAccount)
    return body_html

try:
    LONG_LIVED_ACCESS_TOKEN, EXPIRES_AT = get_extended_access_token(ACCESS_TOKEN, APP_ID, APP_SECRETE)
except Exception as e:
    print(str(e))

def service_execution(accountIds_list,fileDate):
    for acc in accountIds_list:
        FB_Account= str(get_accountName_by_accountId(str(acc))) + "_" + str(acc)
        current_datetime = str(datetime.now().date())
        account_spent=0
        print("Pre-Processing FB Ad Spent Data for FbAccount :",FB_Account)
        try:
            RESP_ACCESS_TOKEN,RESP_TOKEN_TYPE = request_to_access_token(BASE_URL,APP_ID,APP_SECRETE,LONG_LIVED_ACCESS_TOKEN)  
            fb_resp = fb_Graph_api_data_request(BASE_URL,RESP_ACCESS_TOKEN,RESP_TOKEN_TYPE,fileDate,acc)
            if fb_resp != None:
                fb_resp = fb_resp.json()['data']
                fb_resp = get_payload(fb_resp)
                fbCampaignIds = list(set(list(str(x["campaign_id"]) for x in fb_resp)))
                campaignExistence_flag,campaignExistence_resp = account_fbcampaignids_existence_status(tradb,acc,
                                                                                                       fbCampaignIds)
                if campaignExistence_flag == True:
                    fb_costs = fb_data_pre_processing(fb_resp,campaignExistence_resp)
                    final_response = final_exe_push_changes(tradb,es,AD_SPEND_INDEX,
                                                            fb_costs,
                                                            campaignExistence_resp)
                    print("Done")
                else:
                    missing_fbCampaignIds_df = pd.DataFrame(campaignExistence_resp)
                    adAccount= str(campaignExistence_resp[0]["AccountName"]) + str(campaignExistence_resp[0]["AccountId"])
                    msg = (
                        " Alert! Facebook Ad Spent Service, Facebook Campaign Hasn't Linked to Tradb Tags. \n"
                        + missing_fbCampaignIds_df.to_html()
                    )
                    msg2 = (
                        "Alert! Failed to Dump Ad Spend Data for Specific Account Mentioned below\n"
                        + "Please Link Fb Campaigns to Matched Tags to Dump Data Successfully and Run Service Again\n"
                        + "FB Account ID"
                        + " : "
                        + str(campaignExistence_resp[0]["AccountId"])
                        + "\n"
                        + "FB Account Name"
                        + " : "
                        + str(campaignExistence_resp[0]["AccountName"])
                    )
                    hit_teams_channel_alert(msg)
                    hit_teams_channel_alert(msg2)

                    bulk=[]
                    for x in fb_resp:
                        payload={}
                        payload["FbCampaignId"] = str(x["campaign_id"])
                        payload["AdId"] = str(x["ad_id"])
                        payload["AdSetId"] = str(x["adset_id"])
                        bulk.append(payload)
                    fb_campaigns_ads_df = pd.DataFrame(bulk) 
                    result = pd.merge(missing_fbCampaignIds_df, right=fb_campaigns_ads_df, how='inner', on='FbCampaignId')
                    result.FbCampaignId = result.FbCampaignId.astype(str)
                    result.AdId = result.AdId.astype(str)
                    result.AdSetId = result.AdSetId.astype(str)
                    result.AccountId = result.AccountId.astype(str)
                    result.rename(columns=({"AccountId": "FbAccountId",
                                           "AccountName": "FbAccountName"}),inplace=True)
                    result["TraDbTagStatus"] = "Missing"
                    result["TraDbFlowId"] = ""

                    result.to_excel("Missing_FB_Campaign_Tag_Match_Export_"+str(fileDate)+".xlsx",index=False)
                    index_fb_campaign_missing_tags_details(es,
                                                           result,
                                                           FbCampaign_TAG_FIXING_INDEX)
            else:
                print('Request Failed To fetch Data From Graph Api For Fb Account : ',FB_Account)
        except Exception as e:
            print(e)