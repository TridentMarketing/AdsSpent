import pandas as pd
from facepy import GraphAPI
from facepy.utils import get_extended_access_token

from generals import *

try:
    LONG_LIVED_ACCESS_TOKEN, EXPIRES_AT = get_extended_access_token(
        ACCESS_TOKEN, APP_ID, APP_SECRETE
    )
except Exception as e:
    print(str(e))


def service_execution(accountIds_list, fileDate):
    try:
        for acc in accountIds_list:
            RESP_ACCESS_TOKEN, RESP_TOKEN_TYPE = request_to_access_token(
                BASE_URL, APP_ID, APP_SECRETE, LONG_LIVED_ACCESS_TOKEN
            )
            fb_resp = fb_Graph_api_data_request(
                BASE_URL, RESP_ACCESS_TOKEN, RESP_TOKEN_TYPE, fileDate, acc
            )
            if fb_resp != None:
                fb_resp = fb_resp.json()["data"]
                fb_resp = get_payload(fb_resp)
                fbCampaignIds = list(set(list(str(x["campaign_id"]) for x in fb_resp)))
                (
                    campaignExistence_flag,
                    campaignExistence_resp,
                ) = account_fbcampaignids_existence_status(
                    tradbProd, acc, fbCampaignIds
                )
                if campaignExistence_flag == True:
                    fb_costs = fb_data_pre_processing(fb_resp, campaignExistence_resp)
                    final_response = final_exe_push_changes(
                        fb_costs, campaignExistence_resp
                    )

                    return (campaignExistence_resp, final_response)
                else:
                    missing_fbCampaignIds_df = pd.DataFrame(campaignExistence_resp)
                    msg = (
                        "Test Alert! Ad Spent Service, Facebook CampaignId Don't Exist in Tradb Tags Collection:\n"
                        + missing_fbCampaignIds_df.to_html()
                    )
                    msg2 = (
                        "Test Alert! Failed to Dump Ad Spend Data for Specific Account Mentioned below\n"
                        + "Please Impute Missing Tags Details Match for Facebook Camapigns\n"
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
                    bulk = []
                    for x in fb_resp:
                        payload = {}
                        payload["FbCampaignId"] = str(x["campaign_id"])
                        payload["AdId"] = str(x["ad_id"])
                        payload["AdSetId"] = str(x["adset_id"])
                        bulk.append(payload)
                    fb_campaigns_ads_df = pd.DataFrame(bulk)
                    result = pd.merge(
                        missing_fbCampaignIds_df,
                        right=fb_campaigns_ads_df,
                        how="inner",
                        on="FbCampaignId",
                    )
                    result.FbCampaignId = result.FbCampaignId.astype(str)
                    result.AdId = result.AdId.astype(str)
                    result.AdSetId = result.AdSetId.astype(str)
                    result.AccountId = result.AccountId.astype(str)
                    result["TraDbTagStatus"] = "Missing"
                    result.to_excel(
                        "Missing_FB_Campaign_Tag_Match_Export_"
                        + str(fileDate)
                        + ".xlsx",
                        index=False,
                    )
                    index_fb_campaign_missing_tags_details(
                        esProd, result, FB_Campaign_TAG_FIX_INDEX
                    )
                    return fb_resp
            else:
                print(
                    "Request Failed To fetch Data From Graph Api, Nothing to Insert In Database \n"
                )

    except Exception as e:
        print(e)