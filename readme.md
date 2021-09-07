# ![alt text](https://github.com/adam-p/markdown-here/raw/master/src/common/images/icon48.png "Github") **Facebook Ads Data Warehouse Management Services**
##### **CodeBook Author** : _Muhammad Tahir A._
##### **Contributors** : _Shah Fahad_, _Hassan Mehmood_, _Muhammad Tahir A.,
##### **CodeBook Created** : _2 Feb 2020_
##### **CodeBook Last Modified** : _07 Sep 2021_

##### **Description** : Load Ads Data into Warehouse | Scheduled for Daily | Traindex Elastic Ingestion | Tradb Mongodb Data Dumping | Failure Alert System  | Daily Ads Spent Automatic Report | Dynamic Facebook Campaigns Tags Linking Management System

##### **Required Fields** : "adAccountId" | "fbCampaignId" | "campaignManagerId" | "adid" | "adName" | "adSetid" | "LeadCorpWeek" | "dateCreated" | "spent" | "clicks" | "impressions" | "leads" | "cpc" |"frequency" | "cost_per_lead_lp" | "social_reach" | "post_engagement" | "campaign" | "contactAttempts" | "participantid" | "programid" | "subtype" | "type"

##### **Services** : There are four different services to load Ads Data into Warehouse
1. **Fb Ad Spent Service SB VM MI** : This Ad Spent Service loads Data for Participants Visibility Media, Slicebread, and Marketing Informatics.
2. **FB Ad Spent Service GOODSAM** : This Ad Spent Service loads Data for Participants GoodSam.
3. **Ad Spent Service Citadel** : This Ad Spent Service loads Data for Participants Citadel.
4. **Goodle Ad Spent Service VM** : This Ad Spent Service loads Data for Participants Visibility Media, Web automatically triggered from Google Ads Bulk Script on Scheduled Time.

##### **Spent Type** : Two Major types of spents load into warehouse.
1. **Marketing Spent** : Spent on Affiliate and Web ad.
2. **Service Charges** : Calculate by specific percentage of total Marketing Spent, Apply according to defined values for participant by Marketing Team.

##### **Major Rules** : 
1. **Calculate Service Charges** : Slicebread 25% of Marketing Spent, Visibility Media 15% of Marketing Spent, Marketing Informatics 20% of Marketing Spent

##### **Facebook Campaigns Tag Fixing Data** [FbCampaign Tag Fixing](https://ff623d98ea8c4f30bf0d2b5def828a1a.us-east-1.aws.found.io:9243/app/discover#/?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:'2021-09-06T04:00:00.000Z',to:'2021-09-07T03:30:00.000Z'))&_a=(columns:!(_source),filters:!(),index:'9a5d6010-0f4a-11ec-a900-6372c1cde672',interval:auto,query:(language:kuery,query:''),sort:!()))
##### **FB Campaigns Tag Linking Management API** [FastAPI](https://fb-ad-campaigns-tag-fixing.travelresorts.info/docs)