from dotenv import load_dotenv
import os

load_dotenv()

# Load MongoDB Connection Environmental Variables
tradbDev_str =  os.getenv("TRADBDEV_CONN_STRING")
tradbDev_db =  os.getenv("TRADBDEV_DB")
traDataDump_str =  os.getenv("TRADBDATADUMP_CONN_STRING")
traDataDump_db =  os.getenv("TRADBDATADUMP_DB")

# Load WareHouse Connection Environmental Variables
server =  os.getenv("SERVER")
database =  os.getenv("DATABASE")
username =  os.getenv("USERNAME")
password =  os.getenv("PASSWORD")

# Load FTP Connection Environmental Variables
ftp_path =  os.getenv("FTP_PATH")
ftp_username =  os.getenv("FTP_USERNAME")
ftp_password =  os.getenv("FTP_PASSWORD")

# Load Elasticsearch Connection Environmental Variables
elasticsearch_conn_str=os.getenv("ELASTICSEARCH_CONN_STRING")
FbAdSpentIndex = os.getenv("FB_AD_SPENT_INDEX")
FbAdSpentDoctype = os.getenv("FB_AD_SPENT_DOCTYPE")