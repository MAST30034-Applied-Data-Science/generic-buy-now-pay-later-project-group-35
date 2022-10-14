from pyspark.sql import SparkSession
from urllib.request import urlretrieve
from dotenv import load_dotenv
from owslib.wfs import WebFeatureService
import os
import requests
import io
import zipfile
from owslib.util import Authentication
import urllib3

# Create a spark session
spark = (
    SparkSession.builder.appName("download")
    .config("spark.sql.repl.eagerEval.enabled", True) 
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "6g")
    .getOrCreate()
)

# IF ERRORS SUCH AS 'NO SUCH FILE OR DIRECTORY' OCCUR, TRY CHANGING FOLLOWING VALUE TO TRUE
RELATIVE_PATH_TOGGLE = True

if RELATIVE_PATH_TOGGLE:
    RELATIVE_DIR = "data/"
    CRED_DIR = "cred.env"
else:
    RELATIVE_DIR = "../data/"
    CRED_DIR = "../cred.env"

# SA2 Population Dataset: Saved as 'population.xlsx' in data/tables
POPULATION_URL = "https://www.abs.gov.au/statistics/people/population/" + \
                 "regional-population-age-and-sex/2021/32350DS0001_2021.xlsx"
POPULATION_FILE_PATH = f"{RELATIVE_DIR}tables/population.xlsx"
urlretrieve(POPULATION_URL, POPULATION_FILE_PATH)

# Postcode Dataset: Saved as 'australian_postcodes.csv' in data/tables
POSTCODE_URL = "https://raw.githubusercontent.com/matthewproctor/" + \
               "australianpostcodes/master/australian_postcodes.csv"
POSTCODE_FILE_PATH = f"{RELATIVE_DIR}tables/australian_postcodes.csv"
urlretrieve(POSTCODE_URL, POSTCODE_FILE_PATH)

# Postcode Ratio Dataset: Saved as '1270055006_CG_POSTCODE_2011_SA2_2011.xls'
url = "https://www.abs.gov.au/AUSSTATS/subscriber.nsf/" + \
"\log?openagent&1270055006_CG_POSTCODE_2011_SA2_2011.zip&" + \
"1270.0.55.006&Data%20Cubes&70A3CE8A2E6F9A6BCA257A29001979B2&0&" + \
"July%202011&27.06.2012&Latest"
r = requests.get(url, stream=True)
z = zipfile.ZipFile(io.BytesIO(r.content))
z.extractall(f"{RELATIVE_DIR}tables")

# SA2 Income Dataset: Saved as 'datasource-AU_Govt_ABS-UoM_AURIN_DB_3_abs_
# personal_income_total_income_sa2_2011_2018.csv' in data/tables
INCOME_FILE_NAME = "datasource-AU_Govt_ABS-UoM_AURIN_DB_3:abs_personal_" + \
                   "income_total_income_sa2_2011_2018"
load_dotenv(CRED_DIR)
user_name = os.environ.get('USERNAME')
password = os.environ.get('PASSWORD')
url = 'https://adp.aurin.org.au/geoserver/wfs'
auth = Authentication(verify=False)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
adp_client = WebFeatureService(url=url,username=user_name, password=password, 
                               version='2.0.0', auth=auth)
response = adp_client.getfeature(typename=INCOME_FILE_NAME, outputFormat='csv')
out = open(f"{RELATIVE_DIR}tables/{INCOME_FILE_NAME}.csv", 'wb')
out.write(response.read())
out.close()
