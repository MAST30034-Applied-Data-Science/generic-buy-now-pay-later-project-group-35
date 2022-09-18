from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from urllib.request import urlretrieve
from dotenv import load_dotenv
from owslib.wfs import WebFeatureService
import os
import pandas as pd
from pyspark.sql.types import *

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

# Join together the three lots of transaction data
transactiondf1 = spark.read.parquet("../data/tables/transactions_20210228_20210827_snapshot/")
transactiondf2 = spark.read.parquet("../data/tables/transactions_20210828_20220227_snapshot/")
transactiondf3 = spark.read.parquet("../data/tables/transactions_20220228_20220828_snapshot/")
transactiondf12 = transactiondf1.union(transactiondf2)
transactiondf = transactiondf12.union(transactiondf3)

userdf = spark.read.parquet("../data/tables/consumer_user_details.parquet")
consumerdf = spark.read.option("header","true").csv("../data/tables/tbl_consumer.csv", sep="|")
merchantdf = spark.read.parquet("../data/tables/tbl_merchants.parquet")

# Avoid clash of field names
consumerdf = consumerdf.withColumnRenamed("name","customer_name")
merchantdf = merchantdf.withColumnRenamed("name","company_name")

# Replace all square brackets with round brackets
merchantdf = merchantdf.withColumn('tags', regexp_replace('tags', '\\[', '\\('))
merchantdf = merchantdf.withColumn('tags', regexp_replace('tags', '\\]', '\\)'))

# Extract take rate into seperate column
merchantdf = merchantdf.withColumn("take_rate", 
                                   split(col("tags"), "\\),").getItem(2))\
                       .withColumn('take_rate', 
                                   regexp_replace('take_rate', 'take rate: ', 
                                                  ''))\
                       .withColumn('take_rate', 
                                   regexp_replace('take_rate', '\\(', ''))\
                       .withColumn('take_rate', 
                                   regexp_replace('take_rate', '\\)', ''))

# Extract revenue band
merchantdf = merchantdf.withColumn("revenue_band", 
                                   split(col("tags"), "\\),").getItem(1))\
                       .withColumn('revenue_band', 
                                   regexp_replace('revenue_band', '\\(', ''))\
                       .withColumn('revenue_band', 
                                   regexp_replace('revenue_band', '\\)', ''))

# Extract tags band
merchantdf = merchantdf.withColumn("tags", 
                                   split(col("tags"), "\\),").getItem(0))\
                       .withColumn('tags', 
                                   regexp_replace('tags', '\\(', ''))\
                       .withColumn('tags', 
                                   regexp_replace('tags', '\\)', ''))\
                       .withColumn('tags', 
                                   regexp_replace('tags', ' +', ' '))\
                       .withColumn('tags', 
                                   lower('tags'))

# Merge together
mergedf = transactiondf.join(userdf, "user_id")
mergedf = mergedf.join(consumerdf, "consumer_id")
mergedf = mergedf.join(merchantdf, "merchant_abn")

# Postcode to SA2 Dataset
url = "https://raw.githubusercontent.com/matthewproctor/australianpostcodes/master/australian_postcodes.csv"
file_path = "../data/tables/australian_postcodes.csv"
urlretrieve(url, file_path)

postcodedf = spark.read.option("header","true").csv("../data/tables/australian_postcodes.csv")
postcodedf = postcodedf.select("postcode","SA2_MAINCODE_2016")
postcodedf = postcodedf.withColumnRenamed("SA2_MAINCODE_2016","sa2_code")
postcodedf = postcodedf.dropna("any")
postcodedf = postcodedf.distinct()

### JOIN TO DATASET
###
###
###
###

# SA2 income dataset
load_dotenv('../cred.env')
user_name = os.environ.get('USERNAME')
password = os.environ.get('PASSWORD')
url = 'https://adp.aurin.org.au/geoserver/wfs'
adp_client = WebFeatureService(url=url,username=user_name, password=password, version='2.0.0')
response = adp_client.getfeature(typename="datasource-AU_Govt_ABS-UoM_AURIN_DB_3:abs_personal_income_total_income_sa2_2011_2018", 
                                 outputFormat='csv')
out = open("../data/tables/datasource-AU_Govt_ABS-UoM_AURIN_DB_3_abs_personal_income_total_income_sa2_2011_2018.csv", 'wb')
out.write(response.read())
out.close()

incomedf = spark.read.option("header","false").csv("../data/tables/datasource-AU_Govt_ABS-UoM_AURIN_DB_3_abs_personal_income_total_income_sa2_2011_2018.csv")
incomedf = incomedf.select("_c2","_c10","_c17","_c31","_c38")
incomedf = incomedf.withColumnRenamed("_c2","sa2_code")
incomedf = incomedf.withColumnRenamed("_c10","num_earners")
incomedf = incomedf.withColumnRenamed("_c17","median_age")
incomedf = incomedf.withColumnRenamed("_c31","median_income")
incomedf = incomedf.withColumnRenamed("_c38","mean_income")
for field in ('num_earners',"median_age","median_income","mean_income"):
    incomedf = incomedf.withColumn(
        field,
        col(field).cast('int')
    )
incomedf = incomedf.dropna("all")
incomedf = incomedf.filter(incomedf["median_age"] > 20)
incomedf = incomedf.filter(incomedf["median_age"] < 70)

# SA2 population dataset
url = "https://www.abs.gov.au/statistics/people/population/regional-population-age-and-sex/2021/32350DS0001_2021.xlsx"
file_path = f"../data/tables/pop.xlsx"
urlretrieve(url, file_path)

skip = list(range(7)) + [8] + list(range(2481, 2490)) + [2480]

fields_2b_renamed = ['S/T name', 'no.']

field_names = ['S/T name', 'SA2 code', 'SA2 name', 'no.']

for i in range (1, 19):
    string = 'no..' + str(i)
    fields_2b_renamed.append(string)
    field_names.append(string)

rename_to = ['State/Terr']
for i in range(0, 86, 5):
    col_name = "age "

    if i == 85:
        col_name += f"{i}+"
        rename_to.append(col_name)
        continue

    col_name += f"{i}-{i+4}"
    rename_to.append(col_name)

rename_to.append('Total')

rename_cols = dict(zip(fields_2b_renamed, rename_to))

pop_df = pd \
    .read_excel(
        '../data/tables/pop.xlsx',
        sheet_name = 'Table 3',
        skiprows = skip,
    ) \
    .get(field_names) \
    .rename(columns = rename_cols)
drop_cols = [string for string in pop_df.columns if string[:3] == 'age']

groups = {
    'Under 10': [0, 10],
    'Adolescent': [10, 20],
    'Young adult': [20, 35],
    'Middle age': [35, 60],
    'Old': [60, 86]
}

for group, ages in groups.items():

    age_sum = 0
    for i in range(ages[0], ages[1], 5):

        if i == 85:
            age_range_str = f"age {i}+"
        else:
            age_range_str = f"age {i}-{i+4}"
            
        age_sum += pop_df[age_range_str]
        
    pop_df[group] = age_sum

pop_df_mod = pop_df.drop(axis=0, columns=drop_cols)
pop_df_mod = pop_df_mod.convert_dtypes()
pop_df_mod.dtypes
pop_df_mod[pop_df_mod.isnull().any(axis=1)]
pop_df_mod = pop_df_mod.dropna()

mySchema = StructType([
    StructField("State/Terr", StringType()),
    StructField("SA2 code", StringType()),
    StructField("SA2 name", StringType()),
    StructField("Total", IntegerType()),
    StructField("Under 10", IntegerType()),
    StructField("Adolescent", IntegerType()),
    StructField("Young adult", IntegerType()),
    StructField("Middle age", IntegerType()),
    StructField("Old", IntegerType())
])
pop_sdf = spark.createDataFrame(
    pop_df_mod,
    mySchema
)

### JOIN TO DATASET
###
###
###
###

# Consumer fraud
consumerfrauddf = spark.read.option("header","True").csv("../data/tables/consumer_fraud_probability.csv")
consumerfrauddf = consumerfrauddf.withColumn("fraud_probability", col("fraud_probability").cast('double'))
consumerfrauddf = consumerfrauddf.filter(consumerfrauddf['fraud_probability'] > 50)
mergedf = mergedf.join(consumerfrauddf, ['order_datetime', 'user_id'], "leftanti")

# Merchant fraud
merchantfrauddf = spark.read.option("header","True").csv("../data/tables/merchant_fraud_probability.csv")
merchantfrauddf = merchantfrauddf.withColumn("fraud_probability", col("fraud_probability").cast('double'))
merchantfrauddf = merchantfrauddf.filter(merchantfrauddf['fraud_probability'] > 33.33)
mergedf = mergedf.join(merchantfrauddf, ['order_datetime', 'merchant_abn'], "leftanti")

mergedf.write.parquet('../data/curated/mergedf.parquet')