#!/usr/bin/env python
# coding: utf-8

# In[1]:


import boto3
import pandas as pd
from io import StringIO


# In[2]:


AWS_ACCESS_KEY = "Access_key"
AWS_SECRET_KEY = "Secret_key"
AWS_REGION = "Region"
SCHEMA_NAME = "Schema_name"
S3_STAGING_DIR = "staging_area_location"
S3_BUCKET_NAME = "Bucket_name"
S3_OUTPUT_DIRECTORY = "o/p_dir"


# In[3]:


athena_client = boto3.client(
    "athena",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION,
)


# In[5]:


Dict = {}
def download_and_load_query_results(
    client:boto3.client, query_response: Dict
) -> pd.DataFrame:
    while True:
        try:
            # this function only loads the first 1000 rows
            client.get_query_results(
                 QueryExecutionId=query_response["QueryExecutionId"]
            )
            break
        except Exceptation as err:
            if "not yet finished" in str(err):
                time.sleep(0.001)
            else:
                 raise err
    temp_file_location: str = "athena_query_results.csv"
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
    )
    s3_client.download_file(
        S3_BUCKET_NAME,
        f"{S3_OUTPUT_DIRECTORY}/{query_response['QueryExecutionId']}.csv",
        temp_file_location,
    )
    return pd.read_csv(temp_file_location)


# In[4]:


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM enigma_jhud",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)


# In[6]:


response


# In[7]:


enigma_jhud = download_and_load_query_results(athena_client, response)


# In[8]:


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM nytimes_data_in_us_countryus_county",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)


# In[9]:


nytimes_data_in_us_countryus_county = download_and_load_query_results(athena_client, response)


# In[10]:


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM nytimes_data_in_usa_statesus_states",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)


# In[11]:


nytimes_data_in_usa_statesus_states = download_and_load_query_results(athena_client, response)


# In[12]:


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM rearc_covid_19_testing_data_states_dailystates_daily",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)


# In[13]:


rearc_covid_19_testing_data_states_dailystates_daily = download_and_load_query_results(athena_client, response)


# In[14]:


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM rearc_covid_19_testing_data_us_dailyus_daily",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)


# In[15]:


rearc_covid_19_testing_data_us_dailyus_daily = download_and_load_query_results(athena_client, response)


# In[16]:


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM rearc_covid_19_testing_data_us_total_latestus_total_latest",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)


# In[17]:


rearc_covid_19_testing_data_us_total_latestus_total_latest = download_and_load_query_results(athena_client, response)


# In[18]:


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM rearc_usa_hospital_bedsrearc_usa_hospital_beds",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)


# In[19]:


rearc_usa_hospital_bedsrearc_usa_hospital_beds = download_and_load_query_results(athena_client, response)


# In[20]:


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM static_datasets_countrycodecountrycode",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)


# In[21]:


static_datasets_countrycodecountrycode = download_and_load_query_results(athena_client, response)


# In[22]:


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM static_datasets_countypopulationcountypopulation",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)


# In[23]:


static_datasets_countypopulationcountypopulation = download_and_load_query_results(athena_client, response)


# In[24]:


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM static_datasets_state_abvstate_abv",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)


# In[25]:


static_datasets_state_abvstate_abv = download_and_load_query_results(athena_client, response)


# In[39]:


static_datasets_state_abvstate_abv.head()


# In[26]:


new_header = static_datasets_state_abvstate_abv.iloc[0]
static_datasets_state_abvstate_abv = static_datasets_state_abvstate_abv[1:]
static_datasets_state_abvstate_abv.columns = new_header


# In[27]:


static_datasets_state_abvstate_abv.head()


# In[27]:


factCovid_1 = enigma_jhud[['fips','province_state','country_region','confirmed','deaths','recovered','active']]
factCovid_2 = rearc_covid_19_testing_data_states_dailystates_daily[['fips','date','positive','negative','hospitalizedcurrently','hospitalized','hospitalizeddischarged']]
factCovid = pd.merge(factCovid_1, factCovid_2, on='fips', how='inner')


# In[29]:


factCovid.shape


# In[28]:


dimRegion_1 = enigma_jhud[['fips','province_state','country_region','latitude','longitude']]
dimRegion_2 = nytimes_data_in_us_countryus_county[['fips','county','state']]
dimRegion = pd.merge(dimRegion_1, dimRegion_2, on='fips', how= 'inner')


# In[31]:


dimRegion.head()


# In[29]:


dimHospital = rearc_usa_hospital_bedsrearc_usa_hospital_beds[['fips','state_name','latitude','longtitude','hq_address','hospital_name','hospital_type','hq_city','hq_state']]


# In[30]:


dimDate = rearc_covid_19_testing_data_states_dailystates_daily[['fips','date']]


# In[31]:


dimDate['date'] = pd.to_datetime(dimDate['date'], format='%Y%m%d')


# In[32]:


dimDate['year'] = dimDate['date'].dt.year
dimDate['month'] = dimDate['date'].dt.month
dimDate['day_of_week'] = dimDate['date'].dt.dayofweek


# In[36]:


dimDate.head()


# In[ ]:


bucket = 'adesh-project-1'


# In[ ]:


csv_buffer = StringIO()
factCovid.to_csv(csv_buffer)
s3_resource = boto3.resource("s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION)
s3_resource.Object('adesh-project-1', 'output/factCovid.csv').put(Body=csv_buffer.getvalue())


# In[ ]:


csv_buffer = StringIO()
dimRegion.to_csv(csv_buffer)
s3_resource = boto3.resource("s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION)
s3_resource.Object('adesh-project-1', 'output/dimRegion.csv').put(Body=csv_buffer.getvalue())


# In[ ]:


csv_buffer.getvalue()


# In[ ]:


csv_buffer = StringIO()
dimHospital.to_csv(csv_buffer)
s3_resource = boto3.resource("s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION)
s3_resource.Object('adesh-project-1', 'output/dimHospital.csv').put(Body=csv_buffer.getvalue())


# In[ ]:


csv_buffer = StringIO()
dimDate.to_csv(csv_buffer)
s3_resource = boto3.resource("s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION)
s3_resource.Object('adesh-project-1', 'output/dimDate.csv').put(Body=csv_buffer.getvalue())


# In[37]:


dimHospitalsql = pd.io.sql.get_schema(dimHospital.reset_index(), 'dimHospital')
print(''.join(dimHospitalsql))


# In[38]:


factCovidsql = pd.io.sql.get_schema(factCovid.reset_index(), 'factCovid')
print(''.join(factCovidsql))


# In[39]:


dimRegionsql = pd.io.sql.get_schema(dimRegion.reset_index(), 'dimRegion')
print(''.join(dimRegionsql))


# In[40]:


dimDatesql = pd.io.sql.get_schema(dimDate.reset_index(), 'dimDate')
print(''.join(dimDatesql))
