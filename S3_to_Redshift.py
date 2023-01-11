import redshift_connector
import time

conn = redshift_connector.Connection(
    host='redshift-cluster-1.cvgrsdqcjget.ap-northeast-1.redshift.amazonaws.com',
    database='dev',
    user='awsuser',
    password='Password123'
    )

conn.autocommit = True

cursor= redshift_connector.Cursor = conn.cursor()

cursor.execute("""
CREATE TABLE "factCovid" (
"index" INTEGER,
  "fips" REAL,
  "province_state" TEXT,
  "country_region" TEXT,
  "confirmed" REAL,
  "deaths" REAL,
  "recovered" REAL,
  "active" REAL,
  "date" INTEGER,
  "positive" REAL,
  "negative" REAL,
  "hospitalizedcurrently" REAL,
  "hospitalized" REAL,
  "hospitalizeddischarged" REAL
)
""")

cursor.execute("""
copy factCovid from 's3://adesh-project-1/output/factCovid.csv'
credentials 'aws_iam_role=arn:aws:iam::225172342551:role/redshift_s3'
delimiter ','
region 'ap-northeast-1'
IGNOREHEADER 1
""")

cursor.execute("""
CREATE TABLE "dimHospital" (
"index" INTEGER,
  "fips" REAL,
  "state_name" TEXT,
  "latitude" REAL,
  "longtitude" REAL,
  "hq_address" TEXT,
  "hospital_name" TEXT,
  "hospital_type" TEXT,
  "hq_city" TEXT,
  "hq_state" TEXT
)
""")

cursor.execute("""
CREATE TABLE "dimDate" (
"index" INTEGER,
  "fips" INTEGER,
  "date" TIMESTAMP,
  "year" INTEGER,
  "month" INTEGER,
  "day_of_week" INTEGER
)
""")

cursor.execute("""
copy dimHospital from 's3://adesh-project-1/output/dimHospital.csv'
credentials 'aws_iam_role=arn:aws:iam::225172342551:role/redshift_s3'
delimiter ','
region 'ap-northeast-1'
IGNOREHEADER 1
""")

cursor.execute("""
copy dimDate from 's3://adesh-project-1/output/dimDate.csv'
credentials 'aws_iam_role=arn:aws:iam::225172342551:role/redshift_s3'
delimiter ','
region 'ap-northeast-1'
IGNOREHEADER 1
""")

cursor.execute("""
CREATE TABLE "dimRegion" (
"index" INTEGER,
  "fips" REAL,
  "province_state" TEXT,
  "country_region" TEXT,
  "latitude" REAL,
  "longitude" REAL,
  "county" TEXT,
  "state" TEXT
)
""")

cursor.execute("""
copy dimRegion from 's3://adesh-project-1/output/dimRegion.csv'
credentials 'aws_iam_role=arn:aws:iam::225172342551:role/redshift_s3'
delimiter ','
region 'ap-northeast-1'
IGNOREHEADER 1
""")