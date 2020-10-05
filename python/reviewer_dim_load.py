'''
Author: Vaisakh
'''
#importing needed libs
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import unix_timestamp, from_unixtime, to_date, when
from pyspark.sql.types import DateType
from pyspark.sql import DataFrameWriter
from pyspark.sql import DataFrameReader
from pyspark import SparkContext
import os

#reading env variable
projdir=os.environ['PROJDIR']

#Setting up spark
spark = SparkSession.builder.appName('ReviewsOdsSession').getOrCreate()


#Read from DB
#mode = "overwrite"
url = "jdbc:postgresql://" + os.environ['PGHOST'] + "/" + os.environ['DB_DWH']
properties = {"user": os.environ['PGUSER'], "password": os.environ['PGPASSWD'] , "driver": 'org.postgresql.Driver'}

table1 = os.environ['SCHEMA_CORE'] + ".REVIEWER_DIM" 
Reviewer_Core_df = spark.read.jdbc(url=url, table=table1, properties=properties)

table2 = "(SELECT DISTINCT reviewerid,reviewername FROM " + os.environ['SCHEMA_STG'] + ".REVIEWS LIMIT 1000) AS reviews_ods"
Reviews_Ods_df = spark.read.jdbc(url=url, table=table2, properties=properties)
Reviews_Ods_df.show(n=20)
#Selecting only columns we need
#Reviews_Ods_df_2 = Reviews_Ods_df.select("reviewerid","reviewername").distinct()
#Reviews_Ods_df_2.show(n=10)



