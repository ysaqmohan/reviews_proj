'''
Author: Vaisakh
'''
#importing needed libs
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import unix_timestamp, from_unixtime, to_date, when
from pyspark.sql.types import DateType
from pyspark.sql import DataFrameWriter
from pyspark import SparkContext
import os

#reading env variable
projdir=os.environ['PROJDIR']

#Setting up spark
spark = SparkSession.builder.appName('ReviewsOdsSession').getOrCreate()

#establising source files
AmzRevjson = "/edw/Reviews/SourceCode/data/asin_reviews.json"

#read data into a frame
Reviews_df = spark.read.json(AmzRevjson)

#tranformed frame
Cl_Reviews_df = Reviews_df.select('asin',col('overall').alias("reviewRating"),'reviewerID','reviewerName', (when(col('unixReviewTime').isNull(), '1990-01-01').otherwise(from_unixtime('unixReviewTime', 'yyyy-MM-dd'))).cast(DateType()).alias("reviewDate")).filter(col("reviewDate")>='2011-01-01')

#Write to DB
mode = "append"
table = os.environ['SCHEMA_STG'] + ".reviews"
url = "jdbc:postgresql://" + os.environ['PGHOST'] + "/" + os.environ['DB_DWH']
properties = {"user": os.environ['PGUSER'], "password": os.environ['PGPASSWD'] , "driver": 'org.postgresql.Driver'}
Cl_Reviews_df.write.jdbc(url=url, table=table, mode=mode, properties=properties)
