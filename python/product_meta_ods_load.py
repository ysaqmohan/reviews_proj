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
spark = SparkSession.builder.appName('ProdMeta').getOrCreate()

#establising source files
ProdMetajson = "/edw/Reviews/SourceCode/data/asin_metadata.json"

#read data into a frame
Prod_df = spark.read.json(ProdMetajson)

#tranformed frame
Prod_Meta_df = Prod_df.select('asin',col('brand').substr(1,20).alias("brand"),'price', col('title').substr(1,70).alias("title"), col('categories').getItem(0).getItem(0).alias("categories")).where(col('asin').isNotNull())

#DB write
mode = "append"
table = os.environ['SCHEMA_STG'] + ".PRODMETA"
url = "jdbc:postgresql://" + os.environ['PGHOST'] + "/" + os.environ['DB_DWH']
properties = {"user": os.environ['PGUSER'], "password": os.environ['PGPASSWD'] , "driver": 'org.postgresql.Driver'}
Prod_Meta_df.write.jdbc(url=url, table=table, mode=mode, properties=properties)
