'''
Author: Vaisakh
'''
#imprting needed libs
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.functions import unix_timestamp, from_unixtime
from pyspark.sql import DataFrameWriter
#SparkConf, SparkContext, SQLContext

#Setting up spark
spark = SparkSession.builder.appName('BasicProd').getOrCreate()

#establising source files
ProdMetajson = "/edw/Reviews/SourceCode/data/asin_meta.json"

# Create an sql context so that we can query data files in sql like syntax
#sqlContext = SQLContext (sparkcontext)

Prod_df = spark.read.json(ProdMetajson)

#View for Reviews dataframe
Prod_df.createOrReplaceTempView("Prod_df")
#Prod_df.show(n=10)
#Prod_df.printSchema()

#Prod_Meta_df = Prod_df.select('asin','brand','price', 'title', col('categories').getItem(1))
#Prod_Meta_df = Prod_df.select('asin','brand','price', 'title', when(col('categories').getItem(1).isNull, col('categories').getItem(1)).otherwise(when(col('categories').getItem(1).getItem(1).isNull, col('categories').getItem(1)).otherwise(col('categories').getItem(1).getItem(1))))
Prod_Meta_df = Prod_df.select('asin','brand','price', col('title').substr(1,70).alias('title'), col('categories').getItem(0).getItem(0).alias("categories")).where(col('asin').isNotNull())
Prod_Meta_df.write.csv('/edw/Reviews/SourceCode/data/ProdMeta.csv')
#Cl_Reviews_df = Reviews_df.select('asin',col('overall').alias("reviewScore"),'reviewerID',(from_unixtime('unixReviewTime', 'yyyy-mm-dd')).alias("reviewTime"))

#mode = "overwrite"
#url = "jdbc:postgresql://127.0.0.1/DEV_ODS"
#properties = {"user": "postgres","password": "Susi@123","driver": 'org.postgresql.Driver'}
#Cl_Reviews_df.write.jdbc(url=url, table="REVIEWS_STAGING", mode=mode, properties=properties)
