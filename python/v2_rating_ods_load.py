'''
Author: Vaisakh
'''
#importing needed libs
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import unix_timestamp, from_unixtime, to_date, when, current_timestamp
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

#Taking only distinct values of Reviewer name and Id from the dataframe
Cl_Reviews_Subset_df = Cl_Reviews_df.select('reviewerID', 'reviewerName').distinct()

#Reading from customer dimension to update customer dimension
table1 = os.environ['SCHEMA_CORE'] + ".REVIEWER_DIM"
Reviewer_Core_df = spark.read.jdbc(url=url, table=table1, properties=properties)

#New dimesnional records for reviewer
Insert_Reviewer_df = Cl_Reviews_Subset_df.join(Reviewer_Core_df, Cl_Reviews_df.reviewerID == Reviewer_Core_df.reviewer_id, how = 'left').filter(col('reviewer_id').isNull()).select(col('reviewerID').alias("reviewer_id"),col('reviewerName').alias("reviewer_nm")).withColumn("add_td", current_timestamp()).withColumn("modified_td", current_timestamp())

#Change in dimension for reviewer
Update_Reviewer_df = Cl_Reviews_Subset_df.join(Reviewer_Core_df, Cl_Reviews_df.reviewerID == Reviewer_Core_df.reviewer_id).filter((col('reviewer_nm') != col('reviewerName')) & col('reviewerName').isNotNull()).select(col('reviewerID').alias("reviewer_id"),col('reviewerName').alias("reviewer_nm"),'add_td').withColumn("modified_td", current_timestamp())

#Final reviewer dimension and writing to DWH
mode1  = "overwrite"
Reviewer_Core_df=Insert_Reviewer_df.union(Update_Reviewer_df)
Reviewer_Core_df.write.jdbc(url=url, table=table1, mode=mode1, properties=properties)


#Loading in ProdMeta from Staging Schema for load of fact table
table2 =  os.environ['SCHEMA_STG'] + ".prodmeta" 
ProdMeta_Stage_df = spark.read.jdbc(url=url, table=table2, properties=properties)

#Loading in categories dimension for load of fact table
table3 = os.environ['SCHEMA_CORE'] + ".CATEGORY_DIM"
Category_Core_df = spark.read.jdbc(url=url, table=table3, properties=properties)

#Fetching category info for asin
ProdMeta_Category_df = ProdMeta_Stage_df.select('asin','categories','price').join(Category_Core_df, ProdMeta_Stage_df.categories == Category_Core_df.category_nm, how = 'left').select(col('asin').alias('asin_nr'), 'category_id', 'price')

#Forming the fact records
Fact_df = Cl_Reviews_df.join(ProdMeta_Category_df, ProdMeta_Category_df.asin_nr == Cl_Reviews_df.asin, how = 'left').select(col('reviewerID').alias('reviewer_id'), col('asin').alias('asin_id'), col("reviewDate").alias('review_dt'), col("reviewRating").alias('review_vl'), col('price').alias("price_am")).withColumn("add_td", current_timestamp()).withColumn("modified_td", current_timestamp())

Fact_df.show(10)
