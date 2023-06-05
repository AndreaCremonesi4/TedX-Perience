import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, collect_set, regexp_replace, sort_array, struct, explode, monotonically_increasing_id, row_number
from pyspark.sql.window import Window

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

############################## FUNZIONI #####################################

def calc_avg_views(num_views, views):
    for a in range(len(num_views)):
        if(num_views[a]==0):
            num_views[a]=views[a]
        if(views[a]==0):
            views[a]=num_views[a]
    return [(a+b)/2 for a, b in zip(num_views, views)]

#############################################################################

##### FROM FILES
tedx_dataset_path = "s3://tedx-perience-data/tedx_dataset.csv"

##### READ PARAMETERS
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

##### START JOB CONTEXT AND JOB
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

##### READ INPUT FILES TO CREATE AN INPUT DATASET
tedx_dataset = spark.read \
    .option("header","true") \
    .option("quote","\"") \
    .option("escape","\"") \
    .csv(tedx_dataset_path)
    
tedx_dataset.printSchema()

##### FILTER ITEMS WITH NULL POSTING KEY
count_items = tedx_dataset.count()
count_items_null = tedx_dataset.filter("idx is not null").count()

print(f"Number of items from RAW DATA {count_items}")
print(f"Number of items from RAW DATA with NOT NULL KEY {count_items_null}")

##### READ TAGS DATASET
tags_dataset_path = "s3://tedx-perience-data/tags_dataset.csv"
tags_dataset = spark.read.option("header", "true").csv(tags_dataset_path)

##### CREATE THE AGGREGATE MODEL, ADD TAGS TO TEDX DATASET
tags_dataset_agg = tags_dataset.groupBy(col("idx").alias("idx_ref")).agg(collect_list("tag").alias("tags"))
tags_dataset_agg.printSchema()
tedx_dataset_agg = tedx_dataset.join(tags_dataset_agg, tedx_dataset.idx == tags_dataset_agg.idx_ref, "left") \
    .drop("idx_ref") \
    .select(col("idx").alias("_id"), col("*")) \
    .drop("idx") \

tedx_dataset_agg.printSchema()

##### READ WATCH NEXT DATASET
watch_next_dataset_path = "s3://tedx-perience-data/watch_next_dataset.csv"
watch_next_dataset = spark.read.option("header", "true").csv(watch_next_dataset_path)

##### CREATE THE AGGREGATE MODEL WATCH NEXT
watch_next_dataset_agg = watch_next_dataset.groupBy(col("idx").alias("idx_ref")).agg(collect_set("watch_next_idx").alias("watch_next"))
watch_next_dataset_agg.printSchema()
tedx_dataset_agg = tedx_dataset_agg.join(watch_next_dataset_agg, tedx_dataset_agg._id == watch_next_dataset_agg.idx_ref, "left") \
    .drop("idx_ref")
watch_next_dataset_agg.printSchema()
    
##### READ FILE DATA
tedx_newdataset_path = "s3://tedx-perience-data/data.csv"
tedx_newdataset = spark.read \
    .option("header","true") \
    .option("quote","\"") \
    .option("escape","\"") \
    .option("delimiter", ",") \
    .csv(tedx_newdataset_path)
tedx_newdataset = tedx_newdataset.join(tedx_dataset_agg, tedx_newdataset.title == tedx_dataset_agg.title, "inner")

tedx_newdataset = (
    tedx_newdataset
    .withColumn('num_views', regexp_replace('num_views', ',', ''))
    .withColumn('num_views', col('num_views').cast("int"))
    .withColumn('likes', col('likes').cast("int"))
)

tedx_newdataset = (
    tedx_newdataset
    .withColumn('views', regexp_replace('views', ',', ''))
    .withColumn('views', col('views').cast("int"))
)

##### SAVE DATA AS LISTS
num_views = tedx_newdataset.select("num_views").rdd.flatMap(lambda x: x).map(lambda x: int(x) if x else 0).collect()
views =  tedx_newdataset.select("views").rdd.flatMap(lambda x: x).map(lambda x: int(x) if x else 0).collect()

##### FIND AVERAGE VIEWS
avg_views_list = calc_avg_views(num_views, views)
df_avg_views = spark \
    .createDataFrame([(avg_views_list,)], ['lista'])\
    .select(explode(col('lista')).alias('avg_views'))
    
##### ADD INDEX TO DATA FRAME TO JOIN
df_avg_views = df_avg_views.withColumn("index", row_number().over(Window.orderBy(monotonically_increasing_id())))
tedx_newdataset = tedx_newdataset.withColumn("index", row_number().over(Window.orderBy(monotonically_increasing_id())))

##### JOIN DATA FRAMES
tedx_newdataset_complete = tedx_newdataset \
    .join(df_avg_views, tedx_newdataset.index == df_avg_views.index, "inner") \
    .drop("index") \
    .withColumnRenamed("idx", "idx_ref")

##### STORE DATA ON MONGODB
mongo_uri = "mongodb+srv://Ombuman:admin123@tedxcluster.lzu3kfz.mongodb.net"
print(mongo_uri)

write_mongo_options = {
    "uri": mongo_uri,
    "database": "unibg_tedx_2023",
    "collection": "tedx_data",
    "ssl": "true",
    "ssl.domain_match": "false"}
from awsglue.dynamicframe import DynamicFrame
tedx_dataset_dynamic_frame = DynamicFrame.fromDF(tedx_newdataset_complete, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame, connection_type="mongodb", connection_options=write_mongo_options)
