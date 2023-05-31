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

def calc_avg_views(n_v, v ):
    for a in range(len(n_v)):
        if(n_v[a]==0):
            n_v[a]=v[a]
        if(v[a]==0):
            v[a]=n_v[a]
    return [int((a+b)/2) for a, b in zip(n_v, v)]

#############################################################################


##### FROM FILES
tedx_dataset_path = "s3://unibg-tedx-perience-data-2023/tedx_dataset.csv"

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
tags_dataset_path = "s3://unibg-tedx-perience-data-2023/tags_dataset.csv"
tags_dataset = spark.read.option("header", "true").csv(tags_dataset_path)

##### CREATE THE AGGREGATE MODEL, ADD TAGS TO TEDX DATASET
tags_dataset_agg = tags_dataset.groupBy(col("idx").alias("idx_ref")).agg(collect_list("tag").alias("tags"))
tags_dataset_agg.printSchema()
tedx_dataset_agg = tedx_dataset.join(tags_dataset_agg, tedx_dataset.idx == tags_dataset_agg.idx_ref, "left") \
    .drop("idx_ref") \
    .select(col("idx").alias("_id"), col("*")) \
    .drop("idx") \

tedx_dataset_agg.printSchema()

##### READ watch next dataset
watch_next_dataset_path = "s3://unibg-tedx-perience-data-2023/watch_next_dataset.csv"
watch_next_dataset = spark.read.option("header", "true").csv(watch_next_dataset_path)
### Modello aggregato dei watcch next
watch_next_dataset_agg = watch_next_dataset.groupBy(col("idx").alias("idx_ref")).agg(collect_set("watch_next_idx").alias("watch_next_list"))
watch_next_dataset_agg.printSchema()

tedx_dataset_agg = tedx_dataset_agg.join(watch_next_dataset_agg, tedx_dataset_agg._id == watch_next_dataset_agg.idx_ref, "left") \
    .drop("idx_ref") \

# LETTURA FILE DATA
tedx_data_path = "s3://unibg-tedx-perience-data-2023/data.csv"
tedx_data = spark.read \
        .option("header", "true") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .option("delimiter", ",") \
        .csv(tedx_data_path)
tedx_data = tedx_data.join(tedx_dataset_agg, tedx_data.title == tedx_dataset_agg.title, "inner")

tedx_data = (
    tedx_data
    .withColumn('num_views', regexp_replace('num_views', ',', ''))
    .withColumn('num_views', col('num_views').cast("int"))
)

tedx_data = (
    tedx_data
    .withColumn('views', regexp_replace('views', ',', ''))
    .withColumn('views', col('views').cast("int"))
)


# Salva i dati delle colonne num_views e views come liste
num_views = tedx_data.select("num_views").rdd.flatMap(lambda x: x).map(lambda x: int(x) if x else 0).collect()
views =  tedx_data.select("views").rdd.flatMap(lambda x: x).map(lambda x: int(x) if x else 0).collect()


# Calcolo della lista dei valori
avg_views_list = calc_avg_views(num_views, views) 
##avendo unito due dataset che discostano di poco i valori del campo
## calcolo una media delle visualizzazioni

df_avg_views = spark \
    .createDataFrame([(avg_views_list,)], ['lista'])\
    .select(explode(col('lista')).alias('avg_views'))

# Aggiunta degli indici ai Data Frame per eseguire la join
df_avg_views = df_avg_views.withColumn("index", row_number().over(Window.orderBy(monotonically_increasing_id())))
tedx_data = tedx_data.withColumn("index", row_number().over(Window.orderBy(monotonically_increasing_id())))

# Unione dei Data Frame
tedx_data_complete = tedx_data \
    .join(df_avg_views, tedx_data.index == df_avg_views.index, "inner") \
    .drop("index") \
    .withColumnRenamed("idx", "idx_ref") 

##### STORE DATA ON MONGODB
mongo_uri = "mongodb+srv://RomeoOdajiu:Admin123@tedxperience2023.3r6eb23.mongodb.net"
print(mongo_uri)

write_mongo_options = {
    "uri": mongo_uri,
    "database": "unibg_tedx_2023",
    "collection": "tedx_data",
    "ssl": "true",
    "ssl.domain_match": "false"}
from awsglue.dynamicframe import DynamicFrame
tedx_dataset_dynamic_frame = DynamicFrame.fromDF(tedx_data_complete, glueContext, "nested")
