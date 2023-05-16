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

def calc_avg_points(data):
    lists = [x[0] for x in data]
    weights = [x[1] for x in data]
    tot_weights = sum(x for x in weights)

    min_length = min([len(x) for x in lists])

    means = []
    for i in range(0, min_length):
        mean = 0
        for j in range(0, len(weights)):
            mean = mean + lists[j][i] * weights[j]
        means.append(mean / tot_weights * 100)

    return means

#############################################################################

## LEGGI DATASET TEDX

tedx_dataset_path = "s3://unibg-tedx-data-2023-test-acremonesi/tedx_dataset.csv"

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()

glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

tedx_dataset = spark.read \
    .option("header", "true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("multiLine", "true") \
    .csv(tedx_dataset_path)

tedx_dataset = (
    tedx_dataset
    .withColumn('num_views', regexp_replace('num_views', ',', ''))
    .withColumn('num_views', col('num_views').cast("int"))
)



# LEGGI I TAG
tags_dataset_path = "s3://unibg-tedx-data-2023-test-acremonesi/tags_dataset.csv"
tags_dataset = spark.read.option("header", "true").csv(tags_dataset_path)

tags_dataset_agg = tags_dataset.groupBy(col("idx").alias("idx_ref")).agg(collect_list("tag").alias("tags"))



## LEGGI WATCH NEXT

watch_next_dataset_path = "s3://unibg-tedx-data-2023-test-acremonesi/watch_next_dataset.csv"
watch_next_dataset = spark.read.option("header", "true").csv(watch_next_dataset_path)

# Unione 'watch_next_dataset' con 'tedx_dataset' per ottenere 'num_views'
watch_next_dataset = watch_next_dataset.join(tedx_dataset, watch_next_dataset.watch_next_idx == tedx_dataset.idx, "inner") \
    .select(watch_next_dataset.idx, watch_next_dataset.watch_next_idx, tedx_dataset.num_views)

# Ordina 'watch_next_dataset' per 'num_views' e aggrega per 'idx'
watch_next_dataset_agg = (
    watch_next_dataset.groupBy(col("idx").alias("idx_ref")) 
    .agg(
        sort_array(collect_set(struct("num_views", "watch_next_idx")), asc=False)
        .alias("collected_list")
    )
    .withColumn("watch_next_list", col("collected_list.watch_next_idx"))
    .drop("collected_list")
)

watch_next_dataset_agg.printSchema()



## LEGGI IL FILE DATA E CALCOLA IL PUNTEGGIO MEDIO DEI VIDEO

tedx_data_path = "s3://unibg-tedx-data-2023-test-acremonesi/data.csv"
tedx_data = spark.read \
    .option("header", "true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("delimiter", ",") \
    .csv(tedx_data_path)

tedx_data = tedx_data.join(tedx_dataset, tedx_data.title == tedx_dataset.title, "inner") \
    .select(tedx_dataset.idx, tedx_dataset.num_views, tedx_data.likes)

# Salva i dati delle colonne likes e views come liste
likes = tedx_data.select("likes").rdd.flatMap(lambda x: x).map(lambda x: int(x) if x else 0).collect()
views =  tedx_data.select("num_views").rdd.flatMap(lambda x: x).map(lambda x: int(x) if x else 0).collect()

# Normalizzazione dei valori delle liste
n_likes = [x/max(likes) for x in likes]
n_views = [x/max(views) for x in views]

# Calcolo della lista dei valori
avg_points_list = calc_avg_points([(n_likes, 80), (n_views, 20)])

# Creazione Data Frame con valori calcolati
df_avg_points = spark \
    .createDataFrame([(avg_points_list,)], ['lista'])\
    .select(explode(col('lista')).alias('avg_points'))

# Aggiunta degli indici ai Data Frame per eseguire la join
df_avg_points = df_avg_points.withColumn("index", row_number().over(Window.orderBy(monotonically_increasing_id())))
tedx_data = tedx_data.withColumn("index", row_number().over(Window.orderBy(monotonically_increasing_id())))

# Unione dei Data Frame
tedx_data_complete = tedx_data \
    .join(df_avg_points, tedx_data.index == df_avg_points.index, "inner") \
    .drop("index") \
    .withColumnRenamed("idx", "idx_ref") \



## AGGIORNA DATASET

tedx_dataset_agg = tedx_dataset \
    .join(tedx_data_complete, tedx_dataset.idx == tedx_data_complete.idx_ref, "inner") \
    .join(tags_dataset_agg, tedx_dataset.idx == tags_dataset_agg.idx_ref, "left") \
    .join(watch_next_dataset_agg, tedx_dataset.idx == watch_next_dataset_agg.idx_ref, "left") \
    .drop("idx_ref") \
    .select(col("idx").alias("_id"), col("*")) \
    .drop("idx") \



## SAVE TO MONGODB

mongo_uri = "mongodb+srv://admin:admin123@unibgtedx.iqlucmt.mongodb.net"
print(mongo_uri)

write_mongo_options = {
    "uri" : mongo_uri,
    "database": "unibg_tedx_2023",
    "collection": "tedx_data",
    "ssl": "true",
    "ssl.domain_match": "false"
}

from awsglue.dynamicframe import DynamicFrame
tedx_dataset_dynamic_frame = DynamicFrame.fromDF(tedx_dataset_agg, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame, connection_type="mongodb", connection_options=write_mongo_options)