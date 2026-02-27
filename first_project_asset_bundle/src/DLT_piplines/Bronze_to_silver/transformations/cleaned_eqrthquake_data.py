from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    count,
    count_if,
    from_json,
    explode,
    from_unixtime,current_timestamp
)
from utilities import utils
import dlt
from pyspark.sql.types import *

# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.
catalog_name = spark.conf.get("catalog_name")
volume_path = f"/Volumes/{catalog_name}/bronze/earthquake_data/"
primary_key = "id"
properties_schema = StructType(
    [
        StructField("mag", StringType()),
        StructField("place", StringType()),
        StructField("time", StringType()),
        StructField("updated", StringType()),
        StructField("tz", StringType()),
        StructField("url", StringType()),
        StructField("detail", StringType()),
        StructField("felt", StringType()),
        StructField("cdi", StringType()),
        StructField("mmi", StringType()),
        StructField("alert", StringType()),
        StructField("status", StringType()),
        StructField("tsunami", StringType()),
        StructField("sig", StringType()),
        StructField("net", StringType()),
        StructField("code", StringType()),
        StructField("ids", StringType()),
        StructField("sources", StringType()),
        StructField("types", StringType()),
        StructField("nst", StringType()),
        StructField("dmin", StringType()),
        StructField("rms", StringType()),
        StructField("gap", StringType()),
        StructField("magType", StringType()),
        StructField("type", StringType()),
        StructField("title", StringType()),
    ]
)

geometry_schema = StructType([StructField("coordinates", ArrayType(DoubleType()))])

features_schema = StructType(
    [
        StructField("id", StringType()),
        StructField("properties", properties_schema),
        StructField("geometry", geometry_schema),
    ]
)

_schema = ArrayType(features_schema)


@dlt.view(name="earthquake_data_v")
def earthquake_data():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(volume_path)
        .withColumn("_load_timestamp",current_timestamp())
    )
    df = df.withColumn("parsed_data", from_json(col("features"), _schema))
    df = df.select(explode(col("parsed_data")).alias("features"),"_load_timestamp")
    df = df.select(
        "features.properties.*",
        "features.id",
        col("features.geometry.coordinates")[0].alias("longitude"),
        col("features.geometry.coordinates")[1].alias("latitude"),
        col("features.geometry.coordinates")[2].alias("depth"),
        "_load_timestamp"
    )
    df = (
        df.withColumn("time", from_unixtime(col("time") / 1000).cast("timestamp"))
        .withColumn("mag",col("mag").cast("double"))
        .withColumn("nst",col("nst").cast("double"))
        .withColumn("sig",col("sig").cast("double"))
        .withColumn("tsunami",col("tsunami").cast("double"))
        .withColumn("dmin",col("dmin").cast("double"))
        .withColumn("rms",col("rms").cast("double"))
        )
   
    return df

dlt.create_streaming_table(name="earthquake_data_final")

dlt.apply_changes(
        target="earthquake_data_final",
        source="earthquake_data_v",
        keys=[primary_key],
        sequence_by="_load_timestamp",
        stored_as_scd_type='1'
    )

