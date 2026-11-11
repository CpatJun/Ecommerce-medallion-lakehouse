# Databricks notebook source
from dataclasses import dataclass
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import *

secret_key = dbutils.secrets.get(scope="ecommerce_scope", key="encryption_key")

# COMMAND ----------

dbutils.widgets.text("pipeline_name", "person")
pipeline_name = dbutils.widgets.get("pipeline_name")
print("Selected pipeline:", pipeline_name)

# COMMAND ----------

pipeline_conf = (
    spark.table("workspace.ecommerce_project.pipeline_config_table")
    .filter(col("pipeline_name") == pipeline_name)
    .first()
)

print(pipeline_conf)

# COMMAND ----------

import os
import shutil

@dataclass
class BronzeLayer:
    pipeline_name: str
    file_path: str
    header: bool
    delimeter: str
    target_table: str
    cii_cols: List[str]
    cluster_cols: List[str]
    load_type: str

    def bronze_layer(self, raw_df: DataFrame) -> DataFrame:
        # Step 1
        df = (
            raw_df
            .withColumn("_load_dt", current_date())
            .withColumn("_load_dttm", current_timestamp())
            .withColumn("_file_name", col("_metadata.file_name"))
            .withColumn("_file_path", col("_metadata.file_path"))
            .withColumn("_file_size", col("_metadata.file_size"))
            .withColumn("_mod_time", col("_metadata.file_modification_time"))
        )
        # Step 2:
        for c in self.cii_cols:
            df = (
                df
                .withColumn(f"{c}_xxhash64", xxhash64(col(c)))
                .withColumn(f"{c}_encrypt", aes_encrypt(col(c).cast("string"), lit(secret_key).cast("binary"), mode=lit("GCM")))
                .withColumn(f"{c}_decrypt", aes_decrypt(col(f"{c}_encrypt"), lit(secret_key).cast("binary"), mode=lit("GCM")).cast("string"))
            )
        return df

    def load_to_bronze(self) -> None:
        folder_path = os.path.dirname(self.file_path)
        file_name = os.path.basename(self.file_path)

        csv_schema = (
            spark.read.format("csv")
            .option("header", self.header)
            .option("delimiter", self.delimeter)
            .load(self.file_path)
            .schema
        )
        stream_df = (
            spark.readStream.format("csv")
            .option("header", self.header)
            .option("delimiter", self.delimeter)
            .option("pathGlobFilter", file_name)
            .schema(csv_schema)
            .load(folder_path)
        )
h
        def process_batch(batch_df: DataFrame, batch_id: int):
            if batch_df.isEmpty():
                return
            final_df = self.bronze_layer(batch_df)
            (
                final_df.write
                .format("delta")
                .mode(self.load_type)
                .option("overwriteSchema", "true")
                .option("delta.enableChangeDataFeed", "true")
                .clusterBy(*self.cluster_cols)
                .saveAsTable(f"{self.target_table}_bronze")
            )

        chk_path = f"/Volumes/workspace/ecommerce_project/source_files/_checkpoints/{self.pipeline_name}_bronze"
        if self.load_type == "overwrite" and os.path.exists(chk_path):
            shutil.rmtree(chk_path, ignore_errors=True)

        query = (
            stream_df.writeStream
            .foreachBatch(process_batch)
            .option("checkpointLocation", chk_path)
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()
        print(f"Loaded {self.target_table}_bronze successfully")

# COMMAND ----------

b = BronzeLayer(
    pipeline_name=pipeline_conf.pipeline_name,
    file_path=pipeline_conf.file_path,
    header=pipeline_conf.header,
    delimeter=pipeline_conf.delimeter,
    target_table=pipeline_conf.target_table,
    cii_cols=pipeline_conf.cii_cols,
    cluster_cols=pipeline_conf.cluster_cols,
    load_type=pipeline_conf.load_type
)

b.load_to_bronze()

# COMMAND ----------

spark.table(f"{pipeline_conf.target_table}_bronze").display()

# COMMAND ----------

for conf in spark.table("workspace.ecommerce_project.pipeline_config_table").collect():
    b_all = BronzeLayer(
        pipeline_name=conf.pipeline_name,
        file_path=conf.file_path,
        header=conf.header,
        delimeter=conf.delimeter,
        target_table=conf.target_table,
        cii_cols=conf.cii_cols,
        cluster_cols=conf.cluster_cols,
        load_type=conf.load_type
    )
    b_all.load_to_bronze()