# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.ecommerce_project")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.ecommerce_project.source_files")

# COMMAND ----------

import shutil
import os

src_dir = "/Workspace/Users/cpat.jun@gmail.com/project/source"
dst_dir = "/Volumes/workspace/ecommerce_project/source_files"

for fname in ["person.csv", "product.csv", "order.csv", "orderline.csv"]:
    shutil.copy(os.path.join(src_dir, fname), os.path.join(dst_dir, fname))
    print(f"Coppy {fname} -> {dst_dir}/{fname}")

# COMMAND ----------

raw_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("delimiter", ",")
    .load("/Volumes/workspace/ecommerce_project/source_files/person.csv")
)

# COMMAND ----------

final_result = (
    raw_df
    .withColumn("_load_dt",current_date())
    .withColumn("_load_dttm",current_timestamp())
    .withColumn("_file_name",col("_metadata.file_name"))
    .withColumn("_file_path",col("_metadata.file_path"))
    .withColumn("_file_size",col("_metadata.file_size"))
    .withColumn("_mod_time",col("_metadata.file_modification_time"))
)

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import AclPermission

# COMMAND ----------

w = WorkspaceClient()

# COMMAND ----------

w.secrets.put_secret(scope="ecommerce_scope", key="encryption_key",string_value="0123456789abcdef0123456789abcdef")
w.secrets.list_secrets(scope="ecommerce_scope")

# COMMAND ----------

secret_key = dbutils.secrets.get(scope="ecommerce_scope", key="encryption_key")

# COMMAND ----------


bronze_df = (
    final_result
    # 1. ทำ Hashing
    .withColumn("email_xxhash64", xxhash64(col("email")))
    
    # 2. ทำ Encryption ด้วย Secret Key
    .withColumn("email_encrypt", aes_encrypt(col("email").cast("string"), lit(secret_key).cast("binary"), mode=lit("GCM")))
    .withColumn("phone_encrypt", aes_encrypt(col("phone").cast("string"), lit(secret_key).cast("binary"), mode=lit("GCM")))

    # 3. Decrypt
    .withColumn("email_decrypt", aes_decrypt(col("email_encrypt"), lit(secret_key).cast("binary"), mode=lit("GCM")).cast("string"))
    .withColumn("phone_decrypt", aes_decrypt(col("phone_encrypt"), lit(secret_key).cast("binary"), mode=lit("GCM")).cast("string"))
)





# COMMAND ----------


(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("delta.enableChangeDataFeed", "true")
    .clusterBy("person_id", "city")
    .saveAsTable("workspace.ecommerce_project.person_bronze")
)

# COMMAND ----------

# Liquid Clustering
Liquid_table = {
    "product": ["product_id", "category"],
    "order": ["order_id", "status"],
    "orderline": ["orderline_id", "order_id"]
}

for tbl_name, cluster_cols in Liquid_table.items():
    file_path = f"/Volumes/workspace/ecommerce_project/source_files/{tbl_name}.csv"
    target_table = f"workspace.ecommerce_project.{tbl_name}_bronze"
    
    # 1. อ่านไฟล์ CSV
    df_raw = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("delimiter", ",")
        .load(file_path)
    )
    
    df_bronze = (
        df_raw
        .withColumn("_load_dt", current_date())
        .withColumn("_load_dttm", current_timestamp())
        .withColumn("_file_name", col("_metadata.file_name"))
        .withColumn("_file_path", col("_metadata.file_path"))
        .withColumn("_file_size", col("_metadata.file_size"))
        .withColumn("_mod_time", col("_metadata.file_modification_time"))
    )
    
    (
        df_bronze.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("delta.enableChangeDataFeed", "true")
        .clusterBy(*cluster_cols)
        .saveAsTable(target_table)
    )
    
    print(f"Load{target_table} successfully")