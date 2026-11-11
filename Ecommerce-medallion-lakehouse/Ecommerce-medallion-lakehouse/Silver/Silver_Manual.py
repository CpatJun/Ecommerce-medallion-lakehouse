# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

#(Step 1 load Bronze)
bronze_df = (
    spark.table("workspace.ecommerce_project.person_bronze")
    .select(
        monotonically_increasing_id().alias("_sk"),
        col("person_id"),
        col("firstname"),
        col("lastname"),
        col("gender"),
        col("age"),
        col("street"),
        col("streetnumber"),
        col("address_unit"),
        col("postalcode"),
        col("city"),
        col("phone"),
        col("email"),
        col("email_xxhash64"),
        col("email_encrypt"),
        col("phone_encrypt")
    )
)


# COMMAND ----------

#step 2 invalid
integer_regex = "^[0-9]+(\\.0)?$"
email_regex = "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"

invalid_df = (
    bronze_df
    .withColumn(
        "_is_person_id_invalid",
        when(col("person_id").isNull(), lit(False))
        .otherwise(~trim(col("person_id").cast("string")).rlike("^[0-9]+$"))
    )
    .withColumn(
        "_is_age_invalid",
        when(col("age").isNull(), lit(False))
        .otherwise(
            ~trim(col("age").cast("string")).rlike(integer_regex) |
            (col("age").cast("double") <= 0) |
            (col("age").cast("double") > 120)
        )
    )
    .withColumn(
        "_is_postalcode_invalid",
        when(col("postalcode").isNull(), lit(False))
        .otherwise(~trim(col("postalcode").cast("string")).rlike("^[0-9]+$"))
    )
    .withColumn(
        "_is_email_invalid",
        when(col("email").isNull(), lit(False))
        .otherwise(~trim(col("email")).rlike(email_regex))
    )
)

id_cols = [
    "_sk", "person_id", "firstname", "lastname", "gender", "age",
    "street", "streetnumber", "address_unit", "postalcode", "city",
    "phone", "email", "email_xxhash64", "email_encrypt", "phone_encrypt"
]

invalid_df = (
    invalid_df
    .filter(
        col("_is_person_id_invalid") |
        col("_is_age_invalid") |
        col("_is_postalcode_invalid") |
        col("_is_email_invalid")
    )
    .melt(
        ids=id_cols,
        values=["_is_person_id_invalid", "_is_age_invalid", "_is_postalcode_invalid", "_is_email_invalid"],
        variableColumnName="reason",
        valueColumnName="status"
    )
    .filter(col("status") == True)
    .groupBy(*id_cols)
    .agg(
        collect_list("reason").alias("reason")
    )
)

print("Invalid row :", invalid_df.count())

# COMMAND ----------

# step3 : key_null
key_null_df = (
    bronze_df
    .withColumn("_is_person_id_null", col("person_id").isNull())
    .filter(col("_is_person_id_null") == True)
    .melt(
        ids=id_cols,
        values=["_is_person_id_null"],
        variableColumnName="reason",
        valueColumnName="status"
    )
    .filter(col("status") == True)
    .groupBy(*id_cols)
    .agg(
        collect_list("reason").alias("reason")
    )
)



# COMMAND ----------


# step4 :dupicate
data_cols_no_sk = [c for c in id_cols if c != "_sk"]

partition_by_all = Window.partitionBy(*data_cols_no_sk).orderBy("_sk")
partition_by_key = Window.partitionBy("person_id")

# key_null
bronze_key_not_null_df = bronze_df.join(key_null_df, ["_sk"], "left_anti")

# row_dup
row_dup_df = (
    bronze_key_not_null_df
    .withColumn("rn", row_number().over(partition_by_all))
    .filter(col("rn") > 1)
    .withColumn("reason", array(lit("row_dup")))
    .drop("rn")
)

# key_dup
key_dup_df = (
    bronze_key_not_null_df
    .join(row_dup_df, ["_sk"], "left_anti")
    .withColumn("rn", count("*").over(partition_by_key))
    .filter(col("rn") > 1)
    .withColumn("reason", array(lit("key_dup")))
    .drop("rn")
)

dup_df = row_dup_df.unionByName(key_dup_df)
dup_df.display()

# COMMAND ----------

# Step 5:Bad Records
all_bad_rec_df = (
    invalid_df
    .unionByName(key_null_df)
    .unionByName(dup_df)
    .groupBy(*id_cols)
    .agg(flatten(collect_list("reason")).alias("reason"))
)

(
    all_bad_rec_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.ecommerce_project.person_bad_rec")
)

# COMMAND ----------

# Step 6

final_result_df = (
    bronze_df
    .join(all_bad_rec_df, ["_sk"], "left_anti")
    .select(
        col("person_id").cast("int"),
        initcap(trim(col("firstname"))).alias("firstname"),
        initcap(trim(col("lastname"))).alias("lastname"),
        when(upper(trim(col("gender"))).isin("M", "MALE"), lit("MALE"))
        .when(upper(trim(col("gender"))).isin("F", "FEMALE"), lit("FEMALE"))
        .when(col("gender").isNotNull(), lit("OTHER"))
        .otherwise(lit(None))
        .alias("gender"),
        col("age").cast("double").cast("int").alias("age"),
        trim(col("street")).alias("street"),
        col("streetnumber").cast("int"),
        upper(trim(col("address_unit"))).alias("address_unit"),
        col("postalcode").cast("int"),
        upper(trim(col("city"))).alias("city"),
        regexp_replace(trim(col("phone")), "[^0-9]", "").alias("phone"),
        lower(trim(col("email"))).alias("email"),
        col("email_xxhash64"),
        col("email_encrypt"),
        col("phone_encrypt"),
        current_date().alias("_load_dt"),
        current_timestamp().alias("_load_dttm")
    )
)


final_result_df.display()


# COMMAND ----------

# Step 7
(
    final_result_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("delta.enableChangeDataFeed", "true")
    .clusterBy("person_id", "city")
    .saveAsTable("workspace.ecommerce_project.person_silver")
)



# COMMAND ----------

# Step 8: check Table
spark.table("workspace.ecommerce_project.person_silver").display()