# Databricks notebook source

#E-COMMERCE GOLD LAYER
#Jobs & Pipelines: ecommerce_project

"""
from pyspark import pipelines as dp
from pyspark.sql.functions import *

# 1. Fact Streaming Tables (@dp.append_flow)
dp.create_streaming_table("fact_orders", expect_all_or_drop={"valid_order_id": "order_id IS NOT NULL", "valid_person_id": "person_id IS NOT NULL"})
@dp.append_flow(target="fact_orders")
def flow_fact_orders():
    return spark.readStream.option("readChangeFeed", "true").table("workspace.ecommerce_project.order_silver").filter(col("_change_type").isin("insert", "update_postimage")).drop("_change_type", "_commit_version", "_commit_timestamp")

dp.create_streaming_table("fact_orderlines", expect_all_or_drop={"valid_orderline_id": "orderline_id IS NOT NULL", "valid_order_id": "order_id IS NOT NULL"})
@dp.append_flow(target="fact_orderlines")
def flow_fact_orderlines():
    return spark.readStream.option("readChangeFeed", "true").table("workspace.ecommerce_project.orderline_silver").filter(col("_change_type").isin("insert", "update_postimage")).drop("_change_type", "_commit_version", "_commit_timestamp")

# 2. Automated SCD Type 2 Dimensions (create_auto_cdc_flow)
@dp.temporary_view(name="vw_person_silver")
def vw_person_silver():
    return spark.readStream.option("readChangeFeed", "true").table("workspace.ecommerce_project.person_silver").filter(col("_change_type").isin("insert", "update_postimage")).drop("_change_type", "_commit_version", "_commit_timestamp")

dp.create_streaming_table("dim_person")
dp.create_auto_cdc_flow(target="dim_person", source="vw_person_silver", keys=["person_id"], sequence_by=col("_load_dttm"), stored_as_scd_type=2)

@dp.temporary_view(name="vw_product_silver")
def vw_product_silver():
    return spark.readStream.option("readChangeFeed", "true").table("workspace.ecommerce_project.product_silver").filter(col("_change_type").isin("insert", "update_postimage")).drop("_change_type", "_commit_version", "_commit_timestamp")

dp.create_streaming_table("dim_product")
dp.create_auto_cdc_flow(target="dim_product", source="vw_product_silver", keys=["product_id"], sequence_by=col("_load_dttm"), stored_as_scd_type=2)

# 3. Gold Materialized Views (Join Facts + Active SCD2 __END_AT IS NULL)
@dp.materialized_view(name="all_sales")
def mv_all_sales():
    orders_df = spark.table("fact_orders").alias("o")
    lines_df = spark.table("fact_orderlines").alias("ol")
    person_df = spark.table("dim_person").filter(col("__END_AT").isNull()).alias("p")
    product_df = spark.table("dim_product").filter(col("__END_AT").isNull()).alias("pr")

    return (
        lines_df
        .join(orders_df, col("ol.order_id") == col("o.order_id"), "inner")
        .join(person_df, col("o.person_id") == col("p.person_id"), "left")
        .join(product_df, col("ol.product_id") == col("pr.product_id"), "left")
        .select(
            col("ol.orderline_id"), col("o.order_id"), col("o.order_date"),
            col("o.status").alias("order_status"), col("o.payment_method"), col("o.shipping_method"),
            col("p.person_id"), col("p.firstname"), col("p.lastname"), col("p.gender"), col("p.city"),
            col("p.email_xxhash64"), col("p.email_encrypt"),
            col("pr.product_id"), col("pr.name").alias("product_name"), col("pr.category"), col("pr.subcategory"), col("pr.brand"),
            col("ol.quantity"), col("ol.unit_price"), col("ol.subtotal")
        )
    )

@dp.materialized_view(name="total_sales_per_city")
def mv_total_sales_per_city():
    return spark.table("all_sales").groupBy("city").agg(countDistinct("order_id").alias("total_orders"), sum("quantity").alias("total_items_sold"), round(sum("subtotal"), 2).alias("total_sales_amount"))

@dp.materialized_view(name="total_sales_per_category")
def mv_total_sales_per_category():
    return spark.table("all_sales").groupBy("category", "brand").agg(countDistinct("order_id").alias("total_orders"), sum("quantity").alias("total_items_sold"), round(sum("subtotal"), 2).alias("total_sales_amount"))
"""
print("Gold ready.")

# COMMAND ----------

# คิวรีตรวจสอบตาราง Gold และ Dimension SCD Type 2 ที่สร้างจาก Pipeline
print("Sales summary by city (total_sales_per_city)")
spark.table("workspace.ecommerce_project.total_sales_per_city").display()

print("Sales summary by product category (total_sales_per_category)")
spark.table("workspace.ecommerce_project.total_sales_per_category").display()

print("Checktable Type 2 (__START_AT และ __END_AT)")
spark.table("workspace.ecommerce_project.dim_person").select("person_id", "firstname", "city", "__START_AT", "__END_AT").limit(10).display()

print("Alltable all_sales")
spark.table("workspace.ecommerce_project.all_sales").limit(10).display()