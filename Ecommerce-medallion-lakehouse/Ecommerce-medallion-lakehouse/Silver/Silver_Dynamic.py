# Databricks notebook source
from dataclasses import dataclass
from typing import List, Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------


# Step 1:

def get_change_from_cdf(table_name: str, version: int = None) -> DataFrame:
    if version is None:
        hist_df = spark.sql(f"DESC HISTORY {table_name}")
        write_ops = hist_df.filter(
            ~col("operation").isin(
                "OPTIMIZE", "SET TBLPROPERTIES", "ADD CONSTRAINT"
            )
        )
        row_v = write_ops.agg(expr("max(version) as v")).first()

        version = (
            row_v.v
            if (row_v and row_v.v is not None)
            else hist_df.agg(expr("max(version) as v")).first().v
        )
    try:
        df = (
            spark.read
            .option("readChangeFeed", True)
            .option("startingVersion", int(version))
            .option("endingVersion", int(version))
            .table(table_name)
            .filter(col("_change_type").isin("insert", "update_postimage"))
            )

        cc_lists = [c for c in df.columns if c.startswith("_")]

        print(f"Loaded CDF from {table_name} at version {version}")

        return df.drop(*cc_lists)

    except Exception as e:
        print(f"Fallback reading table {table_name} directly (version {version})")
        df = spark.table(table_name)

        cc_lists = [c for c in df.columns if c.startswith("_")]

        return df.drop(*cc_lists)


# COMMAND ----------


# Step2:Bad Records

def get_reason(
    df: DataFrame,
    id_cols: List[str],
    check_cols: List[str]
) -> DataFrame:

    
    if not check_cols:
        return df.limit(0).withColumn( 
            "reason", array().cast("array<string>")
        )
    or_statement = " or ".join(check_cols)

    return (
        df
        .filter(or_statement)
        .melt(
            ids=id_cols,
            values=check_cols,
            variableColumnName="reason",
            valueColumnName="_dq_status"
        )
        .filter(col("_dq_status") == True)
        .groupBy(*id_cols)
        .agg(collect_list("reason").alias("reason"))
    )


# COMMAND ----------

# Step 3:ดึงข้อมูลBronzeจากCDF

def silver_get_raw_df(self) -> DataFrame:
    cdf_df = get_change_from_cdf(
        f"{self.target_table}_bronze"
    )
    self.data_cols = [
        c for c in self.data_cols
        if c in cdf_df.columns
    ]
    self.id_cols = ["_sk"] + self.data_cols
    existing_cols = [
        col(c) for c in self.data_cols
    ]
    return cdf_df.select(
        monotonically_increasing_id().alias("_sk"),
        *existing_cols
    )

# COMMAND ----------

# Step 4:Keynull

def silver_get_key_null(
    self,
    raw_df: DataFrame
) -> DataFrame:
    
    check_key_null = {
        f"_is_{k}_null": col(k).isNull()
        for k in self.keys
    }

    check_cols = list(check_key_null.keys())
    return get_reason(
        raw_df.withColumns(check_key_null),
        self.id_cols,
        check_cols
    )

# COMMAND ----------

# Step 5:invalid

def silver_get_invalid_df(
    self,
    raw_df: DataFrame
) -> DataFrame:
    invalid_rules = {
        "int": r"^[0-9]+(\.0)?$",
        "double": r"^-?[0-9]+(\.[0-9]+)?$",
        "date": r"^\d{4}-\d{2}-\d{2}$"
    }
    email_regex = (
        r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
    )
    invalid_checks = {}
    for col_name, col_type in self.schema_detail.items():
        if col_name == "age":
            invalid_checks[f"_is_{col_name}_invalid"] = (
                when(col(col_name).isNull(), lit(False))
                .otherwise(
                    ~trim(col(col_name).cast("string"))
                    .rlike(invalid_rules["int"])
                    | (col(col_name).cast("double") <= 0)
                    | (col(col_name).cast("double") > 120)
                )
            )

        elif col_name == "stock_quantity":
            invalid_checks[f"_is_{col_name}_invalid"] = (
                when(col(col_name).isNull(), lit(False))
                .otherwise(
                    ~trim(col(col_name).cast("string"))
                    .rlike(invalid_rules["int"])
                    | (col(col_name).cast("double") < 0)
                )
            )

        elif col_name == "email":
            invalid_checks[f"_is_{col_name}_invalid"] = (
                when(col(col_name).isNull(), lit(False))
                .otherwise(
                    ~trim(col(col_name)).rlike(email_regex)
                )
            )

        elif col_type in invalid_rules:
            invalid_checks[f"_is_{col_name}_invalid"] = (
                when(col(col_name).isNull(), lit(False))
                .otherwise(
                    ~trim(col(col_name).cast("string"))
                    .rlike(invalid_rules[col_type])
                )
            )

    check_cols = list(invalid_checks.keys())

    return get_reason(
        raw_df.withColumns(invalid_checks),
        self.id_cols,
        check_cols
    )

# COMMAND ----------

# Step 6:Duplicate

def silver_get_dup_df(
    self,
    raw_df: DataFrame,
    key_null_df: DataFrame
) -> DataFrame:
    partition_by_all = (
        Window.partitionBy(*self.data_cols)
        .orderBy("_sk")
    )
    partition_by_key = Window.partitionBy(*self.keys)
    bronze_key_not_null_df = raw_df.join(
        key_null_df,
        ["_sk"],
        "left_anti"
    )

    row_dup_df = (
        bronze_key_not_null_df
        .withColumn(
            "rn",
            row_number().over(partition_by_all)
        )
        .filter(col("rn") > 1)
        .withColumn("reason", array(lit("row_dup")))
        .drop("rn")
    )

    key_dup_df = (
        bronze_key_not_null_df
        .join(row_dup_df, ["_sk"], "left_anti")
        .withColumn(
            "rn",
            count("*").over(partition_by_key)
        )
        .filter(col("rn") > 1)
        .withColumn("reason", array(lit("key_dup")))
        .drop("rn")
    )
    return row_dup_df.unionByName(key_dup_df)

# COMMAND ----------

# Step 7: badrecord

def silver_get_all_bad_record(
    self,
    invalid_df: DataFrame,
    key_null_df: DataFrame,
    dup_df: DataFrame
) -> DataFrame:

    return (
        invalid_df
        .unionByName(key_null_df)
        .unionByName(dup_df)
        .groupBy(*self.id_cols)
        .agg(
            flatten(
                collect_list("reason")
            ).alias("reason")
        )
    )

# COMMAND ----------

# Step 8:DataCleaning

def silver_get_final_result(
    self,
    raw_df: DataFrame,
    all_bad_rec_df: DataFrame
) -> DataFrame:

    good_df = raw_df.join(
        all_bad_rec_df,
        ["_sk"],
        "left_anti"
    )

    select_exprs = []

    for c in self.data_cols:
        c_type = self.schema_detail.get(c, "string")
        if c in ["firstname", "lastname"]:
            select_exprs.append(
                initcap(trim(col(c))).alias(c)
            )

        elif c == "gender":
            select_exprs.append(
                when(
                    upper(trim(col(c))).isin("M", "Male"),
                    lit("Male")
                )
                .when(
                    upper(trim(col(c))).isin("F", "Female"),
                    lit("Female")
                )
                .when(
                    col(c).isNotNull(),
                    lit("Other")
                )
                .otherwise(lit(None))
                .alias(c)
            )
        elif c in ["city", "address_unit"]:
            select_exprs.append(
                upper(trim(col(c))).alias(c)
            )

        elif c == "phone":
            select_exprs.append(
                regexp_replace(
                    trim(col(c)),
                    "[^0-9]",
                    ""
                ).alias(c)
            )
        elif c == "email":
            select_exprs.append(
                lower(trim(col(c))).alias(c)
            )
        elif c_type == "int":
            select_exprs.append(
                col(c).cast("double").cast("int").alias(c)
            )
        elif c in self.schema_detail:
            select_exprs.append(
                col(c).cast(c_type).alias(c)
            )
        else:
            select_exprs.append(col(c))

    select_exprs.extend([
        current_date().alias("_load_dt"),
        current_timestamp().alias("_load_dttm")
    ])
    return good_df.select(*select_exprs)

# COMMAND ----------

# Step 9: ประมวลผลPipelineและบันทึกลง Delta Table

def silver_load_to_dwh(self) -> None:

    raw_df = self.get_raw_df()
    key_null_df = self.get_key_null(raw_df)
    invalid_df = self.get_invalid_df(raw_df)
    dup_df = self.get_dup_df(raw_df, key_null_df)
    all_bad_rec_df = self.get_all_bad_record(
        invalid_df,
        key_null_df,
        dup_df
    )
    final_df = self.get_final_result(
        raw_df,
        all_bad_rec_df
    )

    (
        all_bad_rec_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{self.target_table}_bad_rec")
    )

    (
        final_df.write
        .format("delta")
        .mode(self.load_type)
        .option("overwriteSchema", "true")
        .option("delta.enableChangeDataFeed", "true")
        .clusterBy(*self.cluster_cols)
        .saveAsTable(f"{self.target_table}_silver")
    )

    print(
        f"Loaded {self.target_table}_silver "
        f"& {self.target_table}_bad_rec successfully!"
    )

# COMMAND ----------


@dataclass
class SilverLayer:
    pipeline_name: str
    target_table: str
    schema_detail: Dict[str, str]
    keys: List[str]
    cii_cols: List[str]
    cluster_cols: List[str]
    load_type: str

    def __post_init__(self):
        self.data_cols = list(self.schema_detail.keys())
        for c in self.cii_cols:
            for suffix in ["xxhash64", "encrypt", "decrypt"]:

                col_extra = f"{c}_{suffix}"
                if col_extra not in self.data_cols:
                    self.data_cols.append(col_extra)

        self.id_cols = ["_sk"] + self.data_cols

    get_raw_df = silver_get_raw_df
    get_key_null = silver_get_key_null
    get_invalid_df = silver_get_invalid_df
    get_dup_df = silver_get_dup_df
    get_all_bad_record = silver_get_all_bad_record
    get_final_result = silver_get_final_result
    load_to_dwh = silver_load_to_dwh

# COMMAND ----------

dbutils.widgets.text("pipeline_name", "person")
pipeline_name = dbutils.widgets.get("pipeline_name")

pipeline_conf = (
    spark.table("workspace.ecommerce_project.pipeline_config_table")
    .filter(col("pipeline_name") == pipeline_name)
    .first()
)

s = SilverLayer(
    pipeline_name=pipeline_conf.pipeline_name,
    target_table=pipeline_conf.target_table,
    schema_detail=pipeline_conf.schema_detail,
    keys=pipeline_conf.keys,
    cii_cols=pipeline_conf.cii_cols,
    cluster_cols=pipeline_conf.cluster_cols,
    load_type=pipeline_conf.load_type
)

s.load_to_dwh()

# COMMAND ----------

for conf in spark.table("workspace.ecommerce_project.pipeline_config_table").collect():
    s_all = SilverLayer(
        pipeline_name=conf.pipeline_name,
        target_table=conf.target_table,
        schema_detail=conf.schema_detail,
        keys=conf.keys,
        cii_cols=conf.cii_cols,
        cluster_cols=conf.cluster_cols,
        load_type=conf.load_type
    )
    s_all.load_to_dwh()

# COMMAND ----------

for name in ["person", "product", "order", "orderline"]:
    tbl = f"workspace.ecommerce_project.{name}_silver"
    bad_tbl = f"workspace.ecommerce_project.{name}_bad_rec"
    print(f"{tbl}: {spark.table(tbl).count()} good rows | {bad_tbl}: {spark.table(bad_tbl).count()} bad rows")

# COMMAND ----------

spark.table("workspace.ecommerce_project.person_silver").display()
spark.table("workspace.ecommerce_project.product_silver").display()
spark.table("workspace.ecommerce_project.order_silver").display()
spark.table("workspace.ecommerce_project.orderline_silver").display()