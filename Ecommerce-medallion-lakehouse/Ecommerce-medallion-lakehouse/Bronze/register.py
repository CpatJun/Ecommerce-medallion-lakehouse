from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, MapType, ArrayType

spark = SparkSession.builder.getOrCreate()

config_schema = StructType([
    StructField("pipeline_name", StringType(), False),
    StructField("file_path", StringType(), False),
    StructField("header", BooleanType(), False),
    StructField("delimeter", StringType(), False),
    StructField("target_table", StringType(), False),
    StructField("schema_detail", MapType(StringType(), StringType()), False),
    StructField("keys", ArrayType(StringType()), False),
    StructField("cii_cols", ArrayType(StringType()), False),
    StructField("cluster_cols", ArrayType(StringType()), False),
    StructField("load_type", StringType(), False)
])

data = [
    {
        "pipeline_name": "person",
        "file_path": "/Volumes/workspace/ecommerce_project/source_files/person.csv",
        "header": True,
        "delimeter": ",",
        "target_table": "workspace.ecommerce_project.person",
        "schema_detail": {
            "person_id": "int",
            "firstname": "string",
            "lastname": "string",
            "gender": "string",
            "age": "int",
            "street": "string",
            "streetnumber": "int",
            "address_unit": "string",
            "postalcode": "int",
            "city": "string",
            "phone": "string",
            "email": "string"
        },
        "keys": ["person_id"],
        "cii_cols": ["email", "phone"],
        "cluster_cols": ["person_id", "city"],
        "load_type": "overwrite"
    },
    {
        "pipeline_name": "product",
        "file_path": "/Volumes/workspace/ecommerce_project/source_files/product.csv",
        "header": True,
        "delimeter": ",",
        "target_table": "workspace.ecommerce_project.product",
        "schema_detail": {
            "product_id": "int",
            "sku": "string",
            "name": "string",
            "description": "string",
            "category": "string",
            "subcategory": "string",
            "brand": "string",
            "price": "double",
            "cost": "double",
            "stock_quantity": "int",
            "weight_kg": "double",
            "length_cm": "double",
            "width_cm": "double",
            "height_cm": "double",
            "status": "string",
            "created_date": "date",
            "rating_average": "double",
            "review_count": "int"
        },
        "keys": ["product_id"],
        "cii_cols": [],
        "cluster_cols": ["product_id", "category"],
        "load_type": "overwrite"
    },
    {
        "pipeline_name": "order",
        "file_path": "/Volumes/workspace/ecommerce_project/source_files/order.csv",
        "header": True,
        "delimeter": ",",
        "target_table": "workspace.ecommerce_project.order",
        "schema_detail": {
            "order_id": "int",
            "person_id": "int",
            "order_date": "date",
            "status": "string",
            "total_amount": "double",
            "payment_method": "string",
            "shipping_method": "string",
            "notes": "string"
        },
        "keys": ["order_id"],
        "cii_cols": [],
        "cluster_cols": ["order_id", "status"],
        "load_type": "overwrite"
    },
    {
        "pipeline_name": "orderline",
        "file_path": "/Volumes/workspace/ecommerce_project/source_files/orderline.csv",
        "header": True,
        "delimeter": ",",
        "target_table": "workspace.ecommerce_project.orderline",
        "schema_detail": {
            "orderline_id": "int",
            "order_id": "int",
            "product_id": "int",
            "quantity": "int",
            "unit_price": "double",
            "subtotal": "double",
            "status": "string"
        },
        "keys": ["orderline_id"],
        "cii_cols": [],
        "cluster_cols": ["orderline_id", "order_id"],
        "load_type": "overwrite"
    }
]

config_df = spark.createDataFrame(data, schema=config_schema)
(
    config_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.ecommerce_project.pipeline_config_table")
)

print("Created pipeline_config_table successfully!")
spark.table("workspace.ecommerce_project.pipeline_config_table").display()