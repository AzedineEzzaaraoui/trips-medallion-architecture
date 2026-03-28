from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="trips.silver.city",
    comment="Raw  data from bronze",
    table_properties={
        "quality": "bronze"
    }
)
@dp.expect("valid_ciy_id","city_id is not null")
def city_silver() :

    df_city_silver=spark.read.table("trips.bronze.city")

    df_silver = df_city_silver.select(
        F.col("city_id").alias("city_id"),
        F.col("city_name").alias("city_name"),
        F.col("ingestion_date").alias("bronze_ingest_timestamp")
    )
    df_silver = df_silver.withColumn(
        "silver_processed_timestamp", F.current_timestamp()
    )

    return df_silver