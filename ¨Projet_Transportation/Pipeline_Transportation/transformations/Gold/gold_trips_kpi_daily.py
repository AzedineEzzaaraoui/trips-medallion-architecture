from pyspark import pipelines as dp

from pyspark.sql import functions as F
@dp.materialized_view(
    name="trips.gold.trips_kpi_daily",
    comment="kpi daily for busines",
    table_properties={
        "quality": "gold"
    }
)

def trips_kpi_daily():
    df_daily=spark.sql("select * from trips.silver.trips")
    df_gold=df_daily.groupBy("business_date").agg(
        F.count("id").alias("number_of_trips") ,
        F.sum("sales_amt").alias("total_revenue"),
        F.avg("distance_kms").alias("Avg_distance") ,
        F.avg("passenger_rating").alias("Avg_passenger_rating") ,
        F.avg("driver_rating").alias("Avg_driver_rating")
         )
    return df_gold 
