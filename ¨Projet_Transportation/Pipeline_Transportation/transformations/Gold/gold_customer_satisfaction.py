from pyspark import pipelines as dp

from pyspark.sql import functions as F
@dp.materialized_view(
    name="trips.gold.customer_satisfaction",
    comment="kpi daily for busines",
    table_properties={
        "quality": "gold"
    }
)

def customer_satisfaction():
    df_trips=spark.sql("select * from trips.silver.trips")
    df_customer_satisfaction=df_trips.agg(
        F.avg("passenger_rating").alias("avg_passenger_rating"),
        F.avg("driver_rating").alias("avg_driver_rating")
    )
    return  df_customer_satisfaction
 
