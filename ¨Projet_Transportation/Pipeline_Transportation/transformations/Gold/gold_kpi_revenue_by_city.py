from pyspark import pipelines as dp

from pyspark.sql import functions as F
@dp.materialized_view(
    name="trips.gold.revenue_by_city",
    comment="kpi daily for busines",
    table_properties={
        "quality": "gold"
    }
)

def revenue_by_city():
    df_trips=spark.sql("select * from trips.silver.trips")
    df_city=spark.sql("select * from trips.silver.city")
    df_revenu_city=df_trips.join(df_city ,df_trips.city_id==df_city.city_id ,"left"  )
    df_revenu_city=df_revenu_city.groupby("city_name").agg(
        F.sum("sales_amt").alias("Total_revenu") ,
        F.count("*").alias("Total_trips")

    )
    return  df_revenu_city
 
