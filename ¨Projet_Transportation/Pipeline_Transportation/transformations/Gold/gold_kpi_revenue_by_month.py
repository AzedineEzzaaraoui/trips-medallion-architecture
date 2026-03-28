from pyspark import pipelines as dp

from pyspark.sql import functions as F
@dp.materialized_view(
    name="trips.gold.revenue_by_month",
    comment="kpi daily for busines",
    table_properties={
        "quality": "gold"
    }
)

def revenue_by_month():
    df_trips=spark.sql("select * from trips.silver.trips")
    df_calendar=spark.sql("select * from trips.silver.calendar")
    df_revenu_city=df_trips.join(df_calendar ,df_trips.business_date==df_calendar.calendar_date ,"left"  )
    df_revenu_city=df_revenu_city.groupby("year" ,"month").agg(
        F.sum("sales_amt").alias("Total_revenu") ,
        F.count("*").alias("Total_trips")

    )
    return  df_revenu_city
 
