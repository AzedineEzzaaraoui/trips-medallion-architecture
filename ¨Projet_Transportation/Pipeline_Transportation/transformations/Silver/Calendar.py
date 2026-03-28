from pyspark import pipelines as dp
from pyspark.sql.functions import col, explode, sequence, to_date, year, quarter, month, date_format, weekofyear, dayofmonth, dayofweek, dayofyear

@dp.materialized_view(
    name="trips.silver.calendar",
    comment="Calendar dimension table with date attributes"
)
def calendar():
    start_date = "2020-01-01"
    end_date = "2030-12-31"
    
    df = spark.sql(f"""
        SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS calendar_date
    """)
    
    calendar_df = (
        df.withColumn("year", year(col("calendar_date")))
          .withColumn("quarter", quarter(col("calendar_date")))
          .withColumn("month", month(col("calendar_date")))
          .withColumn("month_name", date_format(col("calendar_date"), "MMMM"))
          .withColumn("week_of_year", weekofyear(col("calendar_date")))
          .withColumn("day_of_month", dayofmonth(col("calendar_date")))
          .withColumn("day_of_week", dayofweek(col("calendar_date")))
          .withColumn("day_name", date_format(col("calendar_date"), "EEEE"))
          .withColumn("day_of_year", dayofyear(col("calendar_date")))
          .withColumn("is_weekend", (col("day_of_week").isin(1, 7)))
    )
    
    return calendar_df
