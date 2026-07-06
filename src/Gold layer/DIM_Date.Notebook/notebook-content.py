# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "warehouse": {
# META       "default_warehouse": "f7886d0a-5a53-87a2-4caa-5e6b9742fa2a",
# META       "known_warehouses": [
# META         {
# META           "id": "f7886d0a-5a53-87a2-4caa-5e6b9742fa2a",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Imports

# CELL ********************

import com.microsoft.spark.fabric
from datetime import date
from pyspark.sql import functions as F
from com.microsoft.spark.fabric.Constants import Constants

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Default start date
start_date = "1900-01-01"
end_date = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------
# CONFIGURATION
# -----------------------------
start_date = "1900-01-01"
# 10 years into the future from today
end_date = (date.today().replace(year=date.today().year + 10)).isoformat()

# -----------------------------
# BASE CALENDAR
# -----------------------------
df_dim_date = spark.sql(f"""
    SELECT explode(
        sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)
    ) AS date
""")

# -----------------------------
# CORE DATE ATTRIBUTES
# -----------------------------
df_dim_date = (
    df_dim_date
    .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("date"))
    .withColumn("month", F.month("date"))
    .withColumn("day", F.dayofmonth("date"))
    .withColumn("day_of_year", F.dayofyear("date"))
    .withColumn("day_of_week", F.dayofweek("date"))  # 1=Sunday
    .withColumn("week_of_year", F.weekofyear("date"))
    .withColumn("quarter", F.quarter("date"))
)

# -----------------------------
# NAME FIELDS
# -----------------------------
df_dim_date = (
    df_dim_date
    .withColumn("month_name", F.date_format("date", "MMMM"))
    .withColumn("month_short", F.date_format("date", "MMM"))
    .withColumn("day_name", F.date_format("date", "EEEE"))
)

# -----------------------------
# WEEK LOGIC
# -----------------------------
df_dim_date = (
    df_dim_date
    .withColumn("is_weekend", F.expr("dayofweek(date) IN (1,7)"))
    .withColumn("week_start_date", F.expr("date_sub(date, dayofweek(date) - 1)"))
    .withColumn("week_end_date", F.expr("date_add(date, 7 - dayofweek(date))"))
)

# -----------------------------
# MONTH / QUARTER BOUNDARIES
# -----------------------------
df_dim_date = (
    df_dim_date
    .withColumn("month_start_date", F.trunc("date", "month"))
    .withColumn("month_end_date", F.last_day("date"))
    .withColumn("quarter_start_date", F.expr("make_date(year(date), (quarter(date)-1)*3 + 1, 1)"))
)

# -----------------------------
# ISO CALENDAR (enterprise reporting standard)
# -----------------------------
df_dim_date = (
    df_dim_date
    .withColumn("iso_year", F.expr("year(date_add(date, 4 - dayofweek(date)))"))
    .withColumn("iso_week", F.expr("weekofyear(date)"))
)

# -----------------------------
# TIME INTELLIGENCE FLAGS
# -----------------------------
df_dim_date = (
    df_dim_date
    .withColumn("is_month_start", F.expr("day(date) = 1"))
    .withColumn("is_month_end", F.expr("date = last_day(date)"))
    .withColumn("is_quarter_start", F.expr("month(date) IN (1,4,7,10) AND day(date)=1"))
    .withColumn("is_quarter_end", F.expr("month(date) IN (3,6,9,12) AND date = last_day(date)"))
    .withColumn("is_year_start", F.expr("month(date)=1 AND day(date)=1"))
    .withColumn("is_year_end", F.expr("month(date)=12 AND day(date)=31"))
)

# -----------------------------
# WEEKDAY CLASSIFICATION
# -----------------------------
df_dim_date = (
    df_dim_date
    .withColumn(
        "day_type",
        F.expr("""
            CASE 
                WHEN dayofweek(date) IN (1,7) THEN 'Weekend'
                ELSE 'Weekday'
            END
        """)
    )
)

# -----------------------------
# FINAL STRUCTURE
# -----------------------------
df_dim_date = df_dim_date.select(
    "date_key",
    "date",
    "year",
    "quarter",
    "month",
    "month_name",
    "month_short",
    "day",
    "day_name",
    "day_of_week",
    "day_of_year",
    "week_of_year",
    "week_start_date",
    "week_end_date",
    "month_start_date",
    "month_end_date",
    "quarter_start_date",
    "iso_year",
    "iso_week",
    "day_type",
    "is_weekend",
    "is_month_start",
    "is_month_end",
    "is_quarter_start",
    "is_quarter_end",
    "is_year_start",
    "is_year_end"
)

display(df_dim_date)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Write to warehouse

# CELL ********************

df_dim_date.write.format("delta").mode("overwrite").synapsesql("jesper_gold_warehouse.dims.dim_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
