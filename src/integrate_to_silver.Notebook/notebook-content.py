# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fd9e9c25-32d5-4ecc-99ac-c5e050362b63",
# META       "default_lakehouse_name": "dead_by_daylight_bronze_lakehouse",
# META       "default_lakehouse_workspace_id": "605b5796-17cb-4c40-a615-702a6726ae8c",
# META       "known_lakehouses": [
# META         {
# META           "id": "fd9e9c25-32d5-4ecc-99ac-c5e050362b63"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Integrating Dead By Daylight data to the silver lakehouse

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM dead_by_daylight_bronze_lakehouse.dbo.games")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
