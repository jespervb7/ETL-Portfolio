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
# META     },
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

# CELL ********************

# Integrating Dead By Daylight data to my gold data warehouse

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from delta.tables import *

import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from typing import List, Union
from pyspark.sql import DataFrame

def add_surrogate(df: DataFrame, order_cols: Union[str, List[str]], key_name: str) -> DataFrame:
    """Add a surrogate integer key column to a Spark DataFrame.
    Parameters:
    df (pyspark.sql.DataFrame): input Spark DataFrame.
    order_cols (str | list[str]): column name or list of column names to deterministically order rows.
    key_name (str): name of the generated surrogate key column.

    Returns:
    pyspark.sql.DataFrame: the input DataFrame with an added integer surrogate key column named `key_name`.
    """
    # Validate inputs
    if isinstance(order_cols, str):
        order_cols = [order_cols]
    if not order_cols or not all(isinstance(c, str) and c for c in order_cols):
        raise ValueError("order_cols must be a non-empty string or list of non-empty column name strings")
    if not isinstance(key_name, str) or not key_name:
        raise ValueError("key_name must be a non-empty string")
    
    # Create a window specification that orders by the specified columns
    w = Window.orderBy(*[F.col(c) for c in order_cols])
    
    return df.withColumn(key_name, F.row_number().over(w))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_survivors = spark.read.table("dbo.survivors")
df_survivors_with_key = add_surrogate(df_survivors, order_cols=["survivor_id"], key_name="survivor_key")
df_survivors_with_key.show()

# Select only survivor_ingame_name and survivor_key columns. Make survivor_key the first column in the output.
df_survivors_selected = df_survivors_with_key.select("survivor_key", "survivor_ingame_name")
df_survivors_selected.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_survivors_selected.write.format("delta").mode("overwrite").synapsesql("jesper_gold_warehouse.dims.dim_survivor")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Read source tables
games = spark.table("dbo.games")
items = spark.table("dbo.items")
item_type = spark.table("dbo.item_type")
addons = spark.table("dbo.addons")
killers = spark.table("dbo.killers")
maps = spark.table("dbo.maps")
survivors = spark.table("dbo.survivors")

def add_surrogate(df, order_cols, key_name):
    """Add a surrogate integer key column to a Spark DataFrame.
    Parameters:
    df (pyspark.sql.DataFrame): input Spark DataFrame.
    order_cols (str | list[str]): column name or list of column names to deterministically order rows.
    key_name (str): name of the generated surrogate key column.

    Returns:
    pyspark.sql.DataFrame: the input DataFrame with an added integer surrogate key column named `key_name`.
    """
    if isinstance(order_cols, str):
        order_cols = [order_cols]
    if not order_cols or not all(isinstance(c, str) and c for c in order_cols):
        raise ValueError("order_cols must be a non-empty string or list of non-empty column name strings")
    if not isinstance(key_name, str) or not key_name:
        raise ValueError("key_name must be a non-empty string")
    w = Window.orderBy(*[F.col(c) for c in order_cols])
    return df.withColumn(key_name, F.row_number().over(w))

# DIM: items
dim_item_src = (items.join(item_type, on="item_type_id", how="left")
                .select(F.col("item_id").alias("item_code"),
                        F.coalesce(F.col("item_ingame_name"), F.col("item_name")).alias("item_name"),
                        F.col("item_type_id").alias("item_group"))
                .dropDuplicates(["item_code"]))
dim_item = add_surrogate(dim_item_src.orderBy("item_code"), ["item_code"], "item_id").select(F.col("item_id").cast("int").alias("item_id"), "item_code", "item_name", "item_group")
dim_item.write.format("delta").mode("overwrite").saveAsTable("dims.dim_item")

# DIM: addons
dim_addon_src = (addons.select(F.col("addon_id").alias("addon_code"), F.col("addon_ingame_name").alias("addon_name"))
                 .dropDuplicates(["addon_code"]))
dim_addon = add_surrogate(dim_addon_src.orderBy("addon_code"), ["addon_code"], "addon_id").select(F.col("addon_id").cast("int").alias("addon_id"), "addon_code", "addon_name")
dim_addon.write.format("delta").mode("overwrite").saveAsTable("dims.dim_addon")

# DIM: killers
dim_killer_src = (killers.select(F.col("killer_id").alias("killer_code"), F.col("killer_ingame_name").alias("killer_name"))
                 .dropDuplicates(["killer_code"]))
dim_killer = add_surrogate(dim_killer_src.orderBy("killer_code"), ["killer_code"], "killer_id").select(F.col("killer_id").cast("int").alias("killer_id"), "killer_code", "killer_name")
dim_killer.write.format("delta").mode("overwrite").saveAsTable("dims.dim_killer")

# DIM: maps
dim_map_src = (maps.select(F.col("map_id").alias("map_code"), F.col("map_ingame_name").alias("map_name"), F.col("map_realm"))
               .dropDuplicates(["map_code"]))
dim_map = add_surrogate(dim_map_src.orderBy("map_code"), ["map_code"], "map_id").select(F.col("map_id").cast("int").alias("map_id"), "map_code", "map_name", "map_realm")
dim_map.write.format("delta").mode("overwrite").saveAsTable("dims.dim_map")

# DIM: survivors
dim_survivor_src = (survivors.select(F.col("survivor_id").alias("survivor_code"), F.col("survivor_ingame_name").alias("survivor_name"))
                    .dropDuplicates(["survivor_code"]))
dim_survivor = add_surrogate(dim_survivor_src.orderBy("survivor_code"), ["survivor_code"], "survivor_id").select(F.col("survivor_id").cast("int").alias("survivor_id"), "survivor_code", "survivor_name")
dim_survivor.write.format("delta").mode("overwrite").saveAsTable("dims.dim_survivor")

# DIM: date
dim_date_src = (games.select(F.col("date_game_played").alias("date_played"))
                .where(F.col("date_game_played").isNotNull())
                .dropDuplicates(["date_played"]))
dim_date = add_surrogate(dim_date_src.orderBy("date_played"), ["date_played"], "date_id")
dim_date = dim_date.withColumn("year", F.year("date_played")).withColumn("month", F.month("date_played")).withColumn("day", F.dayofmonth("date_played"))
dim_date = dim_date.select(F.col("date_id").cast("int").alias("date_id"), "date_played", "year", "month", "day")
dim_date.write.format("delta").mode("overwrite").saveAsTable("dims.dim_date")

# DIM: time placeholder
dim_time = spark.createDataFrame([(1, None)], ["time_id", "time_value"])
dim_time.write.format("delta").mode("overwrite").saveAsTable("dims.dim_time")

# DIM: gameinfo
dim_gameinfo_src = games.select("game_mode", "game_type").dropDuplicates(["game_mode", "game_type"])
dim_gameinfo = add_surrogate(dim_gameinfo_src.orderBy("game_mode", "game_type"), ["game_mode", "game_type"], "gameinfo_id").select(F.col("gameinfo_id").cast("int").alias("gameinfo_id"), "game_mode", "game_type")
dim_gameinfo.write.format("delta").mode("overwrite").saveAsTable("dims.dim_gameinfo")

# BRIDGE: addons
bridge_addons_src = addons.select(F.col("addon_id").alias("addon_code")).dropDuplicates(["addon_code"])
bridge_addons_join = bridge_addons_src.join(dim_addon.select("addon_code", "addon_id"), on="addon_code", how="left")
bridge_addons = add_surrogate(bridge_addons_join.orderBy("addon_code"), ["addon_code"], "addonbridge_id").select(F.col("addonbridge_id").cast("int").alias("addonbridge_id"), F.col("addon_id").cast("int").alias("addon_id"))
bridge_addons.write.format("delta").mode("overwrite").saveAsTable("bridge.bridge_addons")

# FACT: assemble
facts = (games
         .join(dim_survivor.select("survivor_id", "survivor_code"), games.survivor_id == F.col("survivor_code"), "left")
         .join(dim_map.select("map_id", "map_code"), games.map_id == F.col("map_code"), "left")
         .join(dim_killer.select("killer_id", "killer_code"), games.killer_id == F.col("killer_code"), "left")
         .join(dim_item.select("item_id", "item_code"), games.item_id == F.col("item_code"), "left")
         .join(bridge_addons.select("addonbridge_id", "addon_id"), games.addon_id == F.col("addon_code"), "left")
         .join(dim_date.select(F.col("date_played").alias("date_played"), F.col("date_id").alias("dateplayed_id")), games.date_game_played == F.col("date_played"), "left")
         .join(dim_time)
         .join(dim_gameinfo.select("gameinfo_id", "game_mode", "game_type"), on=["game_mode", "game_type"], how="left")
         .select(
             F.col("survivor_id").cast("int").alias("survivor_id"),
             F.col("map_id").cast("int").alias("map_id"),
             F.col("killer_id").cast("int").alias("killer_id"),
             F.col("item_id").cast("int").alias("item_id"),
             F.col("addonbridge_id").cast("int").alias("addonbridge_id"),
             F.col("dateplayed_id").cast("int").alias("dateplayed_id"),
             F.col("time_id").cast("int").alias("time_id"),
             F.col("gameinfo_id").cast("int").alias("gameinfo_id"),
             F.col("personal_hook_stages").alias("personal_hook_stages"),
             F.col("team_hook_stages").alias("team_hook_stages"),
             F.col("fresh_hooks").alias("fresh_hook_stages"),
             F.col("amount_of_survivors_escaped").alias("amount_of_survivors_escaped"),
             F.col("generators_completed").alias("generators_completed"),
             F.col("blood_points").alias("blood_points")
         ))
facts.write.format("delta").mode("overwrite").saveAsTable("facts.fact_deadbydaylight_games")

# Counts
print("dims.dim_item:", spark.table("dims.dim_item").count())
print("dims.dim_addon:", spark.table("dims.dim_addon").count())
print("bridge.bridge_addons:", spark.table("bridge.bridge_addons").count())
print("facts.fact_deadbydaylight_games:", spark.table("facts.fact_deadbydaylight_games").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Smoke test for add_surrogate — run this cell in the notebook kernel
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# small source DF
df_test = spark.createDataFrame([(100,'alpha'), (200,'beta')], ['item_code','item_name'])
res = add_surrogate(df_test.orderBy('item_code'), ['item_code'], 'item_id')
display(res)
# basic assertions
assert 'item_id' in res.columns, 'item_id column missing'
vals = [r['item_id'] for r in res.select('item_id').collect()]
assert all(v is not None for v in vals), 'null values in item_id'
print('SMOKE_TEST: PASS')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
