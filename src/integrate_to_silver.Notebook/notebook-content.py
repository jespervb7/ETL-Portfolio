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

# MARKDOWN ********************

# # Integrating Dead By Daylight data to my gold data warehouse

# CELL ********************

from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.column import Column
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from delta.tables import *
from typing import List, Union, Optional
from datetime import date

import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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

def add_game_result(
    generators_completed: Column,
    survivors_died: Column
) -> Column:
    """
    Create a Dead by Daylight game result column.

    This function generates a standardized Dead by Daylight game result
    string based on the number of survivors killed and generators completed.

    The output format follows:
        "<kills>K<generators>"

    Examples
    --------
    3 survivors died, 5 generators completed -> "3K5"
    4 survivors died, 2 generators completed -> "4K2"
    0 survivors died, 5 generators completed -> "0K5"

    Parameters
    ----------
    generators_completed : pyspark.sql.column.Column
        Column containing the number of generators completed (0–5).

    survivors_died : pyspark.sql.column.Column
        Column containing the number of survivors who died (0–4).

    Returns
    -------
    pyspark.sql.column.Column
        A PySpark Column containing the formatted game result string.

    Raises
    ------
    TypeError
        If inputs are not PySpark Column objects.
    """

    # -------------------------
    # Input Validation
    # -------------------------
    if not isinstance(generators_completed, Column):
        raise TypeError(
            "generators_completed must be a pyspark.sql.column.Column"
        )

    if not isinstance(survivors_died, Column):
        raise TypeError(
            "survivors_died must be a pyspark.sql.column.Column"
        )

    # -------------------------
    # Value Validation
    # -------------------------
    validated_generators = F.when(
        (generators_completed >= 0) & (generators_completed <= 5),
        generators_completed
    ).otherwise(F.lit(None))

    validated_kills = F.when(
        (survivors_died >= 0) & (survivors_died <= 4),
        survivors_died
    ).otherwise(F.lit(None))

    # -------------------------
    # Construct Result
    # -------------------------
    game_result = F.concat(
        validated_kills.cast("string"),
        F.lit("K"),
        validated_generators.cast("string")
    )

    return game_result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def to_date_key(
    col: Union[str, Column],
    input_format: Optional[str] = None,
    timezone: Optional[str] = None,
    strict: bool = False
) -> Column:
    """
    Convert a column to an integer date key (YYYYMMDD) which is a best practice in dimensional modelling

    Parameters
    ----------
    col : Union[str, Column]
        Input column (name or Column expression).
        Supported types: date, timestamp, string.

    input_format : Optional[str], default None
        Format string for parsing string columns (e.g. 'yyyy-MM-dd').
        If None, Spark will attempt implicit casting.

    timezone : Optional[str], default None
        Timezone to normalize timestamps before conversion (e.g. 'UTC', 'Europe/Amsterdam').

    strict : bool, default False
        If True, invalid parses result in NULL (explicit handling).
        If False, Spark default casting behavior applies.

    Returns
    -------
    Column
        Integer column formatted as YYYYMMDD.

    Notes
    -----
    - Output fits within 32-bit integer range.
    - Uses Spark-native functions (no UDF).
    - Null-safe: invalid or null inputs return null.
    """

    # Resolve column
    c: Column = F.col(col) if isinstance(col, str) else col

    # Handle timezone normalization (only meaningful for timestamps)
    if timezone:
        c = F.to_utc_timestamp(c, timezone)

    # Handle parsing
    if input_format:
        c = F.to_date(c, input_format)
    else:
        c = c.cast("date")

    # Optional strict handling (force null if invalid)
    if strict:
        c = F.when(c.isNotNull(), c).otherwise(F.lit(None))

    # Convert to YYYYMMDD integer
    return F.date_format(c, "yyyyMMdd").cast("int")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def to_bool(col: Column) -> Column:
    return (
        F.when(F.lower(col).isin("true", "1", "yes"), F.lit(True))
         .when(F.lower(col).isin("false", "0", "no"), F.lit(False))
         .otherwise(F.lit(None))
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sources_with_surrogatekey_name = {
    "games": ("game_id", "game_key"),
    "items": ("item_id", "item_key"),
    "item_type": ("item_type_id", "item_type_key"),
    "addons": ("addon_id", "addon_key"),
    "killers": ("killer_id", "killer_key"),
    "maps": ("map_id", "map_key"),
    "survivors": ("survivor_id", "survivor_key"),
}

dfs = {}

for table, (order_col, key_name) in sources_with_surrogatekey_name.items():
    dfs[table] = add_surrogate(
        spark.table(f"dbo.{table}"),
        order_cols=[order_col],
        key_name=key_name
    )

df_games = dfs["games"]
df_items = dfs["items"]
df_item_type = dfs["item_type"]
df_addons = dfs["addons"]
df_killers = dfs["killers"]
df_maps = dfs["maps"]
df_survivors = dfs["survivors"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_survivors_selected = (
    df_survivors.alias("s")
        .select(
            "s.survivor_key",
            "s.survivor_ingame_name",
            "s.survivor_id"
        )
)

display(df_survivors_selected)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_items_selected = (
    df_items.alias("i")
        .join(
            df_item_type.alias("t"),
            on="item_type_id",
            how="left"
        )
        .select(
            "i.item_key",
            "i.item_ingame_name",
            "t.item_name",
            "i.item_id"
        )
)

display(df_items_selected)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_addons_selected = (
    df_addons.alias("a")
        .join(
            df_item_type.alias("t"),
            on="item_type_id",
            how="left"
        )
        .select(
            "a.addon_key",
            "a.addon_ingame_name",
            "t.item_name",
            "a.addon_id"
        )
)

display(df_addons_selected)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_maps_selected = (
    df_maps.alias("m")
        .select(
            "m.map_key",
            "m.map_ingame_name",
            "m.map_realm",
            "m.map_id"
        )
)

display(df_maps_selected)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_killers_selected = (
    df_killers.alias("k")
        .select(
            "k.killer_key",
            "k.killer_ingame_name",
            "k.killer_id"
        )
)

display(df_killers_selected)

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

# # Prepare games table for extracting dimensions and fact

# CELL ********************

df_games_prepared = (
    df_games
        .filter(
            F.trim(F.col("game_id")).isNotNull() &
            (F.trim(F.col("game_id")) != "")
        )
        .withColumn("escaped_through_hatch", to_bool(F.col("escaped_through_hatch")))
        .withColumn("did_killer_quit", to_bool(F.col("did_killer_quit")))
        .withColumn("was_killer_farming", to_bool(F.col("was_killer_farming")))
        .withColumn("did_game_have_cheater", to_bool(F.col("did_game_have_cheater")))
        .withColumn("amount_of_survivors_escaped", F.col("amount_of_survivors_escaped").cast("int"))
        .withColumn("generators_completed", F.col("generators_completed").cast("int"))
        .withColumn("personal_hook_stages", F.col("personal_hook_stages").cast("int"))
        .withColumn("team_hook_stages", F.col("team_hook_stages").cast("int"))
        .withColumn("fresh_hooks", F.col("fresh_hooks").cast("int"))
        .withColumn("blood_points", F.col("blood_points").cast("int"))
        .withColumn(
            "amount_of_survivors_died",
            F.lit(4).cast("int") - F.col("amount_of_survivors_escaped")
        )
        .withColumn(
            "game_result",
            add_game_result(
                F.col("generators_completed"),
                F.col("amount_of_survivors_died")
            )
        )
        .withColumn(
            "date_played_key",
            to_date_key(col="date_game_played")
        )
        .withColumn(
            "did_escape",
            F.col("personal_hook_stages") == 3
        )
)

display(df_games_prepared)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_games_info = (
    add_surrogate(
        df_games_prepared.alias("g")
            .select(
                "g.escaped_through_hatch",
                "g.did_killer_quit",
                "g.was_killer_farming",
                "g.did_game_have_cheater",
                "g.game_mode",
                "g.game_type",
                "g.game_result",
                "g.did_escape"
            )
            .distinct(),
        [
            "escaped_through_hatch",
            "did_killer_quit",
            "was_killer_farming",
            "did_game_have_cheater",
            "game_mode",
            "game_type",
            "game_result",
            "did_escape"
        ],
        "game_info_key"
    )
    .select(
        "game_info_key",
        "escaped_through_hatch",
        "did_killer_quit",
        "was_killer_farming",
        "did_game_have_cheater",
        "game_mode",
        "game_type",
        "game_result",
        "did_escape"
    )
)

df_games_info = df_games_info.withColumn(
    "is_valid_game",
    (
        (F.col("did_killer_quit") == False) &
        (F.col("was_killer_farming") == False) &
        (F.col("did_game_have_cheater") == False) &
        (F.col("game_type") == "Regular")
    )
)

display(df_games_info)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fact_games = (
    df_games_prepared.alias("g")
    .join(
        df_survivors_selected.alias("s"),
        F.col("g.survivor_id") == F.col("s.survivor_id"),
        "left"
    )
    .join(
        df_killers_selected.alias("k"),
        F.col("g.killer_id") == F.col("k.killer_id"),
        "left"
    )
    .join(
        df_maps_selected.alias("m"),
        F.col("g.map_id") == F.col("m.map_id"),
        "left"
    )
    .join(
        df_items_selected.alias("i"),
        F.col("g.item_id") == F.col("i.item_id"),
        "left"
    )
    .join(
        df_addons_selected.alias("a"),
        F.col("g.addon_id") == F.col("a.addon_id"),
        "left"
    )
    .join(
        df_games_info.alias("gi"),
        [
            F.col("g.escaped_through_hatch") == F.col("gi.escaped_through_hatch"),
            F.col("g.did_killer_quit") == F.col("gi.did_killer_quit"),
            F.col("g.was_killer_farming") == F.col("gi.was_killer_farming"),
            F.col("g.did_game_have_cheater") == F.col("gi.did_game_have_cheater"),
            F.col("g.game_mode") == F.col("gi.game_mode"),
            F.col("g.game_type") == F.col("gi.game_type"),
            F.col("g.game_result") == F.col("gi.game_result"),
            F.col("g.did_escape") == F.col("gi.did_escape")
        ],
        "left"
    )
    .select(
        "g.date_played_key",
        "s.survivor_key",
        "k.killer_key",
        "m.map_key",
        "i.item_key",
        "a.addon_key",
        "gi.game_info_key",

        "g.personal_hook_stages",
        "g.team_hook_stages",
        "g.fresh_hooks",
        "g.amount_of_survivors_escaped",
        "g.generators_completed",
        "g.blood_points"
    )
)

display(df_fact_games)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_to_fabric(
    df: DataFrame,
    table_name: str,
    schema: str,
    warehouse: str = "jesper_gold_warehouse",
    mode: str = "overwrite"
) -> None:
    """
    Write a Spark DataFrame to Microsoft Fabric Warehouse.

    Parameters
    ----------
    df : DataFrame
        Spark DataFrame to write

    table_name : str
        Target table name

    schema : str
        Target schema (e.g. dims, facts)

    warehouse : str, default "jesper_gold_warehouse"
        Fabric warehouse name

    mode : str, default "overwrite"
        Write mode: overwrite | append
    """

    full_table_name = f"{warehouse}.{schema}.{table_name}"

    (
        df.write
        .format("delta")
        .mode(mode)
        .synapsesql(full_table_name)
    )

    print(f"Written to {full_table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def drop_id_columns(df: DataFrame) -> DataFrame:
    """
    Drops all columns ending with '_id' from a dataframe.
    Typically used for dimension tables after surrogate keys are created.
    """
    cols_to_drop = [c for c in df.columns if c.endswith("_id")]
    return df.drop(*cols_to_drop)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_games_info)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

write_to_fabric(drop_id_columns(df_survivors_selected), "dim_survivor", "dims")
write_to_fabric(drop_id_columns(df_killers_selected), "dim_killer", "dims")
write_to_fabric(drop_id_columns(df_maps_selected), "dim_map", "dims")
write_to_fabric(drop_id_columns(df_items_selected), "dim_item", "dims")
write_to_fabric(drop_id_columns(df_addons_selected), "dim_addon", "dims")
write_to_fabric(drop_id_columns(df_games_info), "dim_game_info", "dims")
write_to_fabric(df_dim_date, "dim_date", "dims")
write_to_fabric(df_fact_games, "fact_games", "facts")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
