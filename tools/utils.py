from tools.config import CHECKPOINTS_ROOT, CATALOG, VOLUME_LANDING_ROOT
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window as W

# --------------------------------------
# AUTOLOADER
# --------------------------------------

def helper_autoloader(
    spark_session: SparkSession,
    input_path: str,
    target_tbl: str,
    checkpoint_loc: str,
    schema_loc: str,
    file_type: str = "csv",
    header: bool = False,
    multi_line: bool = False,
    metadata: bool = False,
):
    
    df = (spark_session.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", file_type)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", schema_loc)
        .option("header", str(header).lower())
        .option("multiLine", str(multi_line).lower())
        .load(input_path)
    )

    if metadata:
        df = (
            df.withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
        )

    
    (df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_loc)
        .outputMode("append")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target_tbl)
    )

# --------------------------------------
# LIMPIEZA
# --------------------------------------

def dedup_by(
    spark_session: SparkSession,
    df: DataFrame,
    *keys: str
):
    return df.dropDuplicates(list(keys) if keys else df.columns)

def normalize_cdc_latest(
    spark_session: SparkSession,
    df: DataFrame,
    key_col: str,
    seq_col: str = "seq_num",
    ts_col: str = "event_time",
    tiebreakers: list[str] | None = None,
):
    tiebreakers = tiebreakers or []
    
    base = dedup_by(spark_session, df)

    order_exprs = [
        F.col(seq_col).desc_nulls_last(),
        F.col(ts_col).desc_nulls_last()
    ] + [F.col(c).desc_null_lasts() for c in tiebreakers]

    w = W.partitionBy(key_col).orderBy(*order_exprs)

    ranked = base.withColumn("_rn", F.row_number().over(w))
    latest = ranked.filter(F.col("_rn") == 1).drop("_rn")

    return latest

def normalize_ascii_lower(col):
    return F.translate(F.lower(F.trim(col)), "áéíóúüñ", "aeiouun")


    
        
    