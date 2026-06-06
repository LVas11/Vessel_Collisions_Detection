import os
import math
import argparse
import glob
from datetime import timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, IntegerType, LongType)
from pyspark.sql.window import Window
from functools import reduce
from pyspark.sql import DataFrame


# Set Up 
CENTER_LAT = 55.225000     # given
CENTER_LON = 14.245000     # given
RADIUS = 50 
NM_PER_DEG = 60     # 1 degree latitude is 60 nm
GRID_SIZE = 0.05    # degree grid cell
COLLISION_THRESHOLD = 500   # threshold for close encounters (m)
TIME_BUCKET = 60    # seconds
MAX_SPEED = 45      # realistic speed, to avoid GPS glitches
MIN_SPEED = 1       # considered stationary otherwise

EARTH_RADIUS_NM = 3440.065
EARTH_RADIUS_M  = 6371000.0

# Haversine distance function for Spark UDFs
def haversine(lat1, lon1, lat2, lon2, radius):
    """
    Helper function for calculating Haversine distance without using Python UDF
    """
    dphi = F.radians(lat2 - lat1)
    dlam = F.radians(lon2 - lon1)
    a = (F.sin(dphi / 2) * F.sin(dphi / 2)
         + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2))
           * F.sin(dlam / 2) * F.sin(dlam / 2))
    return radius * 2 * F.asin(F.sqrt(a))

#------------------------------------------
# SPARK SETUP
#-------------------------------------------

def build_spark(app_name: str = "AIS_Collision_Detector") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", "50mb")
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

#-----------------------------------------------
# DATA 
# ----------------------------------------------
AIS_columns = StructType([
    StructField("# Timestamp",        StringType(),  True),
    StructField("Type of mobile",     StringType(),  True),
    StructField("MMSI",               LongType(),    True),
    StructField("Latitude",           DoubleType(),  True),
    StructField("Longitude",          DoubleType(),  True),
    StructField("Navigational status",StringType(),  True),
    StructField("ROT",                DoubleType(),  True),
    StructField("SOG",                DoubleType(),  True), 
    StructField("COG",                DoubleType(),  True),
    StructField("Heading",            DoubleType(),  True),
    StructField("IMO",                StringType(),  True),
    StructField("Callsign",           StringType(),  True),
    StructField("Name",               StringType(),  True),
    StructField("Ship type",          StringType(),  True),
])

def clean_column_names(df):
    for c in df.columns:
        new_c = (
            c.strip()
             .replace("# ", "")
             .replace(" ", "_")
        )
        df = df.withColumnRenamed(c, new_c)
    return df
    

def load_dt(spark: SparkSession, data_path: str):
    """
    Loading raw AIS data from CSV files.
    """
    paths = glob.glob(data_path)

    if not paths:
        raise FileNotFoundError(f"No files matched path: {data_path}")

    print(f"Found {len(paths)} input files")
    for p in paths[:5]:
        print(" ", p)
    if len(paths) > 5:
        print("  ...")
    
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("nullValue", "Unknown")
        .option("nullValue", "")
        .schema(AIS_columns)
        .csv(paths)
    )

    if "# Timestamp" in df.columns:
        df = df.withColumnRenamed("# Timestamp", "Timestamp")

    return df


def clean_dt(df):
    """
    Cleaning and filteirng AIS data.
    """

    # Parsing time stamps 
    df = df.withColumn(
        "ts",
        F.to_timestamp(F.col("Timestamp"), "dd/MM/yyyy HH:mm:ss")
    )

    # Dropping rows with NA's in most important fields
    df = df.dropna(subset=["ts", "MMSI", "Latitude", "Longitude", "SOG"])

    # December only
    df = df.filter(
        (F.col("ts") >= F.lit("2021-12-01 00:00:00").cast(TimestampType())) &
        (F.col("ts") <  F.lit("2022-01-01 00:00:00").cast(TimestampType()))
    )

    # Keeping only real vessel MMSIs: 9 digits, starting with 2–7
    df = df.filter(
    F.col("MMSI").cast("string").rlike("^[2-7][0-9]{8}$")
    )

    # Keeping only Class A and B vessels
    df = df.filter(
    F.col("Type_of_mobile").isin(["Class A", "Class B"])
    )

    # Coordinates just for Baltic Sea + some buffer
    df = df.filter(
        (F.col("Latitude")  >= 50)  & (F.col("Latitude")  <= 70) &
        (F.col("Longitude") >= 5) & (F.col("Longitude") <= 35)
    )

    # Filtering bounding box first
    lat_margin = RADIUS / NM_PER_DEG                          
    lon_margin = RADIUS / (NM_PER_DEG * math.cos(math.radians(CENTER_LAT)))

    df = df.filter(
        (F.col("Latitude")  >= CENTER_LAT - lat_margin) &
        (F.col("Latitude")  <= CENTER_LAT + lat_margin) &
        (F.col("Longitude") >= CENTER_LON - lon_margin) &
        (F.col("Longitude") <= CENTER_LON + lon_margin)
    )

    # Exact Haversine-based distance filter 
    df = df.withColumn(
        "dist_from_center_nm",
        haversine(F.lit(CENTER_LAT), F.lit(CENTER_LON),
                  F.col("Latitude"), F.col("Longitude"), EARTH_RADIUS_NM)
    )
    df = df.filter(F.col("dist_from_center_nm") <= RADIUS)

    # Calculating the implied speed for two consecutive points for a vessel
    w = Window.partitionBy("MMSI").orderBy("ts")

    df = df.withColumn("prev_lat",  F.lag("Latitude",  1).over(w))
    df = df.withColumn("prev_lon",  F.lag("Longitude", 1).over(w))
    df = df.withColumn("prev_ts",   F.lag("ts",        1).over(w))
   
    df = df.withColumn(
        "step_nm",
        haversine(F.col("prev_lat"), F.col("prev_lon"),
                  F.col("Latitude"), F.col("Longitude"), EARTH_RADIUS_NM)
    )
    df = df.withColumn(
        "step_hours",
        (F.col("ts").cast("long") - F.col("prev_ts").cast("long")) / 3600.0
    )
    df = df.withColumn(
        "implied_speed_kn",
        F.when(F.col("step_hours") > 0,
               F.col("step_nm") / F.col("step_hours")).otherwise(F.lit(0.0))
    )

    # Filter out rows with unreasonable speed (keeping the first rows as nothing to compare to)
    df = df.filter(
        F.col("prev_ts").isNull() | (F.col("implied_speed_kn") <= MAX_SPEED)
    )

    # Removing stationary vessel -----------------

    # Based on status
    stationary_status = [
        "At anchor", "Moored", "Aground",
        "Not under command", "Restricted manoeuverability", "Constrained by her draught",
        "Engaged in fishing"
    ]
    df = df.filter(
        ~F.col("Navigational_status").isin(stationary_status)
    )

    # Based on min speed (mean and individual)
    mean_sog = (
        df.groupBy("MMSI")
          .agg(F.mean("SOG").alias("mean_sog"))
          .filter(F.col("mean_sog") >= MIN_SPEED)
    )
    df = df.join(mean_sog.select("MMSI"), on="MMSI", how="inner")

    df = df.filter(F.col("SOG") >= 0.5)

    # Exclude rescue / SAR / pilot vessels

    exclude_keywords = [
        "SAR",
        "RESCUE",
        "PILOT",
        "TUG",
        "POLICE",
        "KBV",
        "NAVY",
        "GUARD"
    ]

    name_upper = F.upper(F.coalesce(F.col("Name"), F.lit("")))
    ship_type_upper = F.upper(F.coalesce(F.col("Ship_type"), F.lit("")))

    condition = F.lit(True)

    for word in exclude_keywords: condition = (
        condition
        & ~name_upper.contains(word)
        & ~ship_type_upper.contains(word)
    )

    df = df.filter(condition)

    # Useful columns only
    df = df.select(
        "MMSI", "ts", "Latitude", "Longitude", "SOG", "COG",
        "Navigational_status", "Name", "Ship_type"
    )
    return df


# --------------------------------------
# CREATING BUCKETS
# ---------------------------------------

def add_buckets(df):
    """
    Each vessel record is assigned to grid cell and a time bucket to limit teh number 
    of pair-wise comparisons.
    """
    df = df.withColumn("grid_lat", (F.col("Latitude")  / GRID_SIZE).cast(IntegerType()))
    df = df.withColumn("grid_lon", (F.col("Longitude") / GRID_SIZE).cast(IntegerType()))
    
    # Time bucket
    df = df.withColumn(
        "time_bucket",
        (F.unix_timestamp("ts") / TIME_BUCKET).cast(LongType())
    )
    return df

#--------------------------------------
# DETECTING CANDIDATE ENCOUNTERS
# ---------------------------------------

def find_candidates(df):
    """
    Checking for close encounters by joining vessels in the same time and grid cell.
    Neighbouring grid cells are also checked before joining.
    """

    # Getting neighbouring grid keys
    neighbours = []
    for dlat in [-1, 0, 1]:
        for dlon in [-1, 0, 1]:
            neighbours.append(
                df.withColumn(
                    "join_grid_key",
                    F.concat_ws("_",
                        (F.col("grid_lat") + dlat).cast(IntegerType()),
                        (F.col("grid_lon") + dlon).cast(IntegerType())
                    )
                )
            )

    expanded = reduce(DataFrame.unionByName, neighbours)

    # Join ones with the same grid key and time bucket (different MMSI)
    a = expanded.alias("a")
    b = df.alias("b")

    pairs = a.join(
    b,
    on=(
        (F.col("a.join_grid_key") == F.concat_ws("_",
            F.col("b.grid_lat").cast(IntegerType()),
            F.col("b.grid_lon").cast(IntegerType())
        )) &
        (F.col("a.time_bucket") == F.col("b.time_bucket")) &
        (F.col("a.MMSI") < F.col("b.MMSI"))
    ),
    how="inner"
)

    # Calculating course difference
    pairs = pairs.withColumn(
        "course_diff",
        F.abs(F.col("a.COG") - F.col("b.COG"))
    )

    pairs = pairs.withColumn(
        "course_diff",
        F.when(
            F.col("course_diff") > 180,
            360 - F.col("course_diff")
        ).otherwise(F.col("course_diff"))
    )

    # Removing vessels moving almost parallel
    pairs = pairs.filter(F.col("course_diff") >= 20)

    # Both vessels must be making way (excludes slow harbour activity)
    pairs = pairs.filter((F.col("a.SOG") >= 5) & (F.col("b.SOG") >= 5))

    # Haversine distance
    pairs = pairs.withColumn(
        "dist_m",
        haversine(F.col("a.Latitude"), F.col("a.Longitude"),
                  F.col("b.Latitude"), F.col("b.Longitude"), EARTH_RADIUS_M)
    )

    # Keeping only close encounters
    candidates = pairs.filter(F.col("dist_m") <= COLLISION_THRESHOLD)

    candidates = candidates.select(
    F.col("a.MMSI").alias("mmsi_1"),
    F.col("b.MMSI").alias("mmsi_2"),
    F.col("a.Name").alias("vessel_1"),
    F.col("b.Name").alias("vessel_2"),
    F.col("a.time_bucket").alias("time_bucket"),
    F.col("a.ts").alias("time_1"),
    F.col("b.ts").alias("time_2"),
    F.col("a.Latitude").alias("lat_1"),
    F.col("a.Longitude").alias("lon_1"),
    F.col("b.Latitude").alias("lat_2"),
    F.col("b.Longitude").alias("lon_2"),
    "course_diff",
    "dist_m",
    F.col("a.SOG").alias("sog_1"),
    F.col("b.SOG").alias("sog_2")
)

    # Remove duplicates caused by neighbour expansion
    candidates = candidates.dropDuplicates([
        "mmsi_1", "mmsi_2", "time_1", "time_2"
    ])

    return candidates

#--------------------------------------
# Finding the closest encounter
#---------------------------------------

def find_encounter(candidates):
    """
    Picking closest encounter from the candidates.
    """
    result = (
        candidates
        .select(
            F.col("mmsi_1").alias("mmsi_a"),
            F.col("mmsi_2").alias("mmsi_b"),
            F.col("vessel_1").alias("name_a"),
            F.col("vessel_2").alias("name_b"),
            F.col("time_1").alias("ts_a"),
            F.col("time_2").alias("ts_b"),
            F.col("lat_1").alias("lat_a"),
            F.col("lon_1").alias("lon_a"),
            F.col("lat_2").alias("lat_b"),
            F.col("lon_2").alias("lon_b"),
            "dist_m"
        )
        .orderBy("dist_m")
        .limit(1)
    )

    rows = result.collect()
    return rows[0] if rows else None

#--------------------------------------
# Getting the trajectories
#--------------------------------------

def get_trajectories(df, mmsi_a: int, mmsi_b: int, collision_ts):
    """
    Getting the track of both vessels in the encounter within 20 minutes window.
    """
    t_start = collision_ts - timedelta(minutes=10)
    t_end = collision_ts + timedelta(minutes=10)

    track = (
        df.filter(
            F.col("MMSI").isin([mmsi_a, mmsi_b]) &
            (F.col("ts") >= F.lit(t_start).cast(TimestampType())) &
            (F.col("ts") <= F.lit(t_end).cast(TimestampType()))
        )
        .select("MMSI", "Name", "ts", "Latitude", "Longitude", "SOG")
        .orderBy("MMSI", "ts")
    )
    return track

#--------------------------------------
# MAIN
#--------------------------------------

def main():
    parser = argparse.ArgumentParser(description="AIS Collision Detector")
    parser.add_argument("--data", default="./Data/aisdk-2021-12-*.csv")
    parser.add_argument("--output", default="./output")
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)
    spark = build_spark()

    print("Step 1/6 - Loading raw AIS data")
    raw = load_dt(spark, args.data)
    raw = clean_column_names(raw)

    print("Step 2/6 - Cleaning and filtering")
    clean = clean_dt(raw)
    clean.cache()
    n = clean.count()
    print(f"          {n:,} valid records after filtering")


    print("Step 3/6 - Adding buckets")
    bucketed = add_buckets(clean)

    print("Step 4/6 - Finding collision candidates")
    candidates = find_candidates(bucketed)
    candidates.cache()
    nc = candidates.count()
    print(f"          {nc:,} candidate close encounters (≤{COLLISION_THRESHOLD} m)")

    if nc == 0:
        print("No collision candidates found")
        spark.stop()
        return

    print("Step 5/6 - Identifying closest encounter")
    event = find_encounter(candidates)
    if event is None:
        print("Could not determine closest encounter")
        spark.stop()
        return

    collision_ts = event.ts_a
    lat_c = (event.lat_a + event.lat_b) / 2
    lon_c = (event.lon_a + event.lon_b) / 2

    print("\n" + "=" * 60)
    print("COLLISION EVENT DETECTED")
    print(f"Vessel A: MMSI {event.mmsi_a}  /  {event.name_a}")
    print(f"Vessel B: MMSI {event.mmsi_b}  /  {event.name_b}")
    print(f"Timestamp: {collision_ts}")
    print(f"Location: {lat_c:.6f}°N, {lon_c:.6f}°E")
    print(f"Distance: {event.dist_m:.1f} m")

    # Write summary to file
    summary_path = os.path.join(args.output, "collision_summary.txt")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("AIS COLLISION DETECTION: RESULTS\n")
        f.write("=" * 40 + "\n")
        f.write(f"Vessel A MMSI: {event.mmsi_a}\n")
        f.write(f"Vessel A Name: {event.name_a}\n")
        f.write(f"Vessel B MMSI: {event.mmsi_b}\n")
        f.write(f"Vessel B Name: {event.name_b}\n")
        f.write(f"Timestamp: {collision_ts}\n")
        f.write(f"Latitude: {lat_c:.6f}\n")
        f.write(f"Longitude: {lon_c:.6f}\n")
        f.write(f"Min distance: {event.dist_m:.1f} m\n")

    # Top 10 closest encounters (inspection + report)
    top_path = os.path.join(args.output, "top_encounters.csv")
    (candidates
        .orderBy("dist_m")
        .limit(10)
        .toPandas()
        .to_csv(top_path, index=False))
    print(f"Top 10 encounters saved {top_path}")

    print("Step 6/6 - Extracting trajectories")
    track = get_trajectories(clean, event.mmsi_a, event.mmsi_b, collision_ts)
    track.toPandas().to_csv(os.path.join(args.output, "trajectories.csv"), index=False)
    print("Trajectories saved to trajectories.csv")

    spark.stop()
    print("Done")


if __name__ == "__main__":
    main()
