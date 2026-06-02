'''
0. README
Notebook: Cluster_Preper_Vel_Weight&Grad
Inputs
•	pt_configuration_thesis.alltrucks_v1.salescodecomboXvelocityspectra
•	pt_configuration_thesis.alltrucks_v1.salescodecomboXweightspectra
•	pt_configuration_thesis.alltrucks_v1.salescodecomboXgradientspectra (X = 1,2,....15)
Outputs
•	pt_configuration_thesis.alltrucks_v1.salescodecomboXvelocityspectracomplete
•	pt_configuration_thesis.alltrucks_v1.salescodecomboXweightspectracomplete
•	pt_configuration_thesis.alltrucks_v1.salescodecomboXgradientspectracomplete (X = 1,2,....15)
Purpose
•	This notebook is meant to take as input the generated raw spectras of velocity, weight and gradient and generate as output the completed spectras. This is because TALPY generates spectras of unequal length depending on the maximum value/ last bin of a particular truck.
•	However, since downstream wasserstein distance calculation requires spectras of equal lengths the above described step is necessary.
•	The spectras have also been normalized for initial viewing if everything makes sense. However, they will again me normalized and directly fed into the ML algorithms in later notebooks. Normalization here is purely to make sure of the data quality.
Other remarks
•	This step of ensuring a complete velocity spectrum per vehicle by generating all VIN–interval combinations, filling missing bins with zero counts, and restoring correct numeric ordering of interval bins can be made a bit easier. The current version soles the purpose, but can be updates to make it cleaner in the next versions.
'''

# 1.	HEADERS
import pyspark.sql.functions as f
import pandas as pd

from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from functools import reduce

import datetime
import warnings

from talpy.timeseries import ts_transformer, ts_column_factory
from talpy.helper_functions import table_helper

# 2.	INPUT DATA
df_velocity_spectra = spark.table("pt_configuration_thesis.alltrucks_v1.salescodecombo15velocityspectra")

unique_vins_count_ori = df_velocity_spectra .select("vin").distinct().count()
print(f"Number of vehicles in the dataset: {unique_vins_count_ori}")
df_weight_spectra = spark.table("pt_configuration_thesis.alltrucks_v1.`salescodecombo15weightspectra`")

unique_vins_count_ori = df_weight_spectra .select("vin").distinct().count()
print(f"Number of vehicles in the dataset: {unique_vins_count_ori}")
 
df_gradient_spectra = spark.table("pt_configuration_thesis.alltrucks_v1.`salescodecombo15gradientspectra`")

unique_vins_count_ori = df_gradient_spectra .select("vin").distinct().count()
print(f"Number of vehicles in the dataset: {unique_vins_count_ori}")

# Not part of master thesis, But if necessary can be imported and a similar pre processing pipeline can be done for ccsetspeed spectras also

df_ccsetspeed_spectra = spark.table("pt_configuration_thesis.v9clustering.`salescodecombo3ccsetspeedspectra`")

unique_vins_count_ori = df_ccsetspeed_spectra.select("vin").distinct().count()
print(f"Number of vehicles in the dataset: {unique_vins_count_ori}")

# 3.	VELOCTY SPECTRA PREP
display(df_velocity_spectra)
# Sort by VIN and order by Interval in Ascending Order
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number

# Define a window partitioned by vin and ordered by x1_interval
window_spec = Window.partitionBy("vin").orderBy("x1_interval")

# All rows of a VIN are together and intervals ascending
df_velocity_spectra_sorted = df_velocity_spectra \
    .orderBy("vin", "x1_interval")
display(df_velocity_spectra_sorted)
# Find Global Maximum to make intervals equal

from pyspark.sql.functions import regexp_extract, col, max as spark_max

# Extract upper bound from interval string
# Regex captures the number after the comma, Should be possible to do it in a easier manner, Check later if you have time
df_velocity_spectra_with_upper = df_velocity_spectra.withColumn(
    "upper_bound",
    regexp_extract(col("x1_interval"), r"\[(\d+),(\d+)\)", 2).cast("integer")
)

# Find global maximum upper bound
global_max_upper = df_velocity_spectra_with_upper.agg(spark_max("upper_bound")).collect()[0][0]
print(f"Global max interval upper bound: {global_max_upper}")

from pyspark.sql.functions import lit, coalesce

# Get distinct VINs
vins_df = df_velocity_spectra.select("vin").distinct()

# Create a DataFrame of all intervals
intervals_df = spark.createDataFrame([(i,) for i in all_intervals], ["x1_interval"])

# Cross join VINs with all intervals
vin_intervals_df = vins_df.crossJoin(intervals_df)

# Left join with original data to get distance values
df_velocity_spectra_complete = vin_intervals_df.join(
    df_velocity_spectra,
    on=["vin", "x1_interval"],
    how="left"
).withColumn(
    "count",
    coalesce(col("count"), lit(0))
).orderBy("vin", "x1_interval")
from pyspark.sql.functions import regexp_extract, col
df_velocity_spectra_complete = df_velocity_spectra_complete.withColumn(
    "interval_start",
    regexp_extract(col("x1_interval"), r"\[(\d+),", 1).cast("integer")
)

# Now order by VIN and numeric interval start
df_velocity_spectra_complete = df_velocity_spectra_complete.orderBy("vin", "interval_start")

display(df_velocity_spectra_complete)
from pyspark.sql.functions import sum as spark_sum, col

# Compute total distance per VIN
df_total_distance = df_velocity_spectra_complete.groupBy("vin") \
    .agg(spark_sum("count").alias("total_distance"))

display(df_total_distance)
# Normalized Spectras
df_velocity_normalized = df_velocity_spectra_complete.join(
    df_total_distance, on="vin", how="left"
).withColumn(
    "normalized_count", col("count") / col("total_distance")
).select("vin", "x1_interval", "normalized_count")
display(df_velocity_normalized)

# 4.	WEIGHT SPECTRA PREP

display(df_weight_spectra)
# Sort by VIN and order by Interval in Ascending Order
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number

# Define a window partitioned by vin and ordered by x1_interval
window_spec = Window.partitionBy("vin").orderBy("x1_interval")

# All rows of a VIN are together and intervals ascending
df_weight_spectra_sorted = df_weight_spectra \
    .orderBy("vin", "x1_interval")
display(df_weight_spectra_sorted)
from pyspark.sql.functions import regexp_extract, min as spark_min, max as spark_max, col

# Extract lower and upper bounds
df_weight_spectra_sorted = df_weight_spectra_sorted.withColumn(
    "interval_start", regexp_extract(col("x1_interval"), r"\[(\d+),", 1).cast("integer")
).withColumn(
    "interval_end", regexp_extract(col("x1_interval"), r",(\d+)\)", 1).cast("integer")
)

# Global min and max
global_min = df_weight_spectra_sorted.agg(spark_min("interval_start")).collect()[0][0]
# global_max = df_weight_spectra_sorted.agg(spark_max("interval_end")).collect()[0][0]
global_max = 55000     #Fixed by 99th percentile as 1 outlier causes too many uneceesary extra bins. 

print(f"Global min: {global_min}, Global max: {global_max}")
from pyspark.sql.functions import col, expr

# Approximate 95th percentile of interval_end
percentile_value = df_weight_spectra_sorted.approxQuantile("interval_end", [0.99], 0.0)[0]
print(f"95th percentile of weight upper bound: {percentile_value}")

from pyspark.sql.functions import col, expr

# Approximate 95th percentile of interval_end
percentile_value = df_weight_spectra_sorted.approxQuantile("interval_end", [0.99], 0.0)[0]
print(f"95th percentile of weight upper bound: {percentile_value}")

# Interval width, Can be changed 
bin_width = 1000
all_weight_intervals = [f"[{i},{i+bin_width})" for i in range(global_min, global_max, bin_width)]

from pyspark.sql.functions import lit, coalesce, col

# Get distinct VINs
vins_df = df_weight_spectra_sorted.select("vin").distinct()

# Create a DataFrame of all intervals (already generated)
# all_weight_intervals is the list from global_min to global_max
intervals_df = spark.createDataFrame([(i,) for i in all_weight_intervals], ["x1_interval"])

# Cross join VINs with all intervals
vin_intervals_df = vins_df.crossJoin(intervals_df)

# Left join with original weight data to get distance values
df_weight_spectra_complete = vin_intervals_df.join(
    df_weight_spectra_sorted,
    on=["vin", "x1_interval"],
    how="left"
).withColumn(
    "count",
    coalesce(col("count"), lit(0))   # fill missing distances with 0
).orderBy("vin", "x1_interval")      # sort by VIN and interval

from pyspark.sql.functions import regexp_extract, col

# Extract numeric interval start for sorting
df_weight_spectra_complete = df_weight_spectra_complete.withColumn(
    "interval_start",
    regexp_extract(col("x1_interval"), r"\[(\d+),", 1).cast("integer")
)

# Order by VIN and numeric interval start
df_weight_spectra_complete = df_weight_spectra_complete.orderBy("vin", "interval_start")

display(df_weight_spectra_complete)

from pyspark.sql.functions import sum as spark_sum, col

# Step 1: Compute total distance per VIN
df_total_distance_weight = df_weight_spectra_complete.groupBy("vin") \
    .agg(spark_sum("count").alias("total_distance"))

display(df_total_distance_weight)
# Normalized Spectras
df_weight_normalized = df_weight_spectra_complete.join(
    df_total_distance_weight, on="vin", how="left"
).withColumn(
    "normalized_count", col("count") / col("total_distance")
).select("vin", "x1_interval", "normalized_count")
display(df_weight_normalized)


# 5.	GRADIENT SPECTRA PREP
display(df_gradient_spectra)
# Sort by VIN and order by Interval in Ascending Order
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number

# Define a window partitioned by vin and ordered by x1_interval
window_spec = Window.partitionBy("vin").orderBy("x1_interval")

# All rows of a VIN are together and intervals ascending
df_gradient_spectra_sorted = df_gradient_spectra \
    .orderBy("vin", "x1_interval")
display(df_gradient_spectra_sorted)
from pyspark.sql.functions import regexp_extract, min as spark_min, max as spark_max, col

# Extract lower and upper bounds from gradient intervals
df_gradient_spectra_sorted = df_gradient_spectra_sorted.withColumn(
    "interval_start", regexp_extract(col("x1_interval"), r"\[([+-]?\d+),", 1).cast("integer")
).withColumn(
    "interval_end", regexp_extract(col("x1_interval"), r",([+-]?\d+)\)", 1).cast("integer")
)

global_min = df_gradient_spectra_sorted.agg(spark_min("interval_start")).collect()[0][0]
# global_max = df_gradient_spectra_sorted.agg(spark_max("interval_end")).collect()[0][0]

global_max = 10     # Fixed based on 99th percentile - Meaning 99% of trucks have upper bound inside of this,  Done to prevent too many unecessary bins for ML
                    

print(f"Global min: {global_min}, Global max: {global_max}")
from pyspark.sql.functions import col, expr

# Approximate 95th percentile of interval_end
percentile_value = df_gradient_spectra_sorted.approxQuantile("interval_end", [0.99], 0.0)[0]
print(f"95th percentile of weight upper bound: {percentile_value}")
import numpy as np
# Interval width, Can be changed 

bin_width = 1  # 1% gradient per bin
all_gradient_intervals = [
    f"[{i},{i+bin_width})" for i in np.arange(global_min, global_max, bin_width)
]

display(all_gradient_intervals)
from pyspark.sql.functions import lit, coalesce, col

# Get distinct VINs
vins_df = df_gradient_spectra_sorted.select("vin").distinct()

# Create a DataFrame of all intervals (already generated)
# all_gradient_intervals is the list from global_min to global_max
intervals_df = spark.createDataFrame([(i,) for i in all_gradient_intervals], ["x1_interval"])

# Cross join VINs with all intervals
vin_intervals_df = vins_df.crossJoin(intervals_df)

# Left join with original gradient data to get distance values
df_gradient_spectra_complete = vin_intervals_df.join(
    df_gradient_spectra_sorted,
    on=["vin", "x1_interval"],
    how="left"
).withColumn(
    "count",
    coalesce(col("count"), lit(0))   # fill missing distances with 0
).orderBy("vin", "x1_interval")      # sort by VIN and interval
from pyspark.sql.functions import regexp_extract, col

# Extract numeric interval start for sorting (handles negative values)
df_gradient_spectra_complete = df_gradient_spectra_complete.withColumn(
    "interval_start",
    regexp_extract(col("x1_interval"), r"\[([+-]?\d+),", 1).cast("integer")
)

# Order by VIN and numeric interval start
df_gradient_spectra_complete = df_gradient_spectra_complete.orderBy("vin", "interval_start")

display(df_gradient_spectra_complete)
from pyspark.sql.functions import sum as spark_sum, col

# Step 1: Compute total distance per VIN
df_total_distance_gradient = df_gradient_spectra_complete.groupBy("vin") \
    .agg(spark_sum("count").alias("total_distance"))

display(df_total_distance_gradient)
# Normalized Spectras
df_gradient_normalized = df_gradient_spectra_complete.join(
    df_total_distance_gradient, on="vin", how="left"
).withColumn(
    "normalized_count", col("count") / col("total_distance")
).select("vin", "x1_interval", "normalized_count")
display(df_gradient_normalized)

# 6.	PUSH ESSENTIAL DATA TO CATALOG


df_velocity_spectra_complete.write.mode("overwrite").saveAsTable("pt_configuration_thesis.alltrucks_v1.salescodecombo15velocityspectracomplete")

df_weight_spectra_complete.write.mode("overwrite").saveAsTable("pt_configuration_thesis.alltrucks_v1.salescodecombo15weightspectracomplete")

df_gradient_spectra_complete.write.mode("overwrite").saveAsTable("pt_configuration_thesis.alltrucks_v1.salescodecombo15gradientspectracomplete")


