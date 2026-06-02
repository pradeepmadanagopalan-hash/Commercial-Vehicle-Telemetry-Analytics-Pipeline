'''
0. README
Notebook: Weight_Spectra_Outlier_Detection
Inputs
•	pt_configuration_thesis.alltrucks_v1.combined_velocity_spectra_outlier_removed
•	pt_configuration_thesis.alltrucks_v1.combined_weight_spectra_outlier_removed
•	pt_configuration_thesis.alltrucks_v1.combined_gradient_spectra_outlier_removed
Outputs
•	pt_configuration_thesis.alltrucks_v1.weight_spectra_outliers
Purpose
•	This notebook is meant to take as input the combined velocity, weight and gradient spectras after initial outlier removal from Spectra_Outlier_Removal and add another outlier removal step.
•	Some weight spectras always have zero mileage in all bins. This notebook identifies such vins and creates a single unified list i.e. weight_spectra_outliers.
•	There was no such issue with the velocity and gradient spectras. However since for each vin we need all 3 spectras to be valid, in the subsequent notebooks we will get rid of the vins which have only invalid weight spectras although they have valid velocity and gradient spectras. Hence this notebook only creates a list of invalid weight spectra vins. The actual removal of the vins happens in the net notebooks.
Other remarks
•	None
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
df_velocity_spectra = spark.table("pt_configuration_thesis.alltrucks_v1.`combined_velocity_spectra_outlier_removed`")

unique_vins_count_ori = df_velocity_spectra .select("vin").distinct().count()
print(f"Number of vehicles in the dataset: {unique_vins_count_ori}")

df_weight_spectra = spark.table("pt_configuration_thesis.alltrucks_v1.`combined_weight_spectra_outlier_removed`")

unique_vins_count_ori = df_weight_spectra .select("vin").distinct().count()
print(f"Number of vehicles in the dataset: {unique_vins_count_ori}")

df_gradient_spectra = spark.table("pt_configuration_thesis.alltrucks_v1.`combined_gradient_spectra_outlier_removed`")

unique_vins_count_ori = df_gradient_spectra .select("vin").distinct().count()
print(f"Number of vehicles in the dataset: {unique_vins_count_ori}")

# 3.	PREPARE WEIGHT MATRIX TO IDENTIFY SPECTRAS WITH NULL VALUES
# STEP 1 - JOIN VECTORS 
from pyspark.sql.functions import lit

df_weight_all = df_weight_spectra
df_velocity_all = df_velocity_spectra
df_gradient_all = df_gradient_spectra


display(df_weight_all)

# STEP 2 - COMPUTE BIN CENTER
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

def get_bin_center(interval_str):
    cleaned = interval_str.replace("(", "").replace(")", "").replace("[", "").replace("]", "")
    left, right = cleaned.split(",")
    return (float(left.strip()) + float(right.strip())) / 2

get_bin_center_udf = udf(get_bin_center, DoubleType())

df_weight_all = df_weight_all.withColumn("bin_center", get_bin_center_udf("x1_interval"))

display(df_weight_all)

# STEP 3 - COMPUTE GLOBAL MAXIMA
from pyspark.sql.functions import max as spark_max

global_max_weight = df_weight_all.agg(spark_max("bin_center")).collect()[0][0]
print(f"Global max weight across all trucks: {global_max_weight}")

# STEP 4 - GENERATE ALL BIN CENTERS DEPENDING ON GLOBAL MAXIMA
import numpy as np

bin_interval = 1000
# all_bins = np.arange(0 + bin_interval/2, global_max_velocity + bin_interval, bin_interval)

all_bins = np.arange(12000 + bin_interval/2, 49000 + bin_interval, bin_interval)
print(f"All bin centers: {all_bins}")

# -------------------------------
# STEP 5 - Add missing bins for each truck (weight spectra)
# -------------------------------

# Convert Spark DataFrame → Pandas
df_pd = df_weight_all.select("vin", "bin_center", "count").toPandas()

# Pivot to wide form: rows=trucks, columns=bin centers
df_truck_distributions = df_pd.pivot_table(
    index="vin",
    columns="bin_center",
    values="count",
    aggfunc="sum",
    fill_value=0
)

# Ensure all bins exist (fill missing bins with 0)
df_truck_distributions = df_truck_distributions.reindex(columns=all_bins, fill_value=0)

# Convert counts → probability distribution
X = df_truck_distributions.to_numpy(dtype=float)
row_sums = X.sum(axis=1, keepdims=True)
X_probs = X / row_sums

# Extract bin centers and VINs
bin_centers = df_truck_distributions.columns.values.astype(float)
vins = df_truck_distributions.index.values

# Print shape of probability matrix
print("X_probs shape:", X_probs.shape)

import numpy as np

# 1. Compute row sums before normalization
row_sums_before = X.sum(axis=1)

# 2. Identify all-zero rows, This is the outliers we are trying to eliminate
zero_rows = np.where(row_sums_before == 0)[0]

# 3. Extract VINs of trucks with all-zero weight spectra
vins_to_remove = vins[zero_rows]
print("VINs to remove (all-zero weight):", vins_to_remove)

# 4. Total rows removed
print("Total rows removed:", len(zero_rows))

# 5. Remove all-zero rows from X, X_probs, vins, and df_truck_distributions
X = np.delete(X, zero_rows, axis=0)
vins = np.delete(vins, zero_rows, axis=0)
df_truck_distributions = df_truck_distributions.drop(df_truck_distributions.index[zero_rows])

# 6. Recompute probability distributions for remaining trucks
row_sums = X.sum(axis=1, keepdims=True)
X_probs = X / row_sums

# 7. Display new shape
print("New X_probs shape:", X_probs.shape)

# Same code sections as above, but with metadata (sales group) added for more interpretability
from pyspark.sql.functions import udf
from pyspark.sql.

import numpy as np
import pandas as pd

# 1. Compute row sums before normalization
row_sums_before = X.sum(axis=1)

# 2. Identify all-zero rows, , This is the outliers we are trying to eliminate
zero_rows = np.where(row_sums_before == 0)[0]

# 3. Extract VINs of trucks with all-zero weight spectra
vins_to_remove = vins[zero_rows]

# 4. Get sales_group from the original Spark DataFrame
df_vin_group = df_weight_spectra.select("vin", "sales_group").distinct().toPandas()
# Filter for the VINs to remove
removed_trucks_df = df_vin_group[df_vin_group["vin"].isin(vins_to_remove)].reset_index(drop=True)

print("Trucks to remove (VIN and Sales Group):")
print(removed_trucks_df)

# 5. Total rows removed
print("Total rows removed:", len(zero_rows))

# 6. Remove all-zero rows from X, X_probs, vins, and df_truck_distributions
X = np.delete(X, zero_rows, axis=0)
vins = np.delete(vins, zero_rows, axis=0)
df_truck_distributions = df_truck_distributions.drop(df_truck_distributions.index[zero_rows])

# 7. Recompute probability distributions for remaining trucks
row_sums = X.sum(axis=1, keepdims=True)
X_probs = X / row_sums

# 8. Display new shape
print("New X_probs shape:", X_probs.shape)

display(removed_trucks_df)

# 4.	PUSH OUTLIER DATA TO CATALOG

# Convert Pandas DF to Spark DataFrame
removed_trucks_spark = spark.createDataFrame(removed_trucks_df)

# Save as a Delta table (overwrite if it exists)
removed_trucks_spark.write.format("delta").mode("overwrite").saveAsTable("pt_configuration_thesis.alltrucks_v1.weight_spectra_outliers")


