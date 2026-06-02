'''
0. README
Notebook: Sales_Group_Visualizer
Inputs
•	pt_configuration_thesis.alltrucks_v1.salescodecomboXvelocityspectracomplete
•	pt_configuration_thesis.alltrucks_v1.salescodecomboXweightspectracomplete
•	pt_configuration_thesis.alltrucks_v1.salescodecomboXgradientspectracomplete (X = 1,2,....15)
•	outliervins
Outputs
•	None
Purpose
•	This notebook is meant to create visualizations for velocity, weight and gradient spectras of the different sales groups. For the selected sales group you can choose the number in the code and get as output the plots consisting individual truck spectras (grey lines) and average of the fleet.
•	In section 6 you can also get visualizations that put the averages of the sales groups together.
Other remarks
•	This notebook is purely to visualize and verify all the spectras before going ahead. It does not write any data to the catalog.
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

df_velocity_spectra4 = spark.table("pt_configuration_thesis.alltrucks_v1.`salescodecombo1velocityspectracomplete`")

unique_vins_count_ori = df_velocity_spectra4.select("vin").distinct().count()
print(f"Number of vehicles in the dataset: {unique_vins_count_ori}")
df_weight_spectra4 = spark.table("pt_configuration_thesis.alltrucks_v1.`salescodecombo1weightspectracomplete`")

unique_vins_count_ori = df_weight_spectra4.select("vin").distinct().count()
print(f"Number of vehicles in the dataset: {unique_vins_count_ori}")
df_gradient_spectra4 = spark.table("pt_configuration_thesis.alltrucks_v1.`salescodecombo1gradientspectracomplete`")

unique_vins_count_ori = df_gradient_spectra4.select("vin").distinct().count()
print(f"Number of vehicles in the dataset: {unique_vins_count_ori}")
outlier_vins = spark.table("pt_configuration_thesis.alltrucks_v1.`outliervins`")

unique_vins_count_ori = outlier_vins.select("vin").distinct().count()
print(f"Number of vehicles in the dataset: {unique_vins_count_ori}")


# 3.	FLEET VISUALIZER - BEFORE REMOVING OUTLIERS
# Velocity Spectras
from pyspark.sql import functions as F
import matplotlib.pyplot as plt

# Step 1: Extract lower bound of interval [0, 10) -> 0
df = df_velocity_spectra4.withColumn(
    "x_bin", 
    F.regexp_extract("x1_interval", r"\[(-?\d+),", 1).cast("int")
)

# Step 2: Normalize counts per truck (make distribution sum = 1)
df_norm = df.withColumn("total_count", F.sum("count").over(Window.partitionBy("vin"))) \
            .withColumn("norm_count", F.col("count") / F.col("total_count"))

# Step 3: Compute fleet average (average of normalized distributions across trucks)
df_avg = df_norm.groupBy("x_bin").agg(F.avg("norm_count").alias("avg_norm_count"))

# Step 4: Collect to Pandas
pdf = df_norm.select("vin", "x_bin", "norm_count").toPandas()
pdf_avg = df_avg.toPandas().sort_values("x_bin")

# Step 5: Plot
plt.figure(figsize=(10,6))

# Light lines per truck
for vin, grp in pdf.groupby("vin"):
    plt.plot(grp["x_bin"], grp["norm_count"], color="lightgray", linewidth=0.8, alpha=0.6)

# Dark average line
plt.plot(pdf_avg["x_bin"], pdf_avg["avg_norm_count"], color="darkblue", linewidth=2.5, label="Fleet Average")

# Formatting
plt.xlabel("Velocity (km/h)")
plt.ylabel("Normalized Distance")
plt.title("Normalized Velocity Distribution per Truck vs Sales Group Average")
plt.legend()
plt.grid(True, alpha=0.3)

plt.show()

# Weight Spectras
from pyspark.sql import functions as F
import matplotlib.pyplot as plt

# Step 1: Extract lower bound of interval [0, 10) -> 0
df = df_weight_spectra4.withColumn(
    "x_bin", 
    F.regexp_extract("x1_interval", r"\[(\d+),", 1).cast("int")
)

# Step 2: Normalize counts per truck (make distribution sum = 1)
df_norm = df.withColumn("total_count", F.sum("count").over(Window.partitionBy("vin"))) \
            .withColumn("norm_count", F.col("count") / F.col("total_count"))

# Step 3: Compute fleet average (average of normalized distributions across trucks)
df_avg = df_norm.groupBy("x_bin").agg(F.avg("norm_count").alias("avg_norm_count"))

# Step 4: Collect to Pandas
pdf = df_norm.select("vin", "x_bin", "norm_count").toPandas()
pdf_avg = df_avg.toPandas().sort_values("x_bin")

# Step 5: Plot
plt.figure(figsize=(10,6))

# Light lines per truck
for vin, grp in pdf.groupby("vin"):
    plt.plot(grp["x_bin"], grp["norm_count"], color="lightgray", linewidth=0.8, alpha=0.6)

# Dark average line
plt.plot(pdf_avg["x_bin"], pdf_avg["avg_norm_count"], color="darkblue", linewidth=2.5, label="Fleet Average")

# Formatting
plt.xlabel("Gross Combination Weight (kg)")
plt.ylabel("Normalized Distance")
plt.title("Normalized Weight Distribution per Truck vs Sales Group Average")
plt.legend()
plt.grid(True, alpha=0.3)

plt.show()

# Gradient Spectras
from pyspark.sql import functions as F
import matplotlib.pyplot as plt

# Step 1: Extract lower bound of interval [0, 10) -> 0
df = df_gradient_spectra4.withColumn(
    "x_bin", 
    F.regexp_extract("x1_interval", r"\[(-?\d+),", 1).cast("int")
)

# Step 2: Normalize counts per truck (make distribution sum = 1)
df_norm = df.withColumn("total_count", F.sum("count").over(Window.partitionBy("vin"))) \
            .withColumn("norm_count", F.col("count") / F.col("total_count"))

# Step 3: Compute fleet average (average of normalized distributions across trucks)
df_avg = df_norm.groupBy("x_bin").agg(F.avg("norm_count").alias("avg_norm_count"))

# Step 4: Collect to Pandas
pdf = df_norm.select("vin", "x_bin", "norm_count").toPandas()
pdf_avg = df_avg.toPandas().sort_values("x_bin")

# Step 5: Plot
plt.figure(figsize=(10,6))

# Light lines per truck
for vin, grp in pdf.groupby("vin"):
    plt.plot(grp["x_bin"], grp["norm_count"], color="lightgray", linewidth=0.8, alpha=0.6)

# Dark average line
plt.plot(pdf_avg["x_bin"], pdf_avg["avg_norm_count"], color="darkblue", linewidth=2.5, label="Fleet Average")

# Formatting
plt.xlabel("Std Deviation Road Gradient (%)")
plt.ylabel("Normalized Distance")
plt.title("Normalized Velocity Distribution per Truck vs Sales Group")
plt.legend()
plt.grid(True, alpha=0.3)

plt.show()
# 4.	GENERATE SPECTRA DATA WITHOUT OUTLIERS

# Count unique VINs before removal
vins_before_velocity = df_velocity_spectra4.select("vin").distinct().count()
vins_before_weight   = df_weight_spectra4.select("vin").distinct().count()
vins_before_gradient = df_gradient_spectra4.select("vin").distinct().count()

# Remove SG outliers, select the same sales group as what has been considered as the input for the notebook
outliers_sg15 = outlier_vins.filter(F.col("sales_group") == 1).select("vin").distinct()

df_velocity_clean = df_velocity_spectra4.join(outliers_sg15, on="vin", how="left_anti")
df_weight_clean   = df_weight_spectra4.join(outliers_sg15, on="vin", how="left_anti")
df_gradient_clean = df_gradient_spectra4.join(outliers_sg15, on="vin", how="left_anti")

# Count unique VINs after removal
vins_after_velocity = df_velocity_clean.select("vin").distinct().count()
vins_after_weight   = df_weight_clean.select("vin").distinct().count()
vins_after_gradient = df_gradient_clean.select("vin").distinct().count()

# Print summary
print(f"Velocity spectra: {vins_before_velocity} trucks before, {vins_after_velocity} trucks after removing SG15 outliers")
print(f"Weight spectra: {vins_before_weight} trucks before, {vins_after_weight} trucks after removing SG15 outliers")
print(f"Gradient spectra: {vins_before_gradient} trucks before, {vins_after_gradient} trucks after removing SG15 outliers")

# 5.	FLEET VISUALIZER - AFTER REMOVING OUTLIERS

# Velocity Spectras
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import matplotlib.pyplot as plt

# Step 0: Ensure outliers are removed (SG15)
outliers_sg15 = outlier_vins.filter(F.col("sales_group") == 1).select("vin").distinct()
df_velocity_clean = df_velocity_spectra4.join(outliers_sg15, on="vin", how="left_anti")

# Step 1: Extract lower bound of interval [0, 10) -> 0
df = df_velocity_clean.withColumn(
    "x_bin", 
    F.regexp_extract("x1_interval", r"\[(-?\d+),", 1).cast("int")
)

# Step 2: Normalize counts per truck (make distribution sum = 1)
df_norm = df.withColumn(
    "total_count", 
    F.sum("count").over(Window.partitionBy("vin"))
).withColumn(
    "norm_count", F.col("count") / F.col("total_count")
)

# Step 3: Compute fleet average (average of normalized distributions across trucks)
df_avg = df_norm.groupBy("x_bin").agg(F.avg("norm_count").alias("avg_norm_count"))

# Step 4: Collect to Pandas
pdf = df_norm.select("vin", "x_bin", "norm_count").toPandas()
pdf_avg = df_avg.toPandas().sort_values("x_bin")

# Step 5: Plot
plt.figure(figsize=(10,6))

# Light lines per truck
for vin, grp in pdf.groupby("vin"):
    plt.plot(grp["x_bin"], grp["norm_count"], color="lightgray", linewidth=0.8, alpha=0.6)

# Dark average line
plt.plot(pdf_avg["x_bin"], pdf_avg["avg_norm_count"], color="darkblue", linewidth=2.5, label="Fleet Average")

# Formatting
plt.xlabel("Velocity bin lower bound (km/h)")
plt.ylabel("Normalized Distance")
plt.title("Normalized Velocity Distribution per Truck vs Fleet Average (After Outlier Removal)")
plt.legend()
plt.grid(True, alpha=0.3)
plt.show()


# Weight Spectras
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import matplotlib.pyplot as plt

# Step 0: Ensure outliers are removed (SG15)
outliers_sg15 = outlier_vins.filter(F.col("sales_group") == 1).select("vin").distinct()
df_weight_clean = df_weight_spectra4.join(outliers_sg15, on="vin", how="left_anti")

# Step 1: Extract lower bound of interval [0, 10) -> 0
df = df_weight_clean.withColumn(
    "x_bin", 
    F.regexp_extract("x1_interval", r"\[(\d+),", 1).cast("int")
)

# Step 2: Normalize counts per truck (make distribution sum = 1)
df_norm = df.withColumn(
    "total_count", 
    F.sum("count").over(Window.partitionBy("vin"))
).withColumn(
    "norm_count", F.col("count") / F.col("total_count")
)

# Step 3: Compute fleet average (average of normalized distributions across trucks)
df_avg = df_norm.groupBy("x_bin").agg(F.avg("norm_count").alias("avg_norm_count"))

# Step 4: Collect to Pandas
pdf = df_norm.select("vin", "x_bin", "norm_count").toPandas()
pdf_avg = df_avg.toPandas().sort_values("x_bin")

# Step 5: Plot
plt.figure(figsize=(10,6))

# Light lines per truck
for vin, grp in pdf.groupby("vin"):
    plt.plot(grp["x_bin"], grp["norm_count"], color="lightgray", linewidth=0.8, alpha=0.6)

# Dark average line
plt.plot(pdf_avg["x_bin"], pdf_avg["avg_norm_count"], color="darkblue", linewidth=2.5, label="Fleet Average")

# Formatting
plt.xlabel("Weight bin lower bound (kg)")
plt.ylabel("Normalized Distance")
plt.title("Normalized Weight Distribution per Truck vs Fleet Average (After Outlier Removal)")
plt.legend()
plt.grid(True, alpha=0.3)
plt.show()

# Gradient Spectras
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import matplotlib.pyplot as plt

# Step 0: Ensure outliers are removed (SG15)
outliers_sg15 = outlier_vins.filter(F.col("sales_group") == 1).select("vin").distinct()
df_gradient_clean = df_gradient_spectra4.join(outliers_sg15, on="vin", how="left_anti")

# Step 1: Extract lower bound of interval [0, 10) -> 0
df = df_gradient_clean.withColumn(
    "x_bin", 
    F.regexp_extract("x1_interval", r"\[(-?\d+),", 1).cast("int")
)

# Step 2: Normalize counts per truck (make distribution sum = 1)
df_norm = df.withColumn(
    "total_count", 
    F.sum("count").over(Window.partitionBy("vin"))
).withColumn(
    "norm_count", F.col("count") / F.col("total_count")
)

# Step 3: Compute fleet average (average of normalized distributions across trucks)
df_avg = df_norm.groupBy("x_bin").agg(F.avg("norm_count").alias("avg_norm_count"))

# Step 4: Collect to Pandas
pdf = df_norm.select("vin", "x_bin", "norm_count").toPandas()
pdf_avg = df_avg.toPandas().sort_values("x_bin")

# Step 5: Plot
plt.figure(figsize=(10,6))

# Light lines per truck
for vin, grp in pdf.groupby("vin"):
    plt.plot(grp["x_bin"], grp["norm_count"], color="lightgray", linewidth=0.8, alpha=0.6)

# Dark average line
plt.plot(pdf_avg["x_bin"], pdf_avg["avg_norm_count"], color="darkblue", linewidth=2.5, label="Fleet Average")

# Formatting
plt.xlabel("Gradient bin lower bound (%)")
plt.ylabel("Normalized Distance")
plt.title("Normalized Gradient Distribution per Truck vs Fleet Average (After Outlier Removal)")
plt.legend()
plt.grid(True, alpha=0.3)
plt.show()

# 6.	FLEET AVERAGES PUT TOGETHER

# Just as an example, the average spectras of 4 sales groups have been overlayed. More can be added later if needed. 
# This is again purely a visualization step

from pyspark.sql import functions as F, Window
import matplotlib.pyplot as plt

# List of filtered dataframes and their labels/colors
dfs = [
    (df_velocity_spectra1, "SG1", "blue"),
    (df_velocity_spectra2, "SG2", "green"),
    (df_velocity_spectra3, "SG3", "red"),
    (df_velocity_spectra4, "SG4", "black"),    
]

plt.figure(figsize=(10,6))

for df_filtered, label, color in dfs:
    # Step 1: Extract lower bound of interval [0, 10) -> 0
    df = df_filtered.withColumn(
        "x_bin",
        F.regexp_extract("x1_interval", r"\[(\d+),", 1).cast("int")
    )
    
    # Step 2: Normalize counts per truck
    df_norm = df.withColumn(
        "total_count",
        F.sum("count").over(Window.partitionBy("vin"))
    ).withColumn(
        "norm_count",
        F.col("count") / F.col("total_count")
    )
    
    # Step 3: Compute fleet average
    df_avg = df_norm.groupBy("x_bin").agg(F.avg("norm_count").alias("avg_norm_count"))
    
    # Step 4: Collect to Pandas and sort
    pdf_avg = df_avg.toPandas().sort_values("x_bin")
    
    # Step 5: Plot fleet average
    plt.plot(pdf_avg["x_bin"], pdf_avg["avg_norm_count"], color=color, linewidth=2.5, label=label)

# Formatting
plt.xlabel("Velocity bin lower bound (kmph)")
plt.ylabel("Normalized distance (probability)")
plt.title("Fleet Average Velocity Distribution for 3 Sales Groups (CF Range 0.3 to 0.35)")
plt.legend()
plt.grid(True, alpha=0.3)
plt.show()

# Just as an example, the average spectras of 4 sales groups have been overlayed. More can be added later if needed. 
# This is again purely a visualization step

from pyspark.sql import functions as F, Window
import matplotlib.pyplot as plt

# List of filtered dataframes and their labels/colors
dfs = [
    (df_weight_spectra1, "SG1", "blue"),
    (df_weight_spectra2, "SG2", "green"),
    (df_weight_spectra3, "SG3", "red"),
    (df_weight_spectra4, "SG4", "black")
]

plt.figure(figsize=(10,6))

for df_filtered, label, color in dfs:
    # Step 1: Extract lower bound of interval [0, 10) -> 0
    df = df_filtered.withColumn(
        "x_bin",
        F.regexp_extract("x1_interval", r"\[(\d+),", 1).cast("int")
    )
    
    # Step 2: Normalize counts per truck
    df_norm = df.withColumn(
        "total_count",
        F.sum("count").over(Window.partitionBy("vin"))
    ).withColumn(
        "norm_count",
        F.col("count") / F.col("total_count")
    )
    
    # Step 3: Compute fleet average
    df_avg = df_norm.groupBy("x_bin").agg(F.avg("norm_count").alias("avg_norm_count"))
    
    # Step 4: Collect to Pandas and sort
    pdf_avg = df_avg.toPandas().sort_values("x_bin")
    
    # Step 5: Plot fleet average
    plt.plot(pdf_avg["x_bin"], pdf_avg["avg_norm_count"], color=color, linewidth=2.5, label=label)

# Formatting
plt.xlabel("Weight bin lower bound (kg)")
plt.ylabel("Normalized distance (probability)")
plt.title("Fleet Average Weight Distribution for 3 Sales Groups (CF Range 0.1 to 0.15)")
plt.legend()
plt.grid(True, alpha=0.3)
plt.show()


# Just as an example, the average spectras of 4 sales groups have been overlayed. More can be added later if needed. 
# This is again purely a visualization step

from pyspark.sql import functions as F, Window
import matplotlib.pyplot as plt

# List of filtered dataframes and their labels/colors
dfs = [
    (df_gradient_spectra1, "SG1", "blue"),
    (df_gradient_spectra2, "SG2", "green"),
    (df_gradient_spectra3, "SG3", "red"),
    (df_gradient_spectra4, "SG4", "black")   
]

plt.figure(figsize=(10,6))

for df_filtered, label, color in dfs:
    # Step 1: Extract lower bound of interval [0, 10) -> 0
    df = df_filtered.withColumn(
        "x_bin",
        F.regexp_extract("x1_interval", r"\[(\d+),", 1).cast("int")
    )
    
    # Step 2: Normalize counts per truck
    df_norm = df.withColumn(
        "total_count",
        F.sum("count").over(Window.partitionBy("vin"))
    ).withColumn(
        "norm_count",
        F.col("count") / F.col("total_count")
    )
    
    # Step 3: Compute fleet average
    df_avg = df_norm.groupBy("x_bin").agg(F.avg("norm_count").alias("avg_norm_count"))
    
    # Step 4: Collect to Pandas and sort
    pdf_avg = df_avg.toPandas().sort_values("x_bin")
    
    # Step 5: Plot fleet average
    plt.plot(pdf_avg["x_bin"], pdf_avg["avg_norm_count"], color=color, linewidth=2.5, label=label)

# Formatting
plt.xlabel("Gradient bin lower bound (%)")
plt.ylabel("Normalized distance (probability)")
plt.title("Fleet Average Gradient Distribution for 3 Sales Groups (CF Range 0.1 to 0.15)")
plt.legend()
plt.grid(True, alpha=0.3)
plt.show()




