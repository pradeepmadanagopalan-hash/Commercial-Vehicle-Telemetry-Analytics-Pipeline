'''
0. README
Notebook: Outlier_Detector
Inputs
•	pt_configuration_thesis.alltrucks_v1.sgX_talpy_stats ((X = 1,2,....15))
Outputs
•	outliervins
Purpose
•	This notebook is meant to take as input the generated TALPY Stats from the notebook TALPY_Stats_Generator and identify and create a single unified list of outliers. The outliers in this notebook are of 2 types: vins without a minimum mileage and vins outside a selected velocity IQR range.
Other remarks
•	Sections 2,3 are for initial exploratory analysis. The essential part of the notebook is only from section 4.
'''


# 1.	HEADERS
import pyspark.sql.functions as f
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

# Load table and rename column, just to see one sales group at a time
# If not necessary, directly skip to section - 4
sg1_talpy = spark.table("pt_configuration_thesis.alltrucks_v1.`sg1_talpy_stats`")
unique_vins_count_ori = sg1_talpy.select("vin").distinct().count()
print(f"Number of vehicles in the dataset SG1: {unique_vins_count_ori}")
# Loop through tables sg1_talpy_stats to sg15_talpy_stats
for i in range(1, 16):
    table_name = f"pt_configuration_thesis.alltrucks_v1.`sg{i}_talpy_stats`"
    
    # Load table
    df = spark.table(table_name)
    
    # Assign to a variable in the current namespace (sg2_talpy, sg3_talpy, etc.)
    globals()[f"sg{i}_talpy"] = df
    
    # Count distinct VINs
    vin_count = df.select("vin").distinct().count()
    
    # Print message
    print(f"Number of vehicles in the dataset SG{i}: {vin_count}")


# 3.	COMBINE ALL DATA TOGETHER
# This section can be used if necessary to creat a combined table with all sales groups
# If not needed can directly skip to section - 4

from functools import reduce
from pyspark.sql import functions as f

combined_tables = []

# Loop through SG1 to SG15
for i in range(1, 16):
    table_name = f"pt_configuration_thesis.alltrucks_v1.`sg{i}_talpy_stats`"
    df = spark.table(table_name)
    
    # Add sales group column
    df_with_sg = df.withColumn("sales_group", f.lit(i))
    
    combined_tables.append(df_with_sg)

# Union all tables into one combined DataFrame
combined_df = reduce(lambda a, b: a.unionByName(b), combined_tables)

# Check number of vehicles
unique_vins_count = combined_df.select("vin").distinct().count()
print(f"Number of vehicles in the combined dataset: {unique_vins_count}")

display(combined_df)

# 4.	GET RID OF DATA WITH LESS THAN MINIMUM ANNUAL MILEAGE
from pyspark.sql import functions as f

# Initialize a list to store removed VINs info
removed_vins_list = []

# Initialize a summary list
summary_list = []

# Loop through SG1 to SG15
for i in range(1, 16):
    table_name = f"pt_configuration_thesis.alltrucks_v1.`sg{i}_talpy_stats`"
    df = spark.table(table_name).withColumn("sales_group", f.lit(i))
    
    # Count total vehicles before filtering
    total_vehicles = df.select("vin").distinct().count()
    
    # Filter by minimum mileage, this can be changed depending on need. Will also mention this in the report/ other documentation
    filtered_df = df.filter(f.col("distance_via_speed_km") >= 500)
    
    # Count vehicles after filtering
    filtered_vehicles = filtered_df.select("vin").distinct().count()
    
    # Identify removed VINs
    removed_vins = df.select("vin").exceptAll(filtered_df.select("vin")) \
                     .withColumn("sales_group", f.lit(i))
    
    removed_vins_list.append(removed_vins)
    
    # Add to summary
    summary_list.append((i, total_vehicles, filtered_vehicles))
    
    # Store the filtered table back if needed
    globals()[f"sg{i}_filtered"] = filtered_df

# Combine all removed VINs into a single DataFrame
removed_vins_df = removed_vins_list[0]
for df in removed_vins_list[1:]:
    removed_vins_df = removed_vins_df.unionByName(df)

# Convert summary to a PySpark DataFrame
summary_df = spark.createDataFrame(
    summary_list, 
    ["sales_group", "total_vehicles", "vehicles_after_min_mileage"]
)

# Show the summary
summary_df.show()

# 5.	GET RID OF DATA OUTSIDE +-1.5 IQR
from pyspark.sql import functions as f

# Initialize lists to store removed VINs and summary
removed_vins_speed_list = []
summary_speed_list = []

# Loop through SG1_filtered → SG15_filtered
for i in range(1, 16):
    # Access the already filtered table
    df = globals()[f"sg{i}_filtered"]
    
    # Compute Q1, Q3 and IQR for vehicle_speed_avg_driving_kmh
    quantiles = df.approxQuantile("vehicle_speed_avg_driving_kmh", [0.25, 0.75], 0.0)
    Q1 = quantiles[0]
    Q3 = quantiles[1]
    IQR = Q3 - Q1
    
    lower_bound = Q1 - 1.5 * IQR           # This can be changed depending on need. Will also mention this in the report/ other documentation
    upper_bound = Q3 + 1.5 * IQR           # This can be changed depending on need. Will also mention this in the report/ other documentation
    
    # Count vehicles before speed outlier removal
    vehicles_before_speed_filter = df.select("vin").distinct().count()
    
    # Filter out vehicles outside the IQR limits
    filtered_speed_df = df.filter(
        (f.col("vehicle_speed_avg_driving_kmh") >= lower_bound) &
        (f.col("vehicle_speed_avg_driving_kmh") <= upper_bound)
    )
    
    # Count vehicles after filtering
    vehicles_after_speed_filter = filtered_speed_df.select("vin").distinct().count()
    
    # Identify removed VINs in this step
    removed_speed_vins = df.select("vin").exceptAll(filtered_speed_df.select("vin")) \
                           .withColumn("sales_group", f.lit(i))
    
    removed_vins_speed_list.append(removed_speed_vins)
    
    # Add to summary list
    summary_speed_list.append((i, vehicles_before_speed_filter, vehicles_after_speed_filter))
    
    # Store the filtered table back
    globals()[f"sg{i}_filtered_speed"] = filtered_speed_df

# Combine all removed VINs into a single DataFrame
removed_speed_vins_df = removed_vins_speed_list[0]
for df in removed_vins_speed_list[1:]:
    removed_speed_vins_df = removed_speed_vins_df.unionByName(df)

# Convert summary to a DataFrame
summary_speed_df = spark.createDataFrame(
    summary_speed_list,
    ["sales_group", "vehicles_before_speed_filter", "vehicles_after_speed_filter"]
)

# Show the summary
summary_speed_df.show()

display(removed_speed_vins_df)

# 6.	PLOTS
# Box plot for selected sales group to visualize outliers that are outside +- 1.5 IQR

import matplotlib.pyplot as plt

# Example for SG3_filtered
sg1_df = globals()["sg3_filtered"].select("vehicle_speed_avg_driving_kmh").toPandas()

plt.boxplot(sg1_df["vehicle_speed_avg_driving_kmh"])
plt.title("SG1 vehicle speed distribution with IQR")
plt.ylabel("vehicle_speed_avg_driving_kmh")
plt.show()
# Bar plot for selected sales group to visualize outliers that are outside +- 1.5 IQR

import matplotlib.pyplot as plt

# Convert to pandas
sg1_before = globals()["sg7_filtered"].select("vehicle_speed_avg_driving_kmh").toPandas()
sg1_after = globals()["sg7_filtered_speed"].select("vehicle_speed_avg_driving_kmh").toPandas()

# Plot histograms
plt.figure(figsize=(12,6))

plt.hist(sg1_before["vehicle_speed_avg_driving_kmh"], bins=30, alpha=0.5, label="Before filtering", color='skyblue')
plt.hist(sg1_after["vehicle_speed_avg_driving_kmh"], bins=30, alpha=0.5, label="After filtering", color='orange')

plt.xlabel("vehicle_speed_avg_driving_kmh")
plt.ylabel("Number of vehicles")
plt.title("SG1 Vehicle Speed Distribution Before and After Filtering")
plt.legend()
plt.show()

# 7.	ALL REMOVED VINS
all_removed_vins_df = removed_vins_df.unionByName(removed_speed_vins_df)
display(all_removed_vins_df)

# 8.	PUSH DATA AFTER REMOVAL OF OUTLIERS TO CATALOG

all_removed_vins_df.write.mode("overwrite").saveAsTable("pt_configuration_thesis.alltrucks_v1.outliervins")





