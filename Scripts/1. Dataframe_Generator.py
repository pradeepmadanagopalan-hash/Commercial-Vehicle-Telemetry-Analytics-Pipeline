'''
0. README
Notebook: Dataframe_Generator
Inputs
•	conantic_mbtruck.uc_truck_live.signals
•	customer_data_analytics.truck_live.vehicle_info_enriched
Outputs
•	pt_configuration_thesis.alltrucks_v1.salescodecomboX (X = 1,2,....15)
Purpose
•	This notebook is meant to extract raw telematics data from uc_truck_live for specific PT configs, Vehicle Models and Tire Sizes.
Other remarks
•	None
'''

# 1. HEADERS
import pyspark.sql.functions as f
from pyspark.sql.window import Window
from pyspark.databricks.sql.functions import h3_longlatash3string

from talpy.timeseries.ts_transformer import *
from talpy.timeseries.ts_column_factory import *
from talpy.timeseries.ts_custom_statistics import *
from talpy.timeseries.ts_statistics import *
%run /projects/talpy_modification_ptsys/timeseries_ptsys/ts_custom_statistics_ptsys

# 2. INPUT DATA
df_signals_signals_lookup = spark.table("conantic_mbtruck.uc_truck_live.signals")
display(df_signals_signals_lookup)
df_truck_live_vehicle_info_enriched = spark.table("customer_data_analytics.truck_live.vehicle_info_enriched")
display(df_truck_live_vehicle_info_enriched)

# 3. DETAILS OF ALL 963403 TRUCKS

# Code block to get a count of number of trucks with specific sales codes and vehicle model number

# Filter for the specific vehicle model and tire code
df_filtered = df_truck_live_vehicle_info_enriched.filter(
    (f.col("vehicle_model") == 963403) &
    (f.col("code_group_wheels_rims_ra_code_description_en").contains("I2F"))
)

# Group by engine power, transmission, and axle ratio, then count trucks
df_combinations = df_filtered.groupBy(
    "code_group_powertrain_engine_power_rating_code_description_en",
    "code_group_powertrain_transmission_code_description_en",
    "code_group_powertrain_axle_ratio_code_description_en"
).agg(
    f.count("vin").alias("truck_count")
).orderBy(
    f.desc("truck_count")
)

# Display Result
df_combinations.show(truncate=False)

display(df_combinations)

# 4. FILTER FOR REQUIRED SALES GROUPS AND VEHICLE MODEL
# Manually select the sales codes and vehicle model number 

df_select = df_truck_live_vehicle_info_enriched.filter(
    (f.col("code_group_powertrain_engine_power_rating_code_description_en").contains("M3B")) &   
    (f.col("code_group_powertrain_transmission_code_description_en").contains("G2E")) &          
    (f.col("code_group_powertrain_axle_ratio_code_description_en").contains("A5B")) &            
    (f.col("code_group_wheels_rims_ra_code_description_en").contains("I2F")) &                  
    (f.col("vehicle_model") == 963403)
).select(
    "vehicle_model",
    "vin",
    "code_group_powertrain_engine_power_rating_code_description_en",
    "code_group_powertrain_transmission_code_description_en",
    "code_group_powertrain_axle_ratio_code_description_en",
    "code_group_wheels_rims_ra_code_description_en"
)

# Count matching rows
count = df_select.count()

# Print with a message
print(f"Number of Trucks with Selected Sales Code and Vehicle Model Number: {count}")

# 5. FILTER X RANDOM TRUCKS IF NEEDED
from pyspark.sql import functions as F
# This code block is not needed unless you want to extract a smaller subset from the total population of a sales group

'''
# Pick X random VINs
random_vins = df_select.select("vin") \
                       .orderBy(F.rand()) \
                       .limit(6824) \
                       .select('vin').limit(6824).toPandas()['vin'].tolist()

print(random_vins)
'''

# 6. GET THE CORRESPONDING TELEMATICS DATA FROM UC_TRUCK_LIVE
# df_signals_selected = df_signals_signals_lookup.filter(f.col("vin").isin(random_vins))

# limit() - Select the number of trucks you want to sample

random_vins_df = df_select.orderBy(f.rand()).limit(247)

# Use a join instead of collecting VINs to driver
df_signals_selected = df_signals_signals_lookup.join(
    random_vins_df.select("vin"), on="vin", how="inner"
)
# Meant for cross checking - Expected to take a lot of time - Comment out if not necessary

unique_vins_count_ori = df_signals_selected.select("vin").distinct().count()
print(f"Number of vehicles in the dataset: {unique_vins_count_ori}")

# 7. REDUCE DATA SIZE TO 1 YEAR PER TRUCK
# Filter records within a fixed 1-year time window

from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

# Ensure event_ts is a timestamp
df_signals_selected = df_signals_selected.withColumn(
    "event_ts", F.to_timestamp("timestamp")
)

# Define 1-year timestamp window
start_ts = F.lit("2024-06-01 00:00:00").cast(TimestampType())
end_ts   = F.lit("2025-06-01 00:00:00").cast(TimestampType())  # end is exclusive

# Filter rows between the two fixed dates
df_filtered = df_signals_selected.filter(
    (F.col("event_ts") >= start_ts) &
    (F.col("event_ts") < end_ts)
)


print(f"Row count from June 1, 2024 to May 31, 2025: {df_filtered.count()}")

# Meant for cross checking - Expected to take a lot of time - Comment out if not necessary

unique_vins_count_ori = df_filtered.select("vin").distinct().count()
print(f"Number of vehicles in the dataset: {unique_vins_count_ori}")

# 8. PUSH DATA TO CATALOG
df_filtered.repartition(600).write.mode("overwrite").saveAsTable("pt_configuration_thesis.alltrucks_v1.salescodecombo15")




