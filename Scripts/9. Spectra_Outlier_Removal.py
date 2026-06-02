'''
0. README
Notebook: Spectra_Outlier_Removal
Inputs
•	pt_configuration_thesis.alltrucks_v1.salescodecomboXvelocityspectracomplete
•	pt_configuration_thesis.alltrucks_v1.salescodecomboXweightspectracomplete
•	pt_configuration_thesis.alltrucks_v1.salescodecomboXgradientspectracomplete (X = 1,2,....15)
•	outliervins
Outputs
•	pt_configuration_thesis.alltrucks_v1.combined_velocity_spectra_outlier_removed
•	pt_configuration_thesis.alltrucks_v1.combined_weight_spectra_outlier_removed
•	pt_configuration_thesis.alltrucks_v1.combined_gradient_spectra_outlier_removed
Purpose
•	This notebook is meant to take as input the complete velocity, weight and gradient spectras from the notebook Cluster_Preper_Vel_Weight&Grad and the list of outliers from Outlier_Detector. After this the notebook gets rid of spectras that belong to outliers and produces a single unified spectra each for velocity, weight and gradient consisting only the non outlier vins.
Other remarks
•	None
'''


# 1.	REMOVE OUTLIERS FROM VELOCITY SPECTRAS
# ------------------------
# Libraries
# ------------------------
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql import DataFrame

# ------------------------
# Step 0: load outlier VINs (distinct)
# ------------------------
outlier_vins = spark.table("pt_configuration_thesis.alltrucks_v1.`outliervins`") \
                    .select("vin", "sales_group")  # keep sales_group if present
outlier_vins_distinct = outlier_vins.select("vin").distinct()

# Optional: persist if reused many times (tiny table)
outlier_vins_distinct = outlier_vins_distinct.persist()

# ------------------------
# Step 1: iterate over SG1..SG15, load spectra, remove outliers, collect stats
# ------------------------
clean_dfs = []              # cleaned DataFrames to union
summary_rows = []           # (sales_group, vins_before, vins_after)
removed_per_sg = []         # DataFrames of removed vins per SG (vin, sales_group)

for i in range(1, 16):
    sg = i
    table_name = f"pt_configuration_thesis.alltrucks_v1.`salescodecombo{sg}velocityspectracomplete`"
    
    # Load spectra table for this SG
    df = spark.table(table_name)
    
    # Add sales_group column (int) so we keep track after union
    df = df.withColumn("sales_group", F.lit(sg))
    
    # Count unique VINs before removal
    vins_before = df.select("vin").distinct().count()
    
    # Remove outlier VINs (all rows for any vin in outlier_vins_distinct are removed)
    df_clean = df.join(outlier_vins_distinct, on="vin", how="left_anti")
    
    # Count unique VINs after removal
    vins_after = df_clean.select("vin").distinct().count()
    
    # Record summary
    summary_rows.append((sg, vins_before, vins_after))
    
    # Save cleaned DF for union
    clean_dfs.append(df_clean)
    
    # Build removed VINs for this SG (distinct vins that were in df but removed)
    removed_vins_sg = df.select("vin").distinct().subtract(df_clean.select("vin").distinct()) \
                           .withColumn("sales_group", F.lit(sg))
    removed_per_sg.append(removed_vins_sg)

# ------------------------
# Step 2: union all cleaned dfs into one combined DF
# ------------------------
if len(clean_dfs) == 0:
    combined_velocity_spectra = spark.createDataFrame([], schema=df_clean.schema)  # empty fallback
else:
    combined_velocity_spectra = reduce(lambda a,b: a.unionByName(b), clean_dfs)

# Optional: persist combined if you will reuse
combined_velocity_spectra = combined_velocity_spectra.persist()

# ------------------------
# Step 3: create summary DataFrame (sales_group, vins_before, vins_after, removed)
# ------------------------
summary_df = spark.createDataFrame(summary_rows, schema=["sales_group", "vins_before", "vins_after"])

# Add a column for number removed for convenience
summary_df = summary_df.withColumn("vins_removed", F.col("vins_before") - F.col("vins_after"))

display(summary_df.orderBy("sales_group"))

# ------------------------
# Step 4: union removed VINs per SG into single DataFrame
# ------------------------
if len(removed_per_sg) > 0:
    removed_vins_df = reduce(lambda a,b: a.unionByName(b), removed_per_sg).select("vin", "sales_group").distinct()
else:
    removed_vins_df = spark.createDataFrame([], schema="vin string, sales_group int")

display(removed_vins_df)

# ------------------------
# Optional checks / prints
# ------------------------
total_before = summary_df.agg(F.sum("vins_before").alias("total_before")).collect()[0]["total_before"]
total_after  = summary_df.agg(F.sum("vins_after").alias("total_after")).collect()[0]["total_after"]
total_removed = total_before - total_after

print(f"Total unique VINs across all SGs before removal : {total_before}")
print(f"Total unique VINs across all SGs after removal  : {total_after}")
print(f"Total VINs removed                              : {total_removed}")

display(combined_velocity_spectra)


combined_velocity_spectra.write.format("delta").mode("overwrite").saveAsTable("pt_configuration_thesis.alltrucks_v1.`combined_velocity_spectra_outlier_removed`")

# 2.	REMOVE OUTLIERS FROM WEIGHT SPECTRAS
# ------------------------
# Step 0: Load outlier VINs (distinct)
# ------------------------
outlier_vins = spark.table("pt_configuration_thesis.alltrucks_v1.`outliervins`") \
                    .select("vin", "sales_group")  # keep sales_group if present
outlier_vins_distinct = outlier_vins.select("vin").distinct().persist()

# ------------------------
# Step 1: Iterate over SG1..SG15, load weight spectra, remove outliers, collect stats
# ------------------------
clean_dfs_weight = []       # cleaned DataFrames to union
summary_rows_weight = []    # (sales_group, vins_before, vins_after)
removed_per_sg_weight = []  # DataFrames of removed vins per SG

for i in range(1, 16):
    sg = i
    table_name = f"pt_configuration_thesis.alltrucks_v1.`salescodecombo{sg}weightspectracomplete`"
    
    # Load spectra table for this SG
    df = spark.table(table_name)
    
    # Add sales_group column
    df = df.withColumn("sales_group", F.lit(sg))
    
    # Count unique VINs before removal
    vins_before = df.select("vin").distinct().count()
    
    # Remove outlier VINs
    df_clean = df.join(outlier_vins_distinct, on="vin", how="left_anti")
    
    # Count unique VINs after removal
    vins_after = df_clean.select("vin").distinct().count()
    
    # Record summary
    summary_rows_weight.append((sg, vins_before, vins_after))
    
    # Save cleaned DF for union
    clean_dfs_weight.append(df_clean)
    
    # Build removed VINs for this SG
    removed_vins_sg = df.select("vin").distinct().subtract(df_clean.select("vin").distinct()) \
                           .withColumn("sales_group", F.lit(sg))
    removed_per_sg_weight.append(removed_vins_sg)

# ------------------------
# Step 2: Union all cleaned dfs into one combined DF
# ------------------------
if len(clean_dfs_weight) == 0:
    combined_weight_spectra = spark.createDataFrame([], schema=df_clean.schema)
else:
    combined_weight_spectra = reduce(lambda a,b: a.unionByName(b), clean_dfs_weight)

combined_weight_spectra = combined_weight_spectra.persist()

# ------------------------
# Step 3: Create summary DataFrame
# ------------------------
summary_df_weight = spark.createDataFrame(summary_rows_weight, 
                                          schema=["sales_group", "vins_before", "vins_after"])
summary_df_weight = summary_df_weight.withColumn("vins_removed", 
                                                 F.col("vins_before") - F.col("vins_after"))

display(summary_df_weight.orderBy("sales_group"))

# ------------------------
# Step 4: Union removed VINs per SG into single DataFrame
# ------------------------
if len(removed_per_sg_weight) > 0:
    removed_vins_df_weight = reduce(lambda a,b: a.unionByName(b), removed_per_sg_weight) \
                                .select("vin", "sales_group").distinct()
else:
    removed_vins_df_weight = spark.createDataFrame([], schema="vin string, sales_group int")

display(removed_vins_df_weight)

# ------------------------
# Optional: Total counts
# ------------------------
total_before = summary_df_weight.agg(F.sum("vins_before").alias("total_before")).collect()[0]["total_before"]
total_after  = summary_df_weight.agg(F.sum("vins_after").alias("total_after")).collect()[0]["total_after"]
total_removed = total_before - total_after

print(f"Total unique VINs across all SGs (weight) before removal : {total_before}")
print(f"Total unique VINs across all SGs (weight) after removal  : {total_after}")
print(f"Total VINs removed (weight)                              : {total_removed}")

display(combined_weight_spectra)

combined_weight_spectra.write.format("delta").mode("overwrite").saveAsTable("pt_configuration_thesis.alltrucks_v1.`combined_weight_spectra_outlier_removed`")

# 3.	REMOVE OUTLIERS FROM GRADIENT SPECTRAS
# ------------------------
# Step 0: Load outlier VINs (distinct)
# ------------------------
outlier_vins = spark.table("pt_configuration_thesis.alltrucks_v1.`outliervins`") \
                    .select("vin", "sales_group")  # keep sales_group if present
outlier_vins_distinct = outlier_vins.select("vin").distinct().persist()

# ------------------------
# Step 1: Iterate over SG1..SG15, load gradient spectra, remove outliers, collect stats
# ------------------------
clean_dfs_gradient = []       # cleaned DataFrames to union
summary_rows_gradient = []    # (sales_group, vins_before, vins_after)
removed_per_sg_gradient = []  # DataFrames of removed vins per SG

for i in range(1, 16):
    sg = i
    table_name = f"pt_configuration_thesis.alltrucks_v1.`salescodecombo{sg}gradientspectracomplete`"
    
    # Load spectra table for this SG
    df = spark.table(table_name)
    
    # Add sales_group column
    df = df.withColumn("sales_group", F.lit(sg))
    
    # Count unique VINs before removal
    vins_before = df.select("vin").distinct().count()
    
    # Remove outlier VINs
    df_clean = df.join(outlier_vins_distinct, on="vin", how="left_anti")
    
    # Count unique VINs after removal
    vins_after = df_clean.select("vin").distinct().count()
    
    # Record summary
    summary_rows_gradient.append((sg, vins_before, vins_after))
    
    # Save cleaned DF for union
    clean_dfs_gradient.append(df_clean)
    
    # Build removed VINs for this SG
    removed_vins_sg = df.select("vin").distinct().subtract(df_clean.select("vin").distinct()) \
                           .withColumn("sales_group", F.lit(sg))
    removed_per_sg_gradient.append(removed_vins_sg)

# ------------------------
# Step 2: Union all cleaned dfs into one combined DF
# ------------------------
if len(clean_dfs_gradient) == 0:
    combined_gradient_spectra = spark.createDataFrame([], schema=df_clean.schema)
else:
    combined_gradient_spectra = reduce(lambda a,b: a.unionByName(b), clean_dfs_gradient)

combined_gradient_spectra = combined_gradient_spectra.persist()

# ------------------------
# Step 3: Create summary DataFrame
# ------------------------
summary_df_gradient = spark.createDataFrame(summary_rows_gradient, 
                                            schema=["sales_group", "vins_before", "vins_after"])
summary_df_gradient = summary_df_gradient.withColumn("vins_removed", 
                                                     F.col("vins_before") - F.col("vins_after"))

display(summary_df_gradient.orderBy("sales_group"))

# ------------------------
# Step 4: Union removed VINs per SG into single DataFrame
# ------------------------
if len(removed_per_sg_gradient) > 0:
    removed_vins_df_gradient = reduce(lambda a,b: a.unionByName(b), removed_per_sg_gradient) \
                                .select("vin", "sales_group").distinct()
else:
    removed_vins_df_gradient = spark.createDataFrame([], schema="vin string, sales_group int")

display(removed_vins_df_gradient)

# ------------------------
# Optional: Total counts
# ------------------------
total_before = summary_df_gradient.agg(F.sum("vins_before").alias("total_before")).collect()[0]["total_before"]
total_after  = summary_df_gradient.agg(F.sum("vins_after").alias("total_after")).collect()[0]["total_after"]
total_removed = total_before - total_after

print(f"Total unique VINs across all SGs (gradient) before removal : {total_before}")
print(f"Total unique VINs across all SGs (gradient) after removal  : {total_after}")
print(f"Total VINs removed (gradient)                              : {total_removed}")

display(combined_gradient_spectra)


combined_gradient_spectra.write.format("delta").mode("overwrite").saveAsTable("pt_configuration_thesis.alltrucks_v1.`combined_gradient_spectra_outlier_removed`")


