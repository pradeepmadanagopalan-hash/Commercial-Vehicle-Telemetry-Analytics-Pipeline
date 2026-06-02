'''
0. README
Notebook: Loadspectra_Generator_heatmap_3d_definition
Inputs
•	pt_configuration_thesis.alltrucks_v1.salescodecomboX (X = 1,2,....15)
Outputs
•	pt_configuration_thesis.alltrucks_v1.SGX_EngineSpectra (X = 1,2,....15)
Purpose
•	This notebook is meant to take as input the generated telematics data from DataFrame_Generator and generate as output distance based engine load spectras.
Other remarks
•	Run Loadspectra_Preprocessor - See section 3
•	Most of the code was copied from pre existing notebooks to generate spectras.
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


# 2.	MAKE DICTIONARY

%run /projects/conantic_load_spectra/load_spectra_signal_definition
# group_by = ["vin", "date", "trip_hash"]

group_by = ["vin"]
ls_heatmap_engine = [
    {
        "title": "EngTrq – EngRPM",
        "conf": {
            "group_by": group_by,
            "axis": [
                {"signal": EngRPM_Cval_CPC3, "start": 0, "stop": 5000, "width": 50,},
                {
                    "signal": EngTrq_Cval_PT,
                    "start": -3000, "stop": 3000, "width": 50,
                },
            ],
            "filter": [f.col(vehicle_speed) > 0],
            "tag": "Engine",
        },
    },

]

ls_heatmap_all = (ls_heatmap_engine)

# 3.	INPUT DATA && CALL DATAFRAME PREPROCESSER
# Load table and rename column
df_sg1_all_telematics = spark.table("pt_configuration_thesis.alltrucks_v1.`salescodecombo4`")\
    .withColumnRenamed("20_agg_avg_300", "20")
unique_vins_count_ori = df_sg1_all_telematics.select("vin").distinct().count()
print(f"Number of vehicles in the dataset: {unique_vins_count_ori}")
'''
df_signals = spark.table("pt_configuration_thesis.v2clustering.`salescodecombo1outliersremoved`") \
    .withColumnRenamed("20_agg_avg_300", "20")
'''

df_signals = df_sg1_all_telematics.withColumnRenamed("20_agg_avg_300", "20")
# Original preprocesser copied from existing code 
# Similar file created in /Workspace/Users/pmadana@tbdir.net/master_thesis/Use_Case_Clustering_AllTrucks/Final_Scripts/2. Loadspectra_Preprocesser
# See next code block
'''
%run "/Workspace/Users/pmadana@tbdir.net/master_thesis/Use_Case_Clustering/v2/4. Loadspectra_Preprocesser"
'''

%run /Workspace/Users/pmadana@tbdir.net/master_thesis/Use_Case_Clustering_AllTrucks/Final_Scripts/2. Loadspectra_Preprocesser

df_signals = clean_vehicle_speed_based_on_wheel_speed(df_signals)
df_signals = add_columns_to_signals(df_signals)

# Duplicating signal 20 as 20_cleaned because uc_truck_live does not have wheel speeds. 
# Also note signal 20 is originally 20_agg_avg_300

df_signals = df_signals.withColumn("20_cleaned", f.col("20"))


# 4.	SIGNAL LOOKUP
# Copied from existing Code
# signal lookup

vehicle_net_type = 1026

df_signal_mapping = spark.table("conantic_mbtruck.lookup.signal_mapping")

df_signal_state_mapping = spark.table("conantic_mbtruck.lookup.signal_state_mapping").filter(
    f.col("vehicle_net_type") == vehicle_net_type
)

# 5.	SETTING UP THE BINNING PROCESS FOR LOAD SPECTRA
# Copied from existing code

def generate_ls_heatmap_conantic(ls_heatmap, df_ts_conantic, count_type = "time"):
    # get signal config from ls
    ls_config = ls_heatmap['conf']

    # get x1 signal from ls config
    x1_signal_in_config = ls_config['axis'][0]['signal']

    # get x2 signal from ls config
    x2_signal_in_config = ls_config['axis'][1]['signal']

    # generate the group by list
    group_by_list = [f"{x1_signal_in_config}_quantized", f"{x2_signal_in_config}_quantized"]
    group_by_list += ls_config['group_by']

    # generate output columns list based on count type
    if count_type == 'distance':
        output_cols_list = group_by_list + ['distance_via_speed_km']
    else:
        output_cols_list = group_by_list + ['timestamp']

    # If filter is defined in ls_config.
    filter_description = ''
    if "filter" in ls_config:
        # Filter data on configured filter.
        for ls_filter_id, ls_filter in enumerate(ls_config["filter"]):
            # Default filter is comparison of signal with value (<,>, !=, ...).
            df_ts_conantic = df_ts_conantic.filter(ls_filter)
            filter_str = str(ls_filter).replace("Column<'", "").replace("'>", "")
            if ls_filter_id == 0:
                filter_description = filter_description + filter_str
            else:
                filter_description = filter_description + " AND " + filter_str

    quantization_list_2d = ls_config['axis']
    quantization_list_2d[0]['signal'] = x1_signal_in_config
    quantization_list_2d[1]['signal'] = x2_signal_in_config

    # bin interval for talpy
    bin_interval = ts_transformer.AddBinIntervalDescription(quantization_list_2d) 
    # quantizer for talpy
    quantizer_2d = ts_transformer.TsQuantization(quantization_list_2d, output_cols = output_cols_list)

    # aggregator based on count type
    if count_type == 'distance':
        # count by distance
        aggregator_2d = ts_transformer.Agg(group_by=group_by_list, agg=[f.sum('distance_via_speed_km').alias("count")])
    else:
        # coung by time interval
        aggregator_2d = ts_transformer.Agg(group_by=group_by_list, agg=[f.count('timestamp').alias("count")])

    # calculate ls
    df_ls = bin_interval.transform(aggregator_2d.transform(quantizer_2d.transform(df_ts_conantic)))

    # add signal column
    # df_ls = df_ls.withColumn('x1_signal_in_config', f.lit(x1_signal_in_config))
    # df_ls = df_ls.withColumn('x2_signal_in_config', f.lit(x2_signal_in_config))

    # rename columns to concat results
    df_ls = df_ls.withColumnRenamed(f"{x1_signal_in_config}_quantized", "x1_quantized")
    df_ls = df_ls.withColumnRenamed(f"{x2_signal_in_config}_quantized", "x2_quantized")
    df_ls = df_ls.withColumnRenamed(f"{x1_signal_in_config}_quantized_interval", "x1_interval")
    df_ls = df_ls.withColumnRenamed(f"{x2_signal_in_config}_quantized_interval", "x2_interval")

    # generate metadata
    dict_ls_metadata = {"x1_signal_in_config": x1_signal_in_config, "x2_signal_in_config": x2_signal_in_config,  "description": filter_description, "tag": ls_config["tag"]}
    dict_ls_metadata['count_type'] = count_type

    # take over specific defined title
    if 'title' in ls_heatmap:
        dict_ls_metadata['title'] = ls_heatmap['title']
    else:
        dict_ls_metadata['title'] = ls_config['title']

    df_ls_metadata = spark.createDataFrame([dict_ls_metadata])
    return df_ls, df_ls_metadata

# 6.	DISTANCE BASED AND TIME BASED LOAD SPECTRA EXECUTION
# Copied from existing code 

results = []
results_metadata = []
for id, ls_heatmap in enumerate(ls_heatmap_all):
    # signal in config
    x1_signal_in_config = ls_heatmap["conf"]["axis"][0]["signal"]
    x2_signal_in_config = ls_heatmap["conf"]["axis"][1]["signal"]

    # check if signal available
    if (
        x1_signal_in_config not in df_signals.columns
        or x2_signal_in_config not in df_signals.columns
    ):
        warnings.warn(
            "Signal with id "
            + x1_signal_in_config
            + " or "
            + x2_signal_in_config
            + " not available in timeseries table"
        )
        continue

    # get signal_id signal_name from signal mapping table x1 signal
    if x1_signal_in_config.split("_", 1)[0].isnumeric():
        x1_signal_id = x1_signal_in_config.split("_", 1)[0]
        if len(x1_signal_in_config.split("_", 1)) > 1:
            x1_signal_suffix = x1_signal_in_config.split("_", 1)[1]
        else:
            x1_signal_suffix = None
    else:
        x1_signal_id = x1_signal_in_config
        x1_signal_suffix = None

    # get signal_id signal_name from signal mapping table x2 signal
    if x2_signal_in_config.split("_", 1)[0].isnumeric():
        x2_signal_id = x2_signal_in_config.split("_", 1)[0]
        if len(x2_signal_in_config.split("_", 1)) > 1:
            x2_signal_suffix = x2_signal_in_config.split("_", 1)[1]
        else:
            x2_signal_suffix = None
    else:
        x2_signal_id = x2_signal_in_config
        x2_signal_suffix = None

    # generate ls_config_id
    config_id = 2 * id + 1
    config_id_dist = 2 * id + 2

    # calculate ls time share
    df_ls_time, df_ls_time_metadata = generate_ls_heatmap_conantic(
        ls_heatmap, df_signals, count_type="time"
    )

    # calculate ls dist share
    df_ls_distance, df_ls_distance_metadata = generate_ls_heatmap_conantic(
        ls_heatmap, df_signals, count_type="distance"
    )

    # add ls_config_id_column
    df_ls_time = df_ls_time.withColumn("ls_config_id", f.lit(config_id).cast("int"))

    # add ls_config_id_column dist
    df_ls_distance = df_ls_distance.withColumn(
        "ls_config_id", f.lit(config_id_dist).cast("int")
    )

    # add signal id in load spectra
    df_ls_time = df_ls_time.withColumn("x1_signal_id", f.lit(x1_signal_id)).withColumn(
        "x2_signal_id", f.lit(x2_signal_id)
    )
    df_ls_distance = df_ls_distance.withColumn(
        "x1_signal_id", f.lit(x1_signal_id)
    ).withColumn("x2_signal_id", f.lit(x2_signal_id))

    # add signal_name in metadata
    df_ls_time_metadata = (
        df_ls_time_metadata.withColumn("ls_config_id", f.lit(config_id).cast("int"))
        .withColumn("x1_signal_id", f.lit(x1_signal_id))
        .withColumn("x2_signal_id", f.lit(x2_signal_id))
        .withColumn("x1_signal_suffix", f.lit(x1_signal_suffix).cast(StringType()))
        .withColumn("x2_signal_suffix", f.lit(x2_signal_suffix).cast(StringType()))
    )

    df_ls_distance_metadata = (
        df_ls_distance_metadata.withColumn(
            "ls_config_id", f.lit(config_id_dist).cast("int")
        )
        .withColumn("x1_signal_id", f.lit(x1_signal_id))
        .withColumn("x2_signal_id", f.lit(x2_signal_id))
        .withColumn("x1_signal_suffix", f.lit(x1_signal_suffix).cast(StringType()))
        .withColumn("x2_signal_suffix", f.lit(x2_signal_suffix).cast(StringType()))
    )

    # append result
    results.append(df_ls_time)
    results.append(df_ls_distance)

    # append metadata
    results_metadata.append(df_ls_time_metadata)
    results_metadata.append(df_ls_distance_metadata)

    print(config_id)
    print(config_id_dist)
    
# 7.	MERGE DATA

# Copied from existing code
# merge extened metadata and create dataframe as result
df_load_spectra = reduce(lambda df1,df2: df1.union(df2), results)
df_metadata = reduce(lambda df1,df2: df1.union(df2), results_metadata)

# 8.	LOGIC TO GET TRANSLATED DATA
# Copied from existing code
# x1 info
# signal state mapping
df_signal_state_mapping_x1 = (
    df_signal_state_mapping.select("signal_id", "value", "text")
    .withColumnRenamed("text", "x1_value_to_text")
    .withColumnRenamed("signal_id", "x1_signal_id")
    .withColumnRenamed("value", "x1_quantized")
)

# signal mapping
df_signal_mapping_x1 = df_signal_mapping
for col in df_signal_mapping_x1.columns:
    df_signal_mapping_x1 = df_signal_mapping_x1.withColumnRenamed(col, "x1_" + col)

# Copied from existing code
# x2 info
# signal state mapping
df_signal_state_mapping_x2 = (
    df_signal_state_mapping.select("signal_id", "value", "text")
    .withColumnRenamed("text", "x2_value_to_text")
    .withColumnRenamed("signal_id", "x2_signal_id")
    .withColumnRenamed("value", "x2_quantized")
)

# signal mapping
df_signal_mapping_x2 = df_signal_mapping
for col in df_signal_mapping_x2.columns:
    df_signal_mapping_x2 = df_signal_mapping_x2.withColumnRenamed(col, "x2_" + col)

# Copied from existing code
# load spectra data translated
# x1 info
df_load_spectra_translated = df_load_spectra.join(
    df_signal_state_mapping_x1,
    on=["x1_signal_id", "x1_quantized"],
    how="left",
)

df_load_spectra_translated = df_load_spectra_translated.join(
    df_signal_mapping_x1.select("x1_signal_id", "x1_type", "x1_unit"),
    on="x1_signal_id",
    how="left",
)

df_load_spectra_translated = df_load_spectra_translated.withColumn(
    "x1_value_to_text",
    f.when(
        (
            (
                f.col("x1_value_to_text").isNull()
                & (f.col("x1_type") == "int")
                & (f.col("x1_unit") == "No unit")
                # & (f.col("x1_precision") == 1)
            )
            | (
                (
                    f.col("x1_value_to_text").isNull()
                    & (f.col("x1_type") == "enum")
                    & (f.col("x1_unit") == "No unit")
                )
            )
        ),
        f.col("x1_quantized").cast(StringType()),
    ).otherwise(f.col("x1_value_to_text")),
).withColumn(
    "x1_value_to_text",
    f.when(f.col("x1_value_to_text").isNull(), f.col("x1_interval")).otherwise(f.col("x1_value_to_text")),
)

df_load_spectra_translated = df_load_spectra_translated.drop("x1_type", "x1_unit")

# Copied from existing code
# load speactra data translated
# x2 info
df_load_spectra_translated = df_load_spectra_translated.join(
    df_signal_state_mapping_x2,
    on=["x2_signal_id", "x2_quantized"],
    how="left",
)

df_load_spectra_translated = df_load_spectra_translated.join(
    df_signal_mapping_x2.select("x2_signal_id", "x2_type", "x2_unit"),
    on="x2_signal_id",
    how="left",
)

df_load_spectra_translated = df_load_spectra_translated.withColumn(
    "x2_value_to_text",
    f.when(
        (
            (
                f.col("x2_value_to_text").isNull()
                & (f.col("x2_type") == "int")
                & (f.col("x2_unit") == "No unit")
                # & (f.col("x2_precision") == 1)
            )
            | (
                (
                    f.col("x2_value_to_text").isNull()
                    & (f.col("x2_type") == "enum")
                    & (f.col("x2_unit") == "No unit")
                )
            )
        ),
        f.col("x2_quantized").cast(StringType()),
    ).otherwise(f.col("x2_value_to_text")),
).withColumn(
    "x2_value_to_text",
    f.when(f.col("x2_value_to_text").isNull(), f.col("x2_interval")).otherwise(f.col("x2_value_to_text")),
)

df_load_spectra_translated = df_load_spectra_translated.drop("x2_type", "x2_unit")

# Copied from existing code
# metadata translated
# x1
df_metadata_translated = df_metadata.join(
    df_signal_mapping_x1, on="x1_signal_id", how="left"
)

df_metadata_translated = df_metadata_translated.withColumn(
    "x1_signal",
    f.when(f.col("x1_can_signal_name").isNull(), f.col("x1_signal_name")).otherwise(
        f.col("x1_can_signal_name")
    ),
).withColumn(
    "x1_signal",
    f.when(
        f.col("x1_signal_suffix").isNotNull(),
        f.concat(f.col("x1_signal"), f.lit("_"), f.col("x1_signal_suffix")),
    ).otherwise(f.col("x1_signal")),
)

# metadata translated
# x2
df_metadata_translated = df_metadata_translated.join(
    df_signal_mapping_x2,
    on="x2_signal_id",
    how="left",
)

df_metadata_translated = df_metadata_translated.withColumn(
    "x2_signal",
    f.when(f.col("x2_can_signal_name").isNull(), f.col("x2_signal_name")).otherwise(
        f.col("x2_can_signal_name")
    ),
).withColumn(
    "x2_signal",
    f.when(
        f.col("x2_signal_suffix").isNotNull(),
        f.concat(f.col("x2_signal"), f.lit("_"), f.col("x2_signal_suffix")),
    ).otherwise(f.col("x2_signal")),
)

# 9.	EXTRACT ONLY DISTANCE BASED ENGINE SPECTRA
# Distance based engine_load_spectra ls_config_id = 2
engine_spectra = df_load_spectra_translated.filter(df_load_spectra_translated.ls_config_id == 2)

# 10.	DISPLAY RESULTS AND CROSS VERIFICATION
display(df_load_spectra_translated)
# This section can be ignored..!
# Purely meant to cross verify if the spectras were generated correctly by taking a look at a single vin at a time

from pyspark.sql import functions as F

selected_vin = "c3260524fa0e1acc39efef284a932b766e1d79eb8c5bcb36dca6b29e5df237e4"

# Filter by vin and ls_config_id
df_filtered = df_load_spectra_translated.filter(
    (F.col("vin") == selected_vin) & (F.col("ls_config_id") == 2)
)

df_filtered = df_filtered.orderBy(F.col("x1_quantized").desc())

display(df_filtered)

# 11.	PUSH ESSENTIAL DATA TO CATALOG
engine_spectra.write.mode("overwrite").saveAsTable("pt_configuration_thesis.alltrucks_v1.SG4_EngineSpectra")



