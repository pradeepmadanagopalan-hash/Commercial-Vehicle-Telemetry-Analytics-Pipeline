'''
0. README
Notebook: TALPY_Stats_Generator
Inputs
•	pt_configuration_thesis.alltrucks_v1.salescodecomboX (X = 1,2,....15)
Outputs
•	pt_configuration_thesis.alltrucks_v1.sgX_talpy_stats ((X = 1,2,....15))
Purpose
•	This notebook is meant to take as input the generated telematics data from DataFrame_Generator and generate as output dataframes with aggregated TALPY stats per truck.
Other remarks
•	None
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
# Load Data
df_signals = spark.table("pt_configuration_thesis.alltrucks_v1.`salescodecombo1`")
unique_vins_count_ori = df_signals.select("vin").distinct().count()
print(f"Number of vehicles in the dataset: {unique_vins_count_ori}")

# 3.	GENERATE CUSTOM STATISTICS USING TALPY
%run /projects/talpy_modification_ptsys/timeseries_ptsys/ts_custom_statistics_ptsys
group_by_list = ["vin"]

update_dp_config_dict = {
    "tables": {"ts": {"sample_rate": 1}},
    "signals": {
        "names": {
            "vehicle_speed": "20_agg_avg_300",
            "engine_rpm": "11",
            "mass_flow_rate": "87",
            "vehicle_weight": "5589",
        },
        "sna_values": {"vehicle_speed": 200},
    },
}
statistics_pipeline_talpy = TsStatisticPipeline(
    statistic_objects=[
        # Distance
        Distance(data_source="conantic", group_by = group_by_list, update_dp_config_dict = update_dp_config_dict),

        # Fuel
        FuelConsumption("conantic", group_by = group_by_list, update_dp_config_dict = update_dp_config_dict),
        FuelConsumptionPer100km("conantic", group_by = group_by_list, update_dp_config_dict = update_dp_config_dict),

        # Vehicle
        WeightDistanceBased("conantic", group_by = group_by_list, update_dp_config_dict = update_dp_config_dict),
        RoadGradient("conantic", group_by = group_by_list, update_dp_config_dict = update_dp_config_dict),
        Speed("conantic", group_by = group_by_list, update_dp_config_dict = update_dp_config_dict),
        Weight("conantic", group_by = group_by_list, update_dp_config_dict = update_dp_config_dict),

    ]
)
df_custom_statistics_talpy = statistics_pipeline_talpy.transform(df_signals)
display(df_custom_statistics_talpy)

# 4.	PUSH DATA AFTER REMOVAL OF OUTLIERS TO CATALOG
df_custom_statistics_talpy.write.mode("overwrite").saveAsTable("pt_configuration_thesis.alltrucks_v1.sg1_talpy_stats")


