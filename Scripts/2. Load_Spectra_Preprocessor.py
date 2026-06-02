'''
0. README
Notebook: Loadspectra_Preprocesser
Inputs
•	None
Outputs
•	None
Purpose
•	This notebook is meant to take as input telematics signal data and generate as output cleaned and augmented time-series signals using TALPY-based transformations.
Other remarks
•	This notebook is called from Loadspectra_Generator_Vel_Weight&Grad
'''

# 1.	HEADER
import pyspark.sql.functions as f
from talpy.timeseries.ts_transformer import CleanSignal, WithColumns
from talpy.timeseries.ts_pipeline_classes import TalpyPipeline
from talpy.timeseries.ts_column_factory import ColumnFactory

# 2.	FUNCTION CALL
%run /projects/talpy_modification_ptsys/timeseries_ptsys/ts_transformer_ptsys

# 3.	PREPROCESSING - COLUMN FACTORY
# add columns with column factory
column_factory = ColumnFactory("conantic")
distance_via_speed_column = column_factory.create_distance_via_speed_column_talpy_pipeline()
engine_power_column = column_factory.create_engine_power_column_transformer()
# driving_flag_column = column_factory.create_driving_column_talpy_pipeline()

# add columns
# road gradient absolute value
road_gradiant_abs_columns = WithColumns(["209_abs"], [f.abs(f.col("209"))])

# engine torque percentage, only for vehicles with same powerrating
# engine_torque_max = 2500
# engine_torque_percentage_columns = ts_transformer.WithColumns(["engine_torque_percentage"], [f.when(f.col("4") > 0, f.col("4") / engine_torque_max).otherwise(0)])

# year month columns
year_month_column = WithColumns(["year_month"], [f.date_format("timestamp", "yyyyMM").cast("integer")])

# add odometer 19
odometer_column = WithColumns(["19_transformed"], [f.col("4563")/1000])

# shift matrix columns
shift_matrix_columns = ts_transformer.WithColumns(
    ["6022_lag", "gear_shift_event", "6022_from", "6022_to"],
    [
        f.lag(f.col("6022"), default=0).over(
            Window.partitionBy("vin").orderBy("timestamp")
        ),
        f.when(
            (f.col("6022") != f.col("6022_lag")),
            1,
        ).otherwise(0),
        f.when(
            (f.col("gear_shift_event") == 1),
            f.col("6022_lag"),
        ).otherwise(f.lit(0)),
        f.when(
            (f.col("gear_shift_event") == 1),
            f.col("6022"),
        ).otherwise(f.lit(0)),
    ],
)

diff_time_to_last_timestamp_col = ts_transformer.WithColumns(
    ["timestamp_lag_vin", "diff_time_to_last_timestamp_vin_min"],
    [
        f.lag(f.col("timestamp"), default=None).over(
            Window.partitionBy("vin").orderBy("timestamp")
        ),
        f.when(
            f.isnull(f.col("timestamp") - f.col("timestamp_lag_vin")),
            None,
        ).otherwise(
            (
                (f.col("timestamp").cast("long")
                - f.col("timestamp_lag_vin").cast("long"))/60
            )
        ),
    ],
)



# 4.	PREPROCESSING - SIGNAL CLEANING
def clean_vehicle_speed_based_on_wheel_speed(df_signals):



    # clean engine speed
    clean_engine_rpm = CleanSignal(
        data_source="conantic",
        int_sec_limit=10,
        signal_provided="11",
        output_signal_value=None,
        cleaned_signal_column_suffix="_cleaned",
        clean_condition=f.col("11") > 0,
    )

    # clean fuel mass flow
    clean_fuel_mass_flow = CleanSignal(
        data_source="conantic",
        int_sec_limit=10,
        signal_provided="87",
        output_signal_value=None,
        cleaned_signal_column_suffix="_cleaned",
        clean_condition=f.col("87") > 0,
    )

    clean_signal_pipeline = TalpyPipeline(
        stages=[
            clean_engine_rpm,
            clean_fuel_mass_flow,
        ]
    )

    return clean_signal_pipeline.transform(df_signals).dropna(
        subset=["11_cleaned", "87_cleaned"], how="any"
    )

# 5.	PREPROCESSING - PIPELINE EXECUTION

signals_add_columns_pipeline = TalpyPipeline(
    stages=[
        # add columns
        distance_via_speed_column,
        engine_power_column,
        # driving_flag_column,
        # engine_torque_percentage_columns,
        road_gradiant_abs_columns,
        year_month_column,
        # nshift_matrix_columns,
        # odometer_column,
        diff_time_to_last_timestamp_col,
    ]
)

def add_columns_to_signals(df_signals):
    return signals_add_columns_pipeline.transform(df_signals)

