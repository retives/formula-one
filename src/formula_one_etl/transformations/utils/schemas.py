from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DateType, BooleanType, FloatType, \
    DecimalType, DoubleType, ArrayType, TimestampType

#---------------
#   Passive data schemas
#---------------
driver_championship_schema = StructType([
    StructField('driver_number', IntegerType(), False),
    StructField('meeting_key', IntegerType(), False),
    StructField('points_current', IntegerType()),
    StructField('points_start', IntegerType()),
    StructField('position_current', IntegerType()),
    StructField('position_start', IntegerType()),
    StructField('session_key', IntegerType(), False),
])

teams_championship_schema = StructType([
    StructField('team_name', StringType(), False),
    StructField('meeting_key', IntegerType(), False),
    StructField('points_current', IntegerType()),
    StructField('points_start', IntegerType()),
    StructField('position_current', IntegerType()),
    StructField('position_start', IntegerType()),
    StructField('session_key', IntegerType(), False),
])

drivers_schema = StructType([
    StructField('broadcast_name', StringType(), False),
    StructField("country_code", StringType()),
    StructField("driver_number", IntegerType()),
    StructField("first_name", StringType()),
    StructField("full_name", StringType()),
    StructField("headshot_url", StringType()),
    StructField("last_name", StringType()),
    StructField("meeting_key", IntegerType(), False),
    StructField("name_acronym", StringType()),
    StructField("session_key", IntegerType(), False),
    StructField("team_colour", StringType()),
    StructField("team_name", StringType()),
])

meetings_schema = StructType([
    StructField('meeting_key', IntegerType(), False),
    StructField('meeting_name', StringType()),
    StructField('meeting_official_name', StringType()),

    StructField('circuit_key', IntegerType(), False),
    StructField('circuit_info_url', StringType()),
    StructField('circuit_short_name', StringType()),
    StructField('circuit_type', StringType()),

    StructField('country_name', StringType()),
    StructField('country_code', StringType()),
    StructField('country_key', IntegerType()),

    StructField('date_start', DateType()),
    StructField('date_end', DateType()),
    StructField('gmt_offset', StringType()),
    StructField('location', StringType()),

    StructField('year', IntegerType(), False),

])

sessions_schema = StructType([
    StructField('meeting_key', IntegerType(), False),
    StructField('circuit_key', IntegerType(), False),
    StructField('circuit_short_name', StringType()),

    StructField('country_name', StringType()),
    StructField('country_code', StringType()),
    StructField('country_key', IntegerType()),

    StructField('date_start', DateType()),
    StructField('date_end', DateType()),
    StructField('gmt_offset', StringType()),
    StructField('location', StringType()),

    StructField('session_name', StringType()),
    StructField('session_type', StringType()),
    StructField('year', IntegerType(), False),

])

# session_results_schema = StructType([
#     StructField('dnf', BooleanType()),
#     StructField('dns', BooleanType()),
#     StructField('dsq', BooleanType()),
#     StructField('driver_number', IntegerType(), False),
#     StructField('duration', FloatType()),
#     StructField('gap_to_leader', FloatType()),
#     StructField('number_of_laps', FloatType()),
#     StructField('meeting_key', IntegerType(), False),
#     StructField('position', IntegerType()),
#     StructField('session_key', IntegerType(), False),
# ])

starting_grid_schema = StructType([
    StructField('driver_number', IntegerType(), False),
    StructField('lap_duration', DoubleType()),
    StructField('meeting_key', IntegerType(), False),
    StructField('session_key', IntegerType(), False),
    StructField('position', IntegerType()),

])
#---------------
#   Active data schemas
#---------------

laps_schema = StructType([
    StructField('date_start', TimestampType()),
    StructField('driver_number', IntegerType()),
    StructField('duration_sector_1', DecimalType(10, 3)),
    StructField('duration_sector_2', DecimalType(10, 3)),
    StructField('duration_sector_3', DecimalType(10, 3)),
    StructField('i1_speed', IntegerType()),
    StructField('i2_speed', IntegerType()),
    StructField('is_pit_out_lap', BooleanType()),
    StructField('lap_duration', DecimalType(10, 3)),
    StructField('lap_number', IntegerType()),
    StructField('meeting_key', IntegerType(), False),
    StructField('session_key', IntegerType(), False),
    StructField('segments_sector_1', ArrayType(IntegerType())),
    StructField('segments_sector_2', ArrayType(IntegerType())),
    StructField('segments_sector_3', ArrayType(IntegerType())),
    StructField('st_speed', IntegerType()),
])

overtakes_schema = StructType([
    StructField('date', DateType()),
    StructField('meeting_key', IntegerType(), False),
    StructField('session_key', IntegerType(), False),
    StructField('overtaken_driver_number', IntegerType()),
    StructField('overtaking_driver_number', IntegerType()),
    StructField('position', IntegerType()),

])
pit_stop_schema = StructType([
    StructField("date", DateType(), True),
    StructField("driver_number", IntegerType(), False),
    StructField("lane_duration", DecimalType(10, 3), True),
    StructField("lap_number", IntegerType(), True),
    StructField("meeting_key", IntegerType(), False),
    StructField("pit_duration", DecimalType(10, 3), True),
    StructField("session_key", IntegerType(), False),
    StructField("stop_duration", DecimalType(10, 3), True)
])
position_schema = StructType([
    StructField("date", DateType(), True),
    StructField("driver_number", IntegerType(), True),
    StructField('meeting_key', IntegerType(), False),
    StructField('session_key', IntegerType(), False),
    StructField("position", IntegerType()),
])

race_control_schema = StructType([
    StructField("date", DateType(), True),
    StructField("driver_number", IntegerType(), True),
    StructField("flag", StringType(), True),
    StructField("lap_number", IntegerType(), True),
    StructField("meeting_key", IntegerType(), False),
    StructField("session_key", IntegerType(), False),
    StructField("message", StringType(), True),
    StructField("qualifying_phase", IntegerType(), True),
    StructField("scope", StringType(), True),
    StructField("sector", IntegerType(), True),
])

weather_schema = StructType([
    StructField("date", DateType(), True),
    StructField("meeting_key", IntegerType(), False),
    StructField("session_key", IntegerType(), False),
    StructField("air_temperature", FloatType(), True),
    StructField("humidity", FloatType(), True),
    StructField("pressure", FloatType(), True),
    StructField("rainfall", IntegerType(), True),
    StructField("track_temperature", FloatType(), True),
    StructField("wind_direction", IntegerType(), True),
    StructField("wind_speed", FloatType(), True),
])