from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, BooleanType

#---------------
#   Passive data schemas
#---------------
driver_championship_schema = StructType([
    StructField('driver_number', IntegerType(), False),
    StructField('meeting_key', StringType(), False),
    StructField('points_current', IntegerType()),
    StructField('points_start', IntegerType()),
    StructField('position_current', IntegerType()),
    StructField('position_start', IntegerType()),
    StructField('session_key', StringType(), False),
])

teams_championship_schema = StructType([
    StructField('team_name', StringType(), False),
    StructField('meeting_key', StringType(), False),
    StructField('points_current', IntegerType()),
    StructField('points_start', IntegerType()),
    StructField('position_current', IntegerType()),
    StructField('position_start', IntegerType()),
    StructField('session_key', StringType(), False),
])

drivers_schema = StructType([
    StructField('broadcast_name', StringType(), False),
    StructField("country_code", StringType()),
    StructField("driver_number", IntegerType()),
    StructField("first_name", StringType()),
    StructField("full_name", StringType()),
    StructField("headshot_url", StringType()),
    StructField("last_name", StringType()),
    StructField("meeting_key", StringType(), False),
    StructField("name_acronym", StringType()),
    StructField("session_key", StringType(), False),
    StructField("team_color", StringType()),
    StructField("team_name", StringType()),
])

meetings_schema = StructType([
    StructField('meeting_key', StringType(), False),
    StructField('meeting_name', StringType()),
    StructField('meeting_official_name', StringType()),

    StructField('circuit_key', StringType(), False),
    StructField('circuit_info_url', StringType()),
    StructField('circuit_short_name', StringType()),
    StructField('circuit_type', StringType()),

    StructField('country_name', StringType()),
    StructField('country_code', StringType()),
    StructField('country_key', IntegerType()),

    StructField('date_start', DateType()),
    StructField('date_end', DateType()),
    StructField('gmt_offset', DateType()),
    StructField('location', StringType()),

    StructField('year', IntegerType(), False),

])

sessions_schema = StructType([
    StructField('meeting_key', StringType(), False),
    StructField('circuit_key', StringType(), False),
    StructField('circuit_short_name', StringType()),

    StructField('country_name', StringType()),
    StructField('country_code', StringType()),
    StructField('country_key', IntegerType()),

    StructField('date_start', DateType()),
    StructField('date_end', DateType()),
    StructField('gmt_offset', DateType()),
    StructField('location', StringType()),

    StructField('session_name', StringType()),
    StructField('session_type', StringType()),
    StructField('year', IntegerType(), False),

])

session_results_schema = StructType([
    StructField('dnf', BooleanType()),
    StructField('dns', BooleanType()),
    StructField('dsq', BooleanType()),
    StructField('driver_number', IntegerType(), False),
    StructField('duration', IntegerType()),
    StructField('gap_to_leader', IntegerType()),
    StructField('number_of_laps', IntegerType()),
    StructField('meeting_key', IntegerType()), False,
    StructField('position', IntegerType()),
    StructField('session_key', IntegerType()), False,
])

starting_grid_schema = StructType([
    StructField('driver_number', IntegerType(), False),
    StructField('lap_duration', IntegerType()),
    StructField('meeting_key', IntegerType()), False,
    StructField('session_key', IntegerType()), False,
    StructField('position', IntegerType()),

])
#---------------
#   Active data schemas
#---------------