from pyspark import pipelines as dp

@dp.table(
    name = "bronze_driver_championship"
)