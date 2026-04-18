CATALOG = "dbr_dev"

spark.sql(f"""
    CREATE OR REPLACE FUNCTION {CATALOG}.tokariev_bronze.mask_forbidden_double(
        val DOUBLE
    )
    RETURNS DOUBLE
    RETURN IF(IS_ACCOUNT_GROUP_MEMBER('students'), val, NULL)
""")

for schema in ("tokariev_silver", "tokariev_gold"):
    spark.sql(f"""
        CREATE OR REPLACE FUNCTION {CATALOG}.{schema}.mask_forbidden_decimal(
            val DECIMAL(10,3)
        )
        RETURNS DECIMAL(10,3)
        RETURN IF(IS_ACCOUNT_GROUP_MEMBER('students'), val, NULL)
    """)

spark.sql(f"""
    CREATE OR REPLACE FUNCTION {CATALOG}.tokariev_gold.mask_forbidden_decimal_14_7(
        val DECIMAL(14,7)
    )
    RETURNS DECIMAL(14,7)
    RETURN IF(IS_ACCOUNT_GROUP_MEMBER('students'), val, NULL)
""")


spark.sql(f"""
    CREATE OR REPLACE FUNCTION {CATALOG}.tokariev_bronze.filter_current_year(
        year_val BIGINT
    )
    RETURNS BOOLEAN
    RETURN IS_ACCOUNT_GROUP_MEMBER('students')
           OR year_val >= YEAR(CURRENT_DATE())
""")

spark.sql(f"""
    CREATE OR REPLACE FUNCTION {CATALOG}.tokariev_silver.filter_current_year(
        year_val INT
    )
    RETURNS BOOLEAN
    RETURN IS_ACCOUNT_GROUP_MEMBER('students')
           OR year_val >= YEAR(CURRENT_DATE())
""")

print("All security UDFs created successfully.")
