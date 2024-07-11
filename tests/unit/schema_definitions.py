from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    LongType,
    TimestampType,
    BooleanType,
    IntegerType,
    DecimalType,
    MapType,
)

system_query_history_schema = StructType(
    [
        StructField("account_id", StringType(), False),
        StructField("workspace_id", StringType(), False),
        StructField("statement_id", StringType(), False),
        StructField("executed_by", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("execution_status", StringType(), False),
        StructField(
            "compute",
            StructType(
                [
                    StructField("type", StringType(), True),
                    StructField("cluster_id", StringType(), True),
                    StructField("warehouse_id", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("executed_by_user_id", StringType(), False),
        StructField("statement_text", StringType(), False),
        StructField("statement_type", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("client_application", StringType(), True),
        StructField("client_driver", StringType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), False),
        StructField("update_time", TimestampType(), True),
        StructField("total_duration_ms", LongType(), True),
        StructField("total_task_duration_ms", LongType(), True),
        StructField("waiting_for_compute_duration_ms", LongType(), True),
        StructField("waiting_at_capacity_duration_ms", LongType(), True),
        StructField("execution_duration_ms", LongType(), True),
        StructField("compilation_duration_ms", LongType(), True),
        StructField("result_fetch_duration_ms", LongType(), True),
        StructField("read_partitions", LongType(), True),
        StructField("pruned_files", LongType(), True),
        StructField("read_files", LongType(), True),
        StructField("read_rows", LongType(), True),
        StructField("produced_rows", LongType(), True),
        StructField("read_bytes", LongType(), True),
        StructField("read_io_cache_percent", IntegerType(), True),
        StructField("spilled_local_bytes", LongType(), True),
        StructField("written_bytes", LongType(), True),
        StructField("shuffle_read_bytes", LongType(), True),
        StructField("from_results_cache", BooleanType(), True),
    ]
)

list_prices_schema = StructType(
    [
        StructField("account_id", StringType(), False),
        StructField("cloud", StringType(), False),
        StructField("sku_name", StringType(), False),
        StructField("currency_code", StringType(), False),
        StructField("usage_unit", StringType(), False),
        StructField(
            "pricing",
            StructType(
                [
                    StructField("default", DecimalType(10, 2), True),
                ]
            ),
            True,
        ),
        StructField("price_start_time", TimestampType(), False),
        StructField("price_end_time", TimestampType(), True),
    ]
)

user_info_schema = StructType(
    [
        StructField("user_name", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("display_name", StringType(), False),
        StructField("department", StringType(), False),
        StructField("cost_center", StringType(), True),
    ]
)

system_billing_usage_schema = StructType(
    [
        StructField("record_id", StringType(), False),
        StructField("account_id", StringType(), False),
        StructField("workspace_id", StringType(), False),
        StructField("sku_name", StringType(), False),
        StructField("cloud", StringType(), False),
        StructField("usage_start_time", TimestampType(), False),
        StructField("usage_end_time", TimestampType(), False),
        StructField("usage_date", DateType(), False),
        StructField("custom_tags", MapType(StringType(), StringType()), True),
        StructField("usage_unit", StringType(), False),
        StructField("usage_quantity", DecimalType(10, 4), False),
        StructField(
            "usage_metadata",
            StructType([StructField("warehouse_id", StringType(), True)]),
            True,
        ),
    ]
)

system_cloud_infra_costs_schema = StructType(
    [
        StructField("account_id", StringType(), False),
        StructField("cloud", StringType(), False),
        StructField("record_id", StringType(), False),
        StructField("cloud_account_id", StringType(), False),
        StructField("workspace_id", StringType(), False),
        StructField("usage_start_time", TimestampType(), False),
        StructField("usage_end_time", TimestampType(), False),
        StructField("usage_date", DateType(), False),
        StructField("cost", DecimalType(38, 2), False),
        StructField("currency_code", StringType(), False),
        StructField(
            "usage_metadata",
            StructType(
                [
                    StructField("cluster_id", StringType(), True),
                    StructField("warehouse_id", StringType(), True),
                    StructField("instance_pool_id", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)

user_costs_day_schema = StructType(
    [
        StructField("account_id", StringType(), False),
        StructField("workspace_id", StringType(), False),
        StructField("cloud", StringType(), False),
        StructField("billing_date", DateType(), False),
        StructField("warehouse_id", StringType(), False),
        StructField("user_name", StringType(), False),
        StructField("dbu_contribution_percent", DecimalType(17, 14), False),
        StructField("dbu", DecimalType(38, 2), False),
        StructField("dbu_cost", DecimalType(38, 2), False),
        StructField("cloud_cost", DecimalType(38, 2), True),
        StructField("currency_code", StringType(), False),
        StructField("total_duration_ms", LongType(), True),
        StructField("total_task_duration_ms", LongType(), True),
        StructField("execution_duration_ms", LongType(), True),
        StructField("compilation_duration_ms", LongType(), True),
        StructField("result_fetch_duration_ms", LongType(), True),
        StructField("read_partitions", LongType(), True),
        StructField("pruned_files", LongType(), True),
        StructField("read_files", LongType(), True),
        StructField("read_rows", LongType(), True),
        StructField("produced_rows", LongType(), True),
        StructField("read_bytes", LongType(), True),
        StructField("spilled_local_bytes", LongType(), True),
        StructField("written_bytes", LongType(), True),
        StructField("shuffle_read_bytes", LongType(), True),
    ]
)

user_costs_month_schema = StructType(
    [
        StructField("account_id", StringType(), False),
        StructField("workspace_id", StringType(), False),
        StructField("cloud", StringType(), False),
        StructField("billing_year", IntegerType(), False),
        StructField("billing_month", IntegerType(), False),
        StructField("billing_date", DateType(), False),
        StructField("user_name", StringType(), False),
        StructField("dbu_contribution_percent", DecimalType(17, 14), False),
        StructField("dbu", DecimalType(38, 2), False),
        StructField("dbu_cost", DecimalType(38, 2), False),
        StructField("cloud_cost", DecimalType(38, 2), True),
        StructField("currency_code", StringType(), False),
    ]
)
