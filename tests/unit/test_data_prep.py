from clusters_cost_allocation.cost_calculator import *
from chispa.dataframe_comparer import *
from pyspark.sql import SparkSession
from datetime import datetime
from schema_definitions import *
from decimal import *


prep_system_query_history_schema = StructType(
    [
        StructField("account_id", StringType(), False),
        StructField("workspace_id", StringType(), False),
        StructField("statement_id", StringType(), False),
        StructField("executed_by", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("execution_status", StringType(), False),
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
        StructField("billing_date", DateType(), False),
        StructField("warehouse_id", StringType(), False),
    ]
)


def test_should_prepare_query_history(
    spark_session: SparkSession,
):  # using pytest-spark
    raw_queries_df = spark_session.createDataFrame(
        [
            (
                "account1",
                "workspace1",
                "statement1",
                "user1@databricks.com",
                "session1",
                "FINISHED",
                {"type": "WAREHOUSE", "cluster_id": None, "warehouse_id": "warehouse1"},
                "user1",
                "select 1",
                "SELECT",
                None,
                None,
                None,
                datetime.strptime("2024-01-24 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"),
                datetime.strptime("2024-01-24 23:06:07.260", "%Y-%m-%d %H:%M:%S.%f"),
                datetime.strptime("2024-01-24 23:06:07.550", "%Y-%m-%d %H:%M:%S.%f"),
                100,
                500,
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                12,
                13,
                14,
                15,
                None,
            ),
            (
                "account1",
                "workspace1",
                "statement1",
                "user1@databricks.com",
                "session1",
                "FINISHED",
                {"type": "WAREHOUSE", "cluster_id": None, "warehouse_id": "warehouse1"},
                "user1",
                "select 1",
                "SELECT",
                None,
                None,
                None,
                datetime.strptime("3000-01-24 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"),
                datetime.strptime("3000-01-24 23:06:07.260", "%Y-%m-%d %H:%M:%S.%f"),
                datetime.strptime("3000-01-24 23:06:07.550", "%Y-%m-%d %H:%M:%S.%f"),
                100,
                500,
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                12,
                13,
                14,
                15,
                None,
            ),
            # no metrics, should be filtered out
            (
                "account1",
                "workspace1",
                "statement1",
                "user1@databricks.com",
                "session1",
                "FINISHED",
                {"type": "WAREHOUSE", "cluster_id": None, "warehouse_id": "warehouse1"},
                "user1",
                "select 1",
                "SELECT",
                None,
                None,
                None,
                datetime.strptime("3000-01-24 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"),
                datetime.strptime("3000-01-24 23:06:07.260", "%Y-%m-%d %H:%M:%S.%f"),
                datetime.strptime("3000-01-24 23:06:07.550", "%Y-%m-%d %H:%M:%S.%f"),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ),
        ],
        system_query_history_schema,
    )

    queries_df = CostCalculatorIO.prepare_query_history(raw_queries_df)

    expected_queries_df = spark_session.createDataFrame(
        [
            (
                "account1",
                "workspace1",
                "statement1",
                "user1@databricks.com",
                "session1",
                "FINISHED",
                "user1",
                "select 1",
                "SELECT",
                None,
                None,
                None,
                datetime.strptime("2024-01-24 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"),
                datetime.strptime("2024-01-24 23:06:07.260", "%Y-%m-%d %H:%M:%S.%f"),
                datetime.strptime("2024-01-24 23:06:07.550", "%Y-%m-%d %H:%M:%S.%f"),
                100,
                500,
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                12,
                13,
                14,
                15,
                None,
                datetime.strptime("2024-01-24", "%Y-%m-%d"),
                "warehouse1",
            )
        ],
        prep_system_query_history_schema,
    )

    assert_df_equality(
        queries_df, expected_queries_df, ignore_column_order=True, ignore_nullable=True
    )


def test_should_prepare_query_history_filter_when_checkpoint_is_provided(
    spark_session: SparkSession,
):  # using pytest-spark
    raw_queries_df = spark_session.createDataFrame(
        [
            (
                "account1",
                "workspace1",
                "statement1",
                "user1@databricks.com",
                "session1",
                "FINISHED",
                {"type": "WAREHOUSE", "cluster_id": None, "warehouse_id": "warehouse1"},
                "user1",
                "select 1",
                "SELECT",
                None,
                None,
                None,
                datetime.strptime("2024-01-24 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"),
                datetime.strptime("2024-01-24 23:06:07.260", "%Y-%m-%d %H:%M:%S.%f"),
                datetime.strptime("2024-01-24 23:06:07.550", "%Y-%m-%d %H:%M:%S.%f"),
                100,
                500,
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                12,
                13,
                14,
                15,
                None,
            ),
        ],
        system_query_history_schema,
    )

    # return records greater than 2024-01-24
    queries_df = CostCalculatorIO.prepare_query_history(
        raw_queries_df,
        datetime.strptime("2024-01-24", "%Y-%m-%d").date(),
    )

    assert queries_df.count() == 0


def test_should_prepare_query_history_filter_when_current_date_is_provided(
    spark_session: SparkSession,
):  # using pytest-spark
    raw_queries_df = spark_session.createDataFrame(
        [
            (
                "account1",
                "workspace1",
                "statement1",
                "user1@databricks.com",
                "session1",
                "FINISHED",
                {"type": "WAREHOUSE", "cluster_id": None, "warehouse_id": "warehouse1"},
                "user1",
                "select 1",
                "SELECT",
                None,
                None,
                None,
                datetime.strptime("2024-01-24 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"),
                datetime.strptime("2024-01-24 23:06:07.260", "%Y-%m-%d %H:%M:%S.%f"),
                datetime.strptime("2024-01-24 23:06:07.550", "%Y-%m-%d %H:%M:%S.%f"),
                100,
                500,
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                12,
                13,
                14,
                15,
                None,
            ),
        ],
        system_query_history_schema,
    )

    # return records greater than 2024-01-24
    queries_df = CostCalculatorIO.prepare_query_history(
        raw_queries_df,
        last_checkpoint=None,
        # should only include query metrics from 2024-1-23
        current_date=datetime(year=2024, month=1, day=24).date(),
    )

    assert queries_df.count() == 0


def test_should_prepare_list_prices(spark_session: SparkSession):  # using pytest-spark
    raw_list_prices_df = spark_session.createDataFrame(
        [
            (
                "account1",
                "AZURE",
                "PREMIUM_SQL_PRO_COMPUTE",
                "EUR",
                "DBU",
                {"default": Decimal(0.55)},
                datetime.strptime("2024-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"),
                datetime.strptime("2024-01-02 23:59:59", "%Y-%m-%d %H:%M:%S"),
            ),
            (
                "account1",
                "AZURE",
                "PREMIUM_SQL_PRO_COMPUTE",
                "EUR",
                "DBU",
                {"default": Decimal(0.6)},
                datetime.strptime("2024-01-03 00:00:00", "%Y-%m-%d %H:%M:%S"),
                None,
            ),
        ],
        list_prices_schema,
    )

    list_prices_df = CostCalculatorIO.prepare_list_prices(
        raw_list_prices_df, current_date=datetime.strptime("2024-01-04", "%Y-%m-%d")
    )

    expected_list_prices_schema = StructType(
        [
            StructField("account_id", StringType(), False),
            StructField("cloud", StringType(), False),
            StructField("sku_name", StringType(), False),
            StructField("currency_code", StringType(), False),
            StructField("usage_unit", StringType(), False),
            StructField("pricing", DecimalType(10, 2)),
            StructField("billing_date", DateType(), False),
        ]
    )

    exected_list_prices_df = spark_session.createDataFrame(
        [
            (
                "account1",
                "AZURE",
                "PREMIUM_SQL_PRO_COMPUTE",
                "EUR",
                "DBU",
                Decimal(0.55),
                datetime.strptime("2024-01-01", "%Y-%m-%d"),
            ),
            (
                "account1",
                "AZURE",
                "PREMIUM_SQL_PRO_COMPUTE",
                "EUR",
                "DBU",
                Decimal(0.55),
                datetime.strptime("2024-01-02", "%Y-%m-%d"),
            ),
            (
                "account1",
                "AZURE",
                "PREMIUM_SQL_PRO_COMPUTE",
                "EUR",
                "DBU",
                Decimal(0.6),
                datetime.strptime("2024-01-03", "%Y-%m-%d"),
            ),
            (
                "account1",
                "AZURE",
                "PREMIUM_SQL_PRO_COMPUTE",
                "EUR",
                "DBU",
                Decimal(0.6),
                datetime.strptime("2024-01-04", "%Y-%m-%d"),
            ),
        ],
        expected_list_prices_schema,
    )

    assert_df_equality(
        list_prices_df,
        exected_list_prices_df,
        ignore_column_order=True,
        ignore_nullable=True,
    )


def test_should_prepare_billing_usage(
    spark_session: SparkSession,
):  # using pytest-spark
    raw_billing_df = spark_session.createDataFrame(
        [
            # should aggregate
            (
                "1",
                "account1",
                "workspace1",
                "PREMIUM_SQL_PRO_COMPUTE",
                "AZURE",
                datetime.strptime("2024-01-24 01:06:06.944", "%Y-%m-%d %H:%M:%S.%f"),
                datetime.strptime("2024-01-24 02:06:06.944", "%Y-%m-%d %H:%M:%S.%f"),
                datetime.strptime("2024-01-24", "%Y-%m-%d"),
                None,
                "DBU",
                Decimal(50.0),
                {"warehouse_id": "warehouse1"},
            ),
            (
                "2",
                "account1",
                "workspace1",
                "PREMIUM_SQL_PRO_COMPUTE",
                "AZURE",
                datetime.strptime("2024-01-24 02:06:06.944", "%Y-%m-%d %H:%M:%S.%f"),
                datetime.strptime("2024-01-24 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"),
                datetime.strptime("2024-01-24", "%Y-%m-%d"),
                None,
                "DBU",
                Decimal(50.0),
                {"warehouse_id": "warehouse1"},
            ),
            # should skip if warehouse id not present
            (
                "3",
                "account1",
                "workspace1",
                "PREMIUM_SQL_PRO_COMPUTE",
                "AZURE",
                datetime.strptime("2024-01-25 01:06:06.944", "%Y-%m-%d %H:%M:%S.%f"),
                datetime.strptime("2024-01-25 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"),
                datetime.strptime("2024-01-25", "%Y-%m-%d"),
                None,
                "DBU",
                Decimal(100.0),
                None,
            ),
            (
                "4",
                "account1",
                "workspace1",
                "PREMIUM_SQL_PRO_COMPUTE",
                "AZURE",
                datetime.strptime("2024-01-26 01:06:06.944", "%Y-%m-%d %H:%M:%S.%f"),
                datetime.strptime("2024-01-26 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"),
                datetime.strptime("2024-01-26", "%Y-%m-%d"),
                None,
                "DBU",
                Decimal(100.0),
                {"warehouse_id": None},
            ),
        ],
        system_billing_usage_schema,
    )

    billing_df = CostCalculatorIO.prepare_billing(raw_billing_df)

    expected_billing_usage_schema = StructType(
        [
            StructField("account_id", StringType(), False),
            StructField("warehouse_id", StringType(), False),
            StructField("workspace_id", StringType(), False),
            StructField("sku_name", StringType(), False),
            StructField("cloud", StringType(), False),
            StructField("usage_unit", StringType(), False),
            StructField("billing_date", DateType(), False),
            StructField("usage_quantity", DecimalType(20, 4), True),
        ]
    )

    expected_billing_df = spark_session.createDataFrame(
        [
            (
                "account1",
                "warehouse1",
                "workspace1",
                "PREMIUM_SQL_PRO_COMPUTE",
                "AZURE",
                "DBU",
                datetime.strptime("2024-01-24", "%Y-%m-%d"),
                Decimal(100.00),
            )
        ],
        expected_billing_usage_schema,
    )

    assert_df_equality(
        billing_df, expected_billing_df, ignore_column_order=True, ignore_nullable=True
    )


def test_should_prepare_billing_usage_filter_when_checkpoint_is_provided(
    spark_session: SparkSession,
):  # using pytest-spark
    raw_billing_df = spark_session.createDataFrame(
        [
            (
                "1",
                "account1",
                "workspace1",
                "PREMIUM_SQL_PRO_COMPUTE",
                "AZURE",
                datetime.strptime("2024-01-24 01:06:06.944", "%Y-%m-%d %H:%M:%S.%f"),
                datetime.strptime("2024-01-24 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"),
                datetime.strptime("2024-01-24", "%Y-%m-%d"),
                None,
                "DBU",
                Decimal(100.0),
                {"warehouse_id": "warehouse1"},
            )
        ],
        system_billing_usage_schema,
    )

    # return records greater than 2024-01-24
    billing_df = CostCalculatorIO.prepare_billing(
        raw_billing_df,
        last_checkpoint=datetime.strptime("2024-01-24", "%Y-%m-%d").date(),
    )

    assert billing_df.count() == 0


def test_should_prepare_cloud_infra_cost(
    spark_session: SparkSession,
):  # using pytest-spark
    raw_cloud_infra_cost_df = spark_session.createDataFrame(
        [
            # should aggregate
            (
                "account1",
                "AZURE",
                "1",
                "2",
                "workspace1",
                datetime.strptime("2024-01-24 01:06:06.944", "%Y-%m-%d %H:%M:%S.%f"),
                datetime.strptime("2024-01-24 02:06:06.944", "%Y-%m-%d %H:%M:%S.%f"),
                datetime.strptime("2024-01-24", "%Y-%m-%d"),
                Decimal(50.0),
                "EUR",
                {
                    "cluster_id": None,
                    "warehouse_id": "warehouse1",
                    "instance_pool_id": None,
                },
            ),
            (
                "account1",
                "AZURE",
                "2",
                "3",
                "workspace1",
                datetime.strptime("2024-01-24 01:06:06.944", "%Y-%m-%d %H:%M:%S.%f"),
                datetime.strptime("2024-01-24 02:06:06.944", "%Y-%m-%d %H:%M:%S.%f"),
                datetime.strptime("2024-01-24", "%Y-%m-%d"),
                Decimal(50.0),
                "EUR",
                {
                    "cluster_id": None,
                    "warehouse_id": "warehouse1",
                    "instance_pool_id": None,
                },
            ),
            # should skip if warehouse id not present
            (
                "account1",
                "AZURE",
                "4",
                "5",
                "workspace1",
                datetime.strptime("2024-01-24 01:06:06.944", "%Y-%m-%d %H:%M:%S.%f"),
                datetime.strptime("2024-01-24 02:06:06.944", "%Y-%m-%d %H:%M:%S.%f"),
                datetime.strptime("2024-01-24", "%Y-%m-%d"),
                Decimal(50.0),
                "EUR",
                {"cluster_id": None, "warehouse_id": None, "instance_pool_id": None},
            ),
            (
                "account1",
                "AZURE",
                "6",
                "7",
                "workspace1",
                datetime.strptime("2024-01-24 01:06:06.944", "%Y-%m-%d %H:%M:%S.%f"),
                datetime.strptime("2024-01-24 02:06:06.944", "%Y-%m-%d %H:%M:%S.%f"),
                datetime.strptime("2024-01-24", "%Y-%m-%d"),
                Decimal(50.0),
                "EUR",
                None,
            ),
        ],
        system_cloud_infra_costs_schema,
    )

    cloud_infra_cost_df = CostCalculatorIO.prepare_cloud_infra_cost(
        raw_cloud_infra_cost_df
    )

    expected_cloud_infra_cost_schema = StructType(
        [
            StructField("account_id", StringType(), False),
            StructField("cloud", StringType(), False),
            StructField("warehouse_id", StringType(), False),
            StructField("workspace_id", StringType(), False),
            StructField("billing_date", DateType(), False),
            StructField("cost", DecimalType(38, 2), False),
            StructField("currency_code", StringType(), False),
        ]
    )

    expected_cloud_infra_cost_df = spark_session.createDataFrame(
        [
            (
                "account1",
                "AZURE",
                "warehouse1",
                "workspace1",
                datetime.strptime("2024-01-24", "%Y-%m-%d"),
                Decimal(100.00),
                "EUR",
            )
        ],
        expected_cloud_infra_cost_schema,
    )

    assert_df_equality(
        cloud_infra_cost_df,
        expected_cloud_infra_cost_df,
        ignore_column_order=True,
        ignore_nullable=True,
    )


def test_should_prepare_cloud_infra_cost_when_checkpoint_is_provided(
    spark_session: SparkSession,
):  # using pytest-spark
    raw_cloud_infra_cost_df = spark_session.createDataFrame(
        [
            (
                "account1",
                "AZURE",
                "1",
                "2",
                "workspace1",
                datetime.strptime("2024-01-24 01:06:06.944", "%Y-%m-%d %H:%M:%S.%f"),
                datetime.strptime("2024-01-24 02:06:06.944", "%Y-%m-%d %H:%M:%S.%f"),
                datetime.strptime("2024-01-24", "%Y-%m-%d"),
                Decimal(50.0),
                "EUR",
                {
                    "cluster_id": None,
                    "warehouse_id": "warehouse1",
                    "instance_pool_id": None,
                },
            ),
        ],
        system_cloud_infra_costs_schema,
    )

    cloud_infra_cost_df = CostCalculatorIO.prepare_cloud_infra_cost(
        raw_cloud_infra_cost_df,
        last_checkpoint=datetime.strptime("2024-01-24", "%Y-%m-%d").date(),
    )

    assert cloud_infra_cost_df.count() == 0
