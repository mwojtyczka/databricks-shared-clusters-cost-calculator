from clusters_cost_allocation.cost_calculator import *
from chispa.dataframe_comparer import *
from pyspark.sql import SparkSession
from decimal import *
from datetime import datetime
from schema_definitions import *


def test_should_calculate_daily_costs(
    spark_session: SparkSession,
):  # using pytest-spark
    weights = {"total_duration_ms": 0.5, "total_task_duration_ms": 0.5}

    queries_df = CostCalculatorIO.prepare_query_history(
        spark_session.createDataFrame(
            [
                # Test case 1: multiple queries from 1 user
                (
                    "account1",
                    "workspace1",
                    "statement1",
                    "user1@databricks.com",
                    "session1",
                    "FINISHED",
                    {
                        "type": "WAREHOUSE",
                        "cluster_id": None,
                        "warehouse_id": "warehouse1",
                    },
                    "user1",
                    "select 1",
                    "SELECT",
                    None,
                    None,
                    None,
                    datetime.strptime(
                        "2024-01-24 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-24 23:06:07.260", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-24 23:06:07.550", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    50,
                    250,
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
                    "statement2",
                    "user1@databricks.com",
                    "session2",
                    "FINISHED",
                    {
                        "type": "WAREHOUSE",
                        "cluster_id": None,
                        "warehouse_id": "warehouse1",
                    },
                    "user1",
                    "select 1",
                    "SELECT",
                    None,
                    None,
                    None,
                    datetime.strptime(
                        "2024-01-24 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-24 23:06:07.260", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-24 23:06:07.550", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    50,
                    250,
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
                # Test case 2: queries from 2 users
                (
                    "account1",
                    "workspace1",
                    "statement1",
                    "user1@databricks.com",
                    "session1",
                    "not_important",
                    {
                        "type": "WAREHOUSE",
                        "cluster_id": None,
                        "warehouse_id": "warehouse1",
                    },
                    "user1",
                    "not_important",
                    "not_important",
                    None,
                    None,
                    None,
                    datetime.strptime(
                        "2024-01-25 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 23:06:07.260", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 23:06:07.550", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    50,
                    250,
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
                    "statement2",
                    "user2@databricks.com",
                    "session2",
                    "FINISHED",
                    {
                        "type": "WAREHOUSE",
                        "cluster_id": None,
                        "warehouse_id": "warehouse1",
                    },
                    "user2",
                    "select 1",
                    "SELECT",
                    None,
                    None,
                    None,
                    datetime.strptime(
                        "2024-01-25 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 23:06:07.260", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 23:06:07.550", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    50,
                    250,
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
                    155,
                    None,
                ),
                # Test case 3: different warehouse
                (
                    "account1",
                    "workspace1",
                    "statement1",
                    "user1@databricks.com",
                    "session1",
                    "FINISHED",
                    {
                        "type": "WAREHOUSE",
                        "cluster_id": None,
                        "warehouse_id": "warehouse2",
                    },
                    "user1",
                    "select 1",
                    "SELECT",
                    None,
                    None,
                    None,
                    datetime.strptime(
                        "2024-01-25 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 23:06:07.260", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 23:06:07.550", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    50,
                    250,
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
                # Test case 4: different workspace
                (
                    "account1",
                    "workspace2",
                    "statement1",
                    "user1@databricks.com",
                    "session1",
                    "FINISHED",
                    {
                        "type": "WAREHOUSE",
                        "cluster_id": None,
                        "warehouse_id": "warehouse1",
                    },
                    "user1",
                    "select 1",
                    "SELECT",
                    None,
                    None,
                    None,
                    datetime.strptime(
                        "2024-01-25 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 23:06:07.260", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 23:06:07.550", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    50,
                    250,
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
                # Test case 5: different account
                (
                    "account2",
                    "workspace1",
                    "statement1",
                    "user1@databricks.com",
                    "session1",
                    "FINISHED",
                    {
                        "type": "WAREHOUSE",
                        "cluster_id": None,
                        "warehouse_id": "warehouse1",
                    },
                    "user1",
                    "select 1",
                    "SELECT",
                    None,
                    None,
                    None,
                    datetime.strptime(
                        "2024-01-25 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 23:06:07.260", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 23:06:07.550", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    50,
                    250,
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
                # Test case 6: no billing for account
                (
                    "NO_BILLING_ACCOUNT",
                    "workspace1",
                    "statement1",
                    "user1@databricks.com",
                    "session1",
                    "FINISHED",
                    {
                        "type": "WAREHOUSE",
                        "cluster_id": None,
                        "warehouse_id": "warehouse1",
                    },
                    "user1",
                    "select 1",
                    "SELECT",
                    None,
                    None,
                    None,
                    datetime.strptime(
                        "2024-01-25 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 23:06:07.260", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 23:06:07.550", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    50,
                    250,
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
                # Test case 7: no billing for workspace
                (
                    "account1",
                    "NO_BILLING_WORKSPACE",
                    "statement1",
                    "user1@databricks.com",
                    "session1",
                    "FINISHED",
                    {
                        "type": "WAREHOUSE",
                        "cluster_id": None,
                        "warehouse_id": "warehouse1",
                    },
                    "user1",
                    "select 1",
                    "SELECT",
                    None,
                    None,
                    None,
                    datetime.strptime(
                        "2024-01-25 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 23:06:07.260", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 23:06:07.550", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    50,
                    250,
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
                # Test case 8: no billing for warehouse
                (
                    "account1",
                    "workspace1",
                    "statement1",
                    "user1@databricks.com",
                    "session1",
                    "FINISHED",
                    {
                        "type": "WAREHOUSE",
                        "cluster_id": None,
                        "warehouse_id": "NO_BILLING_WAREHOUSE",
                    },
                    "user1",
                    "select 1",
                    "SELECT",
                    None,
                    None,
                    None,
                    datetime.strptime(
                        "2024-01-25 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 23:06:07.260", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 23:06:07.550", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    50,
                    250,
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
                # Test case 9: no billing for specified date
                (
                    "account1",
                    "workspace1",
                    "statement1",
                    "user1@databricks.com",
                    "session1",
                    "FINISHED",
                    {
                        "type": "WAREHOUSE",
                        "cluster_id": None,
                        "warehouse_id": "warehouse1",
                    },
                    "user1",
                    "select 1",
                    "SELECT",
                    None,
                    None,
                    None,
                    datetime.strptime(
                        "2024-01-30 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-30 23:06:07.260", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-30 23:06:07.550", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    50,
                    250,
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
                # Test case 10: metrics are null
                (
                    "account1",
                    "workspace1",
                    "statement1",
                    "user1@databricks.com",
                    "session1",
                    "FINISHED",
                    {
                        "type": "WAREHOUSE",
                        "cluster_id": None,
                        "warehouse_id": "warehouse1",
                    },
                    "user1",
                    "select 1",
                    "SELECT",
                    None,
                    None,
                    None,
                    datetime.strptime(
                        "2024-01-25 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 23:06:07.260", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 23:06:07.550", "%Y-%m-%d %H:%M:%S.%f"
                    ),
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
                # Test case 10: metrics are null, separate day
                (
                    "account1",
                    "workspace1",
                    "statement1",
                    "user1@databricks.com",
                    "session1",
                    "FINISHED",
                    {
                        "type": "WAREHOUSE",
                        "cluster_id": None,
                        "warehouse_id": "warehouse1",
                    },
                    "user1",
                    "select 1",
                    "SELECT",
                    None,
                    None,
                    None,
                    datetime.strptime(
                        "2024-01-26 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-26 23:06:07.260", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-26 23:06:07.550", "%Y-%m-%d %H:%M:%S.%f"
                    ),
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
                # Test case 11: contribution 0
                (
                    "account1",
                    "workspace1",
                    "statement1",
                    "user1@databricks.com",
                    "session1",
                    "FINISHED",
                    {
                        "type": "WAREHOUSE",
                        "cluster_id": None,
                        "warehouse_id": "warehouse1",
                    },
                    "user1",
                    "select 1",
                    "SELECT",
                    None,
                    None,
                    None,
                    datetime.strptime(
                        "2024-01-27 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-27 23:06:07.260", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-27 23:06:07.550", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    None,
                ),
            ],
            system_query_history_schema,
        ),
    )

    list_prices_df = CostCalculatorIO.prepare_list_prices(
        spark_session.createDataFrame(
            [
                (
                    "account1",
                    "AZURE",
                    "PREMIUM_SQL_PRO_COMPUTE",
                    "EUR",
                    "DBU",
                    {"default": Decimal(0.5)},
                    datetime.strptime("2024-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"),
                    None,
                ),
                (
                    "account2",
                    "AZURE",
                    "PREMIUM_SQL_PRO_COMPUTE",
                    "EUR",
                    "DBU",
                    {"default": Decimal(1.0)},
                    datetime.strptime("2024-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"),
                    None,
                ),
            ],
            list_prices_schema,
        )
    )

    billing_df = CostCalculatorIO.prepare_billing(
        spark_session.createDataFrame(
            [
                # Test case 1: multiple queries from 1 user
                (
                    "1",
                    "account1",
                    "workspace1",
                    "PREMIUM_SQL_PRO_COMPUTE",
                    "AZURE",
                    datetime.strptime(
                        "2024-01-24 01:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-24 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime("2024-01-24", "%Y-%m-%d"),
                    None,
                    "DBU",
                    Decimal(100.0),
                    {"warehouse_id": "warehouse1"},
                ),
                # Test case 2: queries from 2 users
                (
                    "2",
                    "account1",
                    "workspace1",
                    "PREMIUM_SQL_PRO_COMPUTE",
                    "AZURE",
                    datetime.strptime(
                        "2024-01-25 01:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime("2024-01-25", "%Y-%m-%d"),
                    None,
                    "DBU",
                    Decimal(100.0),
                    {"warehouse_id": "warehouse1"},
                ),
                # Test case 3: different warehouse
                (
                    "3",
                    "account1",
                    "workspace1",
                    "PREMIUM_SQL_PRO_COMPUTE",
                    "AZURE",
                    datetime.strptime(
                        "2024-01-25 01:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime("2024-01-25", "%Y-%m-%d"),
                    None,
                    "DBU",
                    Decimal(100.0),
                    {"warehouse_id": "warehouse2"},
                ),
                # Test case 4: different workspace
                (
                    "4",
                    "account1",
                    "workspace2",
                    "PREMIUM_SQL_PRO_COMPUTE",
                    "AZURE",
                    datetime.strptime(
                        "2024-01-25 01:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime("2024-01-25", "%Y-%m-%d"),
                    None,
                    "DBU",
                    Decimal(100.0),
                    {"warehouse_id": "warehouse1"},
                ),
                # Test case 5: different account
                (
                    "5",
                    "account2",
                    "workspace1",
                    "PREMIUM_SQL_PRO_COMPUTE",
                    "AZURE",
                    datetime.strptime(
                        "2024-01-25 01:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime("2024-01-25", "%Y-%m-%d"),
                    None,
                    "DBU",
                    Decimal(100.0),
                    {"warehouse_id": "warehouse1"},
                ),
                (
                    "6",
                    "account1",
                    "workspace1",
                    "PREMIUM_SQL_PRO_COMPUTE",
                    "AZURE",
                    datetime.strptime(
                        "2024-01-26 01:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-26 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime("2024-01-26", "%Y-%m-%d"),
                    None,
                    "DBU",
                    Decimal(100.0),
                    {"warehouse_id": "warehouse1"},
                ),
                (
                    "7",
                    "account1",
                    "workspace1",
                    "PREMIUM_SQL_PRO_COMPUTE",
                    "AZURE",
                    datetime.strptime(
                        "2024-01-27 01:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-27 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime("2024-01-27", "%Y-%m-%d"),
                    None,
                    "DBU",
                    Decimal(100.0),
                    {"warehouse_id": "warehouse1"},
                ),
            ],
            system_billing_usage_schema,
        )
    )

    cloud_infra_cost_df = CostCalculatorIO.prepare_cloud_infra_cost(
        spark_session.createDataFrame(
            [
                # should aggregate
                (
                    "account1",
                    "AZURE",
                    "1",
                    "2",
                    "workspace1",
                    datetime.strptime(
                        "2024-01-24 01:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-24 02:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime("2024-01-24", "%Y-%m-%d"),
                    Decimal(40.0),
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
                    "3",
                    "4",
                    "workspace1",
                    datetime.strptime(
                        "2024-01-24 04:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-24 05:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime("2024-01-24", "%Y-%m-%d"),
                    Decimal(40.0),
                    "EUR",
                    {
                        "cluster_id": None,
                        "warehouse_id": "warehouse1",
                        "instance_pool_id": None,
                    },
                ),
                # should split into users
                (
                    "account1",
                    "AZURE",
                    "5",
                    "6",
                    "workspace1",
                    datetime.strptime(
                        "2024-01-25 04:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 05:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime("2024-01-25", "%Y-%m-%d"),
                    Decimal(60.0),
                    "EUR",
                    {
                        "cluster_id": None,
                        "warehouse_id": "warehouse1",
                        "instance_pool_id": None,
                    },
                ),
                # different account
                (
                    "account2",
                    "AZURE",
                    "7",
                    "8",
                    "workspace1",
                    datetime.strptime(
                        "2024-01-25 01:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 02:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime("2024-01-25", "%Y-%m-%d"),
                    Decimal(60.0),
                    "EUR",
                    {
                        "cluster_id": None,
                        "warehouse_id": "warehouse1",
                        "instance_pool_id": None,
                    },
                ),
                # different workspace
                (
                    "account1",
                    "AZURE",
                    "9",
                    "10",
                    "workspace2",
                    datetime.strptime(
                        "2024-01-25 01:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 02:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime("2024-01-25", "%Y-%m-%d"),
                    Decimal(60.0),
                    "EUR",
                    {
                        "cluster_id": None,
                        "warehouse_id": "warehouse1",
                        "instance_pool_id": None,
                    },
                ),
                # different warehouse
                (
                    "account1",
                    "AZURE",
                    "11",
                    "12",
                    "workspace1",
                    datetime.strptime(
                        "2024-01-25 01:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 02:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime("2024-01-25", "%Y-%m-%d"),
                    Decimal(60.0),
                    "EUR",
                    {
                        "cluster_id": None,
                        "warehouse_id": "warehouse2",
                        "instance_pool_id": None,
                    },
                ),
            ],
            system_cloud_infra_costs_schema,
        )
    )

    expected_cost_agg_day_df = spark_session.createDataFrame(
        [
            # Test case 1: multiple queries from 1 user
            (
                "account1",
                "workspace1",
                "AZURE",
                datetime.strptime("2024-01-24", "%Y-%m-%d"),
                "warehouse1",
                "user1@databricks.com",
                "user1",
                Decimal(100.00),
                Decimal(100.00),
                Decimal(50.00),
                Decimal(80.00),
                "EUR",
            ),
            # Test case 2: queries from 2 users
            (
                "account1",
                "workspace1",
                "AZURE",
                datetime.strptime("2024-01-25", "%Y-%m-%d"),
                "warehouse1",
                "user1@databricks.com",
                "user1",
                Decimal(50.00),
                Decimal(50.00),
                Decimal(25.00),
                Decimal(30.00),
                "EUR",
            ),
            (
                "account1",
                "workspace1",
                "AZURE",
                datetime.strptime("2024-01-25", "%Y-%m-%d"),
                "warehouse1",
                "user2@databricks.com",
                "user2",
                Decimal(50.00),
                Decimal(50.00),
                Decimal(25.00),
                Decimal(30.00),
                "EUR",
            ),
            # Test case 3: different warehouse
            (
                "account1",
                "workspace1",
                "AZURE",
                datetime.strptime("2024-01-25", "%Y-%m-%d"),
                "warehouse2",
                "user1@databricks.com",
                "user1",
                Decimal(100.00),
                Decimal(100.00),
                Decimal(50.00),
                Decimal(60.00),
                "EUR",
            ),
            # Test case 4: different workspace
            (
                "account1",
                "workspace2",
                "AZURE",
                datetime.strptime("2024-01-25", "%Y-%m-%d"),
                "warehouse1",
                "user1@databricks.com",
                "user1",
                Decimal(100.00),
                Decimal(100.00),
                Decimal(50.00),
                Decimal(60.00),
                "EUR",
            ),
            # Test case 5: different account
            (
                "account2",
                "workspace1",
                "AZURE",
                datetime.strptime("2024-01-25", "%Y-%m-%d"),
                "warehouse1",
                "user1@databricks.com",
                "user1",
                Decimal(100.00),
                Decimal(100.00),
                Decimal(100.00),
                Decimal(60.00),
                "EUR",
            ),
            (
                "account1",
                "workspace1",
                "AZURE",
                datetime.strptime("2024-01-26", "%Y-%m-%d"),
                "warehouse1",
                "user1@databricks.com",
                "user1",
                Decimal(0.00),
                Decimal(0.00),
                Decimal(0.00),
                None,
                "EUR",
            ),
            (
                "account1",
                "workspace1",
                "AZURE",
                datetime.strptime("2024-01-27", "%Y-%m-%d"),
                "warehouse1",
                "user1@databricks.com",
                "user1",
                Decimal(0.00),
                Decimal(0.00),
                Decimal(0.00),
                None,
                "EUR",
            ),
        ],
        cost_agg_day_schema,
    )

    cost_agg_day_df = CostCalculator().calculate_cost_agg_day(
        weights, queries_df, list_prices_df, billing_df, cloud_infra_cost_df
    )

    assert_df_equality(
        cost_agg_day_df,
        expected_cost_agg_day_df,
        ignore_nullable=True,
        ignore_column_order=True,
        ignore_row_order=True,
    )


def test_should_calculate_daily_costs_when_missing_cloud_infra_cost(
    spark_session: SparkSession,
):  # using pytest-spark
    weights = {"total_duration_ms": 1.0}

    queries_df = CostCalculatorIO.prepare_query_history(
        spark_session.createDataFrame(
            [
                (
                    "account1",
                    "workspace1",
                    "statement1",
                    "user1@databricks.com",
                    "session1",
                    "FINISHED",
                    {
                        "type": "WAREHOUSE",
                        "cluster_id": None,
                        "warehouse_id": "warehouse1",
                    },
                    "user1",
                    "select 1",
                    "SELECT",
                    None,
                    None,
                    None,
                    datetime.strptime(
                        "2024-01-25 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 23:06:07.260", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 23:06:07.550", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    50,
                    250,
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
                    "user2@databricks.com",
                    "session1",
                    "FINISHED",
                    {
                        "type": "WAREHOUSE",
                        "cluster_id": None,
                        "warehouse_id": "warehouse1",
                    },
                    "user2",
                    "select 1",
                    "SELECT",
                    None,
                    None,
                    None,
                    datetime.strptime(
                        "2024-01-25 23:07:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 23:07:07.260", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 23:07:07.550", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    0,
                    2500,
                    10,
                    20,
                    30,
                    40,
                    50,
                    60,
                    70,
                    80,
                    90,
                    100,
                    110,
                    120,
                    130,
                    140,
                    150,
                    None,
                ),
                (
                    "account1",
                    "workspace1",
                    "statement1",
                    "user1@databricks.com",
                    "session1",
                    "FINISHED",
                    {
                        "type": "WAREHOUSE",
                        "cluster_id": None,
                        "warehouse_id": "warehouse1",
                    },
                    "user1",
                    "select 1",
                    "SELECT",
                    None,
                    None,
                    None,
                    datetime.strptime(
                        "2024-01-26 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-26 23:06:07.260", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-26 23:06:07.550", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    5000,
                    25000,
                    100,
                    200,
                    300,
                    400,
                    500,
                    600,
                    700,
                    800,
                    900,
                    1000,
                    1100,
                    1200,
                    1300,
                    1400,
                    1500,
                    None,
                ),
            ],
            system_query_history_schema,
        ),
    )

    list_prices_df = CostCalculatorIO.prepare_list_prices(
        spark_session.createDataFrame(
            [
                (
                    "account1",
                    "AZURE",
                    "PREMIUM_SQL_PRO_COMPUTE",
                    "EUR",
                    "DBU",
                    {"default": Decimal(0.5)},
                    datetime.strptime("2024-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"),
                    None,
                )
            ],
            list_prices_schema,
        )
    )

    billing_df = CostCalculatorIO.prepare_billing(
        spark_session.createDataFrame(
            [
                (
                    "1",
                    "account1",
                    "workspace1",
                    "PREMIUM_SQL_PRO_COMPUTE",
                    "AZURE",
                    datetime.strptime(
                        "2024-01-25 01:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime("2024-01-25", "%Y-%m-%d"),
                    None,
                    "DBU",
                    Decimal(80.0),
                    {"warehouse_id": "warehouse1"},
                ),
                (
                    "2",
                    "account1",
                    "workspace1",
                    "PREMIUM_SQL_PRO_COMPUTE",
                    "AZURE",
                    datetime.strptime(
                        "2024-01-26 01:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-26 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime("2024-01-26", "%Y-%m-%d"),
                    None,
                    "DBU",
                    Decimal(90.0),
                    {"warehouse_id": "warehouse1"},
                ),
            ],
            system_billing_usage_schema,
        )
    )

    cloud_infra_cost_df = CostCalculatorIO.prepare_cloud_infra_cost(
        spark_session.createDataFrame(
            [
                # should split into users
                (
                    "account1",
                    "AZURE",
                    "1",
                    "2",
                    "workspace1",
                    datetime.strptime(
                        "2024-01-25 04:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 05:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime("2024-01-25", "%Y-%m-%d"),
                    Decimal(90.0),
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
    )

    expected_cost_agg_day_df = spark_session.createDataFrame(
        [
            (
                "account1",
                "workspace1",
                "AZURE",
                datetime.strptime("2024-01-25", "%Y-%m-%d"),
                "warehouse1",
                "user1@databricks.com",
                "user1",
                Decimal(100.00),
                Decimal(80.00),
                Decimal(40.00),
                Decimal(90.00),
                "EUR",
            ),
            # contribution should be 0 since the metric in question is 0
            (
                "account1",
                "workspace1",
                "AZURE",
                datetime.strptime("2024-01-25", "%Y-%m-%d"),
                "warehouse1",
                "user2@databricks.com",
                "user2",
                Decimal(0.00),
                Decimal(0.00),
                Decimal(0.00),
                Decimal(0.00),
                "EUR",
            ),
            (
                "account1",
                "workspace1",
                "AZURE",
                datetime.strptime("2024-01-26", "%Y-%m-%d"),
                "warehouse1",
                "user1@databricks.com",
                "user1",
                Decimal(100.00),
                Decimal(90.00),
                Decimal(45.00),
                None,
                "EUR",
            ),
        ],
        cost_agg_day_schema,
    )

    cost_agg_day_df = CostCalculator().calculate_cost_agg_day(
        weights, queries_df, list_prices_df, billing_df, cloud_infra_cost_df
    )

    assert_df_equality(
        cost_agg_day_df,
        expected_cost_agg_day_df,
        ignore_nullable=True,
        ignore_column_order=True,
        ignore_row_order=True,
    )


def test_should_calculate_daily_costs_for_2_users(
    spark_session: SparkSession,
):  # using pytest-spark
    weights = {"total_duration_ms": 1.0}

    queries_df = CostCalculatorIO.prepare_query_history(
        spark_session.createDataFrame(
            [
                (
                    "account1",
                    "workspace1",
                    "statement1",
                    "user1@databricks.com",
                    "session1",
                    "FINISHED",
                    {
                        "type": "WAREHOUSE",
                        "cluster_id": None,
                        "warehouse_id": "warehouse1",
                    },
                    "user1",
                    "select 1",
                    "SELECT",
                    None,
                    None,
                    None,
                    datetime.strptime(
                        "2024-01-25 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 23:06:07.260", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 23:06:07.550", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    10,
                    0,
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
                    "user2@databricks.com",
                    "session1",
                    "FINISHED",
                    {
                        "type": "WAREHOUSE",
                        "cluster_id": None,
                        "warehouse_id": "warehouse1",
                    },
                    "user2",
                    "select 1",
                    "SELECT",
                    None,
                    None,
                    None,
                    datetime.strptime(
                        "2024-01-25 23:07:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 23:07:07.260", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 23:07:07.550", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    90,
                    0,
                    10,
                    20,
                    30,
                    40,
                    50,
                    60,
                    70,
                    80,
                    90,
                    100,
                    110,
                    120,
                    130,
                    140,
                    150,
                    None,
                ),
            ],
            system_query_history_schema,
        ),
    )

    list_prices_df = CostCalculatorIO.prepare_list_prices(
        spark_session.createDataFrame(
            [
                (
                    "account1",
                    "AZURE",
                    "PREMIUM_SQL_PRO_COMPUTE",
                    "EUR",
                    "DBU",
                    {"default": Decimal(0.5)},
                    datetime.strptime("2024-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"),
                    None,
                )
            ],
            list_prices_schema,
        )
    )

    billing_df = CostCalculatorIO.prepare_billing(
        spark_session.createDataFrame(
            [
                (
                    "1",
                    "account1",
                    "workspace1",
                    "PREMIUM_SQL_PRO_COMPUTE",
                    "AZURE",
                    datetime.strptime(
                        "2024-01-25 01:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 23:06:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime("2024-01-25", "%Y-%m-%d"),
                    None,
                    "DBU",
                    Decimal(100.0),
                    {"warehouse_id": "warehouse1"},
                )
            ],
            system_billing_usage_schema,
        )
    )

    cloud_infra_cost_df = CostCalculatorIO.prepare_cloud_infra_cost(
        spark_session.createDataFrame([], system_cloud_infra_costs_schema)
    )

    expected_cost_agg_day_df = spark_session.createDataFrame(
        [
            (
                "account1",
                "workspace1",
                "AZURE",
                datetime.strptime("2024-01-25", "%Y-%m-%d"),
                "warehouse1",
                "user1@databricks.com",
                "user1",
                Decimal(10.00),
                Decimal(10.00),
                Decimal(5.00),
                None,
                "EUR",
            ),
            (
                "account1",
                "workspace1",
                "AZURE",
                datetime.strptime("2024-01-25", "%Y-%m-%d"),
                "warehouse1",
                "user2@databricks.com",
                "user2",
                Decimal(90.00),
                Decimal(90.00),
                Decimal(45.00),
                None,
                "EUR",
            ),
        ],
        cost_agg_day_schema,
    )

    cost_agg_day_df = CostCalculator().calculate_cost_agg_day(
        weights, queries_df, list_prices_df, billing_df, cloud_infra_cost_df
    )
    cost_agg_day_df.show()
    assert_df_equality(
        cost_agg_day_df,
        expected_cost_agg_day_df,
        ignore_nullable=True,
        ignore_column_order=True,
        ignore_row_order=True,
    )
