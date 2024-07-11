from cost_monitoring.cost_monitoring import *
from chispa.dataframe_comparer import *
from pyspark.sql import SparkSession
from decimal import *
from datetime import datetime
from schema_definitions import *


def test_calculate_daily_costs(spark_session: SparkSession):  # using pytest-spark
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
                # Test case 6: no billing for user
                (
                    "account1",
                    "workspace1",
                    "statement1",
                    "NO_BILLING_USER",
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
                # Test case 7: no billing for account
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
                # Test case 8: no billing for workspace
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
                # Test case 9: no billing for warehouse
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
                # Test case 10: no billing for specified date
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
            ],
            system_query_history_schema,
        )
    )

    weights = {"total_duration_ms": 0.5, "total_task_duration_ms": 0.5}

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

    users_df = spark_session.createDataFrame(
        [
            ("user1@databricks.com", "user1", "Alice Smith", "PS", "1234"),
            ("user2@databricks.com", "user2", "Marcin Wojtyczka", "PS", "A31"),
        ],
        user_info_schema,
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

    expected_user_costs_day_df = spark_session.createDataFrame(
        [
            # Test case 1: multiple queries from 1 user
            (
                "account1",
                "workspace1",
                "AZURE",
                datetime.strptime("2024-01-24", "%Y-%m-%d"),
                "warehouse1",
                "user1@databricks.com",
                Decimal(100.00),
                Decimal(100.00),
                Decimal(50.00),
                Decimal(80.00),
                "EUR",
                100,
                500,
                6,
                8,
                10,
                12,
                14,
                16,
                18,
                20,
                22,
                26,
                28,
                30,
            ),
            # Test case 2: queries from 2 users
            (
                "account1",
                "workspace1",
                "AZURE",
                datetime.strptime("2024-01-25", "%Y-%m-%d"),
                "warehouse1",
                "user1@databricks.com",
                Decimal(50.00),
                Decimal(50.00),
                Decimal(25.00),
                Decimal(30.00),
                "EUR",
                50,
                250,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                13,
                14,
                15,
            ),
            (
                "account1",
                "workspace1",
                "AZURE",
                datetime.strptime("2024-01-25", "%Y-%m-%d"),
                "warehouse1",
                "user2@databricks.com",
                Decimal(50.00),
                Decimal(50.00),
                Decimal(25.00),
                Decimal(30.00),
                "EUR",
                50,
                250,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                13,
                14,
                155,
            ),
            # Test case 3: different warehouse
            (
                "account1",
                "workspace1",
                "AZURE",
                datetime.strptime("2024-01-25", "%Y-%m-%d"),
                "warehouse2",
                "user1@databricks.com",
                Decimal(100.00),
                Decimal(100.00),
                Decimal(50.00),
                Decimal(60.00),
                "EUR",
                50,
                250,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                13,
                14,
                15,
            ),
            # Test case 4: different workspace
            (
                "account1",
                "workspace2",
                "AZURE",
                datetime.strptime("2024-01-25", "%Y-%m-%d"),
                "warehouse1",
                "user1@databricks.com",
                Decimal(100.00),
                Decimal(100.00),
                Decimal(50.00),
                Decimal(60.00),
                "EUR",
                50,
                250,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                13,
                14,
                15,
            ),
            # Test case 5: different account
            (
                "account2",
                "workspace1",
                "AZURE",
                datetime.strptime("2024-01-25", "%Y-%m-%d"),
                "warehouse1",
                "user1@databricks.com",
                Decimal(100.00),
                Decimal(100.00),
                Decimal(100.00),
                Decimal(60.00),
                "EUR",
                50,
                250,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                13,
                14,
                15,
            ),
        ],
        user_costs_day_schema,
    )

    user_costs_day_df = CostCalculator().calculate_daily_user_cost(
        queries_df, weights, list_prices_df, users_df, billing_df, cloud_infra_cost_df
    )

    assert_df_equality(
        user_costs_day_df,
        expected_user_costs_day_df,
        ignore_nullable=True,
        ignore_column_order=True,
    )


def test_calculate_monthly_costs(spark_session: SparkSession):  # using pytest-spark
    user_costs_day_df = spark_session.createDataFrame(
        [
            # Test case 1: multiple days for 1 user within a month
            (
                "account1",
                "workspace1",
                "AZURE",
                datetime.strptime("2024-01-24", "%Y-%m-%d"),
                "warehouse1",
                "user1@databricks.com",
                Decimal(100.00),
                Decimal(100.00),
                Decimal(50.00),
                Decimal(60.00),
                "EUR",
                100,
                500,
                6,
                8,
                10,
                12,
                14,
                16,
                18,
                20,
                22,
                26,
                28,
                30,
            ),
            (
                "account1",
                "workspace1",
                "AZURE",
                datetime.strptime("2024-01-25", "%Y-%m-%d"),
                "warehouse1",
                "user1@databricks.com",
                Decimal(100.00),
                Decimal(100.00),
                Decimal(50.00),
                Decimal(60.00),
                "EUR",
                100,
                500,
                6,
                8,
                10,
                12,
                14,
                16,
                18,
                20,
                22,
                26,
                28,
                30,
            ),
            # Test case 2: different user and multiple warehouses
            (
                "account1",
                "workspace1",
                "AZURE",
                datetime.strptime("2024-02-25", "%Y-%m-%d"),
                "warehouse1",
                "user2@databricks.com",
                Decimal(100.00),
                Decimal(50.00),
                Decimal(25.00),
                Decimal(30.00),
                "EUR",
                50,
                250,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                13,
                14,
                15,
            ),
            (
                "account1",
                "workspace1",
                "AZURE",
                datetime.strptime("2024-02-25", "%Y-%m-%d"),
                "warehouse2",
                "user2@databricks.com",
                Decimal(100.00),
                Decimal(50.00),
                Decimal(25.00),
                Decimal(30.00),
                "EUR",
                50,
                250,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                13,
                14,
                15,
            ),
            # Test case 3: different workspace
            (
                "account1",
                "workspace2",
                "AZURE",
                datetime.strptime("2024-01-25", "%Y-%m-%d"),
                "warehouse1",
                "user1@databricks.com",
                Decimal(100.00),
                Decimal(100.00),
                Decimal(50.00),
                Decimal(60.00),
                "EUR",
                50,
                250,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                13,
                14,
                15,
            ),
            # Test case 4: different account
            (
                "account2",
                "workspace1",
                "AZURE",
                datetime.strptime("2024-01-25", "%Y-%m-%d"),
                "warehouse1",
                "user1@databricks.com",
                Decimal(100.00),
                Decimal(100.00),
                Decimal(100.00),
                Decimal(100.00),
                "EUR",
                50,
                250,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                13,
                14,
                15,
            ),
            # Test case 5: multiple users, same account, workspace and month
            (
                "account1",
                "workspace1",
                "AZURE",
                datetime.strptime("2024-03-24", "%Y-%m-%d"),
                "warehouse1",
                "user1@databricks.com",
                Decimal(100.00),
                Decimal(100.00),
                Decimal(50.00),
                Decimal(60.00),
                "EUR",
                100,
                500,
                6,
                8,
                10,
                12,
                14,
                16,
                18,
                20,
                22,
                26,
                28,
                30,
            ),
            (
                "account1",
                "workspace1",
                "AZURE",
                datetime.strptime("2024-03-25", "%Y-%m-%d"),
                "warehouse1",
                "user2@databricks.com",
                Decimal(100.00),
                Decimal(100.00),
                Decimal(50.00),
                Decimal(60.00),
                "EUR",
                100,
                500,
                6,
                8,
                10,
                12,
                14,
                16,
                18,
                20,
                22,
                26,
                28,
                30,
            ),
        ],
        user_costs_day_schema,
    )

    user_costs_month_df = CostCalculator().calculate_monthly_user_cost(
        user_costs_day_df
    )

    expected_user_costs_month_df = spark_session.createDataFrame(
        [
            # Test case 1: multiple days for 1 user
            (
                "account1",
                "workspace1",
                "AZURE",
                2024,
                1,
                datetime.strptime("2024-01-01", "%Y-%m-%d"),
                "user1@databricks.com",
                Decimal(100.00),
                Decimal(200.00),
                Decimal(100.00),
                Decimal(120.00),
                "EUR",
            ),
            # Test case 2: different user and multiple warehouses
            (
                "account1",
                "workspace1",
                "AZURE",
                2024,
                2,
                datetime.strptime("2024-02-01", "%Y-%m-%d"),
                "user2@databricks.com",
                Decimal(100.00),
                Decimal(100.00),
                Decimal(50.00),
                Decimal(60.00),
                "EUR",
            ),
            # Test case 3: different workspace
            (
                "account1",
                "workspace2",
                "AZURE",
                2024,
                1,
                datetime.strptime("2024-01-01", "%Y-%m-%d"),
                "user1@databricks.com",
                Decimal(100.00),
                Decimal(100.00),
                Decimal(50.00),
                Decimal(60.00),
                "EUR",
            ),
            # Test case 4: different account
            (
                "account2",
                "workspace1",
                "AZURE",
                2024,
                1,
                datetime.strptime("2024-01-01", "%Y-%m-%d"),
                "user1@databricks.com",
                Decimal(100.00),
                Decimal(100.00),
                Decimal(100.00),
                Decimal(100.00),
                "EUR",
            ),
            # Test case 5: multiple users, same account, workspace and month
            (
                "account1",
                "workspace1",
                "AZURE",
                2024,
                3,
                datetime.strptime("2024-03-01", "%Y-%m-%d"),
                "user1@databricks.com",
                Decimal(50.00),
                Decimal(100.00),
                Decimal(50.00),
                Decimal(60.00),
                "EUR",
            ),
            (
                "account1",
                "workspace1",
                "AZURE",
                2024,
                3,
                datetime.strptime("2024-03-01", "%Y-%m-%d"),
                "user2@databricks.com",
                Decimal(50.00),
                Decimal(100.00),
                Decimal(50.00),
                Decimal(60.00),
                "EUR",
            ),
        ],
        user_costs_month_schema,
    )

    assert_df_equality(
        user_costs_month_df,
        expected_user_costs_month_df,
        ignore_nullable=True,
        ignore_column_order=True,
        ignore_row_order=True,
    )


def test_calculate_daily_costs_missing_cloud_infra_cost(
    spark_session: SparkSession,
):  # using pytest-spark
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
                    "user1",
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
                    "user3@databricks.com",
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
                        "2024-01-25 23:07:06.944", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 23:07:07.260", "%Y-%m-%d %H:%M:%S.%f"
                    ),
                    datetime.strptime(
                        "2024-01-25 23:07:07.550", "%Y-%m-%d %H:%M:%S.%f"
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
        )
    )

    weights = {"total_duration_ms": 1.0}

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

    users_df = spark_session.createDataFrame(
        [
            ("user1@databricks.com", "user1", "Alice Smith", "PS", "1234"),
            ("user2@databricks.com", "user2", "Marcin Wojtyczka", "PS", "A31"),
            ("user3@databricks.com", "user3", "Marcin Wojtyczka 2", "PS", "A31"),
        ],
        user_info_schema,
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

    expected_user_costs_day_df = spark_session.createDataFrame(
        [
            (
                "account1",
                "workspace1",
                "AZURE",
                datetime.strptime("2024-01-25", "%Y-%m-%d"),
                "warehouse1",
                "user1@databricks.com",
                Decimal(100.00),
                Decimal(80.00),
                Decimal(40.00),
                Decimal(90.00),
                "EUR",
                50,
                250,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                13,
                14,
                15,
            ),
            # contribution should be 0 since the metric in question is 0
            (
                "account1",
                "workspace1",
                "AZURE",
                datetime.strptime("2024-01-25", "%Y-%m-%d"),
                "warehouse1",
                "user2@databricks.com",
                Decimal(0.00),
                Decimal(0.00),
                Decimal(0.00),
                Decimal(0.00),
                "EUR",
                0,
                2500,
                30,
                40,
                50,
                60,
                70,
                80,
                90,
                100,
                110,
                130,
                140,
                150,
            ),
            # contribution should be 0 since the metric in question is Null
            (
                "account1",
                "workspace1",
                "AZURE",
                datetime.strptime("2024-01-25", "%Y-%m-%d"),
                "warehouse1",
                "user3@databricks.com",
                Decimal(0.00),
                Decimal(0.00),
                Decimal(0.00),
                Decimal(0.00),
                "EUR",
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
            (
                "account1",
                "workspace1",
                "AZURE",
                datetime.strptime("2024-01-26", "%Y-%m-%d"),
                "warehouse1",
                "user1@databricks.com",
                Decimal(100.00),
                Decimal(90.00),
                Decimal(45.00),
                None,
                "EUR",
                5000,
                25000,
                300,
                400,
                500,
                600,
                700,
                800,
                900,
                1000,
                1100,
                1300,
                1400,
                1500,
            ),
        ],
        user_costs_day_schema,
    )

    user_costs_day_df = CostCalculator().calculate_daily_user_cost(
        queries_df, weights, list_prices_df, users_df, billing_df, cloud_infra_cost_df
    )

    user_costs_day_df.show(1000, False)

    assert_df_equality(
        user_costs_day_df,
        expected_user_costs_day_df,
        ignore_nullable=True,
        ignore_column_order=True,
        ignore_row_order=True,
    )
