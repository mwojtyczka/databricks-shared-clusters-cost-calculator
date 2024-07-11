from pyspark.sql.functions import (
    col,
    explode,
    sequence,
    date_format,
    expr,
    when,
    year,
    month,
    sum,
    to_date,
    lit,
    round,
    concat,
    lpad,
)
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    DateType,
)
from datetime import datetime, date
from pyspark.sql import Row
from pyspark.sql.functions import max
from pyspark.sql.window import Window
from functools import reduce
from operator import add


class CostCalculatorIO(object):
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_checkpoint(self, table: str):
        df = self.spark.table(table)
        return self.get_max_date_col(df, "last_processed_date")

    def get_max_date_col(self, df, column: str) -> date | None:
        if df.count() > 0:
            df = df.withColumn(column + "_new", to_date(col(column)))
            date_str = df.agg({column + "_new": "max"}).collect()[0][0]
            df = df.drop(column + "_new")
            return datetime.strptime(str(date_str), "%Y-%m-%d").date()
        else:
            return None

    def save_checkpoint(self, table: str, new_checkpoint_date: date):
        schema = StructType(
            [StructField("last_processed_date", DateType(), nullable=False)]
        )

        if new_checkpoint_date is None:
            new_checkpoint_date = datetime.now()

        df = self.spark.createDataFrame(
            [Row(last_processed_date=new_checkpoint_date)], schema
        )
        df.write.mode("overwrite").saveAsTable(table)

    def save_user_costs(
        self, user_costs_df, table: str, last_checkpoint_date: datetime
    ):
        if last_checkpoint_date:
            self.spark.sql(
                f"DELETE FROM {table} WHERE billing_date > '{last_checkpoint_date}'"
            )  # useful for reprocessing, just need to reset checkpoint

        user_costs_df.write.mode("append").saveAsTable(table)

    def read_query_history(
        self,
        table: str,
        last_checkpoint_date: datetime = None,
        current_date: datetime = datetime.now(),
    ):
        queries = self.spark.table(table)
        return self.prepare_query_history(queries, last_checkpoint_date, current_date)

    def read_user_info(self, table: str):
        return self.spark.table(table)

    def read_billing(self, table: str, last_checkpoint_date: datetime = None):
        df = self.spark.table(table)
        return self.prepare_billing(df, last_checkpoint_date)

    def get_weights(self):
        return {
            "total_task_duration_ms": 0.6,
            "execution_duration_ms": 0.28,
            "compilation_duration_ms": 0.02,
            "read_files": 0.02,
            "read_bytes": 0.02,
            "read_rows": 0.02,
            "produced_rows": 0.02,
            "written_bytes": 0.02,
        }

    def read_list_prices(self, table: str):
        df = self.spark.table(table)
        return self.prepare_list_prices(df)

    def read_cloud_infra_cost(self, table: str):
        df = self.spark.table(table)
        return self.prepare_cloud_infra_cost(df)

    @staticmethod
    def prepare_query_history(
        queries,
        last_checkpoint_date: datetime = None,
        current_date: datetime = datetime.now(),
    ):
        queries = queries.filter(col("end_time") < current_date)

        if last_checkpoint_date:
            queries = queries.filter(col("end_time") > last_checkpoint_date)

        # TODO if a query spans 2 days, we will attribute the cost to the end date only
        #  although DBUs are consumed from day 1 and 2
        # "cloud" col is currently not part of the table
        return (
            queries.withColumn("cloud", lit("AZURE"))
            .withColumn("billing_date", to_date(col("end_time"), "yyyy-MM-dd"))
            .withColumn("warehouse_id", col("compute.warehouse_id"))
            .drop("compute")
        )

    @staticmethod
    def prepare_list_prices(df, current_date: datetime = datetime.now().date()):
        df = df.withColumn(
            "price_end_time",
            when(col("price_end_time").isNull(), to_date(lit(current_date))).otherwise(
                col("price_end_time")
            ),
        ).withColumn("pricing", col("pricing.default"))

        # Generate daily list prices
        daily_df = (
            df.withColumn(
                "dates",
                sequence(
                    col("price_start_time"),
                    col("price_end_time"),
                    expr("interval 1 day"),
                ),
            )
            .select(
                "account_id",
                "cloud",
                "sku_name",
                "currency_code",
                "usage_unit",
                "pricing",
                explode("dates").alias("date"),
            )
            .withColumn(
                "billing_date", date_format(col("date"), "yyyy-MM-dd").cast(DateType())
            )
            .drop("date")
        )
        return daily_df

    @staticmethod
    def prepare_billing(df, last_checkpoint_date: datetime = None):
        df = df.filter(
            "usage_metadata.warehouse_id is not null"
        )  # only interested in sql warehouses

        if last_checkpoint_date:
            df = df.filter(col("usage_date") > last_checkpoint_date)

        df = df.withColumn("warehouse_id", col("usage_metadata.warehouse_id"))

        df = (
            df.groupBy(
                "account_id",
                "warehouse_id",
                "workspace_id",
                "sku_name",
                "cloud",
                "usage_unit",
                "usage_date",
            )
            .agg({"usage_quantity": "sum"})
            .withColumnRenamed("sum(usage_quantity)", "usage_quantity")
            .withColumnRenamed("usage_date", "billing_date")
        )

        return df.withColumn(
            "usage_quantity", col("usage_quantity").cast("decimal(20, 4)")
        )

    @staticmethod
    def prepare_cloud_infra_cost(df, last_checkpoint_date: datetime = None):
        df = df.filter(
            "usage_metadata.warehouse_id is not null"
        )  # only interested in sql warehouses

        if last_checkpoint_date:
            df = df.filter(col("usage_date") > last_checkpoint_date)

        df = df.withColumn("warehouse_id", col("usage_metadata.warehouse_id"))

        df = (
            df.groupBy(
                "account_id",
                "warehouse_id",
                "workspace_id",
                "cloud",
                "usage_date",
                "currency_code",
            )
            .agg({"cost": "sum"})
            .withColumnRenamed("sum(cost)", "cost")
            .withColumnRenamed("usage_date", "billing_date")
        )

        return df.withColumn("cost", col("cost").cast("decimal(38,2)"))


class CostCalculator(object):
    def __init__(self):
        self.query_metrics = [
            "total_duration_ms",
            "execution_duration_ms",
            "compilation_duration_ms",
            "total_task_duration_ms",
            "result_fetch_duration_ms",
            "read_partitions",
            "pruned_files",
            "read_files",
            "read_rows",
            "produced_rows",
            "read_bytes",
            "spilled_local_bytes",
            "written_bytes",
            "shuffle_read_bytes",
        ]

    def normalize_metrics(self, queries_df, users_df):
        # Join with users_df
        user_queries_df = queries_df.join(
            users_df, queries_df.executed_by == users_df.user_name, "inner"
        )

        # Define window specification
        window_spec = Window.partitionBy(
            "account_id",
            "workspace_id",
            "warehouse_id",
            "cloud",
            "user_name",
            "billing_date",
        )

        # Calculate max values using window functions
        max_values = {
            col_to_norm: max(col_to_norm).over(window_spec).alias(f"max_{col_to_norm}")
            for col_to_norm in self.query_metrics
        }

        # Apply normalization
        normalized_df = user_queries_df
        for norm_col, max_col in max_values.items():
            normalized_df = normalized_df.withColumn(
                f"{norm_col}_norm", col(norm_col) / max_col
            )

        return normalized_df

    def calculate_weighted_sum(self, normalized_queries_df, weights):
        # add suffix to each column
        suffix = "_"
        weights = {f"{key}{suffix}": value for key, value in weights.items()}

        # Multiply each metric by its weight
        queries_and_weights_df = normalized_queries_df
        for column in weights.keys():
            queries_and_weights_df = queries_and_weights_df.withColumn(
                column,  # use col with suffix to preserve original metrics
                queries_and_weights_df[column.rstrip(suffix) + "_norm"]
                * lit(weights.get(column)),
            )

        # sum up weighted metrics
        queries_and_weights_df = queries_and_weights_df.withColumn(
            "contribution",
            reduce(
                add,
                [
                    when(col(x).isNotNull(), col(x)).otherwise(lit(0))
                    for x in weights.keys()  # use cols with suffix
                ],
            ),
        )

        return queries_and_weights_df

    def calculate_normalized_contribution(self, weigthed_sum_df):
        columns_to_sum = ["contribution"] + self.query_metrics

        contributions_df = weigthed_sum_df.groupBy(
            "user_name",
            "cloud",
            "billing_date",
            "account_id",
            "warehouse_id",
            "workspace_id",
        ).agg(*[sum(col_to_sum).alias(col_to_sum) for col_to_sum in columns_to_sum])

        # Calculate the total sum of contributions for each account_id and workspace_id
        total_contributions_df = weigthed_sum_df.groupBy(
            "cloud", "billing_date", "account_id", "warehouse_id", "workspace_id"
        ).agg(sum("contribution").alias("total_contribution"))

        # Normalize contributions
        normalized_df = (
            contributions_df.join(
                total_contributions_df,
                ["cloud", "billing_date", "account_id", "warehouse_id", "workspace_id"],
            )
            .withColumn(
                "normalized_contribution",
                (col("contribution") * 100 / col("total_contribution")).cast(
                    "decimal(17, 14)"
                ),
            )
            .drop("total_contribution")
        )

        return normalized_df

    @staticmethod
    def enrich_billing_with_pricing(billing_df, list_prices_df):
        list_prices_df = list_prices_df.where((col("usage_unit") == lit("DBU"))).drop(
            "usage_unit"
        )

        billing_df = billing_df.where(col("usage_unit") == lit("DBU"))

        billing_pricing_df = billing_df.join(
            list_prices_df, on=["account_id", "cloud", "billing_date", "sku_name"]
        ).withColumn("usage_quantity_cost", col("pricing") * col("usage_quantity"))
        return billing_pricing_df

    @staticmethod
    def calculate_dbu_consumption(contribution_df, billing_pricing_df):
        dbu_df = (
            contribution_df.join(
                billing_pricing_df,
                on=[
                    "account_id",
                    "workspace_id",
                    "cloud",
                    "billing_date",
                    "warehouse_id",
                ],
                how="inner",
            )
            .withColumn(
                "dbu",
                round(
                    col("normalized_contribution") * col("usage_quantity") / 100, 2
                ).cast("decimal(38,2)"),
            )
            .withColumn(
                "dbu_cost",
                round(
                    col("normalized_contribution") * col("usage_quantity_cost") / 100, 2
                ).cast("decimal(38,2)"),
            )
            .withColumnRenamed("normalized_contribution", "dbu_contribution_percent")
            .drop(
                "sku_name",
                "usage_quantity",
                "usage_quantity_cost",
                "usage_unit",
                "contribution",
                "usage_unit",
                "pricing",
            )
        )

        return dbu_df

    @staticmethod
    def enrich_with_cloud_infra_cost(dbu_df, cloud_infra_cost_df):
        cost_df = (
            dbu_df.join(
                cloud_infra_cost_df,
                on=[
                    "account_id",
                    "cloud",
                    "billing_date",
                    "warehouse_id",
                    "workspace_id",
                    "currency_code",
                ],
                how="left",
            )
            .withColumn(
                "cloud_cost",
                round(col("cost") * col("dbu_contribution_percent") / 100, 2).cast(
                    "decimal(38,2)"
                ),
            )
            .drop("cost")
        )

        return cost_df

    def calculate_daily_user_cost(
        self,
        queries_df,
        weights,
        list_prices_df,
        users_df,
        billing_df,
        cloud_infra_cost_df,
    ):
        normalized_queries_df = self.normalize_metrics(queries_df, users_df)
        weigthed_sum_df = self.calculate_weighted_sum(normalized_queries_df, weights)
        contribution_df = self.calculate_normalized_contribution(weigthed_sum_df)
        billing_pricing_df = self.enrich_billing_with_pricing(
            billing_df, list_prices_df
        )
        dbu_df = self.calculate_dbu_consumption(contribution_df, billing_pricing_df)
        costs_all_df = self.enrich_with_cloud_infra_cost(dbu_df, cloud_infra_cost_df)

        return costs_all_df

    def calculate_monthly_user_cost(self, daily_user_cost_df):
        df = daily_user_cost_df.withColumn(
            "billing_year", year(col("billing_date"))
        ).withColumn("billing_month", month(col("billing_date")))

        agg_df = df.groupBy(
            "user_name",
            "billing_year",
            "billing_month",
            "cloud",
            "account_id",
            "workspace_id",
            "currency_code",
        ).agg(
            sum("dbu").alias("dbu"),
            sum("dbu_cost").alias("dbu_cost"),
            sum("cloud_cost").alias("cloud_cost"),
        )

        total_dbu_df = agg_df.groupBy(
            "billing_year",
            "billing_month",
            "cloud",
            "account_id",
            "workspace_id",
            "currency_code",
        ).agg(sum("dbu").alias("total_dbu"))

        agg_df = (
            agg_df.join(
                total_dbu_df,
                [
                    "billing_year",
                    "billing_month",
                    "cloud",
                    "account_id",
                    "workspace_id",
                    "currency_code",
                ],
            )
            .withColumn(
                "dbu_contribution_percent",
                round(col("dbu") * 100 / col("total_dbu"), 2).cast("decimal(17, 14)"),
            )
            .drop("total_dbu")
        )

        agg_df = agg_df.withColumn(
            "billing_date",
            to_date(
                concat(
                    col("billing_year"),
                    lit("-"),
                    lpad(col("billing_month"), 2, "0"),
                    lit("-01"),
                ),
                "yyyy-MM-dd",
            ).cast(DateType()),
        )
        return agg_df
