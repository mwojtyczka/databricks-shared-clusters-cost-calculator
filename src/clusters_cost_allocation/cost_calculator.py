from datetime import datetime, date
from functools import reduce
from operator import add
from collections.abc import KeysView

from pyspark.sql import Row
from pyspark.sql.window import Window

from pyspark.sql.functions import (
    col,
    explode,
    sequence,
    date_format,
    expr,
    when,
    to_date,
    lit,
)

from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import max as spark_max

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    DateType,
)


class CostCalculatorIO:
    def __init__(self, spark: SparkSession, catalog_and_schema: str):
        self.spark = spark
        self.catalog_and_schema = catalog_and_schema

    def read_checkpoint(self, table: str) -> date | None:
        full_table = self._construct_full_table(table)
        print(f"Reading checkpoint from `{full_table}`")

        df = self.spark.table(full_table)
        return self.get_max_date(df, "last_processed_date")

    def save_checkpoint(self, table: str, new_checkpoint: date) -> None:
        full_table = self._construct_full_table(table)

        schema = StructType(
            [StructField("last_processed_date", DateType(), nullable=False)]
        )

        if new_checkpoint is None:
            new_checkpoint = datetime.now().date()

        df = self.spark.createDataFrame(
            [Row(last_processed_date=new_checkpoint)], schema
        )
        df.write.mode("overwrite").saveAsTable(full_table)

        print(f"Saved checkpoint to `{full_table}` as {new_checkpoint}")

    def save_costs(self, costs_df, table: str, last_checkpoint: date) -> None:
        full_table = self._construct_full_table(table)

        if last_checkpoint:
            self.spark.sql(
                f"DELETE FROM {full_table} WHERE billing_date > '{last_checkpoint}'"
            )  # useful for reprocessing, just need to reset checkpoint

        costs_df.write.mode("append").saveAsTable(full_table)

        print(f"Saved cost calculation to `{full_table}`")

    def _construct_full_table(self, table: str) -> str:
        return self.catalog_and_schema + "." + table

    def read_query_history(
        self,
        table: str,
        last_checkpoint: date = None,
        current_date: date = datetime.now().date(),
    ):
        print(f"Reading query history from `{table}`")
        queries = self.spark.table(table)
        return self.prepare_query_history(queries, last_checkpoint, current_date)

    def read_billing(self, table: str, last_checkpoint: datetime = None):
        print(f"Reading billing from `{table}`")
        df = self.spark.table(table)
        return self.prepare_billing(df, last_checkpoint)

    def read_list_prices(self, table: str):
        print(f"Reading list prices from `{table}`")
        df = self.spark.table(table)
        return self.prepare_list_prices(df)

    def read_cloud_infra_cost(self, table: str, last_checkpoint: datetime = None):
        print(f"Reading cloud infra cost from `{table}`")
        df = self.spark.table(table)
        return self.prepare_cloud_infra_cost(df, last_checkpoint)

    @staticmethod
    def get_max_date(df, column: str) -> date | None:
        if df.count() > 0:
            df = df.withColumn(column + "_new", to_date(col(column)))
            date_str = df.agg({column + "_new": "max"}).collect()[0][0]
            return datetime.strptime(str(date_str), "%Y-%m-%d").date()
        return None

    @staticmethod
    def prepare_query_history(
        queries_df,
        last_checkpoint: date = None,
        current_date: date = datetime.now().date(),
    ):
        queries_df = (
            # if a query spans 2 days, the cost is attributed to the end date only
            # splitting is not implemented as it would be very computational intense
            queries_df.withColumn(
                "billing_date", to_date(col("end_time"), "yyyy-MM-dd")
            )
            .withColumn("warehouse_id", col("compute.warehouse_id"))
            .drop("compute")
        )

        print(f"Filtering query history to get results before {current_date}")
        queries_df = queries_df.filter(col("billing_date") < current_date)

        if last_checkpoint:
            print(
                f"Filtering query history using checkpoint to get results after {last_checkpoint}"
            )
            queries_df = queries_df.filter(col("billing_date") > last_checkpoint)

        return queries_df

    @staticmethod
    def prepare_list_prices(
        list_prices_df, current_date: datetime = datetime.now().date()
    ):
        list_prices_df = list_prices_df.withColumn(
            "price_end_time",
            when(col("price_end_time").isNull(), to_date(lit(current_date))).otherwise(
                col("price_end_time")
            ),
        ).withColumn("pricing", col("pricing.default"))

        # Generate daily list prices
        daily_df = (
            list_prices_df.withColumn(
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
    def prepare_billing(billing_df, last_checkpoint: date = None):
        billing_df = billing_df.filter(
            "usage_metadata.warehouse_id is not null"
        )  # limit results to sql warehouses only

        if last_checkpoint:
            billing_df = billing_df.filter(col("usage_date") > last_checkpoint)

        billing_df = billing_df.withColumn(
            "warehouse_id", col("usage_metadata.warehouse_id")
        )

        billing_df = (
            billing_df.groupBy(
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

        return billing_df.withColumn(
            "usage_quantity", col("usage_quantity").cast("decimal(20, 4)")
        )

    @staticmethod
    def prepare_cloud_infra_cost(cloud_infra_cost_df, last_checkpoint: date = None):
        cloud_infra_cost_df = cloud_infra_cost_df.filter(
            "usage_metadata.warehouse_id is not null"
        )  # limit results to sql warehouses only

        if last_checkpoint:
            cloud_infra_cost_df = cloud_infra_cost_df.filter(
                col("usage_date") > last_checkpoint
            )

        cloud_infra_cost_df = cloud_infra_cost_df.withColumn(
            "warehouse_id", col("usage_metadata.warehouse_id")
        )

        cloud_infra_cost_df = (
            cloud_infra_cost_df.groupBy(
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

        return cloud_infra_cost_df.withColumn("cost", col("cost").cast("decimal(38,2)"))


class CostCalculator:

    def calculate_cost_agg_day(
        self,
        metric_to_weight_map: dict[str, float],
        queries_df,
        list_prices_df,
        billing_df,
        cloud_infra_cost_df,
    ):
        print("Calculating cost agg day ...")

        normalized_queries_df = self._normalize_metrics(
            queries_df, metric_to_weight_map.keys()
        )
        weigthed_sum_df = self._calculate_weighted_sum(
            normalized_queries_df, metric_to_weight_map
        )
        contribution_df = self._calculate_normalized_contribution(weigthed_sum_df)
        billing_pricing_df = self._enrich_with_list_prices(billing_df, list_prices_df)
        dbu_df = self._calculate_dbu_consumption(contribution_df, billing_pricing_df)
        costs_all_df = self._enrich_with_cloud_infra_cost(dbu_df, cloud_infra_cost_df)

        return costs_all_df

    @staticmethod
    def _normalize_metrics(queries_df, metrics: KeysView[str]):
        queries_df = queries_df.withColumnRenamed("executed_by", "user_name")

        window_spec = Window.partitionBy(
            "account_id",
            "workspace_id",
            "warehouse_id",
            "user_name",
            "billing_date",
        )

        # Calculate max values for each metric
        max_values = {
            col_to_norm: spark_max(col_to_norm)
            .over(window_spec)
            .alias(f"max_{col_to_norm}")
            for col_to_norm in metrics
        }

        # Apply normalization
        normalized_df = queries_df
        for norm_col, max_col in max_values.items():
            normalized_df = normalized_df.withColumn(norm_col, col(norm_col) / max_col)

        return normalized_df

    @staticmethod
    def _calculate_weighted_sum(
        normalized_queries_df, metric_to_weight_map: dict[str, float]
    ):
        # Multiply each metric by its weight
        queries_and_weights_df = normalized_queries_df
        for norm_col in metric_to_weight_map.keys():
            queries_and_weights_df = queries_and_weights_df.withColumn(
                norm_col,
                queries_and_weights_df[norm_col]
                * lit(metric_to_weight_map.get(norm_col)),
            )

        # sum up weighted metrics
        queries_and_weights_df = queries_and_weights_df.withColumn(
            "contribution",
            reduce(
                add,
                [
                    when(col(x).isNotNull(), col(x)).otherwise(lit(0))
                    for x in metric_to_weight_map.keys()  # use cols with suffix
                ],
            ),
        )

        return queries_and_weights_df

    @staticmethod
    def _calculate_normalized_contribution(weighted_sum_df):
        # Calculate the total sum of contributions for each user
        user_contributions_df = weighted_sum_df.groupBy(
            "user_name",
            "billing_date",
            "account_id",
            "warehouse_id",
            "workspace_id",
        ).agg(spark_sum("contribution").alias("contribution"))

        # Calculate the total sum of contributions across all users
        total_contributions_df = weighted_sum_df.groupBy(
            "billing_date", "account_id", "warehouse_id", "workspace_id"
        ).agg(spark_sum("contribution").alias("total_contribution"))

        # Normalize contributions
        normalized_df = (
            user_contributions_df.join(
                total_contributions_df,
                ["billing_date", "account_id", "warehouse_id", "workspace_id"],
            )
            .withColumn(
                "normalized_contribution",
                when(
                    col("total_contribution") != 0,
                    (col("contribution") * 100 / col("total_contribution")).cast(
                        "decimal(17, 14)"
                    ),
                ).otherwise(lit(0).cast("decimal(17, 14)")),
            )
            .drop("total_contribution")
        )

        return normalized_df

    @staticmethod
    def _enrich_with_list_prices(billing_df, list_prices_df):
        list_prices_df = list_prices_df.where((col("usage_unit") == lit("DBU"))).drop(
            "usage_unit"
        )

        billing_df = billing_df.where(col("usage_unit") == lit("DBU"))

        billing_pricing_df = billing_df.join(
            list_prices_df, on=["cloud", "account_id", "billing_date", "sku_name"]
        ).withColumn("usage_quantity_cost", col("pricing") * col("usage_quantity"))
        return billing_pricing_df

    @staticmethod
    def _calculate_dbu_consumption(contribution_df, billing_pricing_df):
        dbu_df = (
            contribution_df.join(
                billing_pricing_df,
                on=[
                    "account_id",
                    "workspace_id",
                    "billing_date",
                    "warehouse_id",
                ],
                how="inner",
            )
            .withColumn(
                "dbu",
                spark_round(
                    col("normalized_contribution") * col("usage_quantity") / 100, 2
                ).cast("decimal(38,2)"),
            )
            .withColumn(
                "dbu_cost",
                spark_round(
                    col("normalized_contribution") * col("usage_quantity_cost") / 100, 2
                ).cast("decimal(38,2)"),
            )
            .withColumnRenamed("normalized_contribution", "dbu_contribution_percent")
        )

        return dbu_df.select(
            "account_id",
            "workspace_id",
            "billing_date",
            "warehouse_id",
            "user_name",
            "dbu_contribution_percent",
            "cloud",
            "currency_code",
            "dbu",
            "dbu_cost",
        )

    @staticmethod
    def _enrich_with_cloud_infra_cost(dbu_df, cloud_infra_cost_df):
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
                spark_round(
                    col("cost") * col("dbu_contribution_percent") / 100, 2
                ).cast("decimal(38,2)"),
            )
            .drop("cost")
        )

        return cost_df
