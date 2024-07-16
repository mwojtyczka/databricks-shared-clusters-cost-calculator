import argparse
from pyspark.sql import SparkSession
from clusters_cost_allocation.cost_calculator import CostCalculatorIO, CostCalculator
from clusters_cost_allocation.metrics import get_metric_to_weight_map

spark = SparkSession.builder.getOrCreate()


def run_cost_agg_day(
    catalog: str,
    schema: str,
):
    print(f"Using output catalog: {catalog}")
    print(f"Using output schema: {schema}")

    catalog_and_schema = catalog + "." + schema
    calculator_io = CostCalculatorIO(spark, catalog_and_schema)

    last_checkpoint = calculator_io.read_checkpoint("checkpoint")
    print(f"Last checkpoint: {last_checkpoint}")

    queries_df = calculator_io.read_query_history(
        "system.query.history", last_checkpoint
    )
    list_prices_df = calculator_io.read_list_prices("system.billing.list_prices")
    billing_df = calculator_io.read_billing("system.billing.usage", last_checkpoint)
    cloud_infra_cost_df = calculator_io.read_cloud_infra_cost(
        "system.billing.cloud_infra_cost", last_checkpoint
    )
    metric_to_weight_map = get_metric_to_weight_map()

    calculator = CostCalculator()
    cost_agg_day_df = calculator.calculate_cost_agg_day(
        metric_to_weight_map,
        queries_df,
        list_prices_df,
        billing_df,
        cloud_infra_cost_df,
    )
    calculator_io.save_costs(cost_agg_day_df, "cost_agg_day", last_checkpoint)

    if cost_agg_day_df.count() == 0:
        print("No data available from daily calculation.")
        return

    new_checkpoint = calculator_io.get_max_date(cost_agg_day_df, "billing_date")
    calculator_io.save_checkpoint("checkpoint", new_checkpoint)

    print("Finished successfully")


def main(catalog: str, schema: str):
    run_cost_agg_day(catalog, schema)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--catalog", type=str, help="Catalog name for storing the results"
    )
    parser.add_argument(
        "--schema", type=str, help="Schema name for storing the results"
    )
    args = parser.parse_args()

    main(args.catalog, args.schema)
