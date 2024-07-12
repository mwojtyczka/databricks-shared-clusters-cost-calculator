import argparse
from clusters_cost_allocation.cost_calculator import *


spark = SparkSession.builder.getOrCreate()


def calculate_daily_costs(
    catalog: str,
    schema: str,
):
    catalog_and_schema = catalog + "." + schema
    print(f"using {catalog_and_schema}")

    io = CostCalculatorIO(spark)

    last_checkpoint = io.read_checkpoint(catalog_and_schema + ".checkpoint_day")
    print(f"last checkpoint for daily calculation: {last_checkpoint}")

    queries_df = io.read_query_history("system.query.history", last_checkpoint)
    weights = io.get_weights()
    list_prices_df = io.read_list_prices("system.billing.list_prices")
    users_df = io.read_user_info(catalog_and_schema + ".user_info")
    billing_df = io.read_billing("system.billing.usage", last_checkpoint)
    cloud_infra_cost_df = io.read_cloud_infra_cost(
        "system.billing.cloud_infra_cost", last_checkpoint
    )

    print("Calculating daily costs ...")
    calculator = CostCalculator()
    user_costs_day_df = calculator.calculate_daily_user_cost(
        queries_df, weights, list_prices_df, users_df, billing_df, cloud_infra_cost_df
    )
    io.save_user_costs(
        user_costs_day_df, catalog_and_schema + ".user_costs_day", last_checkpoint
    )
    print("Calculating daily costs finished")

    if user_costs_day_df.count() == 0:
        print("No data available from daily calculation.")
        return

    print("Saving checkpoint for daily costs")
    new_checkpoint = io.get_max_date_col(user_costs_day_df, "billing_date")
    io.save_checkpoint(catalog_and_schema + ".checkpoint_day", new_checkpoint)
    print(f"Checkpoint saved for daily costs as {new_checkpoint}")


def calculate_monthly_costs(catalog: str, schema: str):
    catalog_and_schema = catalog + "." + schema
    print(f"using {catalog_and_schema}")

    io = CostCalculatorIO(spark)
    calculator = CostCalculator()

    last_checkpoint = io.read_checkpoint(catalog_and_schema + ".checkpoint_month")
    print(f"last checkpoint for monthly calculation: {last_checkpoint}")

    print("Calculating monthly costs")
    user_costs_day_df = spark.table(catalog_and_schema + ".user_costs_day")

    if last_checkpoint:
        user_costs_day_df = user_costs_day_df.filter(
            to_date(
                concat(
                    year("billing_date"),
                    lit("-"),
                    lpad(month("billing_date"), 2, "0"),
                    lit("-01"),
                ),
                "yyyy-MM-dd",
            ).cast(DateType())
            > last_checkpoint
        )

        if user_costs_day_df.count() == 0:
            print("No data available from monthly calculation. Skipping.")
            return

    user_costs_month_df = calculator.calculate_monthly_user_cost(user_costs_day_df)
    io.save_user_costs(
        user_costs_month_df,
        catalog_and_schema + ".user_costs_month",
        last_checkpoint,
    )
    print("Calculating monthly costs finished")

    print("Saving checkpoint for monthly costs")
    new_checkpoint = io.get_max_date_col(user_costs_month_df, "billing_date")
    io.save_checkpoint(catalog_and_schema + ".checkpoint_month", new_checkpoint)
    print(f"Checkpoint saved for monthly costs as {new_checkpoint}")


def main(catalog: str, schema: str):
    calculate_daily_costs(catalog, schema)
    calculate_monthly_costs(catalog, schema)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", type=str, help="Catalog name to use")
    parser.add_argument("--schema", type=str, help="Schema name to use")
    args = parser.parse_args()

    paramName1 = args.paramName1

    main(args.catalog, args.schema)
