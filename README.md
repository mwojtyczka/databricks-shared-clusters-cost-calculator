# Introduction

This project allocates billing usage of Databricks “Shared” SQL Warehouses to individual users and their respective
organizational entities (e.g. cost center, departments / business units) and save the results as a delta table.

Many organizations would like to adopt a "cost centre" approach by invoicing DBU consumption against individual users, 
teams, business units/departments, or cost centers. 
Currently, Databricks provides DBU consumption data for the entire cluster, 
lacking the granularity needed for more detailed billing information. 
This is especially important for Data Warehousing use cases where SQL Warehouses are often shared across different
organizational entities to unify cluster configuration and maximize utilization.

![problem](docs/problem.png?)

An alternative solution is to create separate clusters for each department. 
However, this requires reconfiguration of the existing applications (e.g. PowerBI) 
and does not provide per user cost allocation.

For questions, troubleshooting or bug fixes, please submit [an issue](https://github.com/mwojtyczka/databricks-shared-clusters-cost-calculator/issues).
See [contributing instructions](CONTRIBUTING.md) to help improve this project.

[![build](https://github.com/mwojtyczka/databricks-shared-clusters-cost-calculator/actions/workflows/push.yml/badge.svg)](https://github.com/mwojtyczka/databricks-shared-clusters-cost-calculator/actions/workflows/push.yml) [![codecov](https://codecov.io/github/mwojtyczka/databricks-shared-clusters-cost-calculator/graph/badge.svg?token=p0WKAfW5HQ)](https://codecov.io/github/mwojtyczka/databricks-shared-clusters-cost-calculator)  ![linesofcode](https://aschey.tech/tokei/github/mwojtyczka/databricks-shared-clusters-cost-calculator?category=code)

<!-- TOC -->
* [Prerequisites](#prerequisites)
* [Installation](#installation)
* [Design](#design)
* [Local Development](#local-development)
* [Contribution](#contribution)
* [Limitations](#limitations)
* [Future Work](#future-work)
<!-- TOC -->

# Prerequisites

In order to deploy the project, the following is required:
* At least one Unity Catalog-enabled [workspace](https://docs.databricks.com/en/getting-started/index.html) in your Databricks account.
* Access to [system tables](https://docs.databricks.com/en/admin/system-tables/index.html#grant-access-to-system-tables)

# Installation

The project uses [Databricks Assets Bundles (DAB)](https://docs.databricks.com/en/dev-tools/bundles/index.html) for deployment.

Once the project is deployed, the following Jobs are created in your Databricks Workspace:
* `Granular billing usage: 1. create tables`
* `Granular billing usage: 2. calculate`
* `Granular billing usage: 3. create alerts and dashboard`
* `Granular billing usage: 4. define budget`
* `Granular billing usage: 5. fetch user info`

Deploy and execute them in the given order.

### Steps:

1. Install the Databricks CLI (see [here](https://docs.databricks.com/dev-tools/cli/databricks-cli.html)).

2. Authenticate to your Databricks workspace, if you have not done so already:
    ```
    $ databricks configure
    ```
3. Update job templates in [resources](resources) to provide parameters like output catalog, output schema etc.

4. Update bundle configuration under [databricks.yml](databricks.yml) to provide workspace host 
and user name to use for the deployment.

5. Validate the bundle configuration:
    ```
    $ databricks bundle validate --target dev
    ```

    Note that "dev" is the default target, so the `--target` parameter is optional here. 

6. Deploy a development copy of this project to the workspace:
    ```
    $ databricks bundle deploy --target dev
    ```

7. Similarly, if you want to deploy a production copy of this project to the workspace:
   ```
   $ databricks bundle deploy --target prod
   ```
   
8. Run the jobs:

   a) Deploy tables:
   ```
   $ databricks bundle run create_tables_job --target dev
   ```
   
   b) Run the calculation:
   ```
   $ databricks bundle run calculate_job --target dev
   ```

   c) Deploy Databricks SQL Alerts and Dashboard:
   ```
   $ databricks bundle run create_dbsql_objects_job --target dev
   ```
   
    d) Define budget limits:

    Provide budget info in the `src/define_budget.sql` file. Then run:
    ```
    $ databricks bundle run define_budget_job --target dev
    ```
   
    e) Fetch user info from your Identity Provider (IdP):

    Provide auth credentials for your Entra ID Identity Provider in `src/fetch_user_info_ad.py`. Then run:
    ```
    $ databricks bundle run fetch_user_info_job --target dev
    ```

    If you want to skip the step and pre-populate `user_info` table with sample data for demo purposes,
    import and run [this notebook](lake_view/demo_setup.py).
    You can use this to quickly get started before fetching the user info from your IdP provider.

# Design

The solution provides granular cost allocation for queries run by users on "Shared" SQL Warehouses.
The solution uses numerous system tables to perform the calculation and write the granular cost allocation to a Delta table. 

![architecture](docs/architecture.png?)

### Input

For the **cost calculation** the following system tables are required:
* Billing usage system table ([system.billing.usage](https://docs.databricks.com/en/admin/system-tables/billing.html)) containing DBU consumption of SQL Warehouses per day from all UC-enabled workspaces.
* Query History system table (`system.query.history`) containing query history of SQL Warehouses from all UC-enabled workspaces. Currently in Private Preview.
* List Prices system table ([system.billing.list_prices](https://docs.databricks.com/en/admin/system-tables/pricing.html)) containing historical log of SKU pricing.
* Cloud Infra Cost system table (`system.billing.cloud_infra_cost`) containing cloud costs (VM and cloud storage). Currently in Private Preview.

For the **dashboard** and **alerts** the following tables are required:
* User Info table (`user_info`) contains mapping of users to cost centers and departments. 
This table can be pre-populated based on extension attributes from the IdP providers. 
See example notebook for Microsoft Entra ID [here](src/fetch_user_info_ad.py).
![problem](docs/user_info_table.png?)
* Budget table (`budget`) contains spending limits for a given organizational entity (e.g. department).
![problem](docs/budget_table.png?)

### Output

The result is saved as Delta table and gives DBU consumption for each user and cluster. 
The data is aggregated daily and stored in the Cost Agg Day table (`cost_agg_day`).
Only users that have consumed any DBUs in the given day are included.

![problem](docs/cost_agg_day_table.png?)

[Rows filters](https://docs.databricks.com/en/tables/row-and-column-filters.html) can be applied to the table to ensure
only specific rows can be viewed by specific departments/users. 
The table can be joined to the User Info table (`user_info`) to retrieve additional contextual information 
about the users (e.g. cost center, department), and to the Budget table (`budget`) to get info about spending limits.

### Cost Calculation

Cost calculation is implemented as a Databricks job and is scheduled to run daily. 
The calculation uses a **weighted sum approach** to calculate the contribution of user queries 
in the total DBU consumption of SQL Warehouse clusters. 
Idle time is split according to the contribution of each user in the cluster usage.
The metrics and assigned weights can be found [here](src/clusters_cost_allocation/metrics.py).

The cost allocation is available on a daily granularity.
Current date is excluded from the calculations to avoid partial results.
Only query history of complete days is used.

The last processed day is persisted in a **checkpoint** table. The query history is processed up to
the date persisted in the checkpoint. When the job starts again it will continue where it left.
This is to ensure that the calculation is done incrementally (only for queries not processed yet).

![problem](docs/checkpoint_table.png?)

If you want to re-process the data, reset the checkpoint to a previous date. 
But note that most system tables have small retention time (usually up to a month) so you need to be careful. 

# Contribution

See contribution guidance [here](CONTRIBUTING.md).

# Limitations

* Works for SQL Warehouses only
* A user can only belong to one entity (e.g. cost center, department). 
If the user belongs to multiple entities, it’s unknown in the context of which entity the user executed the query.
* If a query spans multiple days, the cost is attributed to the end date only.
* The solution is compatible with Databricks on AWS, Azure, and GCP. 
However, currently, cloud cost reporting is only available for AWS. 
The reported cloud costs include EC2-related expenses. 
Future updates will include Storage and Network costs, as well as support for other cloud providers.

# Future work

* More IdPs: Extend the User Info workflow to support more IdP providers (e.g. Okta, One Login), not only Microsoft Entra ID.
* Add support for regular Databricks clusters (job and all-purpose).
