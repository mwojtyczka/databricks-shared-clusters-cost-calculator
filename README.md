# Introduction

This project allocates billing usage of Databricks “Shared” SQL Warehouses to individual users and their respective 
organisational entities (e.g. cost center, business units).

The project will deploy the following jobs in your [Databricks Workspace](https://docs.databricks.com/en/getting-started/index.html):
* `granular billing usage: 1. create tables`
* `granular billing usage: 2. calculate`
* `granular billing usage: 3. fetch user info` (requires configuration)

Deploy and execute them in the given order (see [Getting started](#getting-started)).

Optionally, you can import [this sample](lake_view/dashboard.json) LakeView Dashboard for visualization.

# Problem

![problem](docs/problem.png?)

# Solution

![architecture](docs/architecture.png?)

# Prerequisites

In order to deploy the project, the following is required:
* Access to a [Databricks Workspace](https://docs.databricks.com/en/getting-started/index.html) enabled for [Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
* Access to [system tables](https://docs.databricks.com/en/admin/system-tables/index.html#grant-access-to-system-tables)

# Getting started

1. Install the Databricks CLI (see [here](https://docs.databricks.com/dev-tools/cli/databricks-cli.html)).

2. Authenticate to your Databricks workspace, if you have not done so already:
    ```
    $ databricks configure
    ```

3. Deploy a development copy of this project to the workspace:
    ```
    $ databricks bundle deploy --target dev
    ```
    This deploys everything that's defined for this project.
    Note that "dev" is the default target, so the `--target` parameter is optional here.
    
    To customize the deployment (e.g. change target `schema`), update the job templates in [resources](resources).

4. Similarly, if you want to deploy a production copy of this project to the workspace:
   ```
   $ databricks bundle deploy --target prod
   ```

5. Run the jobs:

   ```
   # deploy tables
   $ databricks bundle run create_tables_job --target dev
   
   # run the calculation
   $ databricks bundle run calculate_job --target dev
   
   # run job to fetch user info (requires configuration)
   $ databricks bundle run fetch_user_info_job --target dev
   ```
   
6. Optionally, manually setup a LakeView Dashboard in your Databricks Workspace:

    * Import [this sample dashboard](lake_view/dashboard.json).
    * Import and run [this notebook](lake_view/user_info_demo.py) to pre-populate the `user_info` table with some randomized data.
      Since running the `fetch_user_info_job` job requires configuration, you can use this notebook to quickly get started.

7. For documentation on the Databricks asset bundles format used
   for this project, and for CI/CD configuration, see [here](https://docs.databricks.com/dev-tools/bundles/index.html).

# Local Development

Follow the below steps to run the project on your local machine.

## Building the project: installing dependencies, formatting, linting, testing

```bash
make all
```

## Setting up IDE

### Installing project requirements

```bash
poetry install
```

### Updating project requirements

```bash
poetry update
```

### Get path to poetry virtual env so that you can setup interpreter in your IDE

```bash
echo $(poetry env info --path)/bin
```

Activate poetry virtual environment:

```bash
source $(poetry env info --path)/bin/activate
```

## Running individual tests

* Unit testing:

```
source $(poetry env info --path)/bin/activate
pytest tests/unit --cov
```

* Integration testing:
```
source $(poetry env info --path)/bin/activate
pytest tests/integration --cov
```

* End to End testing:
```
source $(poetry env info --path)/bin/activate
pytest tests/e2e --cov
```

## Reinstalling poetry virtual env

```
poetry env list
poetry env remove project-name-py3.10
poetry install
```

# Contribute

See contribution guidance [here](CONTRIBUTING.md)
