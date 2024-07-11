# Introduction

This project establish a solution to calculate costs of queries run by users on “shared” Databricks SQL Warehouses.
To accomplish this, system tables and numerous metrics provided by query history are used.

# Getting started

1. Install the Databricks CLI from https://docs.databricks.com/dev-tools/cli/databricks-cli.html

2. Authenticate to your Databricks workspace, if you have not done so already:
    ```
    $ databricks configure
    ```

3. To deploy a development copy of this project, type:
    ```
    $ databricks bundle deploy --target dev
    ```
    (Note that "dev" is the default target, so the `--target` parameter
    is optional here.)

    This deploys everything that's defined for this project.
    For example, the default template would deploy a job called
    `[dev yourname] cost_monitoring_job` to your workspace.
    You can find that job by opening your workpace and clicking on **Workflows**.

4. Similarly, to deploy a production copy, type:
   ```
   $ databricks bundle deploy --target prod
   ```

   Note that the default job from the template has a schedule that runs every day
   (defined in resources/cost_monitoring_job.yml). The schedule
   is paused when deploying in development mode (see
   https://docs.databricks.com/dev-tools/bundles/deployment-modes.html).

5. To run a job or pipeline, use the "run" command:
   ```
   $ databricks bundle run
   ```

6. Optionally, install developer tools such as the Databricks extension for Visual Studio Code from
   https://docs.databricks.com/dev-tools/vscode-ext.html. Or read the "getting started" documentation for
   **Databricks Connect** for instructions on running the included Python code from a different IDE.

7. For documentation on the Databricks asset bundles format used
   for this project, and for CI/CD configuration, see
   https://docs.databricks.com/dev-tools/bundles/index.html.

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

### Get path to poetry virtual env

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
