# Contributing

## First Principles

Favoring standard libraries over external dependencies, especially in specific contexts like Databricks, is a best practice in software 
development. 

There are several reasons why this approach is encouraged:
- Standard libraries are typically well-vetted, thoroughly tested, and maintained by the official maintainers of the programming language or platform. This ensures a higher level of stability and reliability. 
- External dependencies, especially lesser-known or unmaintained ones, can introduce bugs, security vulnerabilities, or compatibility issues  that can be challenging to resolve. Adding external dependencies increases the complexity of your codebase. 
- Each dependency may have its own set of dependencies, potentially leading to a complex web of dependencies that can be difficult to manage. This complexity can lead to maintenance challenges, increased risk, and longer build times. 
- External dependencies can pose security risks. If a library or package has known security vulnerabilities and is widely used, it becomes an attractive target for attackers. Minimizing external dependencies reduces the potential attack surface and makes it easier to keep your code secure. 
- Relying on standard libraries enhances code portability. It ensures your code can run on different platforms and environments without being tightly coupled to specific external dependencies. This is particularly important in settings like Databricks, where you may need to run your code on different clusters or setups. 
- External dependencies may have their versioning schemes and compatibility issues. When using standard libraries, you have more control over versioning and can avoid conflicts between different dependencies in your project. 
- Fewer external dependencies mean faster build and deployment times. Downloading, installing, and managing external packages can slow down these processes, especially in large-scale projects or distributed computing environments like Databricks. 
- External dependencies can be abandoned or go unmaintained over time. This can lead to situations where your project relies on outdated or unsupported code. When you depend on standard libraries, you have confidence that the core functionality you rely on will continue to be maintained and improved. 

While minimizing external dependencies is essential, exceptions can be made case-by-case. There are situations where external dependencies are 
justified, such as when a well-established and actively maintained library provides significant benefits, like time savings, performance improvements, 
or specialized functionality unavailable in standard libraries.

# Local Setup

This section provides a step-by-step guide to set up and start working on the project. These steps will help you set up your project environment and dependencies for efficient development.

### Install Poetry

To begin, install Poetry:
https://python-poetry.org/docs/

### Create virtual environment and install dependencies

Assuming you've already cloned the github repo, run:
```shell
make dev
```

### Verify installation

Verify installation by running the tests:
```shell
make test
```

### Set up your IDE

Get path to poetry virtual env so that you can setup Python interpreter in your IDE:
```shell
echo $(poetry env info --path)/bin
```

Optionally, activate the virtual environment:
```shell
source $(poetry env info --path)/bin/activate
```

### Before every commit

Before every commit, apply the consistent formatting of the code, as we want our codebase look consistent:
```shell
make fmt
```

Run automated bug detector (`make lint`) and unit tests (`make test`) to ensure that automated
pull request checks do pass, before your code is reviewed by others: 
```shell
make lint
make test
```

### Running different type of tests

* Unit testing:

```shell
source $(poetry env info --path)/bin/activate
pytest tests/unit --cov
```

* Integration testing:
```shell
source $(poetry env info --path)/bin/activate
pytest tests/integration --cov
```

* End to End testing:
```shell
source $(poetry env info --path)/bin/activate
pytest tests/e2e --cov
```

### Running all steps

If you want to execute all the steps (install dependencies, format, lint, test etc.), run:
```shell
make all
```

## First contribution

Here are the example steps to submit your first contribution:

1. Make a Fork from the repo (if you really want to contribute)
2. `git clone`
3. `git checkout main` (or `gcm` if you're using [ohmyzsh](https://ohmyz.sh/)).
4. `git pull` (or `gl` if you're using [ohmyzsh](https://ohmyz.sh/)).
5. `git checkout -b FEATURENAME` (or `gcb FEATURENAME` if you're using [ohmyzsh](https://ohmyz.sh/)).
6. .. do the work
7. `make fmt`
8. `make lint`
9. .. fix if any
10. `make test`
11. .. fix if any
12. `git commit -a`. Make sure to enter meaningful commit message title.
13. `git push origin FEATURENAME`
14. Go to GitHub UI and create PR. Alternatively, `gh pr create` (if you have [GitHub CLI](https://cli.github.com/) installed). 
    Use a meaningful pull request title because it will appear in the release notes. Use `Resolves #NUMBER` in pull
    request description to [automatically link it](https://docs.github.com/en/get-started/writing-on-github/working-with-advanced-formatting/using-keywords-in-issues-and-pull-requests#linking-a-pull-request-to-an-issue)
    to an existing issue. 
15. Announce PR for the review

## Troubleshooting

If you encounter any package dependency errors after `git pull`, run `make clean`

If you encounter any poetry virtual environment issues, reinstall the environment:
```
poetry env list
poetry env remove project-name-py3.10
poetry install
```
