.PHONY: clean dev fmt lint test coverage display_coverage

# existing VIRTUAL_ENV might mess with poetry, so making sure it is gone
VIRTUAL_ENV=
unexport VIRTUAL_ENV

all: clean dev fmt lint test

help:
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z0-9._-]+:.*?## / {printf "\033[1m\033[36m%-38s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

dev: ## install dependencies and the package to poetry venv
	poetry install
	poetry update

fmt: ## run black to format the code
	poetry run black src tests

lint: ## run linter to check the code
	poetry run pycodestyle src
	poetry run autoflake --check-diff --quiet --recursive src
	poetry run pylint --output-format=colorized -j 0 src

clean:
	rm -fr dist *.egg-info .pytest_cache build coverage .junittest*.xml coverage.xml .coverage* sphinx_docs/_build **/__pycache__

test: ## run pytest
	poetry run pytest -rA -vvs --log-level INFO

coverage:
	poetry run pytest --cov src --cov-report=xml tests/unit --durations 20
	poetry run pytest --cov src tests/unit --cov-report=html --durations 20

display_coverage:
	open htmlcov/index.html