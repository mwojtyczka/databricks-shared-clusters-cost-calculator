.PHONY: clean dev fmt lint test

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
	pycodestyle src
	autoflake --check-diff --quiet --recursive src

clean:
	rm -fr dist *.egg-info .pytest_cache build coverage .junittest*.xml coverage.xml .coverage* sphinx_docs/_build **/__pycache__

test: ## run pytest
	poetry run pytest -rA -vvs --log-level INFO
