isort:
	@uv run isort --profile black ./cabbagok/
	@uv run isort --profile black ./tests/


black:
	@uv run black ./cabbagok/ --preview
	@uv run black ./tests/ --preview


install: uninstall
	uv pip install .
	@echo "Done"


uninstall:
	@uv pip uninstall cabbagok


test:
	@uv run pytest --cov=cabbagok


clean:
	@rm -rf `find . -name __pycache__`
	@rm -f `find . -type f -name '*.py[co]' `
	@rm -f `find . -type f -name '*~' `
	@rm -f `find . -type f -name '.*~' `
	@rm -f `find . -type f -name '@*' `
	@rm -f `find . -type f -name '#*#' `
	@rm -f `find . -type f -name '*.orig' `
	@rm -f `find . -type f -name '*.rej' `
	@rm -f .coverage
	@rm -rf htmlcov
	@rm -rf build
	@rm -rf cover
	@rm -rf .tox
	@rm -f .flake
	@rm -rf .pytest_cache
	@rm -rf dist
	@rm -rf *.egg-info

install-dev: uninstall
	@uv pip install -Ur requirements-dev.txt
	@uv pip install -e .

build:
	@uv build

upload:
	@uv publish dist/*

publish:
	@make clean
	@make build
	@make upload
	@make clean