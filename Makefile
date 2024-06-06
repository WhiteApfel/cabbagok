isort:
	@isort --profile black ./cabbagok/
	@isort --profile black ./tests/


black:
	@black ./cabbagok/ --preview
	@black ./tests/ --preview


install: uninstall
	pip install .
	@echo "Done"


uninstall:
	@pip uninstall cabbagok -y


test:
	@pytest --cov=cabbagok


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
	@pip install -Ur requirements-dev.txt
	@pip install -e .

build:
	@python setup.py sdist bdist_wheel

upload:
	@twine upload dist/*

publish:
	@make clean
	@make build
	@make upload
	@make clean