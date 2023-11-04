install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

format:
	black *.py && black test_*.py

lint:
	pylint --disable=R,C --ignore-patterns=test_*.py *.py

test:
	python -m pytest -vv --cov=test_main test_*.py

deploy:
	# deploy goes here
		
all: install format lint test