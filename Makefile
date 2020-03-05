init:
	pip install pipenv --upgrade
	pipenv install --dev

test:
	detox

ci: init
	pipenv run py.test -n 8 --boxed --junitxml=report.xml

flake8: init
	pipenv run flake8 --ignore=E501,F401,E128,E402,E731,F821 s3mon

coverage: init
	pipenv run py.test --cov-config .coveragerc --verbose --cov-report term --cov-report xml --cov=s3mon tests

publish:
	pip install 'twine>=1.5.0'
	python setup.py sdist bdist_wheel
	twine upload dist/*
	rm -fr build dist .egg s3mon.egg-info

