# Airflow Dags for Streaming Pipeline

## Local Setup
### Prerequisites
* python
* poetry
 
### Steps
* Run `poetry install`
* Run `pipenv -m pytest test`

### Setup IntelliJ

* Install poetry plugin from marketplace
* Run `poetry install`
* Run `poetry show -v` and copy the virtualenv location
* Add airflow folder as a module
* Create new SDK using the virtualenv location
* Add this SDK to airflow module



