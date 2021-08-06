#!/usr/bin/env bash

cd airflow || exit
poetry install
poetry run python -m pytest -p no:warnings

