# Copyright (c) Dynamic Quants and affiliates.
#
# This source code is part of Quantix library and is licensed under the MIT
# license found in the LICENSE file in the root directory of this source tree.

"""TimescaleDB connection client."""

import os

import psycopg2
from dotenv import load_dotenv

# Get the environment variables from the .env file (or .env.test).
load_dotenv()


class TimeScaleClient:
    def __init__(self):
        # Access the environment variables containing the database connection information.
        host = os.environ.get("TIMESCALEDB_HOST")
        user = os.environ.get("TIMESCALEDB_USER")
        password = os.environ.get("TIMESCALEDB_PASSWORD")
        db = os.environ.get("TIMESCALEDB_DB")

        self._connection_string = f"postgresql://{user}:{password}@{host}:5432/{db}"

    def connect(self):
        return psycopg2.connect(self._connection_string)
