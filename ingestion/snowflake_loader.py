"""
Snowflake Loader
Loads raw data into the Snowflake landing schema.
"""

import os

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "FINRISK_360")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")


def load_to_snowflake(table: str, records: list[dict]) -> int:
    """Bulk-load records into a Snowflake table. Returns row count."""
    # TODO: implement Snowflake connector logic
    raise NotImplementedError


if __name__ == "__main__":
    load_to_snowflake("raw_fred", [{"series_id": "GDP", "value": 25000}])
