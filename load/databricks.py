"""Open a Databricks SQL connection from environment variables.

Reuses the same DBX_* values proven by `dbt debug`. Call load_dotenv() in the
entrypoint first.
"""

from __future__ import annotations

import os

from databricks import sql


def connect():
    return sql.connect(
        server_hostname=os.environ["DBX_HOST"],
        http_path=os.environ["DBX_HTTP_PATH"],
        access_token=os.environ["DBX_TOKEN"],
        catalog=os.environ.get("DBX_CATALOG", "workspace"),
        schema=os.environ.get("DBX_SCHEMA", "default"),
    )
