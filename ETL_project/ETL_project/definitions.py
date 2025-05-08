# sqlserver_pipeline/__init__.py
from dagster import Definitions, load_assets_from_modules

from . import assets
from .resources import sql_server_resource_local, sql_server_resource_warehouse # Import the configured resource instance

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        # The key 'sql_server' must match the parameter name in the assets
        "sql_server": sql_server_resource_local,
        "sql_server_warehouse": sql_server_resource_warehouse
    },
)