import pandas as pd
from dagster import asset, AssetIn, MetadataValue, Output, Out, multi_asset,AssetOut
from sqlalchemy import text, inspect
import subprocess
import sys
import os
from dagster import get_dagster_logger
from sqlalchemy.dialects.mssql import NVARCHAR, INTEGER, DECIMAL,DATE, TIME, DATETIME
import os
from dagster import ConfigurableResource
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from pydantic import Field


import subprocess
import sys
import os # Thêm import os
from dagster import asset, get_dagster_logger
class SqlServerResource(ConfigurableResource):
    """Resource for connecting to a SQL Server database."""

    server: str = Field(default="BLUE",description="BLUE")
    database: str = Field(default="QL",description="QL")
    # username: str | None = Field(default=None, description="Username for SQL Server Authentication (optional).")
    # password: str | None = Field(default=None, description="Password for SQL Server Authentication (optional).")
    driver: str = Field(default="ODBC Driver 17 for SQL Server", description="ODBC Driver name.")
    use_integrated_security: bool = Field(default=True, description="Use Windows Integrated Security (True) or SQL Server Authentication (False).")
    # Optional: Add other connection string parameters if needed
    # extra_params: dict[str, str] = Field(default_factory=dict)

    _engine: Engine | None = None

    def _build_connection_string(self) -> str:
        conn_str = f"mssql+pyodbc://@{self.server}/{self.database}?driver={self.driver.replace(' ', '+')}"
        if not self.use_integrated_security:
            if not self.username or not self.password:
                raise ValueError("Username and password must be provided when use_integrated_security is False.")
            conn_str = f"mssql+pyodbc://{self.username}:{self.password}@{self.server}/{self.database}?driver={self.driver.replace(' ', '+')}"
        else:
            # For integrated security with pyodbc
            conn_str += "&Trusted_Connection=yes"

        # Add extra params if any (example)
        # if self.extra_params:
        #     param_str = "&".join([f"{k}={v}" for k, v in self.extra_params.items()])
        #     conn_str += "&" + param_str

        return conn_str

    @property
    def engine(self) -> Engine:
        """Returns a SQLAlchemy engine instance."""
        if self._engine is None:
            conn_str = self._build_connection_string()
            self._engine = create_engine(conn_str, echo=False)
        return self._engine

    def teardown(self, _) -> None:
         """Dispose of the engine connection pool."""
         if self._engine:
             self._engine.dispose()
             self._engine = None

# Example of how to configure this resource later
sql_server_resource_local = SqlServerResource(
    server="BLUE", # IMPORTANT: Change this to your server name/instance
    database="QL",      # IMPORTANT: Change this to your database name
    use_integrated_security=True, # Set to False if using SQL Server login
    # username="your_sql_username", # Uncomment and set if use_integrated_security=False
    # password="your_sql_password", # Uncomment and set if use_integrated_security=False
    # driver="{ODBC Driver 17 for SQL Server}" # Change if you use a different driver
)

# You can also define it using environment variables for better security
# sql_server_resource_from_env = SqlServerResource(
#      server=os.getenv("SQLSERVER_SERVER", "localhost\\SQLEXPRESS"),
#      database=os.getenv("SQLSERVER_DATABASE", "DagsterDemoDB"),
#      username=os.getenv("SQLSERVER_USERNAME"),
#      password=os.getenv("SQLSERVER_PASSWORD"),
#      use_integrated_security=os.getenv("SQLSERVER_USE_INTEGRATED_SECURITY", "true").lower() == "true",
#      driver=os.getenv("SQLSERVER_DRIVER", "{ODBC Driver 17 for SQL Server}")
#  )

ticket = "SELECT * FROM Ticket"
showtime = "SELECT * FROM ShowTime"
movie = "SELECT * FROM Movie"
room = "SELECT * FROM Room"
cinema = "SELECT * FROM Cinema"
invoice = "SELECT * FROM Invoice"
sql_server = sql_server_resource_local
engine = sql_server.engine
with engine.connect() as connection:
    with connection.begin():
        df_invoice = pd.read_sql(invoice, connection)

        df_dates = pd.Series(pd.to_datetime(df_invoice['createDate'].unique()))

        df_dim_date = pd.DataFrame({
            'DateSK' : df_dates.dt.strftime('%Y%m%d%H%M%S').astype(int),
            
        })
        print(len(df_dim_date))
        df_dim_date=df_dim_date.drop_duplicates(subset=["DateSK"], keep="first")
        print(df_dim_date.head())




