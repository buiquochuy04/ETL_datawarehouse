import pandas as pd
from dagster import asset, AssetIn, MetadataValue, Output, Out, multi_asset,AssetOut
from sqlalchemy import text, inspect
import subprocess
import sys
import os
from dagster import get_dagster_logger
from sqlalchemy.dialects.mssql import NVARCHAR, INTEGER, DECIMAL,DATE, TIME, DATETIME
import os
from .resource import SqlServerResource 



import subprocess
import sys
import os # Thêm import os
from dagster import asset, get_dagster_logger

ticket = "SELECT * FROM Ticket"
showtime = "SELECT * FROM ShowTime"
movie = "SELECT * FROM Movie"
room = "SELECT * FROM Room"
cinema = "SELECT * FROM Cinema"
invoice = "SELECT * FROM Invoice"
sql_server = 
engine = sql_server.engine
with engine.connect() as connection:
    with connection.begin():
        ticket_df = pd.read_sql(ticket, connection)
        showtime_df = pd.read_sql(showtime, connection)
        movie_df = pd.read_sql(movie, connection)
        room_df = pd.read_sql(room, connection)
        cinema_df = pd.read_sql(cinema, connection)
        invoice_df = pd.read_sql(invoice, connection)
        print(ticket_df.head())