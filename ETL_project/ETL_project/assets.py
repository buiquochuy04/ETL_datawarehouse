

# sqlserver_pipeline/assets.py
import pandas as pd
from dagster import asset, AssetIn, MetadataValue, Output, Out, multi_asset,AssetOut
from sqlalchemy import text, inspect
import subprocess
import sys
import os
from dagster import get_dagster_logger
from sqlalchemy.dialects.mssql import NVARCHAR, INTEGER, DECIMAL,DATE, TIME, DATETIME

from transform_data.transform import *


# Assuming resources.py is in the same directory or package
from .resources import SqlServerResource 

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
product = "SELECT * FROM Product"
productusage = "SELECT * FROM ProductUsage"

@asset(
  # Định nghĩa các output assets một cách tường minh bằng AssetOut
  # Sử dụng 'outs' (số nhiều) với @multi_asset
  # Vẫn có thể thêm ins và group_name như bình thường
  group_name='extracted'
  # compute_kind="python" # Tùy chọn: chỉ định loại tính toán
)

def extract_data(sql_server: SqlServerResource) -> dict:
    engine = sql_server.engine
    with engine.connect() as connection:
        with connection.begin():
            ticket_df = pd.read_sql(ticket, connection)
            showtime_df = pd.read_sql(showtime, connection)
            movie_df = pd.read_sql(movie, connection)
            room_df = pd.read_sql(room, connection)
            cinema_df = pd.read_sql(cinema, connection)
            invoice_df = pd.read_sql(invoice, connection)
            product_df = pd.read_sql(product, connection)
            productusage_df = pd.read_sql(productusage, connection)
    return {
        "ticket": ticket_df,
        "showtime": showtime_df,
        "movie": movie_df,
        "room": room_df,
        "cinema": cinema_df,
        "invoice": invoice_df,
        "product": product_df,
        "productusage": productusage_df
    }

@asset(
    group_name="transformed",
    ins={"extracted": AssetIn(key="extract_data")},  # Đổi key_prefix thành "extracted"
)
def transform(extracted: dict) -> dict:
    df_ticket = extracted["ticket"]
    df_showtime = extracted["showtime"]
    df_movie = extracted["movie"]
    df_room = extracted["room"]
    df_cinema = extracted["cinema"]
    df_invoice = extracted["invoice"]
    df_product = extracted["product"]
    df_productusage = extracted["productusage"]

    # Transformations
    df_dim_date = transform_DimDate(df_invoice)
    df_dim_movie = transform_DimMovie(df_movie)
    df_dim_cinema = transform_DimCinema(df_cinema)
    df_dim_product = transform_DimProduct(df_product)
    df_fact_sales_revenue = transform_FactSalesRevenue(
        df_invoice, df_showtime, df_movie, df_cinema, df_product, df_ticket, df_room, df_productusage
    )

    return {
        "dim_date": df_dim_date,
        "dim_movie": df_dim_movie,
        "dim_cinema": df_dim_cinema,
        "dim_product": df_dim_product,
        "fact_sales_revenue": df_fact_sales_revenue
    }

@asset(
    ins={"db": AssetIn(key="transform")},
    group_name="load_data",
    compute_kind="sqlserverwarehouse"
)
def load_data_DimDate(context, db: dict, sql_server_warehouse: SqlServerResource) -> dict:
    """Loads fact data into the SQL Server fact table."""
    engine = sql_server_warehouse.engine
    table_name = "dim_date"
    df = db[table_name]

    with engine.connect() as connection:
        with connection.begin(): # Use transaction
            df.to_sql("DimDate", con=connection, if_exists='append', index=False, dtype={
                # "DateSK": INTEGER,             
                "FullDate": DATE,
                "DayNumberOfWeek": INTEGER ,
                "DayNameOfWeek": NVARCHAR(10) ,
                "MonthName": NVARCHAR(10) ,
                "MonthNumberOfYear": INTEGER ,
                "CalendarQuarter": INTEGER ,
                "CalendarYear": INTEGER ,
            })
    return{
        **db
    }

@asset(
    ins={"db": AssetIn(key="transform")},
    group_name="load_data",
    compute_kind="sqlserverwarehouse"
)
def load_data_DimMovie(context, db: dict, sql_server_warehouse: SqlServerResource) -> dict:
    """Loads fact data into the SQL Server fact table."""
    engine = sql_server_warehouse.engine
    table_name = "dim_movie"
    df = db[table_name]

    with engine.connect() as connection:
        with connection.begin(): # Use transaction
            df.to_sql("DimMovie", con=connection, if_exists='append', index=False, dtype={
                "MovieID_OLTP": INTEGER,
                "Title": NVARCHAR(255),
                "Genre": NVARCHAR(100),
            })
    return{
        **db
    }  

@asset(
    ins={"db": AssetIn(key="transform")},
    group_name="load_data",
    compute_kind="sqlserverwarehouse"
)
def load_data_DimCinema(context, db: dict, sql_server_warehouse: SqlServerResource) -> dict:
    """Loads fact data into the SQL Server fact table."""
    engine = sql_server_warehouse.engine
    table_name = "dim_cinema"
    df = db[table_name]

    with engine.connect() as connection:
        with connection.begin(): # Use transaction
            df.to_sql("DimCinema", con=connection, if_exists='append', index=False, dtype={
                "CinemaID_OLTP": INTEGER,
                "Name": NVARCHAR(100),
            })
    return{
        **db
    }  

@asset(
    ins={"db": AssetIn(key="transform")},
    group_name="load_data",
    compute_kind="sqlserverwarehouse"
)
def load_data_DimProduct(context, db: dict, sql_server_warehouse: SqlServerResource) -> dict:
    """Loads fact data into the SQL Server fact table."""
    engine = sql_server_warehouse.engine
    table_name = "dim_product"
    df = db[table_name]

    with engine.connect() as connection:
        with connection.begin(): # Use transaction
            df.to_sql("DimProduct", con=connection, if_exists='append', index=False, dtype={
                "ProductID_OLTP": INTEGER,
                "Name": NVARCHAR(100),
            })
    return{
        **db
    }  

@asset(
    ins={"db": AssetIn(key="load_data_DimCinema"),
         "movie": AssetIn(key="load_data_DimMovie"),
         "dimdate": AssetIn(key="load_data_DimDate"),
         "product": AssetIn(key="load_data_DimProduct")
        },
    group_name="load_data",
    compute_kind="sqlserverwarehouse"
)
def load_data_FactSalesRevenue(context, db: dict, movie:dict, dimdate:dict, product:dict, sql_server_warehouse: SqlServerResource) -> dict:
    """Loads fact data into the SQL Server fact table."""
    engine = sql_server_warehouse.engine
    table_name = "fact_sales_revenue"
    df = db[table_name]

    with engine.connect() as connection:
        with connection.begin(): # Use transaction
            df.to_sql("FactSalesRevenue", con=connection, if_exists='append', index=False, dtype={
                'InvoiceDateSK': INTEGER,        
                'MovieSK': INTEGER,               
                'CinemaSK': INTEGER,              
                'ProductSK': INTEGER,            
                'InvoiceID_OLTP': INTEGER,
                'ItemID_OLTP': INTEGER, 

                'QuantitySold': INTEGER,
                'ItemPrice': DECIMAL(10,2),       
                'LineRevenue_Gross': DECIMAL(10,2), 
                
                'InvoiceTotalDiscount_OLTP': DECIMAL(10,2),
                'InvoiceFinalAmount_OLTP': DECIMAL(10,2),
            })
    return{
        **db
    }  

