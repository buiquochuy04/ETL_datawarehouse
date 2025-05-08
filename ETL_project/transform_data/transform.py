import pandas as pd
def transform_DimDate(df_invoice):
    df_dates = pd.Series(pd.to_datetime(df_invoice['createDate'].unique()))

    df_dim_date = pd.DataFrame({
        'FullDate': df_dates,
        'DateSK' : df_dates.dt.strftime('%Y%m%d%H%M%S').astype(int).astype(str),
        'DayNumberOfWeek': df_dates.dt.weekday + 1,  # 1=Monday
        'DayNameOfWeek': df_dates.dt.day_name(),
        'MonthName': df_dates.dt.month_name(),
        'MonthNumberOfYear': df_dates.dt.month,
        'CalendarQuarter': df_dates.dt.quarter,
        'CalendarYear': df_dates.dt.year
    })
    df_dim_date=df_dim_date.drop_duplicates(subset=["FullDate"], keep="first")  # Remove duplicates
    return df_dim_date

def transform_DimMovie(df_movie):
  df_dim_movie = df_movie.rename(columns={
        'id': 'MovieID_OLTP',
        'title': 'Title',
        'genre': 'Genre'
    })[['MovieID_OLTP', 'Title', 'Genre']] 
  return df_dim_movie

def transform_DimCinema(df_cinema):
  df_dim_cinema = df_cinema.rename(columns={
    'id': 'CinemaID_OLTP',
    'name': 'Name'
  })[['CinemaID_OLTP', 'Name']]
  return df_dim_cinema

def transform_DimProduct(df_product):
  df_dim_product = df_product.rename(columns={
    'id': 'ProductID_OLTP',
    'name': 'Name'
  })[['ProductID_OLTP', 'Name']]

  # Thêm dòng đặc biệt: Vé xem phim
  df_ticket_product = pd.DataFrame([{
      'ProductID_OLTP': -1,
      'Name': 'Vé xem phim'
  }])

  df_dim_product = pd.concat([df_ticket_product, df_dim_product], ignore_index=True)
  return df_dim_product

def transform_FactSalesRevenue(df_invoice, df_showtime, df_movie, df_cinema, df_product, df_ticket, df_room, df_productusage):
    # 1. JOIN Ticket với ShowTime
  df = pd.merge(df_ticket, df_showtime, left_on='showtime_id', right_on='id', suffixes=('_ticket', '_showtime'))

  # 2. JOIN Movie
  df = pd.merge(df, df_movie, left_on='movie_id', right_on='id', suffixes=('', '_movie'))

  # 3. JOIN Room
  df = pd.merge(df, df_room, left_on='room_id', right_on='id', suffixes=('', '_room'))

  # 4. JOIN Cinema
  df = pd.merge(df, df_cinema, left_on='cinema_id', right_on='id', suffixes=('', '_cinema'))

  # 5. JOIN Invoice
  df = pd.merge(df, df_invoice, left_on='invoice_id', right_on='id', suffixes=('', '_invoice'))
    # 6. JOIN ProductUsage để lấy sản phẩm
  df = pd.merge(df, df_productusage, left_on='invoice_id', right_on='invoice_id', suffixes=('', '_productusage'))

  # 7. JOIN Product
  df = pd.merge(df, df_product, left_on='product_id', right_on='id', suffixes=('', '_product'))

  df_fact_sales = pd.DataFrame({
    'InvoiceID_OLTP': df['invoice_id'],  # ID của hóa đơn từ bảng Invoice
    'ItemID_OLTP': df['id_ticket'],  # ID của vé từ bảng Ticket
    'InvoiceDateSK': pd.to_datetime(df['createDate']).dt.strftime('%Y%m%d%H%M%S').astype(int).astype(str),  # Ngày hóa đơn dưới dạng YYYYMMDD
    'MovieSK': df['movie_id'],  # ID của phim
    'CinemaSK': df['cinema_id'],  # ID của rạp chiếu
    'ProductSK': df['product_id'],  # ID của sản phẩm từ bảng Product
    'QuantitySold': 1,  # Mỗi vé bán ra là 1 sản phẩm
    'ItemPrice': df['price'],  # Giá vé hoặc giá sản phẩm
    'LineRevenue_Gross': df['price'],  # Doanh thu trước giảm giá
    'InvoiceTotalDiscount_OLTP': df['totalDiscount'],  # Tổng giảm giá của hóa đơn
    'InvoiceFinalAmount_OLTP': df['totalAmount'],  # Tổng số tiền hóa đơn
  })
  return df_fact_sales


