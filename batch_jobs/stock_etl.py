import findspark
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import yfinance as yf
import pandas as pd
from delta import configure_spark_with_delta_pip
from datetime import datetime, timedelta
from pyspark.sql import Window

findspark.init()
findspark.find()

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_FILEPATH = os.path.join(PROJECT_ROOT, 'data')

tickers = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "AVGO", "BRK-B", "TSLA", "TSM",
    "JPM", "WMT", "LLY", "ORCL", "V", "MA", "NFLX", "XOM", "COST", "JNJ",
    "ABBV", "SAP", "BABA", "GS", "HD", "VRTX", "UNH", "MRK", "PEP", "TMO"
]

# Get yesterday's date
yesterday = datetime.now() - timedelta(days=1)
end_date = yesterday.strftime("%Y-%m-%d")

# Get start date (3 years before yesterday)
start_date = (yesterday - timedelta(days=3*365)).strftime("%Y-%m-%d")

# Download stock data
raw = yf.download(tickers, start=start_date, end=end_date, group_by='ticker')

# convert multi-index DataFrame to flat DataFrame
df_list = []

for ticker in tickers:
    ticker_df = raw[ticker].reset_index()
    ticker_df['Ticker'] = ticker
    df_list.append(ticker_df)

flat_df = pd.concat(df_list, ignore_index=True)

# reorder columns 
cols = ['Ticker', 'Date'] + [col for col in flat_df.columns if col not in ['Ticker', 'Date']]
flat_df = flat_df[cols]

builder = SparkSession.builder \
    .appName("StockETL") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") 

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# store data in spark dataframe
spark_df = spark.createDataFrame(flat_df)

## Feature engineering
# set windows 
window_spec = Window.partitionBy('Ticker').orderBy('Date')
ma_window_12 = Window.partitionBy('Ticker').orderBy('Date').rowsBetween(-11, 0)
ma_window_26 = Window.partitionBy('Ticker').orderBy('Date').rowsBetween(-25, 0)
ma_window_50 = Window.partitionBy('Ticker').orderBy('Date').rowsBetween(-49, 0)
ma_window_200 = Window.partitionBy('Ticker').orderBy('Date').rowsBetween(-199, 0)
window_cum = Window.partitionBy("Ticker").orderBy("Date").rowsBetween(Window.unboundedPreceding, 0)
signal_window = Window.partitionBy('Ticker').orderBy('Date').rowsBetween(-8, 0)


# compute daily return
spark_df = spark_df.withColumn('Prev_close', lag('Close').over(window_spec))

spark_df = spark_df.withColumn(
    'Daily_return',
    round(((col('Close') - col('Prev_close')) / col('Prev_close') * 100), 2)
)

# calculate daily volutility rate 
spark_df = spark_df.withColumn(
    'Volatility',
    round(((col('High') - col('Low')) / col('Low') * 100), 2)
)


# compute moving averages
spark_df = spark_df.withColumn(
    'MA_12',
    round(avg('Close').over(ma_window_12), 2)
).withColumn(
    'MA_26',
    round(avg('Close').over(ma_window_26), 2)
).withColumn(
    'MA_50',
    round(avg('Close').over(ma_window_50), 2)
).withColumn(
    'MA_200',
    round(avg('Close').over(ma_window_200), 2)
)

spark_df = spark_df.withColumn("First_Close", first("Close").over(window_cum))
spark_df = spark_df.withColumn("Cumulative_Return", round(((col("Close") - col("First_Close")) / col("First_Close")) * 100, 2))


# compute 7 day momentum
spark_df = spark_df.withColumn("Close_7_Days_Ago", lag("Close", 7).over(window_spec))
spark_df = spark_df.withColumn(
    "Momentum_7d", 
    round(((col("Close") - col("Close_7_Days_Ago")) / col("Close_7_Days_Ago")) * 100, 2)
)

# compute MACD and signal lines 
spark_df = spark_df.withColumn(
    'MACD',
    col('MA_12') - col('MA_26')
)

spark_df = spark_df.withColumn(
    'Signal_line',
    round(avg(col('MACD')).over(signal_window), 2)
)

# compute draw down
# measure draw from peak close price
peak_close = max(col('Close')).over(Window.partitionBy('Ticker').orderBy('Date').rowsBetween(Window.unboundedPreceding, 0))

spark_df = spark_df.withColumn(
    'DrawDown',
    round(((col('Close') - peak_close) / peak_close * 100), 2)
)

# write to deltalake
output_path = os.path.join(DATA_FILEPATH, "stock_data_delta.parquet")

spark_df.write.format('parquet').mode('overwrite').save(output_path)

print(f'Data succesfully stored prcessed data at -> {output_path}')