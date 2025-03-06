from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests

def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

def get_url(stock_symbol):
    api_key = Variable.get("alphavantage_api_key")
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={stock_symbol}&apikey={api_key}'
    return url

@task
def extract(url):
  r = requests.get(url)
  data = r.json()
  return data

@task
def transform(data):
  """
   - return the last 90 days of the stock prices of symbol as a list of json strings
  """
  results = []   # empty list for now to hold the 90 days of stock info (open, high, low, close, volume)
  for d in data["Time Series (Daily)"]:   # here d is a date: "YYYY-MM-DD"
    results.append({'date': d} | data["Time Series (Daily)"][d])
    # an example of data["Time Series (Daily)"][d] is
    # '1. open': '117.3500', '2. high': '119.6600', '3. low': '117.2500', '4. close': '117.8700', '5. volume': '286038878'}
  return results[:90]

# Implement incremental update using MERGE SQL in Snowflake
@task
def load(data, stock_symbol):
    con = return_snowflake_conn()

    target_table = "DEV.RAW.stock_prices"

    try:
      con.execute("BEGIN;")

      # Perform Full Refresh
      con.execute(f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
          symbol varchar NOT NULL,
          date timestamp_ntz NOT NULL,
          open float,
          high float,
          low float,
          close float,
          volume number,
          PRIMARY KEY (symbol, date)
        );""")
      con.execute(f"DELETE FROM {target_table}")

      for r in data:
        symbol = stock_symbol
        date = r["date"]
        open = r["1. open"]
        high = r["2. high"]
        low = r["3. low"]
        close = r["4. close"]
        volume = r["5. volume"]
        sql = f"INSERT INTO {target_table} (symbol, date, open, high, low, close, volume) VALUES ('{symbol}', '{date}', '{open}', '{high}', '{low}', '{close}', '{volume}')"
        con.execute(sql)
        
      con.execute("COMMIT;")
      con.close()
    except Exception as e:
      con.execute("ROLLBACK;")
      print(e)
      con.close()
      raise e
    

with DAG(
    dag_id = 'ETLStocks',
    start_date = datetime(2024,9,21),
    catchup=False,
    tags=['ETL'],
    schedule = '30 2 * * *'
) as dag:
    stock_symbol = "FIVE"
    
    url = get_url(stock_symbol)

    data = extract(url)
    data = transform(data)
    load(data, stock_symbol)