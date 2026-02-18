import pandas as pd
from pandas_gbq import read_gbq
from pandas_gbq import to_gbq
from google.cloud import bigquery

client = bigquery.Client()

def load_raw_staging():
    try:
        dc = pd.read_csv('01 Retail/customers_df.csv', index_col=0)
        to_gbq(dc, 'bq_retail.raw_stg_dim_customer', project_id='my-dw-demos-01', if_exists='fail')
        print('Customer data loaded successfully.')

        try:
            dp = pd.read_csv('01 Retail/products_df.csv', index_col=0)
            to_gbq(dp, 'bq_retail.raw_stg_dim_product', project_id='my-dw-demos-01', if_exists='fail')
            print ('Products data loaded successfully.')

            try:
                dt = pd.read_csv('01 Retail/transactions_df.csv', index_col=0)
                to_gbq(dt, 'bq_retail.raw_stg_fact_transaction', project_id='my-dw-demos-01', if_exists='fail')
                print('Transactions data loaded successfully.')

            except Exception as error:
                print(f'Error with loading transactions data {error}')

        except Exception as error:
            print(f'Error with loading products data {error}')

    except Exception as error:
        print(f'Error with loading customer data {error}')


try:
    dim_product = '''
    CREATE TABLE IF NOT EXISTS bq_retail.dim_product (
    product_key STRING DEFAULT GENERATE_UUID(),
    product_id STRING,
    product_name STRING,
    category STRING,
    price FLOAT64
    )'''
    query_job = client.query(dim_product)
    query_job.result()

    dim_country = '''
    CREATE TABLE IF NOT EXISTS bq_retail.dim_country (
    country_key STRING DEFAULT GENERATE_UUID(),
    country STRING
    )'''
    query_job = client.query(dim_country)
    query_job.result()

    dim_city = '''
    CREATE TABLE IF NOT EXISTS bq_retail.dim_city (
    city_key STRING DEFAULT GENERATE_UUID(),
    city STRING,
    country STRING,
    country_key STRING
    )'''
    query_job = client.query(dim_city)
    query_job.result()

    dim_customer = '''
    CREATE TABLE IF NOT EXISTS bq_retail.dim_customer (
    customer_key STRING DEFAULT GENERATE_UUID(),
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone_number STRING,
    address STRING,
    age INT64,
    gender STRING,
    city_key STRING
    )'''
    query_job = client.query(dim_customer)
    query_job.result()

    dim_date = '''
    CREATE TABLE IF NOT EXISTS bq_retail.dim_date (
    date_key STRING DEFAULT GENERATE_UUID(),
    transaction_date DATETIME,
    is_weekend BOOLEAN,
    month INT64,
    year INT64,
    quarter INT64,
    half_year INT64
    )'''
    query_job = client.query(dim_date)
    query_job.result()

    fact_transaction = '''
    CREATE TABLE IF NOT EXISTS bq_retail.fact_transaction (
    transaction_key STRING DEFAULT GENERATE_UUID(),
    transaction_id STRING,
    quantity INT64, 
    transaction_price FLOAT64,
    sales FLOAT64,
    product_key STRING,
    customer_key STRING,
    date_key STRING
    )'''
    query_job = client.query(fact_transaction)
    query_job.result()

except Exception as error:
    print(error)


def load_dim_product():
    try:
        dp = read_gbq('bq_retail.raw_stg_dim_product', 'my-dw-demos-01')
        product = dp[['ProductID', 'ProductName', 'Category', 'Price']].copy()
        product = product.rename(columns={'ProductID': 'product_id', 'ProductName': 'product_name',
                                          'Category': 'category', 'Price': 'price'})
        product = product.drop_duplicates(subset=['product_id', 'product_name'], keep='first')

        to_gbq(product, 'bq_retail.dim_product', project_id='my-dw-demos-01', if_exists='fail')

        print('Product data loaded successfully.')

    except Exception as error:
        print(f'Loading failed for dim_product table: {error}')


def load_dim_country():
    try:
        dc = read_gbq('bq_retail.raw_stg_dim_customer', 'my-dw-demos-01')
        country = dc[['Country']].copy()
        country = country.rename(columns={'Country': 'country'})
        country = country.drop_duplicates()

        to_gbq(country, 'bq_retail.dim_country', project_id='my-dw-demos-01', if_exists='append')

        print('Data loaded successfully to dim_country table.')

    except Exception as error:
        print(f'Loading failed for dim_country table: {error}')


def load_dim_city():
    try:
        dcc = read_gbq('bq_retail.raw_stg_dim_customer', 'my-dw-demos-01')
        city = dcc[['City', 'Country']].copy()
        city = city.rename(columns={'City': 'city'})
        city = city.drop_duplicates(subset=['city', 'Country'], keep='first')

        to_gbq(city, 'bq_retail.dim_city', project_id='my-dw-demos-01', if_exists='append')

        print('Data loaded successfully to dim_city table.')

        try:
            update_dim_city = '''
            UPDATE bq_retail.dim_city cc SET country_key = j.country_key FROM (
                SELECT DISTINCT r.City, r.Country, c.country_key
                FROM bq_retail.raw_stg_dim_customer r
                JOIN bq_retail.dim_country c ON r.Country = c.country 
            ) j
            WHERE cc.city = j.City and cc.country = j.Country
            '''
            query_job = client.query(update_dim_city)
            query_job.result()

            print('dim_city updated successfully with country_key.')

        except Exception as error:
            print(f'Loading failed for dim_city table: {error}')

    except Exception as error:
        print(f'Update failed for dim_city table: {error}')


def load_dim_customer():
    try:
        dcs = read_gbq('bq_retail.raw_stg_dim_customer', 'my-dw-demos-01')
        customer = dcs[['CustomerID', 'FirstName', 'LastName', 'Email', 'Phone', 'Address', 'Age', 'Gender']].copy()
        customer = customer.rename(
            columns={'CustomerID': 'customer_id', 'FirstName': 'first_name', 'LastName': 'last_name',
                     'Email': 'email', 'Phone': 'phone_number', 'Address': 'address', 'Age': 'age', 'Gender': 'gender'})
        customer = customer.drop_duplicates(subset=['customer_id', 'first_name', 'last_name'], keep='first')

        to_gbq(customer, 'bq_retail.dim_customer', project_id='my-dw-demos-01', if_exists='append')

        print('Customer data loaded successfully.')

        try:
            update_dim_customer = '''
            UPDATE bq_retail.dim_customer cc SET city_key = j.city_key FROM (
                SELECT * EXCEPT(row_num) FROM (
                    SELECT *, ROW_NUMBER() OVER(PARTITION BY CustomerID) AS row_num
                    FROM bq_retail.raw_stg_dim_customer r
                    JOIN bq_retail.dim_city c ON r.City = c.city
                ) WHERE row_num = 1
            ) AS j
            WHERE cc.customer_id = j.CustomerID
            '''
            query_job = client.query(update_dim_customer)
            query_job.result()

            print('dim_customer updated successfully with city_key.')

        except Exception as error:
            print(f'Update failed for dim_customer table: {error}')

    except Exception as error:
        print(f'Loading failed for dim_customer table: {error}')


def load_dim_date():
    try:
        dt = read_gbq('bq_retail.raw_stg_fact_transaction', 'my-dw-demos-01')
        date = dt[['Timestamp']].copy()
        date['Timestamp'] = pd.to_datetime(date['Timestamp'])
        date['is_weekend'] = date['Timestamp'].dt.weekday >= 5
        date['month'] = date['Timestamp'].dt.month
        date['year'] = date['Timestamp'].dt.year
        date['quarter'] = date['Timestamp'].dt.quarter
        date['half_year'] = ((date['month']-1) // 6) + 1
        date = date.rename(columns={'Timestamp': 'transaction_date'})
        date = date.drop_duplicates(subset=['transaction_date'], keep='first')

        to_gbq(date, 'bq_retail.dim_date', project_id='my-dw-demos-01', if_exists='append')

        print('Date data loaded successfully.')

    except Exception as error:
        print(f'Loading failed for dim_date table: {error}')


def load_fact_transaction():
    try:
        df = read_gbq('bq_retail.raw_stg_fact_transaction', 'my-dw-demos-01')
        fact = df[['TransactionID', 'CustomerID', 'ProductID', 'Timestamp', 'Quantity']].copy()

        fact['Timestamp'] = pd.to_datetime(fact['Timestamp'])
        fact = fact.rename(columns={'CustomerID': 'customer_id', 'ProductID': 'product_id',
                                    'Timestamp': 'transaction_date',})

        dc = read_gbq('bq_retail.dim_customer', 'my-dw-demos-01')
        dp = read_gbq('bq_retail.dim_product', 'my-dw-demos-01')
        dt = read_gbq('bq_retail.dim_date', 'my-dw-demos-01')

        merged_fact = (fact
                       .merge(dc, on='customer_id', how='left')
                       .merge(dp, on='product_id', how='left')
                       .merge(dt, on='transaction_date', how='left')
                       )

        final_fact = merged_fact[['TransactionID', 'customer_key', 'product_key', 'date_key', 'Quantity', 'price']].copy()
        final_fact['sales'] = final_fact['Quantity'] * final_fact['price']

        final_fact = final_fact.rename(columns={'TransactionID': 'transaction_id','Quantity': 'quantity',
                                                'price': 'transaction_price'})
        final_fact = final_fact.drop_duplicates(subset=['transaction_id'], keep='first')

        to_gbq(final_fact, 'bq_retail.fact_transaction', project_id='my-dw-demos-01', if_exists='append')

        print('Fact data loaded successfully.')

    except Exception as error:
        print(f'Error with loading fact_transaction table: {error}')


load_raw_staging()

load_dim_product()

load_dim_country()

load_dim_city()

load_dim_customer()

load_dim_date()

load_fact_transaction()