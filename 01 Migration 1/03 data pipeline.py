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

    dim_customer = '''
    CREATE TABLE IF NOT EXISTS bq_retail.dim_customer (
    customer_key STRING DEFAULT GENERATE_UUID(),
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone_number STRING,
    address STRING,
    city STRING,
    country STRING,
    age INT64,
    gender STRING
    )'''
    query_job = client.query(dim_customer)
    query_job.result()

    fact_transaction = '''
    CREATE TABLE IF NOT EXISTS bq_retail.fact_transaction (
    transaction_key STRING DEFAULT GENERATE_UUID(),
    transaction_id STRING,
    transaction_date DATE,
    quantity INT64, 
    transaction_price FLOAT64,
    sales FLOAT64,
    product_key STRING,
    customer_key STRING
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


def load_dim_customer():
    try:
        dc = read_gbq('bq_retail.raw_stg_dim_customer', 'my-dw-demos-01')
        customer = dc[['CustomerID', 'FirstName', 'LastName', 'Email', 'Phone', 'Address', 'Age', 'Gender']].copy()
        customer = customer.rename(
            columns={'CustomerID': 'customer_id', 'FirstName': 'first_name', 'LastName': 'last_name',
                     'Email': 'email', 'Phone': 'phone_number', 'Address': 'address', 'City': 'city',
                     'Country': 'country', 'Age': 'age', 'Gender': 'gender'})
        customer = customer.drop_duplicates(subset=['customer_id', 'first_name', 'last_name'], keep='first')

        to_gbq(customer, 'bq_retail.dim_customer', project_id='my-dw-demos-01', if_exists='fail')

        print('Customer data loaded successfully.')

    except Exception as error:
        print(f'Loading failed for dim_customer table: {error}')


def load_fact_transaction():
    try:
        df = read_gbq('bq_retail.raw_stg_fact_transaction', 'my-dw-demos-01')
        fact = df[['TransactionID', 'CustomerID', 'ProductID', 'Timestamp', 'Quantity']].head(10000).copy()

        dc = read_gbq('bq_retail.dim_customer', 'my-dw-demos-01')
        dp = read_gbq('bq_retail.dim_product', 'my-dw-demos-01')

        fact = fact.rename(columns={'CustomerID': 'customer_id', 'ProductID': 'product_id'})

        merged_fact = (fact
                       .merge(dc, on='customer_id', how='left')
                       .merge(dp, on='product_id', how='left')
                       )

        final_fact = merged_fact[['TransactionID', 'Timestamp', 'customer_key', 'product_key', 'Quantity', 'price']].copy()
        final_fact['sales'] = final_fact['Quantity'] * final_fact['price']
        final_fact['Timestamp'] = pd.to_datetime(final_fact['Timestamp']).dt.date

        final_fact = final_fact.rename(columns={'TransactionID': 'transaction_id', 'Timestamp': 'transaction_date',
                                                'Quantity': 'quantity', 'price': 'transaction_price'})
        final_fact = final_fact.drop_duplicates(subset=['transaction_id'], keep='first')

        to_gbq(final_fact, 'bq_retail.fact_transaction', project_id='my-dw-demos-01', if_exists='fail')

        print('Fact data loaded successfully.')

    except Exception as error:
        print(f'Error with loading fact_transaction table: {error}')


load_raw_staging()

load_dim_product()

load_dim_customer()

load_fact_transaction()