import pandas as pd
import yaml
from google.cloud import bigquery
from pandas_gbq import read_gbq
from pandas_gbq import to_gbq
from time import time

client = bigquery.Client()

def extract_data():
    try:
        uri = 'gs://my-dw-bucket-01/pharma_raw_data.csv'
        destination_table = 'bq_pharma.raw_stg_pharma_data'

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV, autodetect=True
        )

        load_job = client.load_table_from_uri(uri, destination_table, job_config=job_config)
        load_job.result()

        print(f'Raw pharma data loaded successfully.')

    except Exception as error:
        print(f'Error with loading pharma data to raw staging: {error}')
        raise


def enforcer(table_name, dataframe):
    # Fetching the schema and null handling specifications from a config file.
    with open('Pharma/config_file.yml') as f:
        config = yaml.safe_load(f)

        # Assigning the pre-determined data types from the config and flagging any values not
        # matching the assigned data type (flagged as NaN/NaT).
        schema_spec = config['tables'][table_name]['schema']
        for col, dtype in schema_spec.items():
            if dtype == 'Int64':
                dataframe[col] = pd.to_numeric(dataframe[col], errors="coerce").astype('Int64')
            elif dtype == 'Float64':
                dataframe[col] = pd.to_numeric(dataframe[col], errors="coerce").astype('Float64')
            elif dtype == 'datetime64':
                dataframe[col] = pd.to_datetime(dataframe[col], errors="coerce").dt.normalize()
            elif dtype == 'string':
                dataframe[col] = dataframe[col].astype('string')

        # Replacing any NaN/NaT with None.
        dataframe = dataframe.where(pd.notna(dataframe), None)

        # Instead of generally replacing any errors with None, they may also be replaced with the
        # Business' preferences, which is also stored in the config file. These preferences may
        # not always be available hence the different syntax and conditional statement.
        null_replacement_spec = config['tables'][table_name].get('null_replacement')
        if null_replacement_spec:
            for col, fill_value in null_replacement_spec.items():
                dataframe[col] = dataframe[col].fillna(fill_value)

    return dataframe


# Creating the dimension and fact tables
def create_tables():
    try:
        create_dim_product = '''
        CREATE TABLE IF NOT EXISTS bq_pharma.dim_product (
        product_key STRING DEFAULT GENERATE_UUID(),
        product STRING,
        product_class STRING
        )'''

        query_job = client.query(create_dim_product)
        query_job.result()

        create_dim_distributor = '''
        CREATE TABLE IF NOT EXISTS bq_pharma.dim_distributor (
        distributor_key STRING DEFAULT GENERATE_UUID(),
        distributor STRING
        )'''

        query_job = client.query(create_dim_distributor)
        query_job.result()

        create_dim_sales_rep = '''
        CREATE TABLE IF NOT EXISTS bq_pharma.dim_sales_rep (
        sales_rep_key STRING DEFAULT GENERATE_UUID(),
        sales_rep STRING,
        sales_team STRING,
        manager STRING
        )'''

        query_job = client.query(create_dim_sales_rep)
        query_job.result()

        create_dim_country = '''
        CREATE TABLE IF NOT EXISTS bq_pharma.dim_country (
        country_key STRING DEFAULT GENERATE_UUID(),
        country STRING
        )'''

        query_job = client.query(create_dim_country)
        query_job.result()

        create_dim_city = '''
        CREATE TABLE IF NOT EXISTS bq_pharma.dim_city (
        city_key STRING DEFAULT GENERATE_UUID(),
        city STRING,
        longitude FLOAT64,
        latitude FLOAT64,
        country_key STRING
        )'''

        query_job = client.query(create_dim_city)
        query_job.result()

        create_dim_customer = '''
        CREATE TABLE IF NOT EXISTS bq_pharma.dim_customer (
        customer_key STRING DEFAULT GENERATE_UUID(),
        customer_name STRING,
        channel STRING,
        sub_channel STRING,
        city_key STRING
        )'''

        query_job = client.query(create_dim_customer)
        query_job.result()

        create_dim_distributor_customer = '''
        CREATE TABLE IF NOT EXISTS bq_pharma.dim_distributor_customer (
        distributor_customer_key STRING DEFAULT GENERATE_UUID(),
        distributor_key STRING,
        customer_key STRING
        )'''

        query_job = client.query(create_dim_distributor_customer)
        query_job.result()

        create_dim_date = '''
        CREATE TABLE IF NOT EXISTS bq_pharma.dim_date (
        month_key STRING DEFAULT GENERATE_UUID(),
        month STRING,
        quarter INT64,
        year INT64
        )'''

        query_job = client.query(create_dim_date)
        query_job.result()

        create_fact_sale = '''
        CREATE TABLE IF NOT EXISTS bq_pharma.fact_sale (
        sale_key STRING DEFAULT GENERATE_UUID(),
        price FLOAT64,
        quantity FLOAT64,
        total_sale FLOAT64,
        customer_key STRING,
        product_key STRING,
        month_key STRING,
        sales_rep_key STRING
        )'''

        query_job = client.query(create_fact_sale)
        query_job.result()

        print('All target tables created successfully.')

    except Exception as error:
        print(f'Error with table creation: {error}')
        raise


# Loading the target tables.
def load_dim_product():
    table_name = 'dim_product'

    try:
        dp = read_gbq('bq_pharma.raw_stg_pharma_data', 'my-dw-demos-01')
        product = dp[['Product Name', 'Product Class']].copy()
        product = product.rename(columns={'Product Name': 'product', 'Product Class': 'product_class'})

        enforcer(table_name, product)

        product = product.drop_duplicates(subset=['product', 'product_class'], keep='first')

        t1 = time()
        to_gbq(product, 'bq_pharma.dim_product', project_id='my-dw-demos-01', if_exists='append')
        t2 = time()

        load_time = t2 - t1

        print(f'Rows 0 to {len(product)} loaded successfully for {table_name} in {load_time}s')

    except Exception as error:
        print(f'Error with loading {table_name}: {error}')
        raise


def load_dim_distributor():
    table_name = 'dim_distributor'

    try:
        df = read_gbq('bq_pharma.raw_stg_pharma_data', 'my-dw-demos-01')
        distributor = df[['Distributor']].copy()
        distributor = distributor.rename(columns={'Distributor': 'distributor'})

        enforcer(table_name, distributor)

        distributor = distributor.drop_duplicates(subset=['distributor'], keep='first')

        t1 = time()
        to_gbq(distributor, 'bq_pharma.dim_distributor', project_id='my-dw-demos-01', if_exists='append')
        t2 = time()

        load_time = t2 - t1

        print(f'Rows 0 to {len(distributor)} loaded successfully for {table_name} in {load_time}s')

    except Exception as error:
        print(f'Error with loading {table_name}: {error}')
        raise


def load_sales_rep():
    table_name = 'dim_sales_rep'

    try:
        df = read_gbq('bq_pharma.raw_stg_pharma_data', 'my-dw-demos-01')
        sales_rep = df[['Name of Sales Rep', 'Sales Team', 'Manager']].copy()
        sales_rep = sales_rep.rename(columns={'Name of Sales Rep': 'sales_rep', 'Sales Team': 'sales_team',
                                                  'Manager': 'manager'})

        enforcer(table_name, sales_rep)

        sales_rep = sales_rep.drop_duplicates(subset=['sales_rep', 'sales_team', 'manager'], keep='first')

        t1 = time()
        to_gbq(sales_rep, 'bq_pharma.dim_sales_rep', project_id='my-dw-demos-01', if_exists='append')
        t2 = time()

        load_time = t2 - t1

        print(f'Rows 0 to {len(sales_rep)} loaded successfully for {table_name} in {load_time}s')

    except Exception as error:
        print(f'Error with loading {table_name}: {error}')
        raise


def load_dim_country():
    table_name = 'dim_country'

    try:
        df = read_gbq('bq_pharma.raw_stg_pharma_data', 'my-dw-demos-01')
        country = df[['Country']].copy()
        country = country.rename(columns={'Country': 'country'})

        enforcer(table_name, country)

        country = country.drop_duplicates(subset=['country'], keep='first')

        t1 = time()
        to_gbq(country, 'bq_pharma.dim_country', project_id='my-dw-demos-01', if_exists='append')
        t2 = time()

        load_time = t2 - t1

        print(f'Rows 0 to {len(country)} loaded successfully for {table_name} in {load_time}s')

    except Exception as error:
        print(f'Error with loading {table_name}: {error}')
        raise


def load_dim_city():
    table_name = 'dim_city'

    try:
        df = read_gbq('bq_pharma.raw_stg_pharma_data', 'my-dw-demos-01')
        dc = read_gbq('bq_pharma.dim_country', 'my-dw-demos-01')

        # City alone is not unique and therefore not a good business key, a better business key
        # is city plus its geographical context, therefore city+country_key is used.
        merged_df = (df
                       .merge(dc, left_on='Country', right_on='country', how='left')
                       )

        city = merged_df[['City', 'Longitude', 'Latitude', 'country_key']].copy()
        city = city.rename(columns={'City': 'city', 'Longitude': 'longitude', 'Latitude': 'latitude'})

        enforcer(table_name, city)

        city = city.drop_duplicates(subset=['city', 'country_key'], keep='first')

        t1 = time()
        to_gbq(city, 'bq_pharma.dim_city', project_id='my-dw-demos-01', if_exists='append')
        t2 = time()

        load_time = t2 - t1

        print(f'Rows 0 to {len(city)} loaded successfully for {table_name} in {load_time}s')

    except Exception as error:
        print(f'Error with loading {table_name}: {error}')
        raise


def load_dim_customer():
    table_name = 'dim_customer'

    try:
        df = read_gbq('bq_pharma.raw_stg_pharma_data', 'my-dw-demos-01')
        customer = df[['Customer Name', 'Channel', 'Sub-channel']].copy()
        customer = customer.rename(columns={'Customer Name': 'customer_name', 'Channel': 'channel',
                                                  'Sub-channel': 'sub_channel'})

        enforcer(table_name, customer)

        customer = customer.drop_duplicates(subset=['customer_name', 'channel'], keep='first')

        t1 = time()
        to_gbq(customer, 'bq_pharma.dim_customer', project_id='my-dw-demos-01', if_exists='append')
        t2 = time()

        load_time = t2 - t1

        print(f'Rows 0 to {len(customer)} loaded successfully for {table_name} in {load_time}s')

    except Exception as error:
        print(f'Error with loading {table_name}: {error}')
        raise

    # Fetching surrogate keys from dim_city and updating dim_customer.
    try:
        update_dim_customer = '''
        UPDATE bq_pharma.dim_customer cc SET city_key = j.city_key FROM (
            SELECT * EXCEPT(row_num) FROM (
                SELECT *, ROW_NUMBER() OVER(PARTITION BY `Customer Name` ORDER BY Year DESC) AS row_num
                FROM bq_pharma.raw_stg_pharma_data s
                JOIN bq_pharma.dim_city c ON s.City = c.city
            ) WHERE row_num = 1
        ) AS j
        WHERE cc.customer_name = j.`Customer Name`
        '''
        query_job = client.query(update_dim_customer)
        query_job.result()

        print('dim_customer updated successfully with city_key.')

    except Exception as error:
        print(f'Update failed for dim_customer table: {error}')


def load_dim_distributor_customer():
    try:
        loading_query = '''
        INSERT INTO bq_pharma.dim_distributor_customer (distributor_key, customer_key) (
            SELECT DISTINCT distributor_key, customer_key
            FROM bq_pharma.raw_stg_pharma_data s
            LEFT JOIN bq_pharma.dim_distributor d ON s.`Distributor` = d.distributor
            LEFT JOIN bq_pharma.dim_customer c ON s.`Customer Name` = c.customer_name
            )'''
        query_job = client.query(loading_query)
        query_job.result()

        print('dim_distributor_customer loaded.')

    except Exception as error:
        print(f'Error with loading dim_distributor_customer: {error}')
        raise


def load_dim_date():
    table_name = 'dim_date'
    try:
        df = read_gbq('bq_pharma.raw_stg_pharma_data', 'my-dw-demos-01')
        date = df[['Month', 'Year']].copy()

        # '%B' if full month name, '%b' if shortened e.g. Jan, Feb
        date['quarter'] = pd.to_datetime(df['Month'], format='%B').dt.quarter

        date = date.rename(columns={'Month': 'month', 'Year': 'year'})

        enforcer(table_name, date)

        date = date.drop_duplicates(subset=['month', 'quarter', 'year'], keep='first')

        t1 = time()
        to_gbq(date, 'bq_pharma.dim_date', project_id='my-dw-demos-01', if_exists='append')
        t2 = time()

        load_time = t2 - t1

        print(f'Rows 0 to {len(date)} loaded successfully for {table_name} in {load_time}s')

    except Exception as error:
        print(f'Error with loading {table_name}: {error}')
        raise


def load_fact_sale():
    table_name = 'fact_sale'
    try:
        fact_sale_dataset = '''
        SELECT s.`Price` as price, s.`Quantity` as quantity, s.`Sales` as total_sale, c.customer_key, p.product_key, 
        d.month_key, sr.sales_rep_key 
        FROM bq_pharma.raw_stg_pharma_data s
        JOIN bq_pharma.dim_customer c ON s.`Customer Name` = c.customer_name
        JOIN bq_pharma.dim_product p ON s.`Product Name` = p.product
        JOIN bq_pharma.dim_date d on s.`Month` = d.month
        JOIN bq_pharma.dim_sales_rep sr on s.`Name of Sales Rep` = sr.sales_rep
        '''

        fact_sale = client.query(fact_sale_dataset).to_dataframe()

        enforcer(table_name, fact_sale)

        fact_sale = fact_sale.drop_duplicates(subset=['price', 'quantity', 'total_sale', 'customer_key', 'product_key',
                                                      'sales_rep_key'], keep='first')

        t1 = time()
        to_gbq(fact_sale, 'bq_pharma.fact_sale', project_id='my-dw-demos-01', if_exists='append')
        t2 = time()

        load_time = t2 - t1

        print(f'Rows 0 to {len(fact_sale)} loaded successfully for fact_sale in {load_time}s')

    except Exception as error:
        print(f'Error with fact_sale loading: {error}')
        raise


extract_data()

create_tables()

load_dim_product()

load_dim_distributor()

load_sales_rep()

load_dim_country()

load_dim_city()

load_dim_customer()

load_dim_date()

load_dim_distributor_customer()

load_fact_sale()