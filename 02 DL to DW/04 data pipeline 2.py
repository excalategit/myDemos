import pandas as pd
import yaml
from google.cloud import bigquery
from pandas_gbq import read_gbq
from pandas_gbq import to_gbq
from time import time

client = bigquery.Client()


# Fetching raw data blobs from a GCS bucket into BigQuery tables.
def extract_product():
    try:
        uri = 'gs://my-dw-bucket-02/bq_source_data_04.ndjson'
        destination_table = 'bq_api.raw_stg_prod_data'

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, autodetect=True
        )

        # The API data contains nested JSON objects however, BigQuery can accept nested objects
        # and therefore this is not necessary at this point.
        load_job = client.load_table_from_uri(uri, destination_table, job_config=job_config)
        load_job.result()

        print(f'Raw product data loaded successfully.')

    except Exception as error:
        print(f'Error with loading product data to raw staging: {error}')
        raise


def extract_sales():
    try:
        uri = 'gs://my-dw-bucket-02/bq_source_data_05.ndjson'
        destination_table = 'bq_api.raw_stg_sales_data'

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, autodetect=True
        )

        load_job = client.load_table_from_uri(uri, destination_table, job_config=job_config)
        load_job.result()

        print(f'Raw sales data loaded successfully.')

    except Exception as error:
        print(f'Error with loading sales data to raw staging: {error}')
        raise


def extract_user():
    try:
        uri = 'gs://my-dw-bucket-02/bq_source_data_06.ndjson'
        destination_table = 'bq_api.raw_stg_user_data'

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, autodetect=True
        )

        load_job = client.load_table_from_uri(uri, destination_table, job_config=job_config)
        load_job.result()

        print(f'Raw user data loaded successfully.')

    except Exception as error:
        print(f'Error with loading user data to raw staging: {error}')
        raise


def enforcer(table_name, dataframe):
    # Fetching the schema and null handling specifications from a config file.
    with open('config_file.yml') as f:
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

        # Instead of generally replacing any errors with None, certain cases may be addressed using the
        # Business' preferences, also stored in the config file.
        null_replacement_spec = config['tables'][table_name]['null_replacements']
        for col, fill_value in null_replacement_spec.items():
            dataframe[col] = dataframe[col].fillna(fill_value)

    return dataframe


# Creating the dimension and fact tables
def create_tables():
    try:
        create_dim_product = '''
        CREATE TABLE IF NOT EXISTS bq_api.dim_product (
        product_key STRING DEFAULT GENERATE_UUID(),
        product_id INT64,
        product_name STRING,
        description STRING,
        category STRING,
        image STRING,
        rating FLOAT64
        )'''

        query_job = client.query(create_dim_product)
        query_job.result()

        create_dim_city = '''
        CREATE TABLE IF NOT EXISTS bq_api.dim_city (
        city_key STRING DEFAULT GENERATE_UUID(),
        city STRING
        )'''

        query_job = client.query(create_dim_city)
        query_job.result()

        create_dim_customer = '''
        CREATE TABLE IF NOT EXISTS bq_api.dim_customer (
        customer_key STRING DEFAULT GENERATE_UUID(),
        customer_id INT,
        first_name STRING,
        last_name STRING,
        email STRING,
        username STRING,
        password STRING,
        phone STRING,
        street STRING,
        number INT64,
        zipcode STRING,
        latitude FLOAT64,
        longitude FLOAT64,
        city_key STRING
        )'''

        query_job = client.query(create_dim_customer)
        query_job.result()

        create_dim_date = '''
        CREATE TABLE IF NOT EXISTS bq_api.dim_date (
        date_key STRING DEFAULT GENERATE_UUID(),
        sale_date DATE,
        month INT64,
        year INT64
        )'''

        query_job = client.query(create_dim_date)
        query_job.result()

        create_fact_sale = '''
        CREATE TABLE IF NOT EXISTS bq_api.fact_sale (
        sale_key STRING DEFAULT GENERATE_UUID(),
        sales_id INT64,
        customer_key STRING,
        date_key STRING
        )'''

        query_job = client.query(create_fact_sale)
        query_job.result()

        create_fact_sale_product = '''
        CREATE TABLE IF NOT EXISTS bq_api.fact_sale_product (
        product_sale_key STRING DEFAULT GENERATE_UUID(),
        sale_key STRING,
        product_key STRING,
        price FLOAT64,
        quantity INT64,
        total_sale FLOAT64,
        stock INT64
        )'''

        query_job = client.query(create_fact_sale_product)
        query_job.result()

        print('All target tables created successfully.')

    except Exception as error:
        print(f'Error with table creation: {error}')
        raise


# Loading the target tables.
def load_dim_product():
    table_name = 'product_data_types'

    try:
        # Here, it becomes necessary to flatten the nested objects as pandas (read_gbq) cannot read them unless
        # flattened and named. Since this object is a STRUCT there is no UNNEST required to flatten it.
        flatten_query = '''
        SELECT id as product_id, title as product_name, description, category, image, rating.rate AS rating
        FROM bq_api.raw_stg_prod_data
        '''

        dp = client.query(flatten_query).to_dataframe()

        product = dp[['product_id', 'product_name', 'description', 'category', 'image', 'rating']].copy()

        enforcer(table_name, product)

        product = product.drop_duplicates(subset=['product_id', 'product_name'], keep='first')

        t1 = time()
        to_gbq(product, 'bq_api.dim_product', project_id='my-dw-demos-01', if_exists='append')
        t2 = time()

        load_time = t2 - t1

        print(f'Rows 0 to {len(product)} loaded successfully for dim_product in {load_time}s')

    except Exception as error:
        print(f'Error with loading {table_name}: {error}')
        raise


def load_dim_customer():
    table_name = 'customer_data_types'

    try:
        flatten_query = '''
        SELECT id as customer_id, email, username, password, phone, name.firstname as first_name, 
        name.lastname as last_name, address.street as street, address.number as number, address.zipcode as zipcode, 
        address.geolocation.lat as latitude, address.geolocation.long as longitude
        FROM bq_api.raw_stg_user_data
        '''

        dc = client.query(flatten_query).to_dataframe()
        customer = dc[['customer_id', 'email', 'username', 'password', 'phone', 'first_name', 'last_name',
                       'street', 'number', 'zipcode', 'latitude', 'longitude']].copy()

        # Capitalizing the values in certain columns
        customer['first_name'] = customer['first_name'].str.title()
        customer['last_name'] = customer['last_name'].str.title()
        customer['street'] = customer['street'].str.title()

        # Masking certain data fields in accordance with data governance requirements
        customer['password'] = '***Masked***'
        customer['phone'] = '***Masked***'

        enforcer(table_name, customer)

        customer = customer.drop_duplicates(subset=['customer_id', 'first_name', 'last_name'], keep='first')

        t1 = time()
        to_gbq(customer, 'bq_api.dim_customer', project_id='my-dw-demos-01', if_exists='append')
        t2 = time()

        load_time = t2 - t1

        print(f'Rows 0 to {len(customer)} loaded successfully for dim_customer in {load_time}s')

    except Exception as error:
        print(f'Error with loading {table_name}: {error}')
        raise


def load_dim_date():
    table_name = 'date_data_types'

    try:
        dt = read_gbq('bq_api.raw_stg_sales_data', 'my-dw-demos-01')
        date = dt[['date']].copy()

        enforcer(table_name, date)

        date['month'] = pd.to_datetime(date['sale_date'], errors="coerce").dt.month
        date['year'] = pd.to_datetime(date['sale_date'], errors="coerce").dt.year

        date = date.rename(columns={'date': 'sale_date'})
        date = date.drop_duplicates(subset=['sale_date'], keep='first')

        t1 = time()
        to_gbq(date, 'bq_api.dim_date', project_id='my-dw-demos-01', if_exists='append')
        t2 = time()

        load_time = t2 - t1

        print(f'Rows 0 to {len(date)} loaded successfully for dim_date in {load_time}s')

    except Exception as error:
        print(f'Error with loading {table_name}: {error}')
        raise


def transfer_surrogate_keys():
    try:
        load_dim_city = '''
        INSERT INTO bq_api.dim_city (city)
        SELECT DISTINCT address.city 
        FROM bq_api.raw_stg_user_data
        '''
        query_job = client.query(load_dim_city)
        query_job.result()

        print('dim_city loaded.')

    except Exception as error:
        print(f'Error with dim_city loading: {error}')
        raise

    # Fetching surrogate keys from dim_city and loading to dim_customer.
    try:
        update_dim_customer = '''
        UPDATE bq_api.dim_customer AS dc SET city_key = c.city_key
        FROM bq_api.raw_stg_user_data AS u
        JOIN bq_api.dim_city AS c ON u.address.city = c.city
        WHERE dc.customer_id = u.id
        '''
        query_job = client.query(update_dim_customer)
        query_job.result()

        print('dim_customer updated with city_keys.')

    except Exception as error:
        print(f'Error with dim_customer update: {error}')
        raise


def load_fact_sale():
    table_name = 'fact_sale_data_types'

    try:
        fact_sale_dataset = '''
        SELECT id as sales_id, c.customer_key, d.date_key
        FROM bq_api.raw_stg_sales_data s
        LEFT JOIN bq_api.dim_customer c ON s.`userId` = c.customer_id
        LEFT JOIN bq_api.dim_date d ON CAST (s.date as DATE) = d.sale_date
        '''

        ds = client.query(fact_sale_dataset).to_dataframe()

        fact_sale = ds[['sales_id', 'customer_key', 'date_key']].copy()

        enforcer(table_name, fact_sale)

        fact_sale = fact_sale.drop_duplicates(subset=['sales_id'], keep='first')

        t1 = time()
        to_gbq(fact_sale, 'bq_api.fact_sale', project_id='my-dw-demos-01', if_exists='append')
        t2 = time()

        load_time = t2 - t1

        print(f'Rows 0 to {len(fact_sale)} loaded successfully for fact_sale in {load_time}s')

    except Exception as error:
        print(f'Error with loading {table_name}: {error}')
        raise


def load_fact_sale_product():
    table_name = 'fact_prod_sale_data_types'

    try:
        # The products column is an ARRAY, to flatten it requires UNNEST before it can be used in a query.
        # Also, to preserve the fact table during enrichment with dim surrogate keys an outer join is used.
        fact_sale_product_dataset = '''
        SELECT f.sale_key, p.product_key, pp.price, flat.quantity, pp.price * flat.quantity as total_sale, 
        pp.rating.count as stock
        FROM bq_api.raw_stg_sales_data s, UNNEST (s.products) as flat
        JOIN bq_api.fact_sale f ON s.id = f.sales_id
        LEFT JOIN bq_api.dim_product p ON flat.`productId` = p.product_id
        JOIN bq_api.raw_stg_prod_data pp on p.product_id = pp.id 
        '''

        df = client.query(fact_sale_product_dataset).to_dataframe()

        fact_sale_product = df[['sale_key', 'product_key', 'price', 'quantity', 'total_sale', 'stock']].copy()

        enforcer(table_name, fact_sale_product)

        fact_sale_product = fact_sale_product.drop_duplicates(subset=['sale_key', 'product_key'], keep='first')

        t1 = time()
        to_gbq(fact_sale_product, 'bq_api.fact_sale_product', project_id='my-dw-demos-01', if_exists='append')
        t2 = time()

        load_time = t2 - t1

        print(f'Rows 0 to {len(fact_sale_product)} loaded successfully for fact_sale_product in {load_time}s')

    except Exception as error:
        print(f'Error with fact_product_sale loading: {error}')
        raise


extract_product()

extract_sales()

extract_user()

create_tables()

load_dim_product()

load_dim_customer()

load_dim_date()

transfer_surrogate_keys()

load_fact_sale()

load_fact_sale_product()