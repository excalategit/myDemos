import pandas as pd
from google.cloud import bigquery
from pandas_gbq import read_gbq
from pandas_gbq import to_gbq
from time import time

client = bigquery.Client()

# Fetching raw data blobs from a GCS bucket into BigQuery tables
# and restructuring them so they can be joined together into one raw staging table.
def extract_product():
    try:
        uri = 'gs://my-dw-bucket-02/bq_source_data_04.json'
        destination_table = 'bigdata_api.prod_data_raw'

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, autodetect=True
        )

        load_job = client.load_table_from_uri(uri, destination_table, job_config=job_config)
        load_job.result()

        print(f'Raw product data loaded successfully.')

        try:
            dp = read_gbq('bigdata_api.prod_data_raw', 'my-dw-project-01')
            raw_prod = dp.copy()

            # Flattening nested JSON objects.
            clean_prod = raw_prod.join(pd.json_normalize(raw_prod['rating']))
            clean_prod = clean_prod.drop(columns=['rating'])

            to_gbq(clean_prod, 'bigdata_api.prod_data_clean', project_id='my-dw-project-01',
                   if_exists='fail')

            print('Product data processed successfully.')

        except Exception as error:
            print(f'Error with product table processing {error}')
            raise

    except Exception as error:
        print(f'Error with fetching product data from GCS {error}')
        raise


def extract_sales():
    try:
        uri = 'gs://my-dw-bucket-02/bq_source_data_05.json'
        destination_table = 'bigdata_api.sales_data_raw'

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, autodetect=True
        )

        load_job = client.load_table_from_uri(uri, destination_table, job_config=job_config)
        load_job.result()

        print(f'Raw sales data loaded successfully.')

        try:
            ds = read_gbq('bigdata_api.sales_data_raw', 'my-dw-project-01')
            raw_sales = ds.copy()

            raw_sales_explode = raw_sales.explode('products')
            products_expanded = raw_sales_explode['products'].apply(pd.Series)
            # Concat is used here as an alternative to join()
            clean_sales = pd.concat([raw_sales_explode, products_expanded], axis=1)
            clean_sales = clean_sales.drop(columns=['products'])
            clean_sales['date'] = pd.to_datetime(clean_sales['date']).dt.date
            clean_sales['month'] = pd.to_datetime(clean_sales['date']).dt.month
            clean_sales['year'] = pd.to_datetime(clean_sales['date']).dt.year
            to_gbq(clean_sales, 'bigdata_api.sales_data_clean', project_id='my-dw-project-01',
                   if_exists='fail')

            print('Sales data processed successfully.')

        except Exception as error:
            print(f'Error with sales table processing {error}')
            raise

    except Exception as error:
        print(f'Error with fetching sales data from GCS {error}')
        raise


def extract_user():
    try:
        uri = 'gs://my-dw-bucket-02/bq_source_data_06.json'
        destination_table = 'bigdata_api.user_data_raw'

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, autodetect=True
        )

        load_job = client.load_table_from_uri(uri, destination_table, job_config=job_config)
        load_job.result()

        print(f'Raw user data loaded successfully.')

        try:
            du = read_gbq('bigdata_api.user_data_raw', 'my-dw-project-01')
            raw_user = du.copy()

            raw_user = raw_user.join(pd.json_normalize(raw_user['address']))
            raw_user = raw_user.rename(columns={'geolocation.lat': 'geolocation_lat'})
            raw_user = raw_user.rename(columns={'geolocation.long': 'geolocation_long'})
            raw_user = raw_user.drop(columns=['address'])

            raw_user = raw_user.join(pd.json_normalize(raw_user['name']))
            clean_user = raw_user.drop(columns=['name'])

            to_gbq(clean_user, 'bigdata_api.user_data_clean', project_id='my-dw-project-01',
                   if_exists='fail')

            print('User data processed successfully.')

        except Exception as error:
            print(f'Error with user table processing {error}')
            raise

    except Exception as error:
        print(f'Error with fetching user data from GCS {error}')
        raise


# Creating a combined staging table from all 3 cleaned data sources.
def create_combo_staging():
    try:
        create_combined_stg_table = '''
        CREATE TABLE bigdata_api.stg_table_initial AS
        SELECT a.id as product_id, title, description, category, price, image, rate, count, b.id as sales_id, quantity, 
        date, month, year, c.id as customer_id, firstname, lastname, email, phone, username, password, city, 
        street, number, zipcode, geolocation_lat, geolocation_long
        FROM bigdata_api.prod_data_clean AS a
        LEFT JOIN bigdata_api.sales_data_clean AS b on a.id = b.`productId`
        FULL OUTER JOIN bigdata_api.user_data_clean AS c on b.`userId` = c.id
        '''

        query_job = client.query(create_combined_stg_table)
        query_job.result()

        print('Initial staging table created successfully.')

    except Exception as error:
        print(f'Error with combo staging table creation: {error}')
        raise


def el_transform():
    try:
        ds = read_gbq('bigdata_api.stg_table_initial', 'my-dw-project-01')
        source_table = ds.copy()

        # Setting column data types.
        schema = {
            'product_id': 'Int64',
            'title': 'string',
            'description': 'string',
            'category': 'string',
            'price': 'float',
            'image': 'string',
            'rate': 'float',
            'count': 'Int64',
            'sales_id': 'Int64',
            'quantity': 'Int64',
            'date': 'datetime64',
            'month': 'Int64',
            'year': 'Int64',
            'customer_id': 'Int64',
            'firstname': 'string',
            'lastname': 'string',
            'email': 'string',
            'phone': 'string',
            'username': 'string',
            'password': 'string',
            'city': 'string',
            'street': 'string',
            'number': 'Int64',
            'zipcode': 'string',
            'geolocation_lat': 'float',
            'geolocation_long': 'float'
        }

        # Assigning the pre-determined data types. Any values not
        # matching the assigned data type will be flagged as NaN/NaT.
        for col, dtype in schema.items():
            if dtype == 'Int64':
                source_table[col] = pd.to_numeric(source_table[col], errors="coerce").astype('Int64')
            elif dtype == 'float':
                source_table[col] = pd.to_numeric(source_table[col], errors="coerce").astype('float64')
            elif dtype == 'datetime64':
                source_table[col] = pd.to_datetime(source_table[col], errors="coerce")

        # Replacing NaN/NaT with None.
        source_table = source_table.where(pd.notna(source_table), None)

        # Applying default fills by data type groups
        str_cols = source_table.select_dtypes(include='object').columns
        num_cols = source_table.select_dtypes(include='number').columns
        bool_cols = source_table.select_dtypes(include='bool').columns

        source_table[str_cols] = source_table[str_cols].fillna('NA')
        source_table[num_cols] = source_table[num_cols].fillna(0)
        source_table[bool_cols] = source_table[bool_cols].fillna(False)

        # Capitalizing the values in certain columns
        source_table['firstname'] = source_table['firstname'].str.title()
        source_table['lastname'] = source_table['lastname'].str.title()
        source_table['street'] = source_table['street'].str.title()

        source_table = source_table.rename(columns={'title': 'product_name',
                                                    'rate': 'rating', 'firstname': 'first_name',
                                                    'lastname': 'last_name', 'geolocation_lat': 'latitude',
                                                    'geolocation_long': 'longitude', 'date': 'sale_date'})

        # Masking certain data fields in accordance with data governance requirements
        source_table['password'] = '***Masked***'
        source_table['phone'] = '***Masked***'

        source_table = source_table.drop_duplicates()

        to_gbq(source_table, 'bigdata_api.stg_table_final',
               project_id='my-dw-project-01', if_exists='fail')

        print(f'Extraction to final staging table completed. {len(source_table)} rows loaded.')

        # Addition of surrogate key columns to the staging table
        try:
            update_staging = '''
            ALTER TABLE bigdata_api.stg_table_final
            ADD COLUMN product_key STRING,
            ADD COLUMN customer_key STRING,
            ADD COLUMN date_key STRING,
            ADD COLUMN sale_key STRING
            '''

            query_job = client.query(update_staging)
            query_job.result()

            print('Surrogate key columns added to staging.')

        except Exception as error:
            print(f'Error with creating surrogate key columns: {error}')
            raise

    except Exception as error:
        print(f'Extraction to final staging table failed: {error}')
        raise


# Creating the dimension and fact tables
def create_tables():
    try:
        create_dim_product = '''
        CREATE TABLE IF NOT EXISTS bigdata_api.dim_product (
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
        CREATE TABLE IF NOT EXISTS bigdata_api.dim_city (
        city_key STRING DEFAULT GENERATE_UUID(),
        city STRING
        )'''

        query_job = client.query(create_dim_city)
        query_job.result()

        create_dim_customer = '''
        CREATE TABLE IF NOT EXISTS bigdata_api.dim_customer (
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
        CREATE TABLE IF NOT EXISTS bigdata_api.dim_date (
        date_key STRING DEFAULT GENERATE_UUID(),
        sale_date DATE,
        month INT64,
        year INT64
        )'''

        query_job = client.query(create_dim_date)
        query_job.result()

        create_fact_sale = '''
        CREATE TABLE IF NOT EXISTS bigdata_api.fact_sale (
        sale_key STRING DEFAULT GENERATE_UUID(),
        sales_id INT64,
        customer_key STRING,
        date_key STRING
        )'''

        query_job = client.query(create_fact_sale)
        query_job.result()

        create_fact_sale_product = '''
        CREATE TABLE IF NOT EXISTS bigdata_api.fact_sale_product (
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
    table_name = 'dim_product'

    try:
        dp = read_gbq('bigdata_api.stg_table_final', 'my-dw-project-01')
        product = dp[['product_id', 'product_name', 'description', 'category', 'image', 'rating']].copy()
        product = product.drop_duplicates(subset=['product_id', 'product_name'], keep='first')

        t1 = time()
        to_gbq(product, 'bigdata_api.dim_product', project_id='my-dw-project-01', if_exists='append')
        t2 = time()

        load_time = t2-t1

        print(f'Rows 0 to {len(product)} loaded successfully for {table_name} in {load_time}s')

    except Exception as error:
        print(f'Error with loading {table_name}: {error}')
        raise


def load_dim_customer():
    table_name = 'dim_customer'

    try:
        dc = read_gbq('bigdata_api.stg_table_final', 'my-dw-project-01')
        customer = dc[['customer_id', 'email', 'username', 'password', 'phone', 'first_name', 'last_name',
                   'street', 'number', 'zipcode', 'latitude', 'longitude']].copy()
        customer = customer.drop_duplicates(subset=['customer_id', 'first_name', 'last_name'], keep='first')

        t1 = time()
        to_gbq(customer, 'bigdata_api.dim_customer', project_id='my-dw-project-01', if_exists='append')
        t2 = time()

        load_time = t2 - t1

        print(f'Rows 0 to {len(customer)} loaded successfully for {table_name} in {load_time}s')

    except Exception as error:
        print(f'Error with loading {table_name}: {error}')
        raise


def load_dim_date():
    table_name = 'dim_date'

    try:
        dt = read_gbq('bigdata_api.stg_table_final', 'my-dw-project-01')
        date = dt[['sale_date', 'month', 'year']].copy()
        date = date.drop_duplicates(subset=['sale_date'], keep='first')

        t1 = time()
        to_gbq(date, 'bigdata_api.dim_date', project_id='my-dw-project-01', if_exists='append')
        t2 = time()

        load_time = t2 - t1

        print(f'Rows 0 to {len(date)} loaded successfully for {table_name} in {load_time}s')

    except Exception as error:
        print(f'Error with loading {table_name}: {error}')
        raise


# Filling surrogate key columns with the actual surrogate keys.
def upload_surrogate_keys():
    try:
        product_key = '''
        UPDATE bigdata_api.stg_table_final AS s SET product_key = p.product_key 
        FROM bigdata_api.dim_product AS p WHERE s.product_id = p.product_id AND
        s.product_name = p.product_name'''
        query_job = client.query(product_key)
        query_job.result()

        print('product_key update on staging: check.')

        try:
            customer_key = '''
            UPDATE bigdata_api.stg_table_final AS s SET customer_key = u.customer_key 
            FROM bigdata_api.dim_customer AS u WHERE s.customer_id = u.customer_id AND
            s.first_name = u.first_name AND s.last_name = u.last_name'''
            query_job = client.query(customer_key)
            query_job.result()

            print('customer_key update on staging: check.')

            try:
                date_key = '''
                UPDATE bigdata_api.stg_table_final AS s SET date_key = d.date_key 
                FROM bigdata_api.dim_date AS d WHERE s.sale_date = d.sale_date'''
                query_job = client.query(date_key)
                query_job.result()

                print('date_key update on staging: check.')

                # Fetching surrogate keys from dim_city and loading to dim_customer.
                try:
                    load_dim_city = '''
                    INSERT INTO bigdata_api.dim_city (city)
                    SELECT DISTINCT city FROM bigdata_api.stg_table_final'''
                    query_job = client.query(load_dim_city)
                    query_job.result()

                    print('dim_city loaded.')

                    # Using a window function to filter the source table for unique records (BQ restriction)
                    # before running the cross-table update (WHERE u.customer_id = j.customer_id)
                    try:
                        update_dim_customer = '''
                        UPDATE bigdata_api.dim_customer AS u SET city_key = j.city_key
                        FROM (
                            SELECT * EXCEPT(rank) FROM (
                                SELECT *, ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY sale_date) AS rank
                                FROM bigdata_api.stg_table_final AS s
                                JOIN bigdata_api.dim_city AS c ON s.city = c.city
                                ) WHERE rank = 1
                            ) AS j
                        WHERE u.customer_id = j.customer_id
                        '''
                        query_job = client.query(update_dim_customer)
                        query_job.result()

                        print('dim_customer updated with city_keys.')

                    except Exception as error:
                        print(f'Error with dim_customer update: {error}')
                        raise

                except Exception as error:
                    print(f'Error with dim_city loading: {error}')
                    raise

            except Exception as error:
                print(f'Error with date_key update: {error}')
                raise

        except Exception as error:
            print(f'Error with customer_key update: {error}')
            raise

    except Exception as error:
        print(f'Error with product_key update: {error}')
        raise


def load_fact_sale():
    table_name = 'fact_sale'

    try:
        df = read_gbq('bigdata_api.stg_table_final', 'my-dw-project-01')
        fact = df[['sales_id', 'customer_key', 'date_key']].copy()

        fact = fact.drop_duplicates(subset=['sales_id', 'customer_key'], keep='first')

        # Finally, in order to ensure unique sales data on this fact table, as required,
        # all rows where a sale is not associated with any product should be removed.
        fact = fact[fact['sales_id'] > 0]

        t1 = time()
        to_gbq(fact, 'bigdata_api.fact_sale', project_id='my-dw-project-01', if_exists='append')
        t2 = time()

        load_time = t2 - t1

        print(f'Rows 0 to {len(fact)} loaded successfully for {table_name} in {load_time}s')

    except Exception as error:
        print(f'Error with loading {table_name}: {error}')
        raise


def load_fact_sale_product():
    table_name = 'fact_sale_product'

    # First, load fact table's surrogate key to staging, this way all surrogate keys
    # are complete in staging.
    try:
        sale_key = '''
        UPDATE bigdata_api.stg_table_final AS s SET sale_key = f.sale_key 
        FROM bigdata_api.fact_sale AS f WHERE s.sales_id = f.sales_id AND
        s.customer_key = f.customer_key'''
        query_job = client.query(sale_key)
        query_job.result()

        print('sale_key updated successfully on fact_sale.')

        # Then load all required data from staging to fact_product_sales
        try:
            df = read_gbq('bigdata_api.stg_table_final', 'my-dw-project-01')
            fact = df[['sale_key', 'product_key', 'price', 'quantity', 'count']].copy()
            fact = fact.rename(columns={'count': 'stock'})
            fact['total_sale'] = fact['price'] * fact['quantity']

            # All rows where a sale_key is blank should be removed.
            fact = fact[fact['sale_key'].notna()]

            t1 = time()
            to_gbq(fact, 'bigdata_api.fact_sale_product', project_id='my-dw-project-01', if_exists='append')
            t2 = time()

            load_time = t2 - t1

            print(f'Rows 0 to {len(fact)} loaded successfully for {table_name} in {load_time}s')

        except Exception as error:
            print(f'Error with fact_product_sale loading: {error}')
            raise

    except Exception as error:
        print(f'Error with sale_key update: {error}')
        raise


extract_product()
extract_sales()
extract_user()

create_combo_staging()
el_transform()
create_tables()

load_dim_product()
load_dim_customer()
load_dim_date()
upload_surrogate_keys()

load_fact_sale()
load_fact_sale_product()