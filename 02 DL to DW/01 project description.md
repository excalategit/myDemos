Project Description

The project showcases a pipeline that sources its data from a GCS bucket and 
delivers it to BigQuery. 

The exercise further clarifies that there is no fixed or standard pipeline 
design, rather it is dependent on factors like the technology employed, types 
and number of data sources, data format, and other realities.

The pipeline avoids the creation of a derived staging table and instead fetches
data directly from the raw tables, transforms in-flight, and loads to target 
tables. This avoids a central transformation layer and the need to load 
surrogate keys from dim tables back to derived staging, overall making the 
pipeline flow more streamlined.

Double fact tables: In the data model, a many-to-many relationship exists 
between the fact_sale and the dim_product tables and this was resolved by 
creating a junction table between them, which became a second fact table.

Finally, this script has been further optimized for automation with the 
introduction of 'raise' to the try-except blocks ensuring that during an 
automated execution, errors are not absorbed but 'called out' so they can be 
addressed.

Implications/insights:

- Introduction to STRUCT and ARRAYS in nested JSON, and their implications on 
the code.
- The idea of using a central config layer (YAML file) for managing data 
types was introduced.
- Pandas is still used for data transformation due to the small size of the 
data, instead of dbt and the likes.

