Project Description

The project showcases a pipeline that sources its data from a GCS bucket 
and delivers it to BigQuery. 

The exercise further clarifies that there is no fixed or standard pipeline design, rather it is dependent on factors 
like the technology employed, types and number of data sources, data format, and other realities.

There is a stronger awareness of the need to understand the data and the business insights intended in order to 
determine the table joins required. In this script the use case requires all data i.e. all products in the database, 
whether sold or not, therefore OUTER JOINs were used. It is also possible that the use case could require partial data 
e.g. only products that were sold, in which case INNER JOINs would be used. Also, the ids of the original source tables 
need to be preserved on the final joined table.

Double fact tables: In the data model, a many-to-many relationship exists between the sale (fact) table and the product 
(dimension) table and this was resolved by creating a junction table between them, which became a second fact table. 
This required a design where the surrogate keys of the original fact table were first added to the staging 
table and then copied together with all other surrogate keys and other quantitative data to the junction table.

Finally, this script has been further optimized for automation with the introduction of 'raise' to the try-except blocks 
ensuring that during an automated execution, errors are not absorbed but 'called out' so they can be addressed.

Implications/insights:

- Because the DL contains unstructured data by design, the extraction-to-DW layer was updated to read .json and not .sql
or .xlsx as usual.
- Because the transformation of the data can be carried out only when the data is already in the DW, the blobs were 
first loaded to an initial BigQuery staging table in their raw form, after which they were transformed, before 
finally joining them into one cleaned staging table.
- Pandas is still used for data transformation due to the small size of the data, instead of dbt and the likes.

