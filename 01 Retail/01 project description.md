Project Description

The focus here is to show another data engineering activity which is the migration of a normalized dataset from a source to a target database. It's not always the migration of a denormalized dataset to a normalized one.

The project is showcasing the loading method for a scenario where the data model is consistent between source and target systems, therefore emphasis was taken away from creating a busy transformation layer, or improving the data model as usual.
Where the sale (dim_transaction) and product tables would normally have a many-to-many relationship, here the relationship has already been converted to a one-to-many from the source system (using a junction table), each product is now connected to a unique sale. So the transaction table here is the actual junction table from the source system, where the surrogate keys of the original transaction table have been removed.

3 possible methods of loading fact:
- transform fact_table and join dataframe to dim_table dataframes, load final dataframe to new fact_table (selected method)
- add dim table surrogate keys to raw_fact table, transform and load to new fact_table
- using SQL, create a new fact table from raw_fact and dim tables join (if no transformation is required)
The 3 options above show that a derived staging table is not mandatory, depends on the design.