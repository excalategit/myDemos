Project Description

The focus here is to show another data engineering activity which is 
the migration of an already normalized dataset from a source to a 
target database, while keeping the data model consistent. Data 
engineering is not always about the migration of a denormalized dataset 
to a normalized one.

The project is showcasing the loading method for a scenario where the 
data model is consistent between source and target systems, therefore 
emphasis was taken away from showcasing a heavy transformation layer, 
or improving the data model, as usual.

Where the sale (dim_transaction) and product tables would normally have 
a many-to-many relationship, here the relationship has already been 
converted to a one-to-many from the source system (using a junction 
table). Each product is now connected to a unique sale. So the 
transaction table here is the actual junction table from the source 
system, where the surrogate keys of the original transaction table 
have been removed.

3 methods were investigated for loading the target fact table here:
- transform raw_fact table and join its dataframe to dim_table dataframes, 
then load final joined dataframe to target  fact_table (selected method)
- add target dim table's surrogate keys to raw_fact table, transform 
and load the appended raw_fact table to target fact_table.
- using SQL, create a new fact table from raw_fact and target dim 
- tables join (if no transformation is required)

The 3 options above also show that a derived staging table is not 
mandatory, it all depends on the design.