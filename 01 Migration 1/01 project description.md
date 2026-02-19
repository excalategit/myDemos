Project Description

The focus here is to show another data engineering activity, which is 
the migration of an already normalized dataset from a source to a 
target database, while keeping the data model consistent. DE do not always 
involve the migration of a denormalized dataset to a normalized one.

In the modified version of the pipeline, the model was further normalized to
optimize it for BI purposes i.e. extracting City and Country to their own tables. 
City table now contains additional geographical context, this was missing in 
previous designs.

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
- transform source fact_table, join its dataframe to dim_table dataframes, 
then load final joined dataframe to target fact_table (selected method)
- add target dim_table's surrogate keys to source fact_table, transform 
and load the appended source fact_table to target fact_table.
- using SQL, create a new fact table from source fact_table and target 
dim_tables join (if no transformation is required)

The 3 options above also show that a derived staging table is not 
mandatory, it all depends on the design.