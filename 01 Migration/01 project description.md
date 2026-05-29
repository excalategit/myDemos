Project Description

The project focuses on showcasing another data engineering activity, which is 
the migration of an already normalized dataset from a source to a 
target database, while keeping the data model consistent.

There are 2 files for the pipeline code, in the modified version, the data model 
was further normalized extracting City and Country to their own tables, aligning
with best practice. City alone is not a good enough business key on a city table
because it is not unique, the re-modeling instead uses City together with its 
geographical context, in this case, city+country_key as the business key.

Since the project is showcasing the loading method for a scenario where the 
data model is consistent between source and target systems, the emphasis was 
taken away from showcasing a heavy transformation layer.

Usually, the sale (fact_transaction) and dim_product tables would have a 
many-to-many relationship, but here the relationship has already been converted
to a one-to-many from the source system (using a junction table) ensuring that 
each product is connected to a unique sale. So the fact_transaction table here
is the actual junction table from the source system, except its original 
surrogate keys are absent in the data.

3 methods were investigated for loading the target fact table here:
- transform source fact_table, join its dataframe to dim_table dataframes, 
then load final joined dataframe containing required attributes to target 
fact_table (selected method).
- add target dim_table's surrogate keys to source fact_table, transform 
and load the appended source fact_table to target fact_table.
- using SQL, create a new fact table using source fact_table and target 
dim_tables join (this would have been the selected method if no transformation 
was required).

The 3 options above also show that a derived staging table is not 
mandatory, it all depends on preference or on what is most efficient.