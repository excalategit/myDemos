Project Description

The project uses a denormalized dataset hosted on a data lake as source data
and it showcases the normalization of the data into fact and dimension tables
in a warehouse.

The business key(s) of a table and where it is located impacts the method
employed in loading a table. Here the dim_city table, having city+country_key
as its business key (for uniqueness) employed a different loading method than usual.

The product and sales tables do not involve a many-to-many relationship as usual 
because the data exploration showed that each sale was for only one product.

Pandas and SQL are used interchangeably in the data transformation stages.