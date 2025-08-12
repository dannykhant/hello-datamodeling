# Data Modeling

- The process of creating blueprint of how data is stored, connect, and retrieved
- To make data easier to understand and use, To improve query performance, To make it scalable
- Types
    - Conceptual - High-level, entities & relationships
    - Logical - Attributes and relationships
    - Physical - Implementation
- OLTP vs OLAP
    - OLTP - optimized for transactions - writing & updating (Normalization - 1NF, 2NF, 3NF)
    - OLAP - optimized for reading (dimensional data model) - includes contextual data
- Cloud DW
    - Snowflake
    - Databricks
    - Synapse Analytics
    - BigQuery
    - Redshift
- ETL Pipelines
    - The journey of data
    - Source (RDB, API) → Destination (DWH)
- ETL Layers
    - Staging Layer - To stage the data in transit
        - Persistent - Appending the data
        - Transient - Overwriting the data
    - Transformation Layer - To clean and modify the data
    - Serving Layer - Where the data model is built
    - Raw → Enriched → Curated
- Medallion Architecture
    - Bronze → Silver → Gold
    - Silver - One big table (OBT)
    - Gold - Dimensional data model
- Staging (Bronze)
    - Transient
    - Persistent
        - Incremental loading is required to process
    - Pick a very old date like ‘1900-01-01’ to insert all the data
    - Load all data from the source
        
        ```python
         if spark.catalog.tableExists("datamodel.bronze.bronze_tbl"):
        	last_load_date = spark.sql("select max(order_date) from datamodel.bronze.bronze_tbl").collect()[0][0]
        else:
        	last_load_date = "1900-01-01"
        
        spark.sql("""
        	select * from datamodel.default.source_data
        	where order_date > '{last_load_date}'
        """).createOrReplaceTempView("bronze_source")
        ```
        
        ```sql
        create or replace table datamodel.bronze.bronze_tbl
        as
        select * from bronze_source
        ```
        
    - Update the last_load_date variable at the pipeline
- Silver
    - Apply transformation and add processed_date
        
        ```python
        spark.sql("""select
        	*,
        	upper(customer_name) as Customer_Name_Upper,
        	current_date() as processed_date
        from datamodel.bronze.bronze_tbl
        """).createOrReplaceTempView("silver_source")
        ```
        
    - Apply UPSERT/ MERGE
        
        ```sql
        if spark.catalog.tableExists("datamodel.silver.silver_tbl"):
        	spark.sql("""
        		merge into datamodel.silver.silver_tbl as dst
        		using silver_source src
        		on dst.order_id = src.order_id
        		when matched then
        			update set *
        		when not matched then
        			insert *
        	""")
        else:
        	spark.sql("""
        		create table if not exists datamodel.silver.silver_tbl
        		as
        		select * from silver_source
        	""")
        ```
        
- Gold
    - Fact -  Any figures such as sales, profit, etc..
    - Dimension - Contextual information such customer_name, age, product_name, etc..
    - Dimension → Fact
    - Surrogate Key - Unique identifier assigned to each row in a dimension table
    - dim_customers
        
        ```sql
        create or replace table datamodel.gold.dim_customers
        with dedup as (
        	select distinct
        		customer_id,
        		customer_name,
        		customer_email,
        		customer_email_host
        )
        select
        	row_number() over (order by customer_id) as dim_customer_key,
        	*
        from dedup
        ```
        
    - dim_products
        
        ```sql
        create or replace table datamodel.gold.dim_products
        with dedup as (
        	select distinct
        		product_id,
        		product_name,
        		product_category
        )
        select
        	row_number() over (order by product_id) as dim_product_key,
        	*
        from dedup
        ```
        
    - dim_regions
        
        ```sql
        create or replace table datamodel.gold.dim_regions
        with dedup as (
        	select distinct
        		country
        )
        select
        	row_number() over (order by country) as dim_region_key,
        	*
        from dedup
        ```
        
    - dim_payments
        
        ```sql
        create or replace table datamodel.gold.dim_payments
        with dedup as (
        	select distinct
        		payment_type
        )
        select
        	row_number() over (order by payment_type) as dim_payment_key,
        	*
        from dedup
        ```
        
    - dim_orders
        
        ```sql
        create or replace table datamodel.gold.dim_orders
        with dedup as (
        	select distinct
        		order_id,
        		order_date,
        		customer_id,
        		customer_name,
        		customer_email,
        		customer_email_host,
        		product_id,
        		product_name,
        		product_category,
        		payment_type,
        		country,
        		last_updated,
        		processed_date
        )
        select
        	row_number() over (order by customer_id) as dim_order_key,
        	*
        from dedup
        ```
        
    - fct_sales
        
        ```sql
        create or replace table datamodel.gold.fact_sales
        as
        select
        	od.dim_order_key,
        	pd.dim_product_key,
        	cu.dim_customer_key,
        	pt.dim_payment_key,
        	rg.dim_region_key,
        	sv.quantity as m_quantity,
        	sv.unit_price as m_unit_price
        from datamodel.silver.silver_tbl sv
        left join datamodel.gold.dim_orders od
        	on sv.order_id = od.order_id
        left join datamodel.gold.dim_products pd
        	on sv.product_id = pd.product_id
        left join datamodel.gold.dim_customers cu
        	on sv.customer_id = cu.customer_id
        left join datamodel.gold.dim_payments pt
        	on sv.payment_type = pt.payment_type
        left join datamodel.gold.dim_regions rg
        	on sv.country = rg.country
        ```
        
- Star Schema vs Snowflake Schema
    - Snowflake schema is an extended version of Snow schema.
- Fact Table Types
    - Transactional Fact Table
        - This is the default - 1 row = 1 transaction ← the most granular version
    - Periodic Fact Table
        - Each row represents a period, its field values are aggregated
    - Accumulating Fact Table
        - It describes the journey of a transaction, it has many date fields
    - Factless Fact Table
        - It doesn’t have any measurements.
- Dimension Table Types
    - Conformed Dimensions
        - One dimension is connected to many fact tables.
    - Degenerate Dimensions
        - No associated values
    - Junk Dimensions
        - It has only one or two values (eg., dim_payments)
    - Role Playing Dimensions
        - It has two different roles (columns) of a fact table
- SCD
    - It changes slowly.
    - Type-1 (UPSERT)
        - No history is maintained
        
        ```sql
        merge into datamodel.gold.scdtype1_tbl dst
        using datamodel.default.scdtype1_src src
        on dst.product_id = src.product_idwhen matched and dst.processed_date >= src.processed_date then   
        	update set *
        when not matched then  
        	insert *;
        ```
        
    - Type-2
        - Keep history with start_date, end_date and in_use
        
        ```sql
        -- for existing records
        merge into datamodel.gold.scdtype2_tbl dst
        using datamodel.default.scdtype2_src src
        on dst.product_id = src.product_idand dst.is_current = true
        when matched and (dst.product_name <> src.product_name or dst.product_category <> src.product_category or dst.processed_date <> src.processed_date) then
        	update set  
        		dst.end_date = current_date(),  
        		dst.is_current = false
        ```
        
        ```sql
        -- for updated records
        merge into datamodel.gold.scdtype2_tbl dst
        using datamodel.default.scdtype2_src src
        on dst.product_id = src.product_id
        and dst.is_current = true
        when not matched then
        	insert (product_id, product_name, product_category, processed_date, start_date, end_date, is_current)
        	values (src.product_id, src.product_name, src.product_category, src.processed_date, current_date(), null, true);
        ```