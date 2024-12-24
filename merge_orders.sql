WITH UniqueProducts_orders AS (
    SELECT DISTINCT
        p.product_id,
        c_dwh.craftsman_id,
        cu_dwh.customer_id,
        o.order_created_date,
        o.order_completion_date,
        o.order_status,
        NOW() AS load_dttm
    FROM
        source3.craft_market_orders o
    JOIN
        source3.craft_market_craftsmans c ON o.craftsman_id = c.craftsman_id
    JOIN
        source3.craft_market_customers cu ON o.customer_id = cu.customer_id
    JOIN
        dwh.d_products p ON o.product_name = p.product_name and o.product_price = p.product_price 
        and o.product_description = p.product_description and  o.product_price = p.product_price
        and 3 = p.product_source and o.craftsman_id = p.product_craftsmans
    JOIN
        dwh.d_craftsmans c_dwh ON c.craftsman_name = c_dwh.craftsman_name and c.craftsman_birthday = c_dwh.craftsman_birthday 
        and c.craftsman_address = c_dwh.craftsman_address and c.craftsman_email = c_dwh.craftsman_email
    JOIN
        dwh.d_customers cu_dwh ON cu.customer_name = cu_dwh.customer_name and cu.customer_birthday = cu_dwh.customer_birthday 
        and cu.customer_address = cu_dwh.customer_address and cu.customer_email = cu_dwh.customer_email
    
    UNION 
    
    select distinct 
        p.product_id,
        c_dwh.craftsman_id,
        cu_dwh.customer_id,
        o.order_created_date,
        o.order_completion_date,
        o.order_status,
        NOW() AS load_dttm
    FROM
        source2.craft_market_orders_customers o
    JOIN
        source2.craft_market_masters_products c ON o.craftsman_id = c.craftsman_id
    JOIN
        dwh.d_products p ON c.product_name = p.product_name and c.product_price = p.product_price 
        and c.product_description = p.product_description and  c.product_price = p.product_price
        and 2 = p.product_source and o.craftsman_id = p.product_craftsmans
    JOIN
        dwh.d_craftsmans c_dwh ON c.craftsman_name = c_dwh.craftsman_name and c.craftsman_birthday = c_dwh.craftsman_birthday 
        and c.craftsman_address = c_dwh.craftsman_address and c.craftsman_email = c_dwh.craftsman_email
    JOIN
        dwh.d_customers cu_dwh ON o.customer_name = cu_dwh.customer_name and o.customer_birthday = cu_dwh.customer_birthday 
        and o.customer_address = cu_dwh.customer_address and o.customer_email = cu_dwh.customer_email
    
    UNION 
    
    select distinct 
        p.product_id,
        c_dwh.craftsman_id,
        cu_dwh.customer_id,
        o.order_created_date,
        o.order_completion_date,
        o.order_status,
        NOW() AS load_dttm
    FROM
        source1.craft_market_wide o
    JOIN
        dwh.d_products p ON o.product_name = p.product_name and o.product_price = p.product_price 
        and o.product_description = p.product_description and  o.product_price = p.product_price
        and 1 = p.product_source and o.craftsman_id = p.product_craftsmans
    JOIN
        dwh.d_craftsmans c_dwh ON o.craftsman_name = c_dwh.craftsman_name and o.craftsman_birthday = c_dwh.craftsman_birthday 
        and o.craftsman_address = c_dwh.craftsman_address and o.craftsman_email = c_dwh.craftsman_email
    JOIN
        dwh.d_customers cu_dwh ON o.customer_name = cu_dwh.customer_name and o.customer_birthday = cu_dwh.customer_birthday 
        and o.customer_address = cu_dwh.customer_address and o.customer_email = cu_dwh.customer_email
)   
MERGE INTO dwh.f_orders AS target
USING UniqueProducts_orders AS source
ON (
    target.craftsman_id = source.craftsman_id AND
    target.customer_id = source.customer_id AND
    target.order_created_date = source.order_created_date and target.product_id = source.product_id
)
WHEN MATCHED THEN
    UPDATE SET
        order_completion_date = CASE 
            WHEN target.order_completion_date <> source.order_completion_date THEN source.order_completion_date 
            ELSE target.order_completion_date 
        END,
        order_status = CASE 
            WHEN target.order_status <> source.order_status THEN source.order_status 
            ELSE target.order_status 
        END,
        load_dttm = CASE 
            WHEN target.order_completion_date <> source.order_completion_date OR 
                 target.order_status <> source.order_status THEN NOW() 
            ELSE target.load_dttm 
        END
WHEN NOT MATCHED THEN
    INSERT (
    	product_id,
        craftsman_id,
        customer_id,
        order_created_date,
        order_completion_date,
        order_status,
        load_dttm
    )
    VALUES (
    	source.product_id,
        source.craftsman_id,
        source.customer_id,
        source.order_created_date,
        source.order_completion_date,
        source.order_status,
        source.load_dttm
    );
   
  -----------------------------------------------------------------------------------------------------------------------  
   ------------------------------------------------------------------------------------------------------------------
----------
