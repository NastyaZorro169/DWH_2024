-- Мердж витрины  
MERGE INTO dwh.craftsman_report_datamart AS target
USING (
    SELECT
        c.craftsman_id,  -- идентификатор мастера
        c.craftsman_name,  -- ФИО мастера
        c.craftsman_address,  -- адрес мастера
        c.craftsman_birthday,  -- дата рождения мастера
        c.craftsman_email,  -- электронная почта мастера
        SUM(CASE WHEN o.order_status = 'done' THEN p.product_price * 0.9 ELSE 0 END) AS craftsman_money, -- сколько заработал мастер
        SUM(CASE WHEN o.order_status = 'done' THEN p.product_price * 0.1 ELSE 0 END) AS platform_money, -- сколько заработала платформа
        COUNT(o.order_id) AS count_order,  -- общее количество заказов за месяц
        SUM(CASE WHEN o.order_status = 'done' THEN p.product_price ELSE 0 END) AS avg_price_order, -- сколько с заказов получено
        AVG(EXTRACT(YEAR FROM AGE(cu.customer_birthday))) AS avg_age_customer,  -- средний возраст покупателей
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY o.order_completion_date - o.order_created_date) AS median_time_order_completed,  -- медианное время выполнения заказов
        (SELECT p2.product_type
         FROM dwh.d_products p2
         JOIN dwh.f_orders o2 ON p2.product_id = o2.product_id
         WHERE o2.craftsman_id = c.craftsman_id
         GROUP BY p2.product_type
         ORDER BY COUNT(*) DESC
         LIMIT 1) AS top_product_category,  -- самая популярная категория товаров у мастера
        COUNT(CASE WHEN o.order_status = 'created' THEN 1 END) AS count_order_created,  -- количество созданных заказов за месяц
        COUNT(CASE WHEN o.order_status = 'in progress' THEN 1 END) AS count_order_in_progress,  -- количество заказов в процессе изготовления за месяц
        COUNT(CASE WHEN o.order_status = 'delivery' THEN 1 END) AS count_order_delivery,  -- количество заказов в доставке за месяц
        COUNT(CASE WHEN o.order_status = 'done' THEN 1 END) AS count_order_done,  -- количество завершенных заказов за месяц
        COUNT(CASE WHEN o.order_status != 'done' THEN 1 END) AS count_order_not_done,  -- количество незавершенных заказов за месяц
        TO_CHAR(DATE_TRUNC('month', o.order_created_date), 'YYYY-MM') AS report_period  -- отчетный период (год и месяц)
    FROM
        dwh.f_orders o
    JOIN
        dwh.d_craftsmans c ON o.craftsman_id = c.craftsman_id
    JOIN
        dwh.d_customers cu ON o.customer_id = cu.customer_id
    JOIN
        dwh.d_products p ON o.product_id = p.product_id    
    GROUP BY
        c.craftsman_id, 
        c.craftsman_name, 
        c.craftsman_address, 
        c.craftsman_birthday, 
        c.craftsman_email,
        DATE_TRUNC('month', o.order_created_date)

) AS source
ON (
    target.craftsman_id = source.craftsman_id AND target.report_period = source.report_period
)
WHEN MATCHED THEN
    UPDATE SET
        craftsman_name = source.craftsman_name,
        craftsman_address = source.craftsman_address,
        craftsman_birthday = source.craftsman_birthday,
        craftsman_email = source.craftsman_email,
        craftsman_money = source.craftsman_money,
        platform_money = source.platform_money,
        count_order = source.count_order,
        avg_price_order = source.avg_price_order,
        avg_age_customer = source.avg_age_customer,
        median_time_order_completed = source.median_time_order_completed,
        top_product_category = source.top_product_category,
        count_order_created = source.count_order_created,
        count_order_in_progress = source.count_order_in_progress,
        count_order_delivery = source.count_order_delivery,
        count_order_done = source.count_order_done,
        count_order_not_done = source.count_order_not_done
WHEN NOT MATCHED THEN
    INSERT (
        craftsman_id,
        craftsman_name,
        craftsman_address,
        craftsman_birthday,
        craftsman_email,
        craftsman_money,
        platform_money,
        count_order,
        avg_price_order,
        avg_age_customer,
        median_time_order_completed,
        top_product_category,
        count_order_created,
        count_order_in_progress,
        count_order_delivery,
        count_order_done,
        count_order_not_done,
        report_period
    )
    VALUES (
        source.craftsman_id,
        source.craftsman_name,
        source.craftsman_address,
        source.craftsman_birthday,
        source.craftsman_email,
        source.craftsman_money,
        source.platform_money,
        source.count_order,
        source.avg_price_order,  
        source.avg_age_customer,
        source.median_time_order_completed,
        source.top_product_category,
        source.count_order_created,
        source.count_order_in_progress,
        source.count_order_delivery,
        source.count_order_done,
        source.count_order_not_done,
		source.report_period 
    );  
insert into dwh.load_dates_craftsman_report_datamart (load_dttm) values (CURRENT_TIMESTAMP)
