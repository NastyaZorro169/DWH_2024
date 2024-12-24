-- Получаем максимальный product_id
SELECT COALESCE(MAX(product_id), 0) + 1 AS new_product_id FROM source3.craft_market_orders;
-- Затем используйте это значение в вашем INSERT запросе
INSERT INTO source3.craft_market_orders (
    product_id,
    craftsman_id,
    customer_id,
    order_created_date,
    order_completion_date,
    order_status,
    product_name,
    product_description,
    product_type,
    product_price
)
VALUES (
    (SELECT COALESCE(MAX(product_id), 0) + 1 FROM source3.craft_market_orders), -- новое значение для product_id
    999,  -- идентификатор мастера
    999,  -- идентификатор заказчика
    '2024-10-03',  -- дата создания заказа
    '2024-10-14',  -- дата выполнения заказа
    'done',  -- статус выполнения заказа
    'tovar_2',  -- название товара ручной работы
    'super good tovar',  -- описание товара ручной работы
    'clothes',  -- тип товара ручной работы
    20  -- цена товара ручной работы
);



UPDATE source1.craft_market_wide 
SET 
	order_completion_date = CASE 
        WHEN order_status = 'done' THEN NULL  -- Убираем значение
        ELSE CURRENT_DATE  -- Устанавливаем сегодняшнюю дату
    end,
    order_status = CASE 
        WHEN order_status = 'done' THEN 'delivery'
        ELSE 'done'
    END,
    product_price  = 700
WHERE craftsman_id = 123;

