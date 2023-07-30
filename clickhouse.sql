-- 2.8

CREATE TABLE shops
(
    shop_id Int32,
    shop_name String
)
ENGINE = MergeTree()
PRIMARY KEY (shop_id, shop_name)

INSERT INTO shops [(shop_id, shop_name)]
VALUES (1, 'dns'),
       (2, 'mvideo'),
       (3, 'sitilink')


CREATE TABLE IF NOT EXISTS shop_dns
(
    shop_id Int32,
    datestamp Date,
    product_id Int32,
    sales_cnt UInt32
)
ENGINE = MergeTree()
ORDER BY sales_cnt DESC


CREATE TABLE IF NOT EXISTS shop_mvideo
(
    shop_id Int32,
    datestamp Date,
    product_id Int32,
    sales_cnt UInt32
)
ENGINE = MergeTree()
ORDER BY sales_cnt DESC


CREATE TABLE IF NOT EXISTS shop_sitilink
(
    shop_id Int32,
    datestamp Date,
    product_id Int32,
    sales_cnt UInt32
)
ENGINE = MergeTree()
ORDER BY sales_cnt DESC


CREATE TABLE products
(
    product_id Int32,
    product_name String,
    price Float64
)
ENGINE = MergeTree()
ORDER BY product_name

INSERT INTO products [(product_id, product_name, price)]
VALUES (10, 'Испорченный телефон', 1000),
       (20, 'Сарафанное радио', 2000),
       (30, 'Патефон', 3000)


CREATE TABLE plan
(
    product_id Int32,
    shop_name String,
    plan_cnt UInt32,
    plan_date Date
)
ENGINE = MergeTree (plan_date, (product_id), 8192)


INSERT INTO plan [(product_id, shop_name, plan_cnt, plan_date)]
VALUES (10, 'dns', 100, '30.07.2023'),
       (10, 'mvideo', 100, '30.07.2023'),
       (10, 'sitilink', 100, '30.07.2023'),
       (20, 'dns', 200, '30.07.2023'),
       (20, 'mvideo', 200, '30.07.2023'),
       (20, 'sitilink', 200, '30.07.2023'),
       (30, 'dns', 300, '30.07.2023'),
       (30, 'mvideo', 300, '30.07.2023'),
       (30, 'sitilink', 300, '30.07.2023')


WITH sales_fact AS
(SELECT SUM
        (CASE WHEN sh.shop_name = 'dns' THEN (SELECT SUM(sales_cnt) FROM shop_dns)
              WHEN sh.shop_name = 'mvideo' THEN (SELECT SUM(sales_cnt) FROM shop_mvideo)
              WHEN sh.shop_name = 'sitilink' THEN (SELECT SUM(sales_cnt) FROM shop_sitilink)
              END) 
        AS sum_fact_shops
FROM shops AS sh JOIN shop_dns ON sh.shop_id = shop_dns.shop_id
                 JOIN shop_mvideo ON sh.shop_id = shop_mvideo.shop_id
                 JOIN shop_sitilink ON sh.shop_id = shop_sitilink_id
)

WITH sales_plan AS 
(SELECT SUM
        (CASE WHEN sh.shop_name = 'dns' THEN (SELECT SUM(plant_cnt) FROM plan AS p WHERE p.shop_name = 'dns')
              WHEN sh.shop_name = 'mvideo' THEN (SELECT SUM(plant_cnt) FROM p WHERE p.shop_name = 'mvideo')
              WHEN sh.shop_name = 'sitilink' THEN (SELECT SUM(plan_cnt) FROM p WHERE p.shop_name = 'sitilink'
              END)
        AS sum_plan_shops
        )
)

SELECT sh.shop_name AS shop, 
       p.product_name AS product, 
       sales_fact, 
       sales_plan, 
       sales_fact/sales_plan, 
       multiply(p.price, sales_fact) AS income_fact, 
       multiply(p.price, sales_plan) AS income_plan, 
       income_fact/income_plan
FROM sales AS s
JOIN shops AS sh ON plan AS pl ON sh.shop_name = pl.shop_name
JOIN products AS pr ON pl.product_id = pr.product_id