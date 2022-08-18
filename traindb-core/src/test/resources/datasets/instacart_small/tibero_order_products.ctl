LOAD DATA
INFILE 'order_products__train.csv'
LOGFILE 'tibero_order_products.log'
BADFILE 'tibero_order_products.bad'
APPEND
INTO TABLE order_products
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
(order_id, product_id, add_to_cart_order, reordered )
