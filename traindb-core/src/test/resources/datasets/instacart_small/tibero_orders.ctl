LOAD DATA
INFILE 'orders_small.csv'
LOGFILE 'tibero_orders.log'
BADFILE 'tibero_orders.bad'
APPEND
INTO TABLE orders
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '\\'
LINES TERMINATED BY '\n'
(order_id, user_id, order_number, order_dow, order_hour, day_since_last_order )
