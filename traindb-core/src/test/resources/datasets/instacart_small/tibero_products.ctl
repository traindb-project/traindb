LOAD DATA
INFILE 'products_kairos.csv'
LOGFILE 'tibero_products.log'
BADFILE 'tibero_products.bad'
APPEND
INTO TABLE products
FIELDS TERMINATED BY '^'
LINES TERMINATED BY '\n'
(product_id, product_name, aisle_id, department_id)
