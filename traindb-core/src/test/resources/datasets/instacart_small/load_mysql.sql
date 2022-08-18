# Create database, tables, foreign keys and import data.

SET GLOBAL local_infile=1;

CREATE DATABASE instacart_small;

USE instacart_small;

CREATE TABLE aisles (aisle_id int primary key, aisle VARCHAR(255));
LOAD DATA LOCAL INFILE 'aisles.csv' INTO TABLE aisles FIELDS TERMINATED BY ',' ENCLOSED BY '"' ESCAPED BY '' LINES TERMINATED BY '\n' ;

CREATE TABLE departments (department_id int primary key, department VARCHAR(255));
LOAD DATA LOCAL INFILE 'departments.csv' INTO TABLE departments FIELDS TERMINATED BY ',' ENCLOSED BY '"' ESCAPED BY '' LINES TERMINATED BY '\n'; 

CREATE TABLE orders (order_id int primary key, user_id int, order_number int, order_dow int, order_hour int, day_since_last_order int);
LOAD DATA LOCAL INFILE 'orders_small.csv' INTO TABLE orders FIELDS TERMINATED BY ',' ENCLOSED BY '"' ESCAPED BY '' LINES TERMINATED BY '\n' ;

CREATE TABLE products (product_id int primary key, product_name VARCHAR(255), aisle_id int NOT NULL, foreign key (aisle_id) references aisles(aisle_id), department_id int NOT NULL, foreign key (department_id) references departments(department_id));
LOAD DATA LOCAL INFILE 'products.csv' INTO TABLE products FIELDS TERMINATED BY ',' ENCLOSED BY '"' ESCAPED BY '' LINES TERMINATED BY '\n' ;

CREATE TABLE order_products (order_id int NOT NULL, foreign key (order_id) references orders(order_id), product_id int NOT NULL, foreign key (product_id) references products(product_id), add_to_cart_order int, reordered int);
LOAD DATA LOCAL INFILE 'order_products__train.csv' INTO TABLE order_products FIELDS TERMINATED BY ',' ENCLOSED BY '"' ESCAPED BY '' LINES TERMINATED BY '\n' ;
