DROP TABLE IF EXISTS {dataset_name}.aisles;
DROP TABLE IF EXISTS {dataset_name}.departments;
DROP TABLE IF EXISTS {dataset_name}.orders;
DROP TABLE IF EXISTS {dataset_name}.products;
DROP TABLE IF EXISTS {dataset_name}.order_products;

CREATE TABLE {dataset_name}.aisles (aisle_id int, aisle String); 
CREATE TABLE {dataset_name}.departments (department_id int, department String);
CREATE TABLE {dataset_name}.orders (order_id int, user_id int, order_number int, order_dow int, order_hour int, day_since_last_order int);
CREATE TABLE {dataset_name}.products (product_id int, product_name String, aisle_id int NOT NULL, department_id int NOT NULL);
CREATE TABLE {dataset_name}.order_products (order_id int NOT NULL, product_id int NOT NULL, add_to_cart_order int, reordered int);
