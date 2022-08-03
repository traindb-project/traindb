DROP TABLE order_products;
DROP TABLE products;
DROP TABLE aisles;
DROP TABLE departments;
DROP TABLE orders;

CREATE TABLE aisles (aisle_id int primary key, aisle VARCHAR(255));
CREATE TABLE departments (department_id int primary key, department VARCHAR(255));
CREATE TABLE orders (order_id int primary key, user_id int, order_number int, order_dow int, order_hour int, day_since_last_order int);
CREATE TABLE products (product_id int primary key, product_name VARCHAR(255), aisle_id int NOT NULL, foreign key (aisle_id) references aisles(aisle_id), department_id int NOT NULL, foreign key (department_id) references departments(department_id));
CREATE TABLE order_products (order_id int NOT NULL, foreign key (order_id) references orders(order_id), product_id int NOT NULL, foreign key (product_id) references products(product_id), add_to_cart_order int, reordered int);
