\i schema.sql;

\copy aisles from 'aisles.csv' delimiter ',' ;
\copy departments from 'departments.csv' delimiter ',' ;
\copy orders from 'orders_small.csv' delimiter ',' ;
\copy products from 'products_kairos.csv' delimiter '^' ;
\copy order_products from 'order_products__train.csv' delimiter ',' ;

\i count.sql;
