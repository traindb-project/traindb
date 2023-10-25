\i schema.sql;

copy aisles FROM 's3://{bucket_name}/aisles.csv' CREDENTIALS 'aws_access_key_id={access_key_id};aws_secret_access_key={secret_access_key}' delimiter ',';
copy departments from 's3://{bucket_name}/departments.csv' CREDENTIALS 'aws_access_key_id={access_key_id};aws_secret_access_key={secret_access_key}' delimiter ',' ;
copy orders from 's3://{bucket_name}/orders_small.csv' CREDENTIALS 'aws_access_key_id={access_key_id};aws_secret_access_key={secret_access_key}' delimiter ',' ;
copy products from 's3://{bucket_name}/products_kairos.csv' CREDENTIALS 'aws_access_key_id={access_key_id};aws_secret_access_key={secret_access_key}' delimiter '^' ;
copy order_products from 's3://{bucket_name}/order_products__train.csv' CREDENTIALS 'aws_access_key_id={access_key_id};aws_secret_access_key={secret_access_key}' delimiter ',' ;

\i count.sql;
