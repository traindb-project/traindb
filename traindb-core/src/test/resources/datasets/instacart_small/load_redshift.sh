aws s3 cp aisles.csv s3://{bucket_name}
aws s3 cp departments.csv s3://{bucket_name}
aws s3 cp order_products__train.csv s3://{bucket_name}
aws s3 cp orders_small.csv s3://{bucket_name}
aws s3 cp products_kairos.csv s3://{bucket_name}
PGPASSWORD=password psql -h redshift_endpoint -d database_name -U username -p 5439 < redshift_put.sql
