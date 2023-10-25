bq query --use_legacy_sql=false < bigquery_schema.sql

bq load --source_format=CSV {dataset_name}.aisles ./aisles.csv
bq load --source_format=CSV {dataset_name}.departments ./departments.csv
bq load --source_format=CSV {dataset_name}.orders ./orders_small.csv
bq load --source_format=CSV --skip_leading_rows=1 {dataset_name}.products ./products.csv
bq load --source_format=CSV {dataset_name}.order_products ./order_products__train.csv

bq query --use_legacy_sql=false < bigquery_count.sql
