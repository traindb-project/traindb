@echo "Tibero Loader"
rm *.log
rm *.bad
tbsql sys/tibero < schema.sql
tbloader userid=sys/tibero control=./tibero_aisles.ctl
tbloader userid=sys/tibero control=./tibero_departments.ctl
tbloader userid=sys/tibero control=./tibero_orders.ctl
tbloader userid=sys/tibero control=./tibero_products.ctl
tbloader userid=sys/tibero control=./tibero_order_products.ctl
tbsql sys/tibero < count.sql
