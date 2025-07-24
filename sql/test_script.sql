SELECT * FROM vendor_invoice LIMIT 100;
SELECT * FROM purchase_prices LIMIT 100;
SELECT * FROM purchases LIMIT 100;
SELECT * FROM sales LIMIT 100;
SELECT * FROM begin_inventory LIMIT 100;
SELECT * FROM end_inventory LIMIT 100;
SELECT * FROM vendor_sales_summary LIMIT 100;


SELECT COUNT(*) FROM vendor_invoice;
SELECT COUNT(*) FROM purchase_prices;
SELECT COUNT(*) FROM purchases;
SELECT COUNT(*) FROM sales;
SELECT COUNT(*) FROM begin_inventory;
SELECT COUNT(*) FROM end_inventory;
SELECT COUNT(*) FROM vendor_sales_summary

SELECT tablename FROM pg_tables WHERE schemaname = 'public';

SELECT * FROM pg_tables WHERE schemaname = 'public';