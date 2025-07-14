-- Check current DB
SELECT current_database();

-- Check current schema
SELECT current_schema;


select store from sales s where "VendorNo" = 4466;

SELECT column_name
FROM information_schema.columns
WHERE table_name = 'sales';

ALTER TABLE sales RENAME COLUMN "Store" TO store;


select * from vendor_invoice limit 100;
select * from purchase_prices limit 100;
select * from purchases limit 100;
select * from sales limit 100;
select * from begin_inventory limit 100;
select * from end_inventory limit 100;


select count(*) from vendor_invoice;
select count(*) from purchase_prices;
select count(*) from purchases;
select count(*) from sales;
select count(*) from begin_inventory;
select count(*) from end_inventory;

SELECT tablename FROM pg_tables WHERE schemaname = 'public';

SELECT * FROM pg_tables WHERE schemaname = 'public';

select * from purchases where "VendorNumber" = 4466;

select Store from sales s where "VendorNo" = 4466;


SELECT 
    vendornumber,
    SUM(freight) AS FreightCost
FROM
    vendor_invoice
GROUP BY vendornumber;


SELECT
    p.vendornumber,
    p.vendorname,
    p.brand,
    p.purchaseprice,
    pp.volume,
    pp.price AS actual_price,
    SUM(p.quantity) AS total_purchase_quantity,
    SUM(p.dollars) AS total_purchase_dollars
FROM
    purchases p
JOIN
    purchase_prices pp ON p.brand = pp.brand
GROUP BY p.vendornumber, p.vendorname, p.brand, p.PurchasePrice, pp.Price, pp.Volume
HAVING SUM(p.dollars) > 0
ORDER BY total_purchase_dollars;


SELECT
    vendorno,
    brand,
    SUM(salesdollars) AS totalsalesdollars,
    SUM(salesprice) AS totalsalesprice,
    SUM(salesquantity) AS totalsalesquantity,
    SUM(excisetax) AS totalexcisetax
FROM
    sales
GROUP BY 1, 2;





WITH FrieghtSummary AS (
    SELECT 
        vendornumber,
        SUM(freight) AS freightcost
    FROM
        vendor_invoice
    GROUP BY vendornumber
    ORDER BY vendornumber
),
PurchaseSummary AS (
    SELECT
        p.vendornumber,
        p.vendorname,
        p.brand,
        p.purchaseprice,
        pp.description,
        pp.volume,
        pp.price AS actual_price,
        SUM(p.quantity) AS total_purchase_quantity,
        SUM(p.dollars) AS total_purchase_dollars
    FROM
        purchases p
    JOIN
        purchase_prices pp ON p.brand = pp.brand
    GROUP BY p.vendornumber, p.vendorname, p.brand, p.PurchasePrice, pp.Price, pp.Volume, pp.description
    HAVING SUM(p.dollars) > 0
    ORDER BY total_purchase_dollars
),
SalesSummary AS (
    SELECT
        vendorno,
        brand,
        SUM(salesdollars) AS totalsalesdollars,
        SUM(salesprice) AS totalsalesprice,
        SUM(salesquantity) AS totalsalesquantity,
        SUM(excisetax) AS totalexcisetax
    FROM
        sales
    GROUP BY 1, 2
)
SELECT
    ps.vendornumber,
    ps.vendorname,
    ps.description,
    ps.brand,
    ps.purchaseprice,
    ps.volume,
    ps.actual_price,
    ps.total_purchase_quantity,
    ps.total_purchase_dollars,
    ss.totalsalesdollars,
    ss.totalsalesprice,
    ss.totalsalesquantity,
    ss.totalexcisetax,
    fs.freightcost
    
FROM
    PurchaseSummary ps
        JOIN
    SalesSummary ss ON ps.vendornumber = ss.vendorno AND ps.brand = ss.brand
        LEFT JOIN
    FrieghtSummary fs ON ps.vendornumber = fs.vendornumber
ORDER BY ps.total_purchase_dollars DESC;




SELECT * FROM vendor_sales_summary;

SELECT COUNT(*) FROM vendor_sales_summary;