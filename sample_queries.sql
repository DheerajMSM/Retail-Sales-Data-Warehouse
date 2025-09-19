
-- Total Sales by Product
SELECT p.ProductName, SUM(f.TotalAmount) AS TotalSales
FROM FactSales f
JOIN DimProduct p ON f.ProductKey = p.ProductKey
GROUP BY p.ProductName
ORDER BY TotalSales DESC;


-- Sales by Store and Month
SELECT s.StoreName, d.Year, d.Month, SUM(f.TotalAmount) AS Sales
FROM FactSales f
JOIN DimStore s ON f.StoreKey = s.StoreKey
JOIN DimDate d ON f.DateKey = d.DateKey
GROUP BY s.StoreName, d.Year, d.Month
ORDER BY d.Year DESC, d.Month DESC, Sales DESC;

-- Sales by Customer Region
SELECT c.Region, SUM(f.TotalAmount) AS TotalSales
FROM FactSales f
JOIN DimCustomer c ON f.CustomerKey = c.CustomerKey
GROUP BY c.Region;

-- counts
SELECT
 (SELECT COUNT(*) FROM stgsales) AS staging_rows,
 (SELECT COUNT(*) FROM FactSales) AS fact_rows,
 (SELECT COUNT(*) FROM DimCustomer) AS customers,
 (SELECT COUNT(*) FROM DimProduct) AS products;
