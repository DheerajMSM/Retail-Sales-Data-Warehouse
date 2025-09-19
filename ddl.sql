CREATE DATABASE IF NOT EXISTS retail_dw;
USE retail_dw;

-- DIMENSIONS with surrogate keys and unique business keys
CREATE TABLE DimCustomer (
    CustomerKey INT AUTO_INCREMENT PRIMARY KEY,
    CustomerID VARCHAR(50) NOT NULL,
    CustomerName VARCHAR(100),
    Region VARCHAR(50),
    UNIQUE KEY uq_dim_customer_customerid (CustomerID)
);

CREATE TABLE DimProduct (
    ProductKey INT AUTO_INCREMENT PRIMARY KEY,
    ProductID VARCHAR(50) NOT NULL,
    ProductName VARCHAR(100),
    Category VARCHAR(50),
    Price DECIMAL(12,2),
    UNIQUE KEY uq_dim_product_productid (ProductID)
);

CREATE TABLE DimStore (
    StoreKey INT AUTO_INCREMENT PRIMARY KEY,
    StoreID VARCHAR(50) NOT NULL,
    StoreName VARCHAR(100),
    Location VARCHAR(100),
    UNIQUE KEY uq_dim_store_storeid (StoreID)
);

CREATE TABLE DimDate (
    DateKey INT PRIMARY KEY,             -- e.g., 20250901
    DateValue DATE NOT NULL,
    Year INT, Month INT, Day INT,
    UNIQUE KEY uq_dim_date_datevalue (DateValue)
);

-- FACT with a natural unique key for idempotent upserts
CREATE TABLE FactSales (
    SalesKey INT AUTO_INCREMENT PRIMARY KEY,
    DateKey INT NOT NULL,
    CustomerKey INT NOT NULL,
    ProductKey INT NOT NULL,
    StoreKey INT NOT NULL,
    Quantity INT NOT NULL,
    TotalAmount DECIMAL(14,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_fact (DateKey, CustomerKey, ProductKey, StoreKey),
    FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
    FOREIGN KEY (CustomerKey) REFERENCES DimCustomer(CustomerKey),
    FOREIGN KEY (ProductKey) REFERENCES DimProduct(ProductKey),
    FOREIGN KEY (StoreKey) REFERENCES DimStore(StoreKey)
);

-- Staging table(s)
CREATE TABLE stgsales (
    DateValue date,  -- raw text from CSV
    CustomerID VARCHAR(50),
    ProductID VARCHAR(50),
    StoreID VARCHAR(50),
    Quantity INT
);
