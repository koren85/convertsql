/*
 * Комплексный пример SQL-скрипта с различными конструкциями MS SQL
 */

-- Используем временные переменные
DECLARE @StartDate DATETIME = {startDate};
DECLARE @EndDate DATETIME = {endDate};
DECLARE @CategoryID INT = {categoryID};

-- Создаем временную таблицу для хранения промежуточных результатов
DECLARE @ProductSales TABLE (
    ProductID INT,
    ProductName NVARCHAR(100),
    CategoryID INT,
    CategoryName NVARCHAR(50),
    SalesAmount DECIMAL(18,2),
    SalesCount INT,
    AvgUnitPrice DECIMAL(18,2),
    LastSaleDate DATETIME
);

-- CTE с иерархическим запросом
WITH ProductHierarchy AS (
    -- Базовый запрос для верхнего уровня
    SELECT 
        p.[ProductID],
        p.[ProductName],
        p.[CategoryID],
        CAST(NULL AS INT) AS ParentProductID,
        0 AS HierarchyLevel
    FROM 
        [dbo].[Products] p
    WHERE 
        p.[ParentProductID] IS NULL
        AND (@CategoryID IS NULL OR p.[CategoryID] = @CategoryID)
    
    UNION ALL
    
    -- Рекурсивная часть для получения дочерних продуктов
    SELECT 
        c.[ProductID],
        c.[ProductName],
        c.[CategoryID],
        c.[ParentProductID],
        ph.HierarchyLevel + 1
    FROM 
        [dbo].[Products] c
        INNER JOIN ProductHierarchy ph ON c.[ParentProductID] = ph.[ProductID]
    WHERE
        (@CategoryID IS NULL OR c.[CategoryID] = @CategoryID)
),
-- CTE с агрегацией продаж
SalesSummary AS (
    SELECT 
        od.[ProductID],
        SUM(od.[Quantity] * od.[UnitPrice]) AS TotalSales,
        SUM(od.[Quantity]) AS TotalQuantity,
        AVG(od.[UnitPrice]) AS AvgPrice,
        MAX(o.[OrderDate]) AS LastOrderDate,
        COUNT(DISTINCT o.[CustomerID]) AS UniqueCustomers,
        RANK() OVER (PARTITION BY p.[CategoryID] ORDER BY SUM(od.[Quantity] * od.[UnitPrice]) DESC) AS SalesRank
    FROM 
        [dbo].[OrderDetails] od
        INNER JOIN [dbo].[Orders] o ON od.[OrderID] = o.[OrderID]
        INNER JOIN [dbo].[Products] p ON od.[ProductID] = p.[ProductID]
    WHERE 
        o.[OrderDate] BETWEEN @StartDate AND @EndDate
        AND (@CategoryID IS NULL OR p.[CategoryID] = @CategoryID)
    GROUP BY 
        od.[ProductID], p.[CategoryID]
)

-- Вставляем данные во временную таблицу
INSERT INTO @ProductSales (
    ProductID, 
    ProductName, 
    CategoryID, 
    CategoryName, 
    SalesAmount, 
    SalesCount, 
    AvgUnitPrice, 
    LastSaleDate
)
SELECT 
    ph.[ProductID],
    ph.[ProductName],
    ph.[CategoryID],
    c.[CategoryName],
    ISNULL(ss.[TotalSales], 0) AS SalesAmount,
    ISNULL(ss.[TotalQuantity], 0) AS SalesCount,
    ISNULL(ss.[AvgPrice], 0) AS AvgUnitPrice,
    ss.[LastOrderDate]
FROM 
    ProductHierarchy ph
    LEFT JOIN [dbo].[Categories] c ON ph.[CategoryID] = c.[CategoryID]
    LEFT JOIN SalesSummary ss ON ph.[ProductID] = ss.[ProductID]
ORDER BY 
    ph.[HierarchyLevel], ph.[ProductName];

-- Используем оконные функции и условные выражения
SELECT TOP 20
    ps.[ProductID],
    ps.[ProductName],
    ps.[CategoryName],
    ps.[SalesAmount],
    ps.[SalesCount],
    ps.[AvgUnitPrice],
    CONVERT(VARCHAR(10), ps.[LastSaleDate], 120) AS LastSaleDate,
    CASE 
        WHEN ps.[SalesAmount] > 10000 THEN 'High Volume'
        WHEN ps.[SalesAmount] > 5000 THEN 'Medium Volume'
        WHEN ps.[SalesAmount] > 0 THEN 'Low Volume'
        ELSE 'No Sales'
    END AS SalesVolume,
    ROW_NUMBER() OVER (PARTITION BY ps.[CategoryID] ORDER BY ps.[SalesAmount] DESC) AS CategoryRank,
    PERCENT_RANK() OVER (ORDER BY ps.[SalesAmount]) AS PercentRank,
    DATEADD(day, -DATEDIFF(day, 0, ps.[LastSaleDate]), ps.[LastSaleDate]) AS LastSaleTime,
    FORMAT(ps.[SalesAmount], 'C', 'en-US') AS FormattedSales
FROM 
    @ProductSales ps
WHERE 
    ps.[SalesAmount] > {minSalesAmount}
ORDER BY 
    ps.[SalesAmount] DESC;
