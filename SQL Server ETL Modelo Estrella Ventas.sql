
-- PROCESOS ETL

-- Pasos:

-- Utilizaremos códis de SQL Server para crear tablas de hechos y tablas de dimensiones.
-- Crearemos vistas a partir de cada consulta
-- Insertaremos los valores de las vistas en una base de datos nuevas (DataWarehouse)
-- Creamos una base de datos nueva llamada DataWarehouse
-- Estas tareas se pueden automatizar utilizando diferentes programas de integración como 
--	SQL Server Integration Services (SSIS) / Oracle Data Integrator / Pentaho

-- Nota 1: Para borrar una vista o una tabla, se debe utilizar la clausula DROP
-- e.g. DROP VIEW VWVentas
-- e.g. DROP TABLE [DataWarehouse].dbo.FactVentas
-- Nota 2: ¡OJO! Esta estructura NO es la misma que viene presente en AdventureWorksDW2017

--DROP VIEW VWVentas
--DROP VIEW VWDimFecha
--DROP VIEW VWShipMethod
--DROP VIEW VWDimProduct
--DROP VIEW VWDimClientesIN
--DROP VIEW VWDimClienteT
--DROP VIEW VWDimTerritory;

--DROP TABLE  [DataWarehouse].dbo.FactVentas
--DROP TABLE [DataWarehouse].dbo.DimFecha
--DROP TABLE [DataWarehouse].dbo.DimShipMethod
--DROP TABLE [DataWarehouse].dbo.DimProduct
--DROP TABLE [DataWarehouse].dbo.DimClientesIN
--DROP TABLE [DataWarehouse].dbo.DimClientesT
--DROP TABLE [DataWarehouse].dbo.DimTerritory


-- ESCENARIO VENTAS 

DROP VIEW VWVentas
USE AdventureWorks2017;
GO
-- Fuente: Tabla de Hechos Ventas

CREATE VIEW VWVentas
AS
-- Ejecutemos la consulta antes de convertirla en vista, para comprobar su funcionamiento
SELECT soh.SalesOrderID,
		soh.OrderDate,
		soh.DueDate,
		soh.ShipDate,
		DATEDIFF(dd,soh.ShipDate, soh.DueDate) AS DayToShip,
		soh.SalesOrderNumber,
		soh.CustomerID,
		soh.TerritoryID,
		soh.ShipMethodID,
		soh.SubTotal,
		soh.TaxAmt,
		soh.TaxAmt/SubTotal AS TaxAmountPercentage,
		soh.SubTotal + soh.TaxAmt AS TotalPrice,
		sod.UnitPrice * sod.OrderQty / (soh.SubTotal + soh.TaxAmt) AS PercentageTotalSaleOrder,
		sod.OrderQty,
		sod.ProductID,
		sod.UnitPrice,
		sod.UnitPriceDiscount,
		sod.UnitPrice * sod.OrderQty AS TotalPriceProduct
FROM Sales.SalesOrderHeader AS soh
	INNER JOIN Sales.SalesOrderDetail AS sod 
		ON sod.SalesOrderID = soh.SalesOrderID;
GO
		
-- Destino: Tabla de Hechos Ventas

CREATE TABLE [DataWarehouse].dbo.FactVentas(
	SalesOrderID INT,
	OrderDate  DATETIME,
	DueDate DATETIME,
	ShipDate DATETIME,
	DayToShip INT,
	SalesOrderNumber VARCHAR(100),
	CustomerID INT,
	TerritoryID INT,
	ShipMethodID INT, 
	SubTotal NUMERIC(38,4),
	TaxAmt NUMERIC(38,4),
	TaxAmountPercentage NUMERIC(38,4),
	TotalPrice NUMERIC(38,4),
	PercentageTotalSaleOrder NUMERIC(38,4),
	OrderQty INT,
	ProductID INT,
	UnitPrice NUMERIC(38,4),
	UnitPriceDiscount NUMERIC(38,4),
	TotalPriceProduct NUMERIC(38,4)
	);

-- Realizamos la migración de la Base de Datos de AdventureWorks a Datawarehouse

INSERT INTO [DataWarehouse].FactVentas
SELECT * FROM VWVentas;
GO

SELECT*FROM FactVentas
---------------------------------------------------------------------------------------------------------------


-- Fuente: Tabla de Dimension Fecha
CREATE VIEW VWDimFecha
AS
-- Ejecutemos la consulta antes de convertirla en vista, para comprobar su funcionamiento
SELECT DISTINCT
    OrderDate AS [Date] ,
    DATEPART(DAY,OrderDate) AS [Day] ,
    DATENAME(WEEKDAY,OrderDate) AS [DayName],
    DATEPART(WEEK,OrderDate) AS [Week],
    DATEPART(WEEKDAY,OrderDate) AS [DayOfWeek],
    DATEPART(MONTH,OrderDate) AS [Month],
    DATENAME(MONTH,OrderDate) AS [MonthName],
    DATEPART(Quarter,OrderDate) AS [Quarter],
    DATEPART(YEAR,OrderDate) AS [Year],
    DATEPART(DAYOFYEAR,OrderDate) AS [DayOfYear]
FROM Sales.SalesOrderHeader;
GO


-- Destino: Tabla de DimFecha

CREATE TABLE [DataWarehouse].dbo.DimFecha(
	[Date]  DATETIME,
    [Day] INT, 
    [DayName] VARCHAR(50),
    [Week]  INT,
    [DayOfWeek]  INT,
    [Month] INT,
    [MonthName]  VARCHAR(50),
    [Quarter]  INT,
    [Year]    INT,
    [DayOfYear] INT,
	);

-- Realizamos la migración de la Base de Datos de AdventureWorks a Datawarehouse

INSERT INTO [DataWarehouse].dbo.DimFecha
SELECT * FROM VWDimFecha;
GO

select* from DimFecha
---------------------------------------------------------------------------------------------------------------

-- Fuente: Tabla de Dimension Metodo Envio 

CREATE VIEW VWShipMethod
AS
-- Ejecutemos la consulta antes de convertirla en vista, para comprobar su funcionamiento
SELECT ShipMethodID,
	Name AS ShipMethodName,
	ShipBase,
	ShipRate
FROM Purchasing.ShipMethod;
GO


CREATE TABLE [DataWarehouse].dbo.DimShipMethod(
	ShipMethodID INT,
	ShipMethodName VARCHAR(100),
	ShipBase NUMERIC(4,2),
	ShipRate NUMERIC(4,2)
	);

-- Realizamos la migración de la Base de Datos de AdventureWorks a Datawarehouse

INSERT INTO [DataWarehouse].dbo.DimShipMethod
SELECT * FROM VWShipMethod;
GO

-- Fuente: Tabla de Dimension Producto

CREATE VIEW VWDimProduct
AS
-- Ejecutemos la consulta antes de convertirla en vista, para comprobar su funcionamiento
SELECT  pp.ProductID,
		pp.Name AS ProductName,
		pp.StandardCost, 
		pp.ListPrice,
		CASE	
			WHEN pp.ListPrice = 0 OR pp.StandardCost = 0 THEN 0
			ELSE pp.ListPrice / pp.StandardCost
		END AS Earning,
		ISNULL(pp.Size, 'No') AS Size,
		ISNULL(pp.SizeUnitMeasureCode, 'No') AS SizeUnitMeasure,
		ISNULL(pp.WeightUnitMeasureCode, 'No') AS WeightUnitMeasure, 
		ISNULL(pp.Weight, 0) AS Weight,
		ISNULL(pp.ProductLine, 'No') AS ProductLine, -- R = Road, M = Mountain, T = Touring, S = Standard,
		ISNULL(pp.Class, 'No') AS Class, -- H = High, M = Medium, L = Low,
		ISNULL(pp.Style, 'No') AS Style, -- W = Womens, M = Mens, U = Universal
		ISNULL(psc.Name, 'No SubCat') AS SubCatery,
		ISNULL(pm.Name, 'No Model') AS ProductModel
FROM  Production.Product AS pp
	LEFT JOIN Production.ProductSubcategory AS psc
		on psc.ProductSubcategoryID = pp.ProductSubcategoryID
	LEFT JOIN Production.ProductModel AS pm
		ON pm.ProductModelID = pp.ProductModelID;
GO


CREATE TABLE [DataWarehouse].dbo.DimProduct(
	ProductID INT,
	ProductName VARCHAR(MAX),
	StandardCost NUMERIC(38,4),
	ListPrice NUMERIC(38,4),
	Earning NUMERIC(38,4),
	Size VARCHAR(100),
	SizeUnitMeasure VARCHAR(100),
	WeightUnitMeasure VARCHAR(100),
	[Weight] NUMERIC(38,4),
	ProductLine VARCHAR(100),
	Class VARCHAR(100),
	Style VARCHAR(100),
	SubCatery VARCHAR(100),
	ProductModel VARCHAR(100)
	);

INSERT INTO [DataWarehouse].dbo.DimProduct
SELECT * FROM VWDimProduct;
GO
---------------------------------------------------------------------------------------------------------------


-- Fuente: Tabla de Dimension Clientes Individuos

CREATE VIEW VWDimClientesIN
AS
-- Ejecutemos la consulta antes de convertirla en vista, para comprobar su funcionamiento
SELECT sc.CustomerID, 
		pp.FirstName + ' ' + pp.LastName AS ClientName, 
		vpd.MaritalStatus,
		vpd.YearlyIncome,
		vpd.Gender,
		vpd.TotalChildren,
		vpd.NumberChildrenAtHome,
		vpd.Education,
		vpd.Occupation
FROM Sales.Customer AS sc
	INNER JOIN Person.Person AS pp 
		ON pp.BusinessEntityID = sc.PersonID
	INNER JOIN Sales.vPersonDemographics AS vpd 
		ON vpd.BusinessEntityID = sc.PersonID
WHERE pp.PersonType = 'IN';
GO


CREATE TABLE [DataWarehouse].dbo.DimClientesIN(
	CustomerID INT,
	ClientName VARCHAR(MAX),
	MaritalStatus VARCHAR(1),
	YearlyIncome VARCHAR(255),
	Gender VARCHAR(1),
	TotalChildren INT,
	NumberChildrenAtHome INT,
	Education VARCHAR(100),
	Occupation VARCHAR(100)
);

INSERT INTO [DataWarehouse].dbo.DimClientesIN
SELECT * FROM VWDimClientesIN;
GO
---------------------------------------------------------------------------------------------------------------


-- Fuente: Tabla de Dimension Clientes Tiendas

CREATE VIEW VWDimClienteT
AS
-- Ejecutemos la consulta antes de convertirla en vista, para comprobar su funcionamiento
SELECT sc.CustomerID,
ss.Name AS StoreName,
sst.Name AS Territory,
sst.CountryRegionCode,
sst.[Group] AS RegionGroup
FROM sales.Customer AS sc
	INNER JOIN Sales.Store AS ss 
		ON ss.BusinessEntityID = sc.StoreID
	INNER JOIN Sales.SalesTerritory AS sst 
		ON sst.TerritoryID = sc.TerritoryID;
GO


CREATE TABLE [DataWarehouse].dbo.DimClientesT(
	CustomerID INT,
	StoreName VARCHAR(100),
	Territory VARCHAR(50),
	CountryRegionCode VARCHAR(3),
	RegionGroup  VARCHAR(50)
	);


INSERT INTO [DataWarehouse].dbo.DimClientesT
SELECT * FROM VWDimClienteT;
GO
---------------------------------------------------------------------------------------------------------------



-- Fuente: Tabla de Dimension Territorio

CREATE VIEW VWDimTerritory
AS
-- Ejecutemos la consulta antes de convertirla en vista, para comprobar su funcionamiento
 SELECT TerritoryID,
		Name AS TerritoryName,
		CountryRegionCode, 
		[Group],
		CASE	
			WHEN CountryRegionCode = 'US' THEN 37.09024
			WHEN CountryRegionCode = 'CA' THEN 37.2502200
			WHEN CountryRegionCode = 'FR' THEN 46.227638
			WHEN CountryRegionCode = 'DE' THEN 51.165691
			WHEN CountryRegionCode = 'AU' THEN -25.274398
			WHEN CountryRegionCode = 'GB' THEN 55.378051
		ELSE 'Check'
		END AS Latitud,
		CASE	
			WHEN CountryRegionCode = 'US' THEN -95.712891
			WHEN CountryRegionCode = 'CA' THEN -119.7512600
			WHEN CountryRegionCode = 'FR' THEN 	2.213749
			WHEN CountryRegionCode = 'DE' THEN 10.451526
			WHEN CountryRegionCode = 'AU' THEN 133.775136
			WHEN CountryRegionCode = 'GB' THEN -3.435973
		ELSE 'Check'
		END AS Longitud
 FROM Sales.SalesTerritory;
 GO
 
 
CREATE TABLE [DataWarehouse].dbo.DimTerritory(
	TerritoryID INT,
	TerritoryName VARCHAR(100),
	CountryRegionCode VARCHAR(5),
	[Group] VARCHAR(100),
	Latitud FLOAT,
	Longitud FLOAT
	);

INSERT INTO [DataWarehouse].dbo.DimTerritory
SELECT * FROM VWDimTerritory;

---------------------------------------------------------------------------------------------------------------
