DECLARE @StartDateNew DATETIME = DATETIME2FROMPARTS(2018,01,01,00,00,00,00,00)/*(2018,01,01)*/

/***************************************************************************Артикул выборки****************************************************************************************************/
DECLARE @Art NVARCHAR(25) = 'фк380'
DECLARE @Whouse NVARCHAR(25) = 'Питер НОВЫЙ'

/****************************************************************************Начальное количество************************************************************************************************/
SELECT 
	T4._Description as Склад,
	T3._Fld3394 as Артикул,
	CAST(SUM(T1.Fld16878Balance_) AS INT) as Qnty
FROM (SELECT
	T2._Period AS _Period,
	T2._Fld16873RRef AS Fld16873RRef,
	T2._Fld16875RRef AS Fld16875RRef,
	CAST(SUM(T2._Fld16878) AS NUMERIC(32, 8)) AS Fld16878Balance_
FROM 
	dbo._AccumRgT16881 T2
WHERE 
	(T2._Fld725 = 0.0)
	AND DATEFROMPARTS(DATEPART ( yy , T2._Period ) - 2000, DATEPART ( mm , T2._Period ), DATEPART ( dd , T2._Period  )) = @StartDateNew
	AND (T2._Fld16878 <> 0.0) 
	AND (T2._Fld16878 <> 0.0)
GROUP BY T2._Fld16873RRef,
T2._Fld16875RRef,
T2._Period
HAVING (CAST(SUM(T2._Fld16878) AS NUMERIC(32, 8))) <> 0.0) T1
LEFT OUTER JOIN dbo._Reference142 T3
ON (T1.Fld16873RRef = T3._IDRRef) AND (T3._Fld725 = 0.0)
LEFT OUTER JOIN dbo._Reference223 T4
ON (T1.Fld16875RRef = T4._IDRRef) AND (T4._Fld725 = 0.0)
/*WHERE 
	T3._Fld3394 = @Art
	AND (T4._Description = @Whouse)*/
GROUP BY 
	CONVERT(varchar(10),DATEFROMPARTS(DATEPART ( yy , T1._Period ) - 2000,DATEPART ( mm , T1._Period ), DATEPART ( dd , T1._Period  )),104),
	T4._Description,
	T3._Fld3394