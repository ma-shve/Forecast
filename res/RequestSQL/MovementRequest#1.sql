DECLARE @StartDateNew DATETIME = DATETIME2FROMPARTS(2018,01,01,00,00,00,00,00)/*(2018,01,01)*/
DECLARE @EndTime DATETIME = DATETIME2FROMPARTS(2018,05,31,00,00,00,00,00)/*(2018,05,31)*/
/***************************************************************************Артикул выборки****************************************************************************************************/
DECLARE @Art NVARCHAR(25) = 'фк380'
DECLARE @Whouse NVARCHAR(25) = 'Питер НОВЫЙ'
/***********************************************************************************************************************************************************************************************/

/***********************************************************************Движение по выбранным складам****************************************************************************************************/
SELECT
	DATEFROMPARTS(DATEPART ( yy , T1._Period ) - 2000,DATEPART ( mm , T1._Period ), DATEPART ( dd , T1._Period  )) as Period,
	T3._Description as Whouse,
	T2._Fld3394 as Art,
	CASE	
		WHEN T1._RecordKind = 1
			THEN CAST(SUM(T1._Fld16878) AS INT) * (-1)
		ELSE CAST(SUM(T1._Fld16878) AS INT)
	END as Количество 
/*INTO #tt2*/
FROM dbo._AccumRg16872 T1
LEFT OUTER JOIN dbo._Reference142 T2
ON (T1._Fld16873RRef = T2._IDRRef) AND (T2._Fld725 = 0.0)
LEFT OUTER JOIN dbo._Reference223 T3
ON (T1._Fld16875RRef = T3._IDRRef) AND (T3._Fld725 = 0.0)
WHERE 
	(T1._Fld725 = 0.0)
	/*AND T3._Description = @Whouse
	AND (T2._Fld3394 = @Art)*/
	AND (DATEFROMPARTS(DATEPART ( yy , T1._Period ) - 2000,DATEPART ( mm , T1._Period ), DATEPART ( dd , T1._Period  )) >= @StartDateNew
	AND DATEFROMPARTS(DATEPART ( yy , T1._Period ) - 2000,DATEPART ( mm , T1._Period ), DATEPART ( dd , T1._Period  ))  <= @EndTime)
GROUP BY 
	T1._Period,
	T3._Description,
	T2._Fld3394,
	T1._RecordKind	