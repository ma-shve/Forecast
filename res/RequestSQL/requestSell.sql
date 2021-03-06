DECLARE @StartDate DATETIME = DATETIME2FROMPARTS(2018,1,1,00,00,00,00,00)

DECLARE @EndDate DATETIME = DATETIME2FROMPARTS(2018,1,31,23,59,59,00,00)

SELECT
	
	T3._Fld3394 as Art,
	
	DATEFROMPARTS(DATEPART ( yy , T1._Period ) - 2000,DATEPART ( mm , T1._Period ), DATEPART ( dd , T1._Period  )) as Period,
	
	CASE 
		
		WHEN T1._Fld15445_TYPE = 0x08 
			
			AND T1._Fld15445_RTRef = 0x000000F1 THEN T4._Description WHEN T1._Fld15445_TYPE = 0x08 
			
			AND T1._Fld15445_RTRef = 0x000000A1 THEN T5._Description WHEN T1._Fld15445_TYPE = 0x08 
			
			AND T1._Fld15445_RTRef = 0x000000DF THEN T6._Description ELSE CAST(NULL AS NVARCHAR(100)) 
	
	END as WH,
	
	CAST(SUM(T1._Fld15450) AS INT) as Qnty

FROM 
	
	dbo._AccumRg15437 T1
	
	LEFT OUTER JOIN dbo._Reference120 T2
		
		ON (T1._Fld15438RRef = T2._IDRRef) AND (T2._Fld725 = 0.0)
	
	LEFT OUTER JOIN dbo._Reference142 T3
		
		ON (T2._Fld3030RRef = T3._IDRRef) AND (T3._Fld725 = 0.0)
	
	LEFT OUTER JOIN dbo._Reference241 T4
		
		ON (T1._Fld15445_TYPE = 0x08 AND T1._Fld15445_RTRef = 0x000000F1 AND T1._Fld15445_RRRef = T4._IDRRef) AND (T4._Fld725 = 0.0)
	
	LEFT OUTER JOIN dbo._Reference161 T5
		
		ON (T1._Fld15445_TYPE = 0x08 AND T1._Fld15445_RTRef = 0x000000A1 AND T1._Fld15445_RRRef = T5._IDRRef) AND (T5._Fld725 = 0.0)
	
	LEFT OUTER JOIN dbo._Reference223 T6
		
		ON (T1._Fld15445_TYPE = 0x08 AND T1._Fld15445_RTRef = 0x000000DF AND T1._Fld15445_RRRef = T6._IDRRef) AND (T6._Fld725 = 0.0)

WHERE 
	
	T1._Fld725 = 0.0
	
	AND T3._Fld3394 = '10'
	
	AND (DATEADD (YYYY , -2000 , T1._Period ) >= @StartDate
	
	AND DATEADD (YYYY , -2000 , T1._Period ) <= @EndDate)
	
	AND 
	
	CASE 
		
		WHEN T1._Fld15445_TYPE = 0x08 
			
			AND T1._Fld15445_RTRef = 0x000000F1 THEN T4._Description WHEN T1._Fld15445_TYPE = 0x08 
			
			AND T1._Fld15445_RTRef = 0x000000A1 THEN T5._Description WHEN T1._Fld15445_TYPE = 0x08 
			
			AND T1._Fld15445_RTRef = 0x000000DF THEN T6._Description ELSE CAST(NULL AS NVARCHAR(100)) END IS NOT NULL

GROUP BY 
	
	T3._Fld3394,
	
	T1._Period,
	
	CASE 
		
		WHEN T1._Fld15445_TYPE = 0x08 
			
		AND T1._Fld15445_RTRef = 0x000000F1 THEN T4._Description WHEN T1._Fld15445_TYPE = 0x08 
			
		AND T1._Fld15445_RTRef = 0x000000A1 THEN T5._Description WHEN T1._Fld15445_TYPE = 0x08 
			
		AND T1._Fld15445_RTRef = 0x000000DF THEN T6._Description ELSE CAST(NULL AS NVARCHAR(100)) 
	END
