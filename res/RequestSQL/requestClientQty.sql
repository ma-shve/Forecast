DECLARE @StartDate DATETIME = DATETIME2FROMPARTS(2018,1,1,00,00,00,00,00)
DECLARE @EndDate DATETIME = DATETIME2FROMPARTS(2019,12,31,00,00,00,00,00)

SELECT
	�������.Period,
	�������.�������,
	�������.�����,
	ISNULL(CASE
		WHEN �������.����������� = 0
			THEN 0
		ELSE �������.�������/�������.�����������
	END,0) as �������������	
FROM
(SELECT
ClientQty.Period,
ClientQty.Art as �������,
ClientQty.Whouse as �����,
SUM(ClientQty.Qnty) as �����������
FROM
(
SELECT
	T3._Fld3394 as Art,
	CASE
		WHEN T6._Description = '���������' or T6._Description = '��������� (��������-��)' or T6._Description = '��������� (��� "������ ���� �����")'
			THEN '���������'
		ELSE
			CASE
				WHEN T6._Description = '����� �����' or T6._Description = '����� ����'  
					THEN '�����' 
				ELSE T6._Description
			END
	END	as Whouse, 
	DATEFROMPARTS(DATEPART ( yy , T4._Date_Time ) - 2000,DATEPART ( mm , T4._Date_Time ), DATEPART ( dd , T4._Date_Time  )) as Period,
	CAST(COUNT(DISTINCT T5._Description) AS INT) as Qnty
FROM 
	dbo._Document369_VT11487 T2
		LEFT OUTER JOIN dbo._Reference142 T3
			ON (T2._Fld11489RRef = T3._IDRRef) AND (T3._Fld725 = 0.0)
		LEFT OUTER JOIN dbo._Document369 T4
			ON (T2._Document369_IDRRef = T4._IDRRef) AND (T4._Fld725 = 0.0)
		LEFT OUTER JOIN dbo._Reference161 T5
			ON (T4._Fld11439RRef = T5._IDRRef) AND (T5._Fld725 = 0.0)
		LEFT OUTER JOIN dbo._Reference223 T6
			ON (T4._Fld11444RRef = T6._IDRRef) AND (T6._Fld725 = 0.0)
WHERE 
	(T2._Fld725 = 0.0)
	AND (T4._Posted = 0x01)
	AND (DATEADD (YYYY , -2000 , T4._Date_Time ) >= @StartDate
		AND DATEADD (YYYY , -2000 , T4._Date_Time ) <= @EndDate)
	AND T6._Description in ('���������', '��������', '���������', '��������� (��������-��)', '��������� (��� "������ ���� �����")', 
		'������ ����.�����', '�����������', '����� �����', '����� ����', '�����������')
	/*AND T3._Fld3394 = '3891'*/
GROUP BY 
	T3._Fld3394,
	T6._Description,
	DATEFROMPARTS(DATEPART ( yy , T4._Date_Time ) - 2000,DATEPART ( mm , T4._Date_Time ), DATEPART ( dd , T4._Date_Time  ))) as ClientQty
GROUP BY
	ClientQty.Period,
	ClientQty.Art,
	ClientQty.Whouse) as �������

LEFT JOIN

(SELECT
	Sell.Art as �������,
	Sell.Period,
	SUM(Sell.Qnty) as �������,
	CASE
		WHEN Sell.WH = '���������' or Sell.WH = '��������� (��������-��)' or Sell.WH = '��������� (��� "������ ���� �����")'
			THEN '���������'
		ELSE
			CASE
				WHEN Sell.WH = '����� �����' or Sell.WH = '����� ����'  
					THEN '�����' 
				ELSE Sell.WH
			END
	END	as �����
FROM
(SELECT	
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
	/*AND T3._Fld3394 = '3891'	*/
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
	END) as Sell

GROUP BY

	Sell.Art ,
	Sell.Period,
	CASE
		WHEN Sell.WH = '���������' or Sell.WH = '��������� (��������-��)' or Sell.WH = '��������� (��� "������ ���� �����")'
			THEN '���������'
		ELSE
			CASE
				WHEN Sell.WH = '����� �����' or Sell.WH = '����� ����'  
					THEN '�����' 
				ELSE Sell.WH
			END
	END) as �������

ON �������.������� = �������.�������
	AND �������.Period = �������.Period
	AND �������.����� = �������.�����