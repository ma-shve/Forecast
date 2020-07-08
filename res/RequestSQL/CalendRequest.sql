DECLARE @StartDate DATETIME = DATETIME2FROMPARTS(2018,01,01,00,00,00,00,00) /* @StartDateNew - 1 */
DECLARE @StartDateNew DATETIME = DATETIME2FROMPARTS(2018,01,01,00,00,00,00,00)/*(2018,01,01)*/
DECLARE @EndTime DATETIME = DATETIME2FROMPARTS(2018,05,31,00,00,00,00,00)/*(2018,05,31)*/
DECLARE @Qnty INT = 0

/*******************************************************************************Календарь****************************************************************************************************/	
SELECT * 
/*INTO #tcalen*/
FROM   (SELECT @StartDate + RN - 1  AS Period 
        FROM   (SELECT ROW_NUMBER() 
                         OVER ( 
                           ORDER BY (SELECT NULL)) RN 
                FROM   master..[spt_values]) T) T1
		 
WHERE  T1.Period <= @EndTime 