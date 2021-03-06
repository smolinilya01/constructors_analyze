SELECT HistoryZakazov.IDZAKAZA,
'''' + CASE WHEN sprGroupKontrAgentov.NUMBERGROUPKONTRAGENTOV != 10 THEN 
  RIGHT(YEAR(HistoryZakazov.CURDATE), 1) + 
  CAST (sprGroupKontrAgentov.NUMBERGROUPKONTRAGENTOV AS VARCHAR) + 
  CASE WHEN LEN(HistoryZakazov.NUMZAKAZA) < 2 THEN '0' + 
  CAST (HistoryZakazov.NUMZAKAZA AS VARCHAR) ELSE 
  CAST (HistoryZakazov.NUMZAKAZA AS VARCHAR) END ELSE 
  HistoryZakazov.NUMZAKAZA end NUMZAKAZA,      

dbo.DateToRusStr([CURDATE])AS DATE_CREATE,

dbo.DateToRusStr((SELECT TOP (1) sprNumListov.DATE_CREATE
  FROM sprNumListov
  WHERE sprNumListov.IDZAKAZA = HistoryZakazov.IDZAKAZA
  ORDER BY 1)) AS DATE_CREATE_FIRST_SHEET,

dbo.DateToRusStr((SELECT TOP (1) sprNumListov.DATE_CREATE
  FROM sprNumListov
  WHERE sprNumListov.IDZAKAZA = HistoryZakazov.IDZAKAZA
  ORDER BY 1 DESC)) AS DATE_CREATE_LAST_SHEET,

dbo.DateToRusStr((SELECT TOP (1) HistoryOtprChartTehn.DATEOTPR
  FROM sprNumListov, HistoryOtprChartTehn
  WHERE sprNumListov.IDZAKAZA = HistoryZakazov.IDZAKAZA
  AND HistoryOtprChartTehn.IDNUMLISTA = sprNumListov.IDNUMLISTA
  AND sprNumListov.TYPELISTA = 1
  ORDER BY 1 DESC)) AS DATE_SEND_LAST_SHEET_TO_TECH,


/*Дата проверки последнего листа технологом*/
dbo.DateToRusStr((SELECT TOP (1) sprNumListov.DATE_CHECKED_OGT
  FROM sprNumListov, HistoryOtprChartTehn
  WHERE sprNumListov.IDZAKAZA = HistoryZakazov.IDZAKAZA
  AND HistoryOtprChartTehn.IDNUMLISTA = sprNumListov.IDNUMLISTA
  ORDER BY 1 DESC)) AS DATE_CHECK_LAST_SHEET_BY_TECH,


/*Дата отправки первой марки в Пр.Отд.*/
dbo.DateToRusStr((SELECT TOP (1) DATEOTPRINPROIZ 
  FROM HistoryMarok 
  WHERE HistoryMarok.IDZAKAZA = HistoryZakazov.IDZAKAZA
  AND OTPRINPROIZV = 1
  ORDER BY 1)) AS DATE_SEND_FIRST_SHEET_IN_PDO,

/*Дата отправки последней марки в Пр.Отд.*/
dbo.DateToRusStr((SELECT TOP (1) DATEOTPRINPROIZ 
  FROM HistoryMarok 
  WHERE HistoryMarok.IDZAKAZA = HistoryZakazov.IDZAKAZA
  AND OTPRINPROIZV = 1
  ORDER BY 1 DESC)) AS DATE_SEND_LAST_SHEET_IN_PDO,



HistoryZakazov.KB_APPLYING_DOCUMENTATION,

ISNULL(CAST(STUFF((SELECT DISTINCT ', ' + sprUser.NameUser
  FROM sprUser, HistoryMarok
  WHERE sprUser.IDUSER = HistoryMarok.IDUSER
  AND HistoryMarok.IDZAKAZA = HistoryZakazov.IDZAKAZA
  GROUP BY sprUser.NameUser
  ORDER BY 1 
FOR XML PATH('')), 1, 2, '') AS VARCHAR(1000)), '') AS LIST_OF_CONSTRUCTORS,

ROUND(ISNULL((SELECT SUM(HistoryMarok.KOLVOT *       
  /*CASE WHEN HistoryPosition.NOSUMVES = 1 THEN 0 ELSE */
    ((HistoryPosition.KOLVOT + HistoryPosition.KOLVON) * 
    HistoryPosition.MASSAONEPOS) * ((HistoryMarok.PROCENTSVARKI + 100) / 100) 
  /*END*/) 

FROM HistoryMarok, HistoryPosition
WHERE HistoryMarok.IDZAKAZA = HistoryZakazov.IDZAKAZA
AND HistoryMarok.IDMAROK = HistoryPosition.IDMAROK 
/*AND HistoryPosition.NOSUMVES = 0*/), 0), 5) AS MASSA_KMD,

ISNULL((SELECT SUM(1)
  FROM sprNumListov
  WHERE sprNumListov.IDZAKAZA = HistoryZakazov.IDZAKAZA
  /*AND sprNumListov.TYPELISTA = 1*/), 0) AS SHEETS_AMOUNT,

ISNULL( (SELECT SUM(1) 
  FROM HistoryMarok
  WHERE HistoryMarok.IDZAKAZA = HistoryZakazov.IDZAKAZA), 0) AS MARKS_NUMBER_AMOUNT,

ISNULL( (SELECT SUM(HistoryMarok.KOLVOT) 
  FROM HistoryMarok
  WHERE HistoryMarok.IDZAKAZA = HistoryZakazov.IDZAKAZA), 0) AS MARKS_AMOUNT,

ISNULL( (SELECT SUM(1) 
  FROM HistoryPosition
  WHERE HistoryPosition.IDZAKAZA = HistoryZakazov.IDZAKAZA), 0) AS POS_NUMBER_AMOUNT,

ISNULL( (SELECT SUM(HistoryMarok.KOLVOT * (HistoryPosition.KOLVOT + HistoryPosition.KOLVON) ) 
  FROM HistoryPosition, HistoryMarok
  WHERE HistoryMarok.IDZAKAZA = HistoryZakazov.IDZAKAZA
  AND HistoryPosition.IDMAROK = HistoryMarok.IDMAROK), 0) AS POS_AMOUNT


FROM HistoryZakazov, sprGroupKontrAgentov
WHERE HistoryZakazov.[CURDATE] >= '10/01/2017' 
  /*YEAR([CURDATE]) BETWEEN 2018 AND 2020*/

AND sprGroupKontrAgentov.IDGROUPKONTRAGENTOV = HistoryZakazov.IDGROUPKONTRAGENTOV 

/*Только заказы с применением КМД с других заказов*/
/*AND HistoryZakazov.KB_APPLYING_DOCUMENTATION = 1*/

AND sprGroupKontrAgentov.NUMBERGROUPKONTRAGENTOV <> 5
/*AND HistoryZakazov.IDZAKAZA IN
  (SELECT HistoryMarok.IDZAKAZA
  FROM HistoryMarok)*/

ORDER BY 1