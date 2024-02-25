CREATE VIEW [silver.FNC].VW_ACCOUNT_DIM
AS
    SELECT 
        UPPER(stm.ACCOUNT_NUMBER) AS UQ_KEY,
        HASHBYTES('MD5', UPPER(stm.ACCOUNT_NUMBER)) AS UQ_KEY_HASH,
        HASHBYTES('MD5', CONCAT(
            stm.ACCOUNT_TITLE,
            stm.ACCOUNT_NORMAL_BALANCE,
            stm.ACCOUNT_TYPE,
            stm.ACCOUNT_LOCATION_KEY,
            stm.IS_DELETED
        )) AS ROW_HASH_SUM,
        stm.*
    FROM (
        SELECT DISTINCT
	        tbl.[Accountno] AS ACCOUNT_NUMBER,
	        tbl.[Accounttitle] AS ACCOUNT_TITLE,
	        tbl.[Accountnormalbalance] AS ACCOUNT_NORMAL_BALANCE,
	        tbl.[Accounttype] AS ACCOUNT_TYPE,
	        tbl.[Accountlocationkey] AS ACCOUNT_LOCATION_KEY,
            0 AS IS_DELETED -- TBD
        FROM [bronze.FNC].[Glacctgrphierarchy] AS tbl
    ) AS stm;
