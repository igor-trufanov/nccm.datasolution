CREATE VIEW [silver.FNC].VW_TRANSACTION_TITLE_DIM
AS
    SELECT 
        UPPER(CONCAT(stm.GLACCTGRP_KEY, '||', stm.RECORD_NUMBER, '||', stm.ACCOUNT_NUMBER)) AS UQ_KEY,
        HASHBYTES('MD5', UPPER(CONCAT(stm.GLACCTGRP_KEY, '||', stm.RECORD_NUMBER, '||', stm.ACCOUNT_NUMBER))) AS UQ_KEY_HASH,
        HASHBYTES('MD5', CONCAT(
            stm.GLACCTGRP_NAME,
            stm.TOTAL_TITLE,
            stm.MEMBER_TYPE,
            stm.GLACCTGRP_TITLE,
            stm.IS_DELETED
        )) AS ROW_HASH_SUM,
        stm.*
    FROM (
        SELECT
	        tbl.Glacctgrpkey AS GLACCTGRP_KEY,
            tbl2.Name AS GLACCTGRP_NAME,
            tbl2.Totaltitle AS TOTAL_TITLE,
            tbl2.Membertype AS MEMBER_TYPE,
            tbl2.Recordno AS RECORD_NUMBER,
            tbl.Glacctgrptitle AS GLACCTGRP_TITLE,
            tbl.Accountno AS ACCOUNT_NUMBER,
            mpg.DIM_ID AS ACCOUNT_DIM_ID,
            0 AS IS_DELETED -- TBD
        FROM [bronze.FNC].[Glacctgrphierarchy] AS tbl
            LEFT JOIN [bronze.FNC].[Glacctgrp_PROD] AS tbl2
                ON tbl2.Recordno = tbl.Glacctgrpkey
            LEFT JOIN [silver.FNC].MAPPING_SURROGATE_KEYS_DIMS as mpg
                ON mpg.UQ_KEY = tbl.Accountno
                AND mpg.DIMENSION = 'ACCOUNT_DIM'
    ) AS stm;
