CREATE VIEW [silver.FNC].VW_ACCOUNT_DIM
AS
    SELECT 
        UPPER(CONCAT(stm.GLACCTGRP_KEY, '||', stm.GLACCTGRP_NAME, '||', stm.RECORD_NUMBER)) AS UQ_KEY, -- TBD Account_Number part of UQ?
        HASHBYTES('MD5', UPPER(CONCAT(stm.GLACCTGRP_KEY, '||', stm.GLACCTGRP_NAME, '||', stm.RECORD_NUMBER))) AS UQ_KEY_HASH,
        HASHBYTES('MD5', CONCAT(
            stm.ACCOUNT_NUMBER,
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
            tbl.Accountno AS ACCOUNT_NUMBER,
            tbl2.Totaltitle AS TOTAL_TITLE,
            tbl2.Membertype AS MEMBER_TYPE,
            tbl2.Recordno AS RECORD_NUMBER,
            tbl.Glacctgrptitle AS GLACCTGRP_TITLE,
            0 AS IS_DELETED -- TBD
        FROM [raw.FNC].[Glacctgrphierarchy] AS tbl
            LEFT JOIN [raw.FNC].[Glacctgrp_PROD] AS tbl2
                ON tbl2.Recordno = tbl.Glacctgrpkey
    ) AS stm;
