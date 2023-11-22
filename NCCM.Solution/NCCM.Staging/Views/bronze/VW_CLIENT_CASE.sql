﻿CREATE VIEW [bronze.NCCM].VW_CLIENT_CASE
AS
    SELECT 
        stm.*,
        CONVERT(VARCHAR(32), HASHBYTES('MD5', CONCAT(
           stm.CLIENT_CONTACT__C,
           stm.FUNDING_PROGRAM__C,
           stm.FUNDING_REGION,
           stm.STAGE__C,
           stm.CLIENT_COMMENCEMENT_DATE__C,
           stm.ISDELETED)), 2
        ) AS ROW_HASH_SUM
    FROM (
        SELECT
            NULLIF(NULLIF(ca.ID, 'NULL'), '') AS ID,
            NULLIF(NULLIF(ca.CLIENT_CONTACT__C, 'NULL'), '') AS CLIENT_CONTACT__C,
            NULLIF(NULLIF(ca.FUNDING_PROGRAM__C, 'NULL'), '') AS FUNDING_PROGRAM__C,
            NULLIF(NULLIF(ca.FUNDING_REGION, 'NULL'), '') AS FUNDING_REGION,
            NULLIF(NULLIF(ca.STAGE__C, 'NULL'), '') AS STAGE__C,
            CAST(NULLIF(NULLIF(ca.CLIENT_COMMENCEMENT_DATE__C, 'NULL'), '') AS DATE) AS CLIENT_COMMENCEMENT_DATE__C,
            (CASE LTRIM(RTRIM(LOWER(NULLIF(ca.ISDELETED, 'NULL')))) WHEN 'false' THEN 0 WHEN 'true' THEN 1 ELSE NULL END) AS ISDELETED
        FROM [raw.NCCM].FFS_CLIENT_CASE AS tbl
            CROSS APPLY (
                SELECT 
                    *
                FROM OPENJSON(tbl.RECORD)
                WITH (
                    ID VARCHAR(18) 'lax$."ID"',
                    CLIENT_CONTACT__C VARCHAR(18) 'lax$."CLIENT_CONTACT__C"',
                    FUNDING_PROGRAM__C VARCHAR(255) 'lax$."FUNDING_PROGRAM__C"',
                    FUNDING_REGION VARCHAR(255) 'lax$."FUNDING_REGION"',
                    STAGE__C VARCHAR(255) 'lax$."STAGE__C"',
                    CLIENT_COMMENCEMENT_DATE__C VARCHAR(255) 'lax$."CLIENT_COMMENCEMENT_DATE__C"',
                    ISDELETED VARCHAR(255) 'lax$."ISDELETED"'
                )
            ) AS ca
    ) AS stm;