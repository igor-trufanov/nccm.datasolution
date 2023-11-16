CREATE VIEW [bronze.NCCM].VW_JOB_ORDERS
AS
    SELECT 
        stm.*,
        CONVERT(VARCHAR(32), HASHBYTES('MD5', CONCAT(
            stm.HOST_ORGANISATION,
            stm.JOB_ORDER_TYPE,
            stm.JOB_TITLE,
            stm.JOB_TYPE,
            stm.ISDELETED)), 2
        ) AS ROW_HASH_SUM
    FROM (
        SELECT
            NULLIF(NULLIF(ca.ID, 'NULL'), '') AS ID,
            NULLIF(NULLIF(ca.HOST_ORGANISATION__C, 'NULL'), '') AS HOST_ORGANISATION,
            NULLIF(NULLIF(ca.JOB_ORDER_TYPE__C, 'NULL'), '') AS JOB_ORDER_TYPE,
            NULLIF(NULLIF(ca.JOB_TITLE__C, 'NULL'), '') AS JOB_TITLE,
            NULLIF(NULLIF(ca.JOB_TYPE__C, 'NULL'), '') AS JOB_TYPE,
            (CASE LTRIM(RTRIM(LOWER(NULLIF(ca.ISDELETED, 'NULL')))) WHEN 'false' THEN 0 WHEN 'true' THEN 1 ELSE NULL END) AS ISDELETED
        FROM [raw.NCCM].FFS_JOB_ORDERS AS tbl
            CROSS APPLY (
                SELECT 
                    *
                FROM OPENJSON(tbl.RECORD)
                WITH (
                    ID VARCHAR(18) 'lax$."ID"',
                    HOST_ORGANISATION__C  VARCHAR(18) 'lax$."HOST_ORGANISATION__C"',
                    JOB_ORDER_TYPE__C  VARCHAR(255) 'lax$."JOB_ORDER_TYPE__C"',
                    JOB_TITLE__C  NVARCHAR(1024) 'lax$."JOB_TITLE__C"',
                    JOB_TYPE__C  NVARCHAR(1024) 'lax$."JOB_TYPE__C"',
                    ISDELETED VARCHAR(255) 'lax$."ISDELETED"'
                )
            ) AS ca
    ) AS stm;