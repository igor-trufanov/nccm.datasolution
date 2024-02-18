CREATE VIEW [bronze.NCCM].VW_OUTCOME
AS
    SELECT 
        stm.*,
        CONVERT(VARCHAR(32), HASHBYTES('MD5', CONCAT(
            stm.WRD_PHASE__C,
            stm.RECORD_TYPE_NAME__C,
            stm.CLIENT_CASE__C,
            stm.ISDELETED)), 2
        ) AS ROW_HASH_SUM
    FROM (
        SELECT
            NULLIF(NULLIF(ca.WRD_PHASE__C, 'NULL'), '') AS WRD_PHASE__C,
            NULLIF(NULLIF(ca.RECORD_TYPE_NAME__C, 'NULL'), '') AS RECORD_TYPE_NAME__C,
            NULLIF(NULLIF(ca.ID, 'NULL'), '') AS ID,
            NULLIF(NULLIF(ca.CLIENT_CASE__C, 'NULL'), '') AS CLIENT_CASE__C,
            (CASE LTRIM(RTRIM(LOWER(NULLIF(ca.ISDELETED, 'NULL')))) WHEN 'false' THEN 0 WHEN 'true' THEN 1 ELSE NULL END) AS ISDELETED
        FROM [raw.NCCM].FFS_OUTCOME AS tbl
            CROSS APPLY (
                SELECT 
                    *
                FROM OPENJSON(tbl.RECORD)
                WITH (
                    WRD_PHASE__C VARCHAR(1024) 'lax$."WRD_PHASE__C"',
                    RECORD_TYPE_NAME__C VARCHAR(1024) 'lax$."RECORD_TYPE_NAME__C"',
                    ID VARCHAR(18) 'lax$."ID"',
                    CLIENT_CASE__C VARCHAR(18) 'lax$."CLIENT_CASE__C"',
                    ISDELETED VARCHAR(255) 'lax$."ISDELETED"'
                )
            ) AS ca
    ) AS stm;