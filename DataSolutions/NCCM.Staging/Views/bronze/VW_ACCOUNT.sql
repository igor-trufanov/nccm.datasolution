CREATE VIEW [bronze.NCCM].VW_ACCOUNT
AS
    SELECT 
        stm.*,
        CONVERT(VARCHAR(32), HASHBYTES('MD5', CONCAT(
            stm.EMPLOYER__C,
            stm.ISDELETED)), 2
        ) AS ROW_HASH_SUM
    FROM (
        SELECT
            NULLIF(NULLIF(ca.ID, 'NULL'), '') AS ID,
            NULLIF(NULLIF(ca.EMPLOYER__C, 'NULL'), '') AS EMPLOYER__C,
            (CASE LTRIM(RTRIM(LOWER(CAST(ca.ISDELETED AS VARCHAR(255))))) WHEN 'false' THEN 0 WHEN 'true' THEN 1 ELSE NULL END) AS ISDELETED
        FROM [raw.NCCM].FFS_ACCOUNT AS tbl
            CROSS APPLY (
                SELECT 
                    *
                FROM OPENJSON(tbl.RECORD)
                WITH (
                    ID VARCHAR(18) 'lax$."ID"',
                    EMPLOYER__C NVARCHAR(2048) 'lax$."EMPLOYER__C"',
                    ISDELETED VARCHAR(18) 'lax$."ISDELETED"'
                )
            ) AS ca
    ) AS stm;
