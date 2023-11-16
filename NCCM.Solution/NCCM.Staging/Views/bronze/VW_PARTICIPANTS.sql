CREATE VIEW [bronze.NCCM].VW_PARTICIPANTS
AS
    SELECT 
        stm.*,
        CONVERT(VARCHAR(32), HASHBYTES('MD5', CONCAT(
            stm.CONTACT__C,
            stm.PROGRAM_ENGAGEMENT__C,
            stm.ISDELETED)), 2
        ) AS ROW_HASH_SUM
    FROM (
        SELECT
            NULLIF(NULLIF(ca.ID, 'NULL'), '') AS ID,
            NULLIF(NULLIF(ca.CONTACT__C, 'NULL'), '') AS CONTACT__C,
            NULLIF(NULLIF(ca.PROGRAM_ENGAGEMENT__C, 'NULL'), '') AS PROGRAM_ENGAGEMENT__C,
            (CASE LTRIM(RTRIM(LOWER(NULLIF(ca.ISDELETED, 'NULL')))) WHEN 'false' THEN 0 WHEN 'true' THEN 1 ELSE NULL END) AS ISDELETED
        FROM [raw.NCCM].FFS_PARTICIPANTS AS tbl
            CROSS APPLY (
                SELECT 
                    *
                FROM OPENJSON(tbl.RECORD)
                WITH (
                    ID VARCHAR(18) 'lax$."ID"',
                    CONTACT__C VARCHAR(18) 'lax$."CONTACT__C"',
                    PROGRAM_ENGAGEMENT__C VARCHAR(18) 'lax$."PROGRAM_ENGAGEMENT__C"',
                    ISDELETED VARCHAR(255) 'lax$."ISDELETED"'
                )
            ) AS ca
    ) AS stm;