CREATE VIEW [bronze.NCCM].VW_PLACEMENT
AS
    SELECT 
        stm.*,
        CONVERT(VARCHAR(32), HASHBYTES('MD5', CONCAT(
            stm.ANCHOR_DATE__C,
            stm.CONTACT__C,
            stm.END_DATE__C,
            stm.INTERVIEW_DATE__C,
            stm.PLACED_DATE__C,
            stm.PLACEMENT_TYPE__C,
            stm.REFERRED_TO_EMPLOYER_DATE__C,
            stm.START_DATE__C,
            stm.STATUS__C,
            stm.VACANCY_ID__C,
            stm.ISDELETED)), 2
        ) AS ROW_HASH_SUM
    FROM (
        SELECT
            NULLIF(NULLIF(ca.ID, 'NULL'), '') AS ID,
            CAST(NULLIF(NULLIF(NULLIF(ca.ANCHOR_DATE__C, 'NULL'), ''), 'NULL') AS DATE) AS ANCHOR_DATE__C,
            NULLIF(NULLIF(ca.CONTACT__C, 'NULL'), '') AS CONTACT__C,
            CAST(NULLIF(NULLIF(NULLIF(ca.END_DATE__C, 'NULL'), ''), 'NULL') AS DATE) AS END_DATE__C,
            CAST(NULLIF(NULLIF(NULLIF(ca.INTERVIEW_DATE__C, 'NULL'), ''), 'NULL') AS DATE) AS INTERVIEW_DATE__C,
            CAST(NULLIF(NULLIF(NULLIF(ca.PLACED_DATE__C, 'NULL'), ''), 'NULL') AS DATE) AS PLACED_DATE__C,
            NULLIF(NULLIF(ca.PLACEMENT_TYPE__C, 'NULL'), '') AS PLACEMENT_TYPE__C,
            NULLIF(NULLIF(ca.REFERRED_TO_EMPLOYER_DATE__C, 'NULL'), '') AS REFERRED_TO_EMPLOYER_DATE__C,
            CAST(NULLIF(NULLIF(NULLIF(ca.START_DATE__C, 'NULL'), ''), 'NULL') AS DATE) AS START_DATE__C,
            NULLIF(NULLIF(ca.STATUS__C, 'NULL'), '') AS STATUS__C,
            NULLIF(NULLIF(ca.VACANCY_ID__C, 'NULL'), '') AS VACANCY_ID__C,
            (CASE LTRIM(RTRIM(LOWER(NULLIF(ca.ISDELETED, 'NULL')))) WHEN 'false' THEN 0 WHEN 'true' THEN 1 ELSE NULL END) AS ISDELETED
        FROM [raw.NCCM].FFS_PLACEMENT AS tbl
            CROSS APPLY (
                SELECT 
                    *
                FROM OPENJSON(tbl.RECORD)
                WITH (
                    ID VARCHAR(18) 'lax$."ID"',
                    ANCHOR_DATE__C VARCHAR(255) 'lax$."ANCHOR_DATE__C"',
                    CONTACT__C VARCHAR(18) 'lax$."CONTACT__C"',
                    END_DATE__C VARCHAR(255) 'lax$."END_DATE__C"',
                    INTERVIEW_DATE__C VARCHAR(255) 'lax$."INTERVIEW_DATE__C"',
                    PLACED_DATE__C VARCHAR(255) 'lax$."PLACED_DATE__C"',
                    PLACEMENT_TYPE__C VARCHAR(255) 'lax$."PLACEMENT_TYPE__C"',
                    REFERRED_TO_EMPLOYER_DATE__C VARCHAR(255) 'lax$."REFERRED_TO_EMPLOYER_DATE__C"',
                    START_DATE__C VARCHAR(255) 'lax$."START_DATE__C"',
                    STATUS__C VARCHAR(255) 'lax$."STATUS__C"',
                    VACANCY_ID__C VARCHAR(18) 'lax$."VACANCY_ID__C"',
                    ISDELETED VARCHAR(255) 'lax$."ISDELETED"'
                )
            ) AS ca
    ) AS stm;