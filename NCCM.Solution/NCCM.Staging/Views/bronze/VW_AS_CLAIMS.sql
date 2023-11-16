CREATE VIEW [bronze.NCCM].VW_AS_CLAIMS
AS
    SELECT 
        stm.*,
        CONVERT(VARCHAR(32), HASHBYTES('MD5', CONCAT(
            stm.AS_APPROVED_AMOUNT__C,
            stm.AS_CLAIM_AMOUNT__C,
            stm.AS_CLAIM_ID__C,
            stm.AS_CLAIM_RATE_TYPE__C,
            stm.AS_GST_AMOUNT__C,
            stm.AS_JOBSEEKER_ID__C,
            stm.AS_STATUS_DATE__C,
            stm.AS_STATUS__C,
            stm.NAME,
            stm.OUTCOME_TYPE__C,
            stm.SITE_CODE__C,
            stm.ISDELETED)), 2
        ) AS ROW_HASH_SUM
    FROM (
        SELECT
            CAST(CAST(NULLIF(NULLIF(ca.AS_APPROVED_AMOUNT__C, 'NULL'), '') AS NUMERIC(18, 0)) AS INT) AS AS_APPROVED_AMOUNT__C,
            CAST(CAST(NULLIF(NULLIF(ca.AS_CLAIM_AMOUNT__C, 'NULL'), '') AS NUMERIC(18, 0)) AS INT) AS AS_CLAIM_AMOUNT__C,
            NULLIF(NULLIF(ca.AS_CLAIM_ID__C, 'NULL'), '') AS AS_CLAIM_ID__C,
            NULLIF(NULLIF(ca.AS_CLAIM_RATE_TYPE__C, 'NULL'), '') AS AS_CLAIM_RATE_TYPE__C,
            CAST(CAST(NULLIF(NULLIF(ca.AS_GST_AMOUNT__C, 'NULL'), '') AS NUMERIC(18, 0)) AS INT) AS AS_GST_AMOUNT__C,
            NULLIF(NULLIF(ca.AS_JOBSEEKER_ID__C, 'NULL'), '') AS AS_JOBSEEKER_ID__C,
            CAST(NULLIF(NULLIF(NULLIF(ca.AS_STATUS_DATE__C, 'NULL'), ''), 'NULL') AS DATE) AS AS_STATUS_DATE__C,
            NULLIF(NULLIF(ca.AS_STATUS__C, 'NULL'), '') AS AS_STATUS__C,
            NULLIF(NULLIF(ca.ID, 'NULL'), '') AS ID,
            NULLIF(NULLIF(ca.NAME, 'NULL'), '') AS NAME,
            NULLIF(NULLIF(ca.OUTCOME_TYPE__C, 'NULL'), '') AS OUTCOME_TYPE__C,
            NULLIF(NULLIF(ca.SITE_CODE__C, 'NULL'), '') AS SITE_CODE__C,
            (CASE LTRIM(RTRIM(LOWER(NULLIF(ca.ISDELETED, 'NULL')))) WHEN 'false' THEN 0 WHEN 'true' THEN 1 ELSE NULL END) AS ISDELETED
        FROM [raw.NCCM].FFS_AS_CLAIMS AS tbl
            CROSS APPLY (
                SELECT
                    *
                FROM OPENJSON(tbl.RECORD)
                WITH (
                    AS_APPROVED_AMOUNT__C VARCHAR(255) 'lax$."AS_APPROVED_AMOUNT__C"',
                    AS_CLAIM_AMOUNT__C VARCHAR(255) 'lax$."AS_CLAIM_AMOUNT__C"',
                    AS_CLAIM_ID__C VARCHAR(18) 'lax$."AS_CLAIM_ID__C"',
                    AS_CLAIM_RATE_TYPE__C VARCHAR(255) 'lax$."AS_CLAIM_RATE_TYPE__C"',
                    AS_GST_AMOUNT__C VARCHAR(255) 'lax$."AS_GST_AMOUNT__C"',
                    AS_JOBSEEKER_ID__C VARCHAR(18) 'lax$."AS_JOBSEEKER_ID__C"',
                    AS_STATUS_DATE__C VARCHAR(255) 'lax$."AS_STATUS_DATE__C"',
                    AS_STATUS__C VARCHAR(255) 'lax$."AS_STATUS__C"',
                    ID VARCHAR(18) 'lax$."ID"',
                    NAME NVARCHAR(1024) 'lax$."NAME"',
                    OUTCOME_TYPE__C VARCHAR(255) 'lax$."OUTCOME_TYPE__C"',
                    SITE_CODE__C VARCHAR(255) 'lax$."SITE_CODE__C"',
                    ISDELETED VARCHAR(255) 'lax$."ISDELETED"'
                )
            ) AS ca
    ) AS stm;