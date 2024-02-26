CREATE VIEW [bronze.NCCM].VW_CLIENT_CASE
AS
    SELECT 
        stm.*,
        CONVERT(VARCHAR(32), HASHBYTES('MD5', CONCAT(
           stm.CLIENT_CONTACT__C,
           stm.FUNDING_PROGRAM__C,
           stm.STAGE__C,
           stm.PROGRAM_ENGAGEMENT_ID,
           stm.CREATED_DATE,
           stm.DATE_OF_EXIT,
           stm.OWNER_ID,
           stm.SUBURB__C,
           stm.POSTAL__CODE,
           stm.CLIENT_COMMENCEMENT_DATE__C,
           stm.ISDELETED)), 2
        ) AS ROW_HASH_SUM
    FROM (
        SELECT
            NULLIF(NULLIF(ca.ID, 'NULL'), '') AS ID,
            NULLIF(NULLIF(ca.CLIENT_CONTACT__C, 'NULL'), '') AS CLIENT_CONTACT__C,
            NULLIF(NULLIF(ca.FUNDING_PROGRAM__C, 'NULL'), '') AS FUNDING_PROGRAM__C,
            NULLIF(NULLIF(ca.STAGE__C, 'NULL'), '') AS STAGE__C,
            NULLIF(NULLIF(ca.PROGRAM_ENGAGEMENT__C, 'NULL'), '') AS PROGRAM_ENGAGEMENT_ID,
            CAST(NULLIF(NULLIF(ca.CREATEDDATE, 'NULL'), '') AS DATE) AS CREATED_DATE,
            CAST(NULLIF(NULLIF(ca.DATE_OF_EXIT__C, 'NULL'), '') AS DATE) AS DATE_OF_EXIT,
            NULLIF(NULLIF(ca.OWNERID, 'NULL'), '') AS OWNER_ID,
            NULLIF(NULLIF(ca.SUBURB__C, 'NULL'), '') AS SUBURB__C,
            NULLIF(NULLIF(ca.POSTCODE__C, 'NULL'), '') AS POSTAL__CODE,
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
                    STAGE__C VARCHAR(255) 'lax$."STAGE__C"',
                    PROGRAM_ENGAGEMENT__C VARCHAR(255) 'lax$."PROGRAM_ENGAGEMENT__C"',
                    CREATEDDATE VARCHAR(255) 'lax$."CREATEDDATE"',
                    DATE_OF_EXIT__C VARCHAR(255) 'lax$."DATE_OF_EXIT__C"',
                    OWNERID VARCHAR(255) 'lax$."OWNERID"',
                    SUBURB__C VARCHAR(255) 'lax$."SUBURB__C"',
                    POSTCODE__C VARCHAR(255) 'lax$."POSTCODE__C"',
                    CLIENT_COMMENCEMENT_DATE__C VARCHAR(255) 'lax$."CLIENT_COMMENCEMENT_DATE__C"',
                    ISDELETED VARCHAR(255) 'lax$."ISDELETED"'
                   
                )
            ) AS ca
    ) AS stm;