CREATE VIEW [bronze.NCCM].VW_CONTACTS
AS
    SELECT 
        stm.*,
        CONVERT(VARCHAR(32), HASHBYTES('MD5', CONCAT(
            stm.LASTNAME,
            stm.FIRSTNAME,
            stm.ACCOUNTID,
            stm.GENDER__C,
            stm.JOBSEEKERID__C,
            stm.BIRTHDATE,
            stm.ISDELETED,
            stm.REGION__C)), 2
        ) AS ROW_HASH_SUM
    FROM (
        SELECT
            NULLIF(NULLIF(ca.ID, 'NULL'), '') AS ID,
            NULLIF(NULLIF(ca.LASTNAME, 'NULL'), '') AS LASTNAME,
            NULLIF(NULLIF(ca.FIRSTNAME, 'NULL'), '') AS FIRSTNAME,
            CAST(NULLIF(NULLIF(ca.BIRTHDATE, 'NULL'), '') AS DATE) AS BIRTHDATE,
            NULLIF(NULLIF(ca.GENDER__C, 'NULL'), '') AS GENDER__C,
            NULLIF(NULLIF(ca.JOBSEEKERID__C, 'NULL'), '') AS JOBSEEKERID__C,
            NULLIF(NULLIF(ca.ACCOUNTID, 'NULL'), '') AS ACCOUNTID,
            NULLIF(NULLIF(ca.REGION__C, 'NULL'), '') AS REGION__C,
            (CASE LTRIM(RTRIM(LOWER(CAST(ca.ISDELETED AS VARCHAR(255))))) WHEN 'false' THEN 0 WHEN 'true' THEN 1 ELSE NULL END) AS ISDELETED
        FROM [raw.NCCM].FFS_CONTACT AS tbl
            CROSS APPLY (
                SELECT 
                    *
                FROM OPENJSON(tbl.RECORD)
                WITH (
                    ID VARCHAR(18) 'lax$."ID"',
                    LASTNAME NVARCHAR(1024) 'lax$."LASTNAME"',
                    FIRSTNAME NVARCHAR(1024) 'lax$."FIRSTNAME"',
                    BIRTHDATE VARCHAR(255) 'lax$."BIRTHDATE"',
                    GENDER__C VARCHAR(255) 'lax$."GENDER__C"',
                    JOBSEEKERID__C VARCHAR(18) 'lax$."JOBSEEKERID__C"',
                    ACCOUNTID VARCHAR(18) 'lax$."ACCOUNTID"',
                    REGION__C VARCHAR(255) 'lax$."REGION__C"',
                    ISDELETED VARCHAR(18) 'lax$."ISDELETED"'
                )
            ) AS ca
    ) AS stm;
