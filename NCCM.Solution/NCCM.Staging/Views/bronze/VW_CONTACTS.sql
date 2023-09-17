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
            stm.ISDELETED)), 2
        ) AS ROW_HASH_SUM
    FROM (
        SELECT
            NULLIF(CAST(tbl.ID AS VARCHAR(18)), '') AS ID,
            NULLIF(CAST(tbl.LASTNAME AS NVARCHAR(1024)), '') AS LASTNAME,
            NULLIF(CAST(tbl.FIRSTNAME AS NVARCHAR(1024)), '') AS FIRSTNAME,
            NULLIF(CAST(tbl.ACCOUNTID AS VARCHAR(18)), '') AS ACCOUNTID,
            NULLIF(CAST(tbl.GENDER__C AS VARCHAR(255)), '') AS GENDER__C,
            NULLIF(CAST(tbl.JOBSEEKERID__C AS VARCHAR(18)), '') AS JOBSEEKERID__C,
            CAST(NULLIF(NULLIF(CAST(tbl.BIRTHDATE AS VARCHAR(255)), ''), 'NULL') AS DATE) AS BIRTHDATE,
            CAST(CAST(tbl.ISDELETED AS CHAR) AS INT) AS ISDELETED
        FROM [$(Staging)].dbo.[SFICMS_CONTACT] AS tbl
    ) AS stm;
