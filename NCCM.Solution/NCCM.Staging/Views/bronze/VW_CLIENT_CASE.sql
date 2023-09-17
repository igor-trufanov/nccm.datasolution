CREATE VIEW [bronze.NCCM].VW_CLIENT_CASE
AS
    SELECT 
        stm.*,
        CONVERT(VARCHAR(32), HASHBYTES('MD5', CONCAT(
           stm.CLIENT_CONTACT__C,
           stm.FUNDING_PROGRAM__C,
           stm.ISDELETED)), 2
        ) AS ROW_HASH_SUM
    FROM (
        SELECT
            NULLIF(CAST(tbl.ID  AS VARCHAR(18)), '') AS ID,
            NULLIF(CAST(tbl.CLIENT_CONTACT__C  AS VARCHAR(18)), '') AS CLIENT_CONTACT__C,
            NULLIF(CAST(tbl.FUNDING_PROGRAM__C  AS VARCHAR(255)), '') AS FUNDING_PROGRAM__C,
            CAST(CAST(tbl.ISDELETED AS CHAR) AS INT) AS ISDELETED
        FROM [$(Staging5)].dbo.SFICMS_Client_Case__c AS tbl
    ) AS stm;