CREATE VIEW [bronze.NCCM].VW_CASE_CLIENT
AS
    SELECT 
        stm.*,
        CONVERT(VARCHAR(32), HASHBYTES('MD5', CONCAT(
           stm.CLIENT_CONTACT__C,
           stm.ISDELETED)), 2
        ) AS RowHashSum
    FROM (
        SELECT
            NULLIF(CAST(tbl.ID  AS VARCHAR(18)), '') AS ID,
            NULLIF(CAST(tbl.CLIENT_CONTACT__C  AS VARCHAR(18)), '') AS CLIENT_CONTACT__C,
            CAST(CAST(tbl.ISDELETED AS CHAR) AS INT) AS ISDELETED
        FROM [$(Staging5)].dbo.SFICMS_Client_Case__c AS tbl
    ) AS stm;