CREATE VIEW [bronze.NCCM].VW_OUTCOME
AS
    SELECT 
        stm.*,
        CONVERT(VARCHAR(32), HASHBYTES('MD5', CONCAT(
            stm.WRD_PHASE__C,
            stm.RECORD_TYPE_NAME__C,
            stm.CLIENT_CASE__C,
            stm.ISDELETED)), 2
        ) AS RowHashSum
    FROM (
        SELECT
            NULLIF(CAST(tbl.WRD_PHASE__C  AS VARCHAR(1024)), '') AS WRD_PHASE__C,
            NULLIF(CAST(tbl.RECORD_TYPE_NAME__C  AS VARCHAR(1024)), '') AS RECORD_TYPE_NAME__C,
            NULLIF(CAST(tbl.ID  AS VARCHAR(18)), '') AS ID,
            NULLIF(CAST(tbl.CLIENT_CASE__C  AS VARCHAR(18)), '') AS CLIENT_CASE__C,
            CAST(CAST(tbl.ISDELETED AS CHAR) AS INT) AS ISDELETED
        FROM [$(Staging5)].dbo.SFICMS_Outcomes__c AS tbl
    ) AS stm;