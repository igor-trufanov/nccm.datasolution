CREATE VIEW [bronze.NCCM].VW_JOB_ORDERS
AS
    SELECT 
        stm.*,
        CONVERT(VARCHAR(32), HASHBYTES('MD5', CONCAT(
            stm.HOST_ORGANISATION,
            stm.JOB_ORDER_TYPE,
            stm.JOB_TITLE,
            stm.JOB_TYPE,
            stm.ISDELETED)), 2
        ) AS ROW_HASH_SUM
    FROM (
        SELECT
            NULLIF(CAST(tbl.ID AS VARCHAR(18)), '') AS ID,
            NULLIF(CAST(tbl.HOST_ORGANISATION__C  AS VARCHAR(18)), '') AS HOST_ORGANISATION,
            NULLIF(CAST(tbl.JOB_ORDER_TYPE__C  AS VARCHAR(255)), '') AS JOB_ORDER_TYPE,
            NULLIF(CAST(tbl.JOB_TITLE__C  AS NVARCHAR(1024)), '') AS JOB_TITLE,
            NULLIF(CAST(tbl.JOB_TYPE__C  AS NVARCHAR(1024)), '') AS JOB_TYPE,
            (CASE LTRIM(RTRIM(LOWER(CAST(tbl.ISDELETED AS VARCHAR(255))))) WHEN 'false' THEN 0 WHEN 'true' THEN 1 ELSE NULL END) AS ISDELETED
        FROM [$(Staging5)].dbo.SFICMS_Job_Order__C AS tbl
    ) AS stm;