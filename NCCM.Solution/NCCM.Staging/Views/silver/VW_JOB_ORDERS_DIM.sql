CREATE VIEW [silver.NCCM].VW_JOB_ORDERS_DIM
AS
    SELECT 
        CONCAT(stm.[JOB_ORDER_ID], '||') AS UQ_KEY,
        CONVERT(VARCHAR(32), HASHBYTES('MD5', CONCAT(
            stm.[HOST_ORGANISATION],
            stm.[JOB_ORDER_TYPE],
            stm.[JOB_TITLE],
            stm.[JOB_TYPE],
            stm.[IS_DELETED]
        )), 2) AS ROW_HASH_SUM,
        stm.*
    FROM (
        SELECT
            tbl.[ID] AS [JOB_ORDER_ID],
            tbl.[HOST_ORGANISATION] AS [HOST_ORGANISATION],
            tbl.[JOB_ORDER_TYPE] AS [JOB_ORDER_TYPE],
            tbl.[JOB_TITLE] AS [JOB_TITLE],
            tbl.[JOB_TYPE] AS [JOB_TYPE],
            tbl.[ISDELETED] AS [IS_DELETED]
        FROM [bronze.NCCM].JOB_ORDERS AS tbl
    ) AS stm;
