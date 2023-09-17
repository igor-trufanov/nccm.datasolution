CREATE VIEW [silver.NCCM].VW_JOB_ORDERS_DIM
AS
    SELECT 
        UPPER(stm.JOB_ORDER_ID) AS UQ_KEY,
        HASHBYTES('MD5', UPPER(stm.JOB_ORDER_ID)) AS UQ_KEY_HASH,
        '||' AS NK_STRING,
        HASHBYTES('MD5', UPPER(stm.CONTACT)) AS CLIENT_CONTACT_HASH_KEY,
        CONVERT(VARCHAR(32), HASHBYTES('MD5', CONCAT(
            stm.HOST_ORGANISATION,
            stm.JOB_ORDER_TYPE,
            stm.JOB_TITLE,
            stm.JOB_TYPE,
            stm.IS_DELETED
        )), 2) AS ROW_HASH_SUM,
        stm.*
    FROM (
        SELECT
            tbl.ID AS JOB_ORDER_ID,
            tbl.HOST_ORGANISATION AS HOST_ORGANISATION,
            tbl.JOB_ORDER_TYPE AS JOB_ORDER_TYPE,
            tbl.JOB_TITLE AS JOB_TITLE,
            tbl.JOB_TYPE AS JOB_TYPE,
            tbl.ISDELETED AS IS_DELETED,
            prg.PMDM__CONTACT__C AS CONTACT
        FROM [bronze.NCCM].JOB_ORDERS AS tbl
            LEFT JOIN [bronze.NCCM].PROGRAMENGAGEMENT AS prg
                ON prg.PMDM__ACCOUNT__C = tbl.HOST_ORGANISATION
                -- migth be two records?
    ) AS stm;
