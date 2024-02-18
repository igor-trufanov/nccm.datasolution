CREATE VIEW [gold.NCCM].VW_JOB_ORDERS_DIM
AS
    SELECT
        tbl.[JOB_ORDER_DIM_ID],
        tbl.[ARCHIVAL],
        tbl.[JOB_ORDER_ID],
        tbl.[CONTACT_DIM_ID],
        tbl.[HOST_ORGANISATION],
        tbl.[JOB_ORDER_TYPE],
        tbl.[JOB_TITLE],
        tbl.[JOB_TYPE],
        tbl.[IS_DELETED],
        tbl.[ROW_CALENDAR_DATE],
        tbl.[ROW_CREATED_AT],
        tbl.[ROW_UPDATED_AT]
    FROM [silver.NCCM].JOB_ORDERS_DIM AS tbl