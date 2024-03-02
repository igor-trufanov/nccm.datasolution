CREATE PROCEDURE [bronze.NCCM].SP_JOB_ORDERS
AS
BEGIN

    MERGE [bronze.NCCM].JOB_ORDERS AS trg
    USING [bronze.NCCM].VW_JOB_ORDERS AS src
    ON (trg.ID = src.ID)
    
    WHEN MATCHED AND trg.ROW_HASH_SUM <> src.ROW_HASH_SUM THEN
        UPDATE SET
            trg.HOST_ORGANISATION = src.HOST_ORGANISATION,
            trg.JOB_ORDER_TYPE = src.JOB_ORDER_TYPE,
            trg.JOB_TITLE = src.JOB_TITLE,
            trg.JOB_TYPE = src.JOB_TYPE,
            trg.ISDELETED = src.ISDELETED,
            trg.ROW_HASH_SUM = src.ROW_HASH_SUM
    WHEN NOT MATCHED BY TARGET THEN 
        INSERT (
            ID,
            HOST_ORGANISATION,
            JOB_ORDER_TYPE,
            JOB_TITLE,
            JOB_TYPE,
            ISDELETED,
            ROW_HASH_SUM
        ) VALUES (
            src.ID,
            src.HOST_ORGANISATION,
            src.JOB_ORDER_TYPE,
            src.JOB_TITLE,
            src.JOB_TYPE,
            src.ISDELETED,
            src.ROW_HASH_SUM
        );


    UPDATE trg
            SET trg.ISDELETED = 1
        FROM [bronze.NCCM].JOB_ORDERS AS trg
            LEFT JOIN [bronze.NCCM].VW_JOB_ORDERS AS src
                ON trg.ID = src.ID
        WHERE 
            trg.ISDELETED = 0 
            AND src.ID IS NULL;

END;