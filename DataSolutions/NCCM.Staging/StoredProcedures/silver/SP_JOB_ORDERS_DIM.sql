CREATE PROCEDURE [silver.NCCM].SP_JOB_ORDERS_DIM
AS
BEGIN

    SET XACT_ABORT ON;
    SET NOCOUNT OFF;
    SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

    BEGIN TRANSACTION TR1;

    DECLARE @DimName VARCHAR(255) = 'JOB_ORDERS_DIM';
    DECLARE @Timestamp DATETIME = GETUTCDATE();
    DECLARE @Date DATE = @Timestamp;

    ;WITH trg AS (
        SELECT * FROM [silver.NCCM].JOB_ORDERS_DIM WHERE ARCHIVAL = 0
    )
    MERGE trg
    USING (
        SELECT 
            dm.*,
            ks.DIM_ID AS JOB_ORDER_DIM_ID,
            ksc.DIM_ID AS CONTACT_DIM_ID
        FROM [silver.NCCM].VW_JOB_ORDERS_DIM AS dm
            LEFT JOIN [silver.NCCM].MAPPING_SURROGATE_KEYS_DIMS as ks
                ON ks.UQ_KEY_HASH = dm.UQ_KEY_HASH
                AND ks.DIMENSION = @DimName
            LEFT JOIN [silver.NCCM].MAPPING_SURROGATE_KEYS_DIMS as ksc
                ON ksc.UQ_KEY_HASH = dm.CLIENT_CONTACT_HASH_KEY
                AND ksc.DIMENSION = 'CONTACTS_DIM'
    ) AS src
    ON (
        trg.JOB_ORDER_DIM_ID = src.JOB_ORDER_DIM_ID
    )
    WHEN MATCHED AND trg.[ROW_HASH_SUM] <> src.[ROW_HASH_SUM] THEN 
        UPDATE SET
            trg.[NK_STRING] = src.[NK_STRING],
            trg.[ROW_HASH_SUM] = src.[ROW_HASH_SUM],
            trg.[JOB_ORDER_ID] = src.[JOB_ORDER_ID],
            trg.[CONTACT_DIM_ID] = src.[CONTACT_DIM_ID],
            trg.[HOST_ORGANISATION] = src.[HOST_ORGANISATION],
            trg.[JOB_ORDER_TYPE] = src.[JOB_ORDER_TYPE],
            trg.[JOB_TITLE] = src.[JOB_TITLE],
            trg.[JOB_TYPE] = src.[JOB_TYPE],
            trg.[IS_DELETED] = src.[IS_DELETED],
            trg.[ROW_UPDATED_AT] = @Timestamp
    WHEN NOT MATCHED BY TARGET THEN 
        INSERT (
            [JOB_ORDER_DIM_ID],
            [NK_STRING],
            [ROW_HASH_SUM],
            [JOB_ORDER_ID],
            [CONTACT_DIM_ID],
            [HOST_ORGANISATION],
            [JOB_ORDER_TYPE],
            [JOB_TITLE],
            [JOB_TYPE],
            [IS_DELETED],
            [ROW_CALENDAR_DATE],
            [ROW_CREATED_AT],
            [ROW_UPDATED_AT]
        ) VALUES (
            src.[JOB_ORDER_DIM_ID],
            src.[NK_STRING],
            src.[ROW_HASH_SUM],
            src.[JOB_ORDER_ID],
            src.[CONTACT_DIM_ID],
            src.[HOST_ORGANISATION],
            src.[JOB_ORDER_TYPE],
            src.[JOB_TITLE],
            src.[JOB_TYPE],
            src.[IS_DELETED],
            @Date, -- ROW_CALENDAR_DATE
            @Timestamp, -- ROW_CREATED_AT
            @Timestamp -- ROW_UPDATED_AT
        );

    COMMIT TRANSACTION TR1;

END;