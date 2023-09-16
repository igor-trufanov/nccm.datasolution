CREATE PROCEDURE [bronze.NCCM].SP_JOB_ORDERS
AS
BEGIN

    MERGE [bronze.NCCM].JOB_ORDERS AS trg
    USING [bronze.NCCM].VW_JOB_ORDERS AS src
    ON (trg.ID = src.ID)
    
    WHEN MATCHED AND trg.RowHashSum <> src.RowHashSum THEN
        UPDATE SET
            trg.[HOST_ORGANISATION] = src.[HOST_ORGANISATION],
            trg.[JOB_ORDER_TYPE] = src.[JOB_ORDER_TYPE],
            trg.[JOB_TITLE] = src.[JOB_TITLE],
            trg.[JOB_TYPE] = src.[JOB_TYPE],
            trg.[ISDELETED] = src.[ISDELETED],
            trg.[RowHashSum] = src.[RowHashSum]
    WHEN NOT MATCHED BY TARGET THEN 
        INSERT (
            [ID],
            [HOST_ORGANISATION],
            [JOB_ORDER_TYPE],
            [JOB_TITLE],
            [JOB_TYPE],
            [ISDELETED],
            [RowHashSum]
        ) VALUES (
            src.[ID],
            src.[HOST_ORGANISATION],
            src.[JOB_ORDER_TYPE],
            src.[JOB_TITLE],
            src.[JOB_TYPE],
            src.[ISDELETED],
            src.[RowHashSum]
        );

END;