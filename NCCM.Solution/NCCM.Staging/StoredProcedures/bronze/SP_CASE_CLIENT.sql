CREATE PROCEDURE [bronze.NCCM].SP_CASE_CLIENT
AS
BEGIN

    MERGE [bronze.NCCM].CASE_CLIENT AS trg
    USING [bronze.NCCM].VW_CASE_CLIENT AS src
    ON (trg.ID = src.ID)
    
    WHEN MATCHED AND trg.RowHashSum <> src.RowHashSum THEN
        UPDATE SET
            trg.[CLIENT_CONTACT__C] = src.[CLIENT_CONTACT__C],
            trg.[ISDELETED] = src.[ISDELETED],
            trg.[RowHashSum] = src.[RowHashSum]
    WHEN NOT MATCHED BY TARGET THEN 
        INSERT (
            [ID],
            [CLIENT_CONTACT__C],
            [ISDELETED],
            [RowHashSum]
        ) VALUES (
            src.[ID],
            src.[CLIENT_CONTACT__C],
            src.[ISDELETED],
            src.[RowHashSum]
        );

END;