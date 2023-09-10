CREATE PROCEDURE [bronze.NCCM].SP_CLIENT_CASE
AS
BEGIN

    MERGE [bronze.NCCM].CLIENT_CASE AS trg
    USING [bronze.NCCM].VW_CLIENT_CASE AS src
    ON (trg.ID = src.ID)
    
    WHEN MATCHED AND trg.RowHashSum <> src.RowHashSum THEN
        UPDATE SET
            trg.[CLIENT_CONTACT__C] = src.[CLIENT_CONTACT__C],
            trg.[FUNDING_PROGRAM__C] = src.[FUNDING_PROGRAM__C],
            trg.[ISDELETED] = src.[ISDELETED],
            trg.[RowHashSum] = src.[RowHashSum]
    WHEN NOT MATCHED BY TARGET THEN 
        INSERT (
            [ID],
            [CLIENT_CONTACT__C],
            [FUNDING_PROGRAM__C],
            [ISDELETED],
            [RowHashSum]
        ) VALUES (
            src.[ID],
            src.[CLIENT_CONTACT__C],
            src.[FUNDING_PROGRAM__C],
            src.[ISDELETED],
            src.[RowHashSum]
        );

END;