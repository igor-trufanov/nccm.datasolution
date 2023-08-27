CREATE PROCEDURE [bronze.NCCM].SP_PARTICIPANTS
AS
BEGIN

    MERGE [bronze.HSP].PARTICIPANTS AS trg
    USING [bronze.HSP].VW_PARTICIPANTS AS src
    ON (trg.Id = src.Id)
    
    WHEN MATCHED AND trg.RowHashSum <> src.RowHashSum THEN
        UPDATE SET
            trg.[CONTACT__C] = src.[CONTACT__C],
            trg.[PROGRAM_ENGAGEMENT__C] = src.[PROGRAM_ENGAGEMENT__C],
            trg.[IsDeleted] = src.[IsDeleted],
            trg.[RowHashSum] = src.[RowHashSum]
    WHEN NOT MATCHED BY TARGET THEN 
        INSERT (
            [Id],
            [CONTACT__C],
            [PROGRAM_ENGAGEMENT__C],
            [IsDeleted],
            [RowHashSum]
        ) VALUES (
            src.[Id],
            src.[CONTACT__C],
            src.[PROGRAM_ENGAGEMENT__C],
            src.[IsDeleted],
            src.[RowHashSum]
        );

END;