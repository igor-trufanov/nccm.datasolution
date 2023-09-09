CREATE PROCEDURE [bronze.NCCM].SP_PLACEMENT
AS
BEGIN

    MERGE [bronze.NCCM].PLACEMENT AS trg
    USING [bronze.NCCM].VW_PLACEMENT AS src
    ON (trg.ID = src.ID)
    
    WHEN MATCHED AND trg.RowHashSum <> src.RowHashSum THEN
        UPDATE SET
            trg.[ANCHOR_DATE__C] = src.[ANCHOR_DATE__C],
            trg.[CONTACT__C] = src.[CONTACT__C],
            trg.[END_DATE__C] = src.[END_DATE__C],
            trg.[INTERVIEW_DATE__C] = src.[INTERVIEW_DATE__C],
            trg.[PLACED_DATE__C] = src.[PLACED_DATE__C],
            trg.[PLACEMENT_TYPE__C] = src.[PLACEMENT_TYPE__C],
            trg.[REFERRED_TO_EMPLOYER_DATE__C] = src.[REFERRED_TO_EMPLOYER_DATE__C],
            trg.[START_DATE__C] = src.[START_DATE__C],
            trg.[STATUS__C] = src.[STATUS__C],
            trg.[VACANCY_ID__C] = src.[VACANCY_ID__C],
            trg.[ISDELETED] = src.[ISDELETED],
            trg.[RowHashSum] = src.[RowHashSum]
    WHEN NOT MATCHED BY TARGET THEN 
        INSERT (
            [ID],
            [ANCHOR_DATE__C],
            [CONTACT__C],
            [END_DATE__C],
            [INTERVIEW_DATE__C],
            [PLACED_DATE__C],
            [PLACEMENT_TYPE__C],
            [REFERRED_TO_EMPLOYER_DATE__C],
            [START_DATE__C],
            [STATUS__C],
            [VACANCY_ID__C],
            [ISDELETED],
            [RowHashSum]
        ) VALUES (
            src.[ID],
            src.[ANCHOR_DATE__C],
            src.[CONTACT__C],
            src.[END_DATE__C],
            src.[INTERVIEW_DATE__C],
            src.[PLACED_DATE__C],
            src.[PLACEMENT_TYPE__C],
            src.[REFERRED_TO_EMPLOYER_DATE__C],
            src.[START_DATE__C],
            src.[STATUS__C],
            src.[VACANCY_ID__C],
            src.[ISDELETED],
            src.[RowHashSum]
        );

END;