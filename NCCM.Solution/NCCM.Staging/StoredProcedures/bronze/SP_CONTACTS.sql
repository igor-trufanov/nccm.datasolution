CREATE PROCEDURE [bronze.NCCM].SP_CONTACTS
AS
BEGIN

    MERGE [bronze.NCCM].CONTACTS AS trg
    USING [bronze.NCCM].VW_CONTACTS AS src
    ON (trg.ID = src.ID)
    
    WHEN MATCHED AND trg.RowHashSum <> src.RowHashSum THEN
        UPDATE SET
            trg.[LASTNAME] = src.[LASTNAME],
            trg.[FIRSTNAME] = src.[FIRSTNAME],
            trg.[BIRTHDATE] = src.[BIRTHDATE],
            trg.[GENDER__C] = src.[GENDER__C],
            trg.[JOBSEEKERID__C] = src.[JOBSEEKERID__C],
            trg.[ACCOUNTID] = src.[ACCOUNTID],
            trg.[ISDELETED] = src.[ISDELETED],
            trg.[RowHashSum] = src.[RowHashSum]
    WHEN NOT MATCHED BY TARGET THEN 
        INSERT (
            [ID],
            [LastName],
            [FirstName],
            [Birthdate],
            [Gender__c],
            [ISDELETED],
            [JobSeekerId__c],
            [AccountId],
            [RowHashSum]
        ) VALUES (
            src.[ID],
            src.[LastName],
            src.[FirstName],
            src.[Birthdate],
            src.[Gender__c],
            src.[ISDELETED],
            src.[JobSeekerId__c],
            src.[AccountId],
            src.[RowHashSum]
        );

END;