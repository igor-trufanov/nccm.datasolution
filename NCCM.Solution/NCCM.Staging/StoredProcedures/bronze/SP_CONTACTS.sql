CREATE PROCEDURE [bronze.NCCM].SP_CONTACTS
AS
BEGIN

    MERGE [bronze.HSP].CONTACTS AS trg
    USING [bronze.HSP].VW_CONTACTS AS src
    ON (trg.Id = src.Id)
    
    WHEN MATCHED AND trg.RowHashSum <> src.RowHashSum THEN
        UPDATE SET
            trg.[LastName] = src.[LastName],
            trg.[FirstName] = src.[FirstName],
            trg.[Birthdate] = src.[Birthdate],
            trg.[Gender__c] = src.[Gender__c],
            trg.[IsDeleted] = src.[IsDeleted],
            trg.[JobSeekerId__c] = src.[JobSeekerId__c],
            trg.[AccountId] = src.[AccountId],
            trg.[RowHashSum] = src.[RowHashSum]
    WHEN NOT MATCHED BY TARGET THEN 
        INSERT (
            [Id],
            [LastName],
            [FirstName],
            [Birthdate],
            [Gender__c],
            [IsDeleted],
            [JobSeekerId__c],
            [AccountId],
            [RowHashSum]
        ) VALUES (
            src.[Id],
            src.[LastName],
            src.[FirstName],
            src.[Birthdate],
            src.[Gender__c],
            src.[IsDeleted],
            src.[JobSeekerId__c],
            src.[AccountId],
            src.[RowHashSum]
        );

END;