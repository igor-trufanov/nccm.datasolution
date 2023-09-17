CREATE PROCEDURE [bronze.NCCM].SP_CONTACTS
AS
BEGIN

    MERGE [bronze.NCCM].CONTACTS AS trg
    USING [bronze.NCCM].VW_CONTACTS AS src
    ON (trg.ID = src.ID)
    
    WHEN MATCHED AND trg.ROW_HASH_SUM <> src.ROW_HASH_SUM THEN
        UPDATE SET
            trg.LASTNAME = src.LASTNAME,
            trg.FIRSTNAME = src.FIRSTNAME,
            trg.BIRTHDATE = src.BIRTHDATE,
            trg.GENDER__C = src.GENDER__C,
            trg.JOBSEEKERID__C = src.JOBSEEKERID__C,
            trg.ACCOUNTID = src.ACCOUNTID,
            trg.ISDELETED = src.ISDELETED,
            trg.ROW_HASH_SUM = src.ROW_HASH_SUM
    WHEN NOT MATCHED BY TARGET THEN 
        INSERT (
            ID,
            LastName,
            FirstName,
            Birthdate,
            Gender__c,
            ISDELETED,
            JobSeekerId__c,
            AccountId,
            ROW_HASH_SUM
        ) VALUES (
            src.ID,
            src.LastName,
            src.FirstName,
            src.Birthdate,
            src.Gender__c,
            src.ISDELETED,
            src.JobSeekerId__c,
            src.AccountId,
            src.ROW_HASH_SUM
        );

END;