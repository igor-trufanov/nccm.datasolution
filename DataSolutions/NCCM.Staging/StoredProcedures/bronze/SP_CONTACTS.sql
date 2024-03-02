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
            trg.REGION__C = src.REGION__C,
            trg.ISDELETED = src.ISDELETED,
            trg.ROW_HASH_SUM = src.ROW_HASH_SUM
    WHEN NOT MATCHED BY TARGET THEN 
        INSERT (
            ID,
            LASTNAME,
            FIRSTNAME,
            BIRTHDATE,
            GENDER__C,
            ISDELETED,
            JOBSEEKERID__C,
            ACCOUNTID,
            REGION__C,
            ROW_HASH_SUM
        ) VALUES (
            src.ID,
            src.LASTNAME,
            src.FIRSTNAME,
            src.BIRTHDATE,
            src.GENDER__C,
            src.ISDELETED,
            src.JOBSEEKERID__C,
            src.ACCOUNTID,
            src.REGION__C,
            src.ROW_HASH_SUM
        );

    UPDATE trg
            SET trg.ISDELETED = 1
        FROM [bronze.NCCM].CONTACTS AS trg
            LEFT JOIN [bronze.NCCM].VW_CONTACTS AS src
                ON trg.ID = src.ID
        WHERE 
            trg.ISDELETED = 0 
            AND src.ID IS NULL;

END;