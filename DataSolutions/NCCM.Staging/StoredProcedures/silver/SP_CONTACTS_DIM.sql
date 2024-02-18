CREATE PROCEDURE [silver.NCCM].SP_CONTACTS_DIM
AS
BEGIN

    SET XACT_ABORT ON;
    SET NOCOUNT OFF;
    SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

    BEGIN TRANSACTION TR1;

    DECLARE @DimName VARCHAR(255) = 'CONTACTS_DIM';
    DECLARE @Timestamp DATETIME = GETUTCDATE();
    DECLARE @Date DATE = @Timestamp;

    ;WITH trg AS (
        SELECT * FROM [silver.NCCM].CONTACTS_DIM WHERE ARCHIVAL = 0
    )
    MERGE trg
    USING (
        SELECT 
            dm.*,
            ks.DIM_ID AS CONTACT_DIM_ID
        FROM [silver.NCCM].VW_CONTACTS_DIM AS dm
            LEFT JOIN [silver.NCCM].MAPPING_SURROGATE_KEYS_DIMS as ks
                ON ks.UQ_KEY_HASH = dm.UQ_KEY_HASH
                AND ks.DIMENSION = @DimName
    ) AS src
    ON (
        trg.CONTACT_DIM_ID = src.CONTACT_DIM_ID
    )

    WHEN MATCHED AND trg.[ROW_HASH_SUM] <> src.[ROW_HASH_SUM] THEN 
        UPDATE SET
            trg.[NK_STRING] = src.[NK_STRING],
            trg.[ROW_HASH_SUM] = src.[ROW_HASH_SUM],
            trg.[CONTACT_ID] = src.[CONTACT_ID],
            trg.[LAST_NAME] = src.[LAST_NAME],
            trg.[FIRST_NAME] = src.[FIRST_NAME],
            trg.[BIRTH_DATE] = src.[BIRTH_DATE],
            trg.[GENDER] = src.[GENDER],
            trg.[JOB_SEEKER_ID] = src.[JOB_SEEKER_ID],
            trg.[ACCOUNT_ID] = src.[ACCOUNT_ID],
            trg.[DFV_ABUSE_COERCIVE_CONTROL] = src.[DFV_ABUSE_COERCIVE_CONTROL],
            trg.[DFV_ABUSE_CULTURAL_SPIRITUAL] = src.[DFV_ABUSE_CULTURAL_SPIRITUAL],
            trg.[DFV_ABUSE_DOMESTIC_SERVITUDE] = src.[DFV_ABUSE_DOMESTIC_SERVITUDE],
            trg.[DFV_ABUSE_DOWRY] = src.[DFV_ABUSE_DOWRY],
            trg.[DFV_ABUSE_EMOTIONAL_PSYCHOLOGICAL] = src.[DFV_ABUSE_EMOTIONAL_PSYCHOLOGICAL],
            trg.[DFV_ABUSE_FINANCIAL] = src.[DFV_ABUSE_FINANCIAL],
            trg.[DFV_ABUSE_FORCED_MARRIAGE] = src.[DFV_ABUSE_FORCED_MARRIAGE],
            trg.[DFV_ABUSE_HARRASSMENT_STALKING] = src.[DFV_ABUSE_HARRASSMENT_STALKING],
            trg.[DFV_ABUSE_IMMIGRATION_FACILITATED] = src.[DFV_ABUSE_IMMIGRATION_FACILITATED],
            trg.[DFV_ABUSE_MULTIPERPETRATOR] = src.[DFV_ABUSE_MULTIPERPETRATOR],
            trg.[DFV_ABUSE_PHYSICAL] = src.[DFV_ABUSE_PHYSICAL],
            trg.[DFV_ABUSE_REPRODUCTIVE] = src.[DFV_ABUSE_REPRODUCTIVE],
            trg.[DFV_ABUSE_SEXUAL] = src.[DFV_ABUSE_SEXUAL],
            trg.[DFV_ABUSE_STRANGULATION] = src.[DFV_ABUSE_STRANGULATION],
            trg.[DFV_ABUSE_SYSTEMS] = src.[DFV_ABUSE_SYSTEMS],
            trg.[DFV_ABUSE_TECHNOLOGY_FACILITATED] = src.[DFV_ABUSE_TECHNOLOGY_FACILITATED],
            trg.[DFV_ABUSE_VERBAL] = src.[DFV_ABUSE_VERBAL],
            trg.[DFV_BARRIER_TO_DISCLOSURE_CULTURAL] = src.[DFV_BARRIER_TO_DISCLOSURE_CULTURAL],
            trg.[DFV_BARRIER_TO_DISCLOSURE_LANGUAGE] = src.[DFV_BARRIER_TO_DISCLOSURE_LANGUAGE],
            trg.[DFV_BARRIER_TO_DISCLOSURE_MISIDENTIFY] = src.[DFV_BARRIER_TO_DISCLOSURE_MISIDENTIFY],
            trg.[DFV_BARRIER_TO_DISCLOSURE_SYSTEM] = src.[DFV_BARRIER_TO_DISCLOSURE_SYSTEM],
            trg.[DFV_CASE_CONFERENCE_DATE] = src.[DFV_CASE_CONFERENCE_DATE],
            trg.[DFV_CHILDREN_UNDER_18] = src.[DFV_CHILDREN_UNDER_18],
            trg.[VULNERABLE_YOUTH] = src.[VULNERABLE_YOUTH],
            trg.[VULNERABLE_YOUTH_STUDENT] = src.[VULNERABLE_YOUTH_STUDENT],
            trg.[DISABILITY_TYPE] = src.[DISABILITY_TYPE],
            trg.[INTERPRETER_LANGUAGE] = src.[INTERPRETER_LANGUAGE],
            trg.[MENTAL_ILLNESS_FLAG] = src.[MENTAL_ILLNESS_FLAG],
            trg.[CALD] = src.[CALD],
            trg.[EDUCATION_LEVEL] = src.[EDUCATION_LEVEL],
            trg.[AGE_OF_YOUNGEST_CHILD] = src.[AGE_OF_YOUNGEST_CHILD],
            trg.[PROGRAM_ENGAGEMENT_ID] = src.[PROGRAM_ENGAGEMENT_ID],
            trg.[EMPLOYER] = src.[EMPLOYER],
            trg.[IS_DELETED] = src.[IS_DELETED],
            trg.[ROW_UPDATED_AT] = @Timestamp
    WHEN NOT MATCHED BY TARGET THEN 
        INSERT (
            [CONTACT_DIM_ID],
            [NK_STRING],
            [ROW_HASH_SUM],
            [CONTACT_ID],
            [LAST_NAME],
            [FIRST_NAME],
            [BIRTH_DATE],
            [GENDER],
            [JOB_SEEKER_ID],
            [ACCOUNT_ID],
            [DFV_ABUSE_COERCIVE_CONTROL],
            [DFV_ABUSE_CULTURAL_SPIRITUAL],
            [DFV_ABUSE_DOMESTIC_SERVITUDE],
            [DFV_ABUSE_DOWRY],
            [DFV_ABUSE_EMOTIONAL_PSYCHOLOGICAL],
            [DFV_ABUSE_FINANCIAL],
            [DFV_ABUSE_FORCED_MARRIAGE],
            [DFV_ABUSE_HARRASSMENT_STALKING],
            [DFV_ABUSE_IMMIGRATION_FACILITATED],
            [DFV_ABUSE_MULTIPERPETRATOR],
            [DFV_ABUSE_PHYSICAL],
            [DFV_ABUSE_REPRODUCTIVE],
            [DFV_ABUSE_SEXUAL],
            [DFV_ABUSE_STRANGULATION],
            [DFV_ABUSE_SYSTEMS],
            [DFV_ABUSE_TECHNOLOGY_FACILITATED],
            [DFV_ABUSE_VERBAL],
            [DFV_BARRIER_TO_DISCLOSURE_CULTURAL],
            [DFV_BARRIER_TO_DISCLOSURE_LANGUAGE],
            [DFV_BARRIER_TO_DISCLOSURE_MISIDENTIFY],
            [DFV_BARRIER_TO_DISCLOSURE_SYSTEM],
            [DFV_CASE_CONFERENCE_DATE],
            [DFV_CHILDREN_UNDER_18],
            [VULNERABLE_YOUTH],
            [VULNERABLE_YOUTH_STUDENT],
            [DISABILITY_TYPE],
            [INTERPRETER_LANGUAGE],
            [MENTAL_ILLNESS_FLAG],
            [CALD],
            [EDUCATION_LEVEL],
            [AGE_OF_YOUNGEST_CHILD],
            [PROGRAM_ENGAGEMENT_ID],
            [EMPLOYER],
            [IS_DELETED],
            [ROW_CALENDAR_DATE],
            [ROW_CREATED_AT],
            [ROW_UPDATED_AT]
        ) VALUES (
            src.[CONTACT_DIM_ID],
            src.[NK_STRING],
            src.[ROW_HASH_SUM],
            src.[CONTACT_ID],
            src.[LAST_NAME],
            src.[FIRST_NAME],
            src.[BIRTH_DATE],
            src.[GENDER],
            src.[JOB_SEEKER_ID],
            src.[ACCOUNT_ID],
            src.[DFV_ABUSE_COERCIVE_CONTROL],
            src.[DFV_ABUSE_CULTURAL_SPIRITUAL],
            src.[DFV_ABUSE_DOMESTIC_SERVITUDE],
            src.[DFV_ABUSE_DOWRY],
            src.[DFV_ABUSE_EMOTIONAL_PSYCHOLOGICAL],
            src.[DFV_ABUSE_FINANCIAL],
            src.[DFV_ABUSE_FORCED_MARRIAGE],
            src.[DFV_ABUSE_HARRASSMENT_STALKING],
            src.[DFV_ABUSE_IMMIGRATION_FACILITATED],
            src.[DFV_ABUSE_MULTIPERPETRATOR],
            src.[DFV_ABUSE_PHYSICAL],
            src.[DFV_ABUSE_REPRODUCTIVE],
            src.[DFV_ABUSE_SEXUAL],
            src.[DFV_ABUSE_STRANGULATION],
            src.[DFV_ABUSE_SYSTEMS],
            src.[DFV_ABUSE_TECHNOLOGY_FACILITATED],
            src.[DFV_ABUSE_VERBAL],
            src.[DFV_BARRIER_TO_DISCLOSURE_CULTURAL],
            src.[DFV_BARRIER_TO_DISCLOSURE_LANGUAGE],
            src.[DFV_BARRIER_TO_DISCLOSURE_MISIDENTIFY],
            src.[DFV_BARRIER_TO_DISCLOSURE_SYSTEM],
            src.[DFV_CASE_CONFERENCE_DATE],
            src.[DFV_CHILDREN_UNDER_18],
            src.[VULNERABLE_YOUTH],
            src.[VULNERABLE_YOUTH_STUDENT],
            src.[DISABILITY_TYPE],
            src.[INTERPRETER_LANGUAGE],
            src.[MENTAL_ILLNESS_FLAG],
            src.[CALD],
            src.[EDUCATION_LEVEL],
            src.[AGE_OF_YOUNGEST_CHILD],
            src.[PROGRAM_ENGAGEMENT_ID],
            src.[EMPLOYER],
            src.[IS_DELETED],
            @Date, -- ROW_CALENDAR_DATE
            @Timestamp, -- ROW_CREATED_AT
            @Timestamp -- ROW_UPDATED_AT
        );

    COMMIT TRANSACTION TR1;

END;