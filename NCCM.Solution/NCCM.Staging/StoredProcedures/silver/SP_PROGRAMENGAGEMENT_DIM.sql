CREATE PROCEDURE [silver.NCCM].SP_PROGRAMENGAGEMENT_DIM
AS
BEGIN

    SET XACT_ABORT ON;
    SET NOCOUNT OFF;
    SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

    BEGIN TRANSACTION TR1;

    DECLARE @DimName VARCHAR(255) = 'PROGRAMENGAGEMENT_DIM';
    DECLARE @Timestamp DATETIME = GETUTCDATE();
    DECLARE @Date DATE = @Timestamp;

    INSERT INTO [silver.NCCM].MAPPING_SURROGATE_KEYS_DIMS (UQ_KEY, UQ_KEY_HASH, DIM_ID, DIMENSION)
    SELECT 
        dm.UQ_KEY,
        dm.UQ_KEY_HASH,
        NEXT VALUE FOR [silver.NCCM].SEQ_PROGRAMENGAGEMENT_DIM AS DIM_ID, 
        @DimName AS DIMENSION
    FROM [silver.NCCM].VW_PROGRAMENGAGEMENT_DIM AS dm
        LEFT JOIN [silver.NCCM].MAPPING_SURROGATE_KEYS_DIMS as ks
            ON ks.UQ_KEY_HASH = dm.UQ_KEY_HASH
            AND ks.DIMENSION = @DimName
    WHERE ks.UQ_KEY_HASH IS NULL;

    ;WITH trg AS (
        SELECT * FROM [silver.NCCM].PROGRAMENGAGEMENT_DIM WHERE ARCHIVAL = 0
    )
    MERGE trg
    USING (
        SELECT 
            dm.*,
            ks.DIM_ID AS PROGRAMENGAGEMENT_DIM_ID
        FROM [silver.NCCM].VW_PROGRAMENGAGEMENT_DIM AS dm
            LEFT JOIN [silver.NCCM].MAPPING_SURROGATE_KEYS_DIMS as ks
                ON ks.UQ_KEY_HASH = dm.UQ_KEY_HASH
                AND ks.DIMENSION = @DimName
    ) AS src
    ON (
        trg.PROGRAMENGAGEMENT_DIM_ID = src.PROGRAMENGAGEMENT_DIM_ID
    )
    WHEN MATCHED AND trg.[ROW_HASH_SUM] <> src.[ROW_HASH_SUM] THEN 
        UPDATE SET
            trg.[NK_STRING] = src.[NK_STRING],
            trg.[ROW_HASH_SUM] = src.[ROW_HASH_SUM],
            trg.[PROGRAM_ENGAGEMENT_ID] = src.[PROGRAM_ENGAGEMENT_ID],
            trg.[COMMENCEMENT_OF_SERVICING_DATE] = src.[COMMENCEMENT_OF_SERVICING_DATE],
            trg.[PMDM__CONTACT] = src.[PMDM__CONTACT],
            trg.[CONTRACT_REFERENCE_ID] = src.[CONTRACT_REFERENCE_ID],
            trg.[CONTRACT_REFERRAL_START_DATE] = src.[CONTRACT_REFERRAL_START_DATE],
            trg.[CONTRACT_REFERRAL_STATUS] = src.[CONTRACT_REFERRAL_STATUS],
            trg.[CRN] = src.[CRN],
            trg.[DES_PROGRAM_TYPE_CODE] = src.[DES_PROGRAM_TYPE_CODE],
            trg.[ECM_CASE_ID] = src.[ECM_CASE_ID],
            trg.[EMPLOYMENT_BENCHMARK] = src.[EMPLOYMENT_BENCHMARK],
            trg.[ESAT_JCA_ASSESSMENT_DATE] = src.[ESAT_JCA_ASSESSMENT_DATE],
            trg.[EXIT_DATE] = src.[EXIT_DATE],
            trg.[FUNDING_LEVEL] = src.[FUNDING_LEVEL],
            trg.[INDIGENOUS_INDICATOR] = src.[INDIGENOUS_INDICATOR],
            trg.[JOB_PLAN_STATUS] = src.[JOB_PLAN_STATUS],
            trg.[JSCI_ASSESSMENT_DATE] = src.[JSCI_ASSESSMENT_DATE],
            trg.[JSCI_STATUS] = src.[JSCI_STATUS],
            trg.[LATEST_RESUME_DATE] = src.[LATEST_RESUME_DATE],
            trg.[LINKED_ACTIVITY] = src.[LINKED_ACTIVITY],
            trg.[MATURE_AGE] = src.[MATURE_AGE],
            trg.[NEIS_FLAG] = src.[NEIS_FLAG],
            trg.[NEXT_APPOINTMENT_DATE] = src.[NEXT_APPOINTMENT_DATE],
            trg.[NEXT_SERVICE_DELIVERY] = src.[NEXT_SERVICE_DELIVERY],
            trg.[ORIGINAL_COMMENCEMENT_DATE] = src.[ORIGINAL_COMMENCEMENT_DATE],
            trg.[OTHER_PERSONAL_ISSUES_BARRIERS] = src.[OTHER_PERSONAL_ISSUES_BARRIERS],
            trg.[OWNERID] = src.[OWNERID],
            trg.[PARTICIPATION_PLAN_STATUS] = src.[PARTICIPATION_PLAN_STATUS],
            trg.[PERIOD_OF_SERVICE] = src.[PERIOD_OF_SERVICE],
            trg.[POI_CHECKED] = src.[POI_CHECKED],
            trg.[PRINCIPAL_PARENT_CARER] = src.[PRINCIPAL_PARENT_CARER],
            trg.[PROGRAM_NAME] = src.[PROGRAM_NAME],
            trg.[REFERRAL_DATE] = src.[REFERRAL_DATE],
            trg.[REFERRAL_PHASE_CODE] = src.[REFERRAL_PHASE_CODE],
            trg.[REFERRAL_STATUS_CODE] = src.[REFERRAL_STATUS_CODE],
            trg.[REFUGEE_HUMANITARIAN_VISA] = src.[REFUGEE_HUMANITARIAN_VISA],
            trg.[SERVICING_PLACEMENT_STATUS] = src.[SERVICING_PLACEMENT_STATUS],
            trg.[SITE_CODE] = src.[SITE_CODE],
            trg.[SSI_SITE] = src.[SSI_SITE],
            trg.[SUSPENSION_REASON_DESCRIPTION] = src.[SUSPENSION_REASON_DESCRIPTION],
            trg.[THINK_TIME_DAYS] = src.[THINK_TIME_DAYS],
            trg.[THINK_TIME_ELECTED] = src.[THINK_TIME_ELECTED],
            trg.[TRAFFIC_LIGHT] = src.[TRAFFIC_LIGHT],
            trg.[IS_DELETED] = src.[IS_DELETED],
            trg.[ORIGINAL_SYSTEM_CREATED_DATE] = src.[ORIGINAL_SYSTEM_CREATED_DATE],
            trg.[APPLICATION_DATE] = src.[APPLICATION_DATE],
            trg.[ACCOUNT] = src.[ACCOUNT],
            trg.[IS_DELETED] = src.[IS_DELETED],
            trg.[ROW_UPDATED_AT] = @Timestamp
    WHEN NOT MATCHED BY TARGET THEN 
        INSERT (
            [PROGRAMENGAGEMENT_DIM_ID],
            [NK_STRING],
            [ROW_HASH_SUM],
            [PROGRAM_ENGAGEMENT_ID],
            [COMMENCEMENT_OF_SERVICING_DATE],
            [PMDM__CONTACT],
            [CONTRACT_REFERENCE_ID],
            [CONTRACT_REFERRAL_START_DATE],
            [CONTRACT_REFERRAL_STATUS],
            [CRN],
            [DES_PROGRAM_TYPE_CODE],
            [ECM_CASE_ID],
            [EMPLOYMENT_BENCHMARK],
            [ESAT_JCA_ASSESSMENT_DATE],
            [EXIT_DATE],
            [FUNDING_LEVEL],
            [INDIGENOUS_INDICATOR],
            [JOB_PLAN_STATUS],
            [JSCI_ASSESSMENT_DATE],
            [JSCI_STATUS],
            [LATEST_RESUME_DATE],
            [LINKED_ACTIVITY],
            [MATURE_AGE],
            [NEIS_FLAG],
            [NEXT_APPOINTMENT_DATE],
            [NEXT_SERVICE_DELIVERY],
            [ORIGINAL_COMMENCEMENT_DATE],
            [OTHER_PERSONAL_ISSUES_BARRIERS],
            [OWNERID],
            [PARTICIPATION_PLAN_STATUS],
            [PERIOD_OF_SERVICE],
            [POI_CHECKED],
            [PRINCIPAL_PARENT_CARER],
            [PROGRAM_NAME],
            [REFERRAL_DATE],
            [REFERRAL_PHASE_CODE],
            [REFERRAL_STATUS_CODE],
            [REFUGEE_HUMANITARIAN_VISA],
            [SERVICING_PLACEMENT_STATUS],
            [SITE_CODE],
            [SSI_SITE],
            [SUSPENSION_REASON_DESCRIPTION],
            [THINK_TIME_DAYS],
            [THINK_TIME_ELECTED],
            [TRAFFIC_LIGHT],
            [IS_DELETED],
            [ORIGINAL_SYSTEM_CREATED_DATE],
            [APPLICATION_DATE],
            [ACCOUNT],
            [IS_DELETED],
            [ROW_CALENDAR_DATE],
            [ROW_CREATED_AT],
            [ROW_UPDATED_AT]
        ) VALUES (
            src.[PROGRAMENGAGEMENT_DIM_ID],
            src.[NK_STRING],
            src.[ROW_HASH_SUM],
            src.[PROGRAM_ENGAGEMENT_ID],
            src.[COMMENCEMENT_OF_SERVICING_DATE],
            src.[PMDM__CONTACT],
            src.[CONTRACT_REFERENCE_ID],
            src.[CONTRACT_REFERRAL_START_DATE],
            src.[CONTRACT_REFERRAL_STATUS],
            src.[CRN],
            src.[DES_PROGRAM_TYPE_CODE],
            src.[ECM_CASE_ID],
            src.[EMPLOYMENT_BENCHMARK],
            src.[ESAT_JCA_ASSESSMENT_DATE],
            src.[EXIT_DATE],
            src.[FUNDING_LEVEL],
            src.[INDIGENOUS_INDICATOR],
            src.[JOB_PLAN_STATUS],
            src.[JSCI_ASSESSMENT_DATE],
            src.[JSCI_STATUS],
            src.[LATEST_RESUME_DATE],
            src.[LINKED_ACTIVITY],
            src.[MATURE_AGE],
            src.[NEIS_FLAG],
            src.[NEXT_APPOINTMENT_DATE],
            src.[NEXT_SERVICE_DELIVERY],
            src.[ORIGINAL_COMMENCEMENT_DATE],
            src.[OTHER_PERSONAL_ISSUES_BARRIERS],
            src.[OWNERID],
            src.[PARTICIPATION_PLAN_STATUS],
            src.[PERIOD_OF_SERVICE],
            src.[POI_CHECKED],
            src.[PRINCIPAL_PARENT_CARER],
            src.[PROGRAM_NAME],
            src.[REFERRAL_DATE],
            src.[REFERRAL_PHASE_CODE],
            src.[REFERRAL_STATUS_CODE],
            src.[REFUGEE_HUMANITARIAN_VISA],
            src.[SERVICING_PLACEMENT_STATUS],
            src.[SITE_CODE],
            src.[SSI_SITE],
            src.[SUSPENSION_REASON_DESCRIPTION],
            src.[THINK_TIME_DAYS],
            src.[THINK_TIME_ELECTED],
            src.[TRAFFIC_LIGHT],
            src.[IS_DELETED],
            src.[ORIGINAL_SYSTEM_CREATED_DATE],
            src.[APPLICATION_DATE],
            src.[ACCOUNT],
            src.[IS_DELETED],
            @Date, -- ROW_CALENDAR_DATE
            @Timestamp, -- ROW_CREATED_AT
            @Timestamp -- ROW_UPDATED_AT
        );

    COMMIT TRANSACTION TR1;

END;