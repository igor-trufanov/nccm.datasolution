CREATE TABLE [silver.NCCM].PROGRAMENGAGEMENT_DIM (
    [PROGRAMENGAGEMENT_DIM_ID] INT NOT NULL,
    [NK_STRING] NVARCHAR(MAX) NOT NULL,
    [ROW_HASH_SUM] BINARY(16) NOT NULL,

    [ARCHIVAL] BIT NOT NULL CONSTRAINT DF_PROGRAMENGAGEMENT_DIM_ARCHIVAL DEFAULT(0),

    -- nullable
    [PROGRAM_ENGAGEMENT_ID] VARCHAR(18) NULL,
    [COMMENCEMENT_OF_SERVICING_DATE] DATE NULL,
    [PMDM__CONTACT] VARCHAR(18) NULL,
    [CONTRACT_REFERENCE_ID] VARCHAR(255) NULL,
    [CONTRACT_REFERRAL_START_DATE] DATE NULL,
    [CONTRACT_REFERRAL_STATUS] VARCHAR(255) NULL,
    [CRN] VARCHAR(255) NULL,
    [DES_PROGRAM_TYPE_CODE] VARCHAR(255) NULL,
    [ECM_CASE_ID] VARCHAR(255) NULL,
    [EMPLOYMENT_BENCHMARK] VARCHAR(255) NULL,
    [ESAT_JCA_ASSESSMENT_DATE] DATE NULL,
    [EXIT_DATE] DATE NULL,
    [FUNDING_LEVEL] VARCHAR(255) NULL,
    [INDIGENOUS_INDICATOR] VARCHAR(255) NULL,
    [JOB_PLAN_STATUS] VARCHAR(255) NULL,
    [JSCI_ASSESSMENT_DATE] DATE NULL,
    [JSCI_STATUS] VARCHAR(255) NULL,
    [LATEST_RESUME_DATE] DATE NULL,
    [LINKED_ACTIVITY] VARCHAR(255) NULL,
    [MATURE_AGE] VARCHAR(255) NULL,
    [NEIS_FLAG] VARCHAR(255) NULL,
    [NEXT_APPOINTMENT_DATE] DATE NULL,
    [NEXT_SERVICE_DELIVERY] VARCHAR(255) NULL,
    [ORIGINAL_COMMENCEMENT_DATE] DATE NULL,
    [OTHER_PERSONAL_ISSUES_BARRIERS] VARCHAR(255) NULL,
    [OWNERID] VARCHAR(255) NULL,
    [PARTICIPATION_PLAN_STATUS] VARCHAR(255) NULL,
    [PERIOD_OF_SERVICE] VARCHAR(255) NULL,
    [POI_CHECKED] VARCHAR(255) NULL,
    [PRINCIPAL_PARENT_CARER] VARCHAR(255) NULL,
    [PROGRAM_NAME] VARCHAR(255) NULL,
    [REFERRAL_DATE] DATE NULL,
    [REFERRAL_PHASE_CODE] VARCHAR(255) NULL,
    [REFERRAL_STATUS_CODE] VARCHAR(255) NULL,
    [REFUGEE_HUMANITARIAN_VISA] VARCHAR(255) NULL,
    [SERVICING_PLACEMENT_STATUS] VARCHAR(255) NULL,
    [SITE_CODE] VARCHAR(255) NULL,
    [SSI_SITE] VARCHAR(255) NULL,
    [SUSPENSION_REASON_DESCRIPTION] VARCHAR(255) NULL,
    [THINK_TIME_DAYS] VARCHAR(255) NULL,
    [THINK_TIME_ELECTED] VARCHAR(255) NULL,
    [TRAFFIC_LIGHT] VARCHAR(255) NULL,
    [IS_DELETED] INT NULL,
    [ORIGINAL_SYSTEM_CREATED_DATE] DATETIME NULL,
    [APPLICATION_DATE] DATE NULL,
    [ACCOUNT] VARCHAR(18) NULL,

    [ROW_CALENDAR_DATE] DATE NOT NULL,
    [ROW_CREATED_AT] DATETIME NOT NULL,
    [ROW_UPDATED_AT] DATETIME NOT NULL,
    [ROW_VERSION] ROWVERSION NOT NULL,

    [VALID_FROM] DATETIME2 GENERATED ALWAYS AS ROW START NOT NULL,
    [VALID_TO] DATETIME2 GENERATED ALWAYS AS ROW END NOT NULL,
    PERIOD FOR SYSTEM_TIME ([VALID_FROM], [VALID_TO]),

    CONSTRAINT [PK_PROGRAMENGAGEMENT_DIM] PRIMARY KEY (PROGRAMENGAGEMENT_DIM_ID)
)
WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [silver.NCCM].PROGRAMENGAGEMENT_DIM_HISTORY));