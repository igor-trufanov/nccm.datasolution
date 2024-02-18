CREATE TABLE [gold.NCCM].[CLAIMS_DIM](
    [CLAIM_DIM_ID] [int] NOT NULL,
    [ARCHIVAL] [bit] NOT NULL,
    [APPROVED_AMOUNT] [int] NULL,
    [CLAIM_AMOUNT] [int] NULL,
    [AS_CLAIM_ID] [varchar](18) NULL,
    [CLAIM_RATE_TYPE] [varchar](255) NULL,
    [GST_AMOUNT] [int] NULL,
    [JOB_SEEKER_ID] [varchar](18) NULL,
    [STATUS_DATE] [date] NULL,
    [STATUS] [varchar](255) NULL,
    [NAME] [nvarchar](1024) NULL,
    [OUTCOME_TYPE] [varchar](255) NULL,
    [SITE_CODE] [varchar](255) NULL,
    [IS_DELETED] [int] NOT NULL,
    [ROW_CALENDAR_DATE] [date] NOT NULL,
    [ROW_CREATED_AT] [datetime] NOT NULL,
    [ROW_UPDATED_AT] [datetime] NOT NULL
)