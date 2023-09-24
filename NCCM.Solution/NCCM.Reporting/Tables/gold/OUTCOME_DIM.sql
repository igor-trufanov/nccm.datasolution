CREATE TABLE [gold.NCCM].[OUTCOME_DIM](
    [OUTCOME_DIM_ID] [int] NOT NULL,
    [ARCHIVAL] [bit] NOT NULL,
    [OUTCOME_ID] [varchar](18) NULL,
    [CLIENT_CASE_ID] [varchar](18) NULL,
    [CONTACT_DIM_ID] [int] NULL,
    [WRD_PHASE] [varchar](1024) NULL,
    [RECORD_TYPE_NAME] [varchar](1024) NULL,
    [IS_DELETED] [int] NULL,
    [ROW_CALENDAR_DATE] [date] NOT NULL,
    [ROW_CREATED_AT] [datetime] NULL,
    [ROW_UPDATED_AT] [datetime] NULL
)