CREATE TABLE [gold.NCCM].[PLACEMENT_DIM](
    [PLACEMENT_DIM_ID] [int] NOT NULL,
    [ARCHIVAL] [bit] NOT NULL,
    [PLACEMENT_ID] [varchar](18) NULL,
    [ANCHOR_DATE] [date] NULL,
    [CONTACT_DIM_ID] [int] NULL,
    [END_DATE] [date] NULL,
    [INTERVIEW_DATE] [date] NULL,
    [PLACED_DATE] [date] NULL,
    [PLACEMENT_TYPE] [varchar](255) NULL,
    [REFERRED_TO_EMPLOYER_DATE] [date] NULL,
    [START_DATE] [date] NULL,
    [STATUS] [varchar](255) NULL,
    [VACANCY_ID] [varchar](255) NULL,
    [IS_DELETED] [int] NULL,
    [ROW_CALENDAR_DATE] [date] NOT NULL,
    [ROW_CREATED_AT] [datetime] NULL,
    [ROW_UPDATED_AT] [datetime] NULL
)