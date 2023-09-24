CREATE TABLE [gold.NCCM].[JOB_ORDERS_DIM](
    [JOB_ORDER_DIM_ID] [int] NOT NULL,
    [ARCHIVAL] [bit] NOT NULL,
    [JOB_ORDER_ID] [varchar](18) NULL,
    [CONTACT_DIM_ID] [int] NULL,
    [HOST_ORGANISATION] [varchar](18) NULL,
    [JOB_ORDER_TYPE] [varchar](255) NULL,
    [JOB_TITLE] [nvarchar](1024) NULL,
    [JOB_TYPE] [nvarchar](1024) NULL,
    [IS_DELETED] [int] NULL,
    [ROW_CALENDAR_DATE] [date] NOT NULL,
    [ROW_CREATED_AT] [datetime] NULL,
    [ROW_UPDATED_AT] [datetime] NULL
)