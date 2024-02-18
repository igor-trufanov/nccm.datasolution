CREATE TABLE [raw.FNC].[Glacctgrphierarchy](
	[Glacctgrpkey] [bigint] NULL,
	[Glacctgrpname] [nvarchar](255) NULL,
	[Glacctgrptitle] [nvarchar](255) NULL,
	[Glacctgrpnormalbalance] [nvarchar](2000) NULL,
	[Glacctgrpmembertype] [nvarchar](2000) NULL,
	[Glacctgrphowcreated] [nvarchar](2000) NULL,
	[Glacctgrplocationkey] [bigint] NULL,
	[Accountkey] [bigint] NULL,
	[Accountno] [nvarchar](4000) NULL,
	[Accounttitle] [nvarchar](4000) NULL,
	[Accountnormalbalance] [nvarchar](2000) NULL,
	[Accounttype] [nvarchar](2000) NULL,
	[Accountlocationkey] [bigint] NULL
);