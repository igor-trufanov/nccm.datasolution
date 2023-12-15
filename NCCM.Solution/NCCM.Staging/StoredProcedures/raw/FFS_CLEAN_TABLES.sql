CREATE PROCEDURE [raw.NCCM].FFS_CLEAN_TABLES
AS
BEGIN

    DBCC CHECKIDENT ('[raw.NCCM].FFS_AS_CLAIMS', RESEED, 1);
    DBCC CHECKIDENT ('[raw.NCCM].FFS_CLIENT_CASE', RESEED, 1);
    DBCC CHECKIDENT ('[raw.NCCM].FFS_CONTACT', RESEED, 1);
    DBCC CHECKIDENT ('[raw.NCCM].FFS_JOB_ORDERS', RESEED, 1);
    DBCC CHECKIDENT ('[raw.NCCM].FFS_OUTCOME', RESEED, 1);
    DBCC CHECKIDENT ('[raw.NCCM].FFS_PARTICIPANTS', RESEED, 1);
    DBCC CHECKIDENT ('[raw.NCCM].FFS_PLACEMENT', RESEED, 1);
    DBCC CHECKIDENT ('[raw.NCCM].FFS_PMDM__PROGRAMENGAGEMENT__C', RESEED, 1);
    DBCC CHECKIDENT ('[raw.NCCM].FFS_ACCOUNT', RESEED, 1);

    TRUNCATE TABLE [raw.NCCM].FFS_AS_CLAIMS;
    TRUNCATE TABLE [raw.NCCM].FFS_CLIENT_CASE;
    TRUNCATE TABLE [raw.NCCM].FFS_CONTACT;
    TRUNCATE TABLE [raw.NCCM].FFS_JOB_ORDERS;
    TRUNCATE TABLE [raw.NCCM].FFS_OUTCOME;
    TRUNCATE TABLE [raw.NCCM].FFS_PARTICIPANTS;
    TRUNCATE TABLE [raw.NCCM].FFS_PLACEMENT;
    TRUNCATE TABLE [raw.NCCM].FFS_PMDM__PROGRAMENGAGEMENT__C;
    TRUNCATE TABLE [raw.NCCM].FFS_ACCOUNT;

END;