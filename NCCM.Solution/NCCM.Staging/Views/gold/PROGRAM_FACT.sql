CREATE VIEW [gold.NCCM].VW_PROGRAM_FACT
AS
    SELECT
        prg.APPLICATION_DATE,
        prg.PROGRAM_NAME,
        cnt.CONTACT_DIM_ID,
        clm.CLAIM_DIM_ID,
        prg.PROGRAMENGAGEMENT_DIM_ID
    FROM [silver.NCCM].PROGRAMENGAGEMENT_DIM AS prg
        LEFT JOIN [silver.NCCM].CONTACTS_DIM AS cnt
            ON cnt.CONTACT_ID = prg.PMDM__CONTACT
        LEFT JOIN [silver.NCCM].CLAIMS_DIM AS clm
            ON clm.JOB_SEEKER_ID = cnt.JOB_SEEKER_ID
