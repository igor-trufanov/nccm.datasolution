﻿CREATE VIEW [bronze.NCCM].VW_PMDM_PROGRAMENGAGEMENT
AS
    SELECT 
        stm.*,
        CONVERT(VARCHAR(32), HASHBYTES('MD5', CONCAT(
            stm.AGE_OF_YOUNGEST_CHILD__C,
            stm.CALD__C,
            stm.DFV_ABUSE_COERCIVE_CONTROL__C,
            stm.DFV_ABUSE_CULTURAL_SPIRITUAL__C,
            stm.DFV_ABUSE_DOMESTIC_SERVITUDE__C,
            stm.DFV_ABUSE_DOWRY__C,
            stm.DFV_ABUSE_EMOTIONAL_PSYCHOLOGICAL__C,
            stm.DFV_ABUSE_FINANCIAL__C,
            stm.DFV_ABUSE_FORCED_MARRIAGE__C,
            stm.DFV_ABUSE_HARRASSMENT_STALKING__C,
            stm.DFV_ABUSE_IMMIGRATION_FACILITATED__C,
            stm.DFV_ABUSE_MULTIPERPETRATOR__C,
            stm.DFV_ABUSE_PHYSICAL__C,
            stm.DFV_ABUSE_REPRODUCTIVE__C,
            stm.DFV_ABUSE_SEXUAL__C,
            stm.DFV_ABUSE_STRANGULATION__C,
            stm.DFV_ABUSE_SYSTEMS__C,
            stm.DFV_ABUSE_TECHNOLOGY_FACILITATED__C,
            stm.DFV_ABUSE_VERBAL__C,
            stm.DFV_BARRIER_TO_DISCLOSURE_CULTURAL__C,
            stm.DFV_BARRIER_TO_DISCLOSURE_LANGUAGE__C,
            stm.DFV_BARRIER_TO_DISCLOSURE_MISIDENTIFY__C,
            stm.DFV_BARRIER_TO_DISCLOSURE_SYSTEM__C,
            stm.DFV_CASE_CONFERENCE_DATE__C,
            stm.DFV_CHILDREN_UNDER_18__C,
            stm.DISABILITY_TYPE__C,
            stm.EDUCATION_LEVEL__C,
            stm.INTERPRETER_LANGUAGE__C,
            stm.MENTAL_ILLNESS_FLAG__C,
            stm.VULNERABLE_YOUTH__C,
            stm.VULNERABLE_YOUTH_STUDENT__C,
            stm.COMMENCEMENT_OF_SERVICING_DATE__C,
            stm.PMDM__CONTACT__C,
            stm.CONTRACT_REFERENCE_ID__C,
            stm.CONTRACT_REFERRAL_START_DATE__C,
            stm.CONTRACT_REFERRAL_STATUS__C,
            stm.CRN__C,
            stm.DES_PROGRAM_TYPE_CODE__C,
            stm.ECM_CASE_ID__C,
            stm.EMPLOYMENT_BENCHMARK__C,
            stm.ESAT_JCA_ASSESSMENT_DATE__C,
            stm.EXIT_DATE__C,
            stm.FUNDING_LEVEL__C,
            stm.INDIGENOUS_INDICATOR__C,
            stm.JOB_PLAN_STATUS__C,
            stm.JSCI_ASSESSMENT_DATE__C,
            stm.JSCI_STATUS__C,
            stm.LATEST_RESUME_DATE__C,
            stm.LINKED_ACTIVITY__C,
            stm.MATURE_AGE__C,
            stm.NEIS_FLAG__C,
            stm.NEXT_APPOINTMENT_DATE__C,
            stm.NEXT_SERVICE_DELIVERY__C,
            stm.ORIGINAL_COMMENCEMENT_DATE__C,
            stm.OTHER_PERSONAL_ISSUES_BARRIERS__C,
            stm.OWNERID__C,
            stm.PARTICIPATION_PLAN_STATUS__C,
            stm.PERIOD_OF_SERVICE__C,
            stm.POI_CHECKED__C,
            stm.PRINCIPAL_PARENT_CARER__C,
            stm.PROGRAM_NAME__C,
            stm.REFERRAL_DATE__C,
            stm.REFERRAL_PHASE_CODE__C,
            stm.REFERRAL_STATUS_CODE__C,
            stm.REFUGEE_HUMANITARIAN_VISA__C,
            stm.SERVICING_PLACEMENT_STATUS__C,
            stm.SITE_CODE__C,
            stm.SSI_SITE__C,
            stm.SUSPENSION_REASON_DESCRIPTION__C,
            stm.THINK_TIME_DAYS__C,
            stm.THINK_TIME_ELECTED__C,
            stm.TRAFFIC_LIGHT__C,
            stm.ISDELETED,
            stm.CREATEDDATE,
            stm.PMDM__APPLICATIONDATE__C,
            stm.PMDM__ACCOUNT__C,
            stm.LAST_CONTACT_ACROSS_ALL_THEIR_ENGAGEMENTS)), 2
        ) AS ROW_HASH_SUM
    FROM (
        SELECT 
            nst.*,
            ROW_NUMBER() OVER(PARTITION BY nst.PMDM__CONTACT__C ORDER BY nst.CREATEDDATE DESC) AS LAST_CONTACT_ACROSS_ALL_THEIR_ENGAGEMENTS
        FROM (
            SELECT
                CAST(CAST(NULLIF(CAST(tbl.AGE_OF_YOUNGEST_CHILD__C AS VARCHAR(255)), '') AS NUMERIC(18, 0)) AS INT) AS AGE_OF_YOUNGEST_CHILD__C,
                NULLIF(CAST(tbl.CALD__C AS VARCHAR(255)), '') AS CALD__C,
                NULLIF(CAST(tbl.DFV_ABUSE_COERCIVE_CONTROL__C AS VARCHAR(255)), '') AS DFV_ABUSE_COERCIVE_CONTROL__C,
                NULLIF(CAST(tbl.DFV_ABUSE_CULTURAL_SPIRITUAL__C AS VARCHAR(255)), '') AS DFV_ABUSE_CULTURAL_SPIRITUAL__C,
                NULLIF(CAST(tbl.DFV_ABUSE_DOMESTIC_SERVITUDE__C AS VARCHAR(255)), '') AS DFV_ABUSE_DOMESTIC_SERVITUDE__C,
                NULLIF(CAST(tbl.DFV_ABUSE_DOWRY__C AS VARCHAR(255)), '') AS DFV_ABUSE_DOWRY__C,
                NULLIF(CAST(tbl.DFV_ABUSE_EMOTIONAL_PSYCHOLOGICAL__C AS VARCHAR(255)), '') AS DFV_ABUSE_EMOTIONAL_PSYCHOLOGICAL__C,
                NULLIF(CAST(tbl.DFV_ABUSE_FINANCIAL__C AS VARCHAR(255)), '') AS DFV_ABUSE_FINANCIAL__C,
                NULLIF(CAST(tbl.DFV_ABUSE_FORCED_MARRIAGE__C AS VARCHAR(255)), '') AS DFV_ABUSE_FORCED_MARRIAGE__C,
                NULLIF(CAST(tbl.DFV_ABUSE_HARRASSMENT_STALKING__C AS VARCHAR(255)), '') AS DFV_ABUSE_HARRASSMENT_STALKING__C,
                NULLIF(CAST(tbl.DFV_ABUSE_IMMIGRATION_FACILITATED__C AS VARCHAR(255)), '') AS DFV_ABUSE_IMMIGRATION_FACILITATED__C,
                NULLIF(CAST(tbl.DFV_ABUSE_MULTIPERPETRATOR__C AS VARCHAR(255)), '') AS DFV_ABUSE_MULTIPERPETRATOR__C,
                NULLIF(CAST(tbl.DFV_ABUSE_PHYSICAL__C AS VARCHAR(255)), '') AS DFV_ABUSE_PHYSICAL__C,
                NULLIF(CAST(tbl.DFV_ABUSE_REPRODUCTIVE__C AS VARCHAR(255)), '') AS DFV_ABUSE_REPRODUCTIVE__C,
                NULLIF(CAST(tbl.DFV_ABUSE_SEXUAL__C AS VARCHAR(255)), '') AS DFV_ABUSE_SEXUAL__C,
                NULLIF(CAST(tbl.DFV_ABUSE_STRANGULATION__C AS VARCHAR(255)), '') AS DFV_ABUSE_STRANGULATION__C,
                NULLIF(CAST(tbl.DFV_ABUSE_SYSTEMS__C AS VARCHAR(255)), '') AS DFV_ABUSE_SYSTEMS__C,
                NULLIF(CAST(tbl.DFV_ABUSE_TECHNOLOGY_FACILITATED__C AS VARCHAR(255)), '') AS DFV_ABUSE_TECHNOLOGY_FACILITATED__C,
                NULLIF(CAST(tbl.DFV_ABUSE_VERBAL__C AS VARCHAR(255)), '') AS DFV_ABUSE_VERBAL__C,
                NULLIF(CAST(tbl.DFV_BARRIER_TO_DISCLOSURE_CULTURAL__C AS VARCHAR(255)), '') AS DFV_BARRIER_TO_DISCLOSURE_CULTURAL__C,
                NULLIF(CAST(tbl.DFV_BARRIER_TO_DISCLOSURE_LANGUAGE__C AS VARCHAR(255)), '') AS DFV_BARRIER_TO_DISCLOSURE_LANGUAGE__C,
                NULLIF(CAST(tbl.DFV_BARRIER_TO_DISCLOSURE_MISIDENTIFY__C AS VARCHAR(255)), '') AS DFV_BARRIER_TO_DISCLOSURE_MISIDENTIFY__C,
                NULLIF(CAST(tbl.DFV_BARRIER_TO_DISCLOSURE_SYSTEM__C AS VARCHAR(255)), '') AS DFV_BARRIER_TO_DISCLOSURE_SYSTEM__C,
                CAST(NULLIF(NULLIF(CAST(tbl.DFV_CASE_CONFERENCE_DATE__C AS VARCHAR(255)), ''), 'NULL') AS DATE) AS DFV_CASE_CONFERENCE_DATE__C,
                NULLIF(CAST(tbl.DFV_CHILDREN_UNDER_18__C AS VARCHAR(255)), '') AS DFV_CHILDREN_UNDER_18__C,
                NULLIF(CAST(tbl.DISABILITY_TYPE__C AS VARCHAR(255)), '') AS DISABILITY_TYPE__C,
                NULLIF(CAST(tbl.EDUCATION_LEVEL__C AS VARCHAR(255)), '') AS EDUCATION_LEVEL__C,
                NULLIF(CAST(tbl.INTERPRETER_LANGUAGE__C AS VARCHAR(255)), '') AS INTERPRETER_LANGUAGE__C,
                NULLIF(CAST(tbl.MENTAL_ILLNESS_FLAG__C AS VARCHAR(255)), '') AS MENTAL_ILLNESS_FLAG__C,
                NULLIF(CAST(tbl.VULNERABLE_YOUTH__C AS VARCHAR(255)), '') AS VULNERABLE_YOUTH__C,
                NULLIF(CAST(tbl.VULNERABLE_YOUTH_STUDENT__C AS VARCHAR(255)), '') AS VULNERABLE_YOUTH_STUDENT__C,
                CAST(NULLIF(NULLIF(CAST(tbl.COMMENCEMENT_OF_SERVICING_DATE__C AS VARCHAR(255)), ''), 'NULL') AS DATE) AS COMMENCEMENT_OF_SERVICING_DATE__C,
                NULLIF(CAST(tbl.PMDM__CONTACT__C AS VARCHAR(18)), '') AS PMDM__CONTACT__C,
                NULLIF(CAST(tbl.CONTRACT_REFERENCE_ID__C AS VARCHAR(18)), '') AS CONTRACT_REFERENCE_ID__C,
                CAST(NULLIF(NULLIF(CAST(tbl.CONTRACT_REFERRAL_START_DATE__C AS VARCHAR(255)), ''), 'NULL') AS DATE) AS CONTRACT_REFERRAL_START_DATE__C,
                NULLIF(CAST(tbl.CONTRACT_REFERRAL_STATUS__C AS VARCHAR(255)), '') AS CONTRACT_REFERRAL_STATUS__C,
                NULLIF(CAST(tbl.CRN__C AS VARCHAR(255)), '') AS CRN__C,
                NULLIF(CAST(tbl.DES_PROGRAM_TYPE_CODE__C AS VARCHAR(255)), '') AS DES_PROGRAM_TYPE_CODE__C,
                NULLIF(CAST(tbl.ECM_CASE_ID__C AS VARCHAR(18)), '') AS ECM_CASE_ID__C,
                NULLIF(CAST(tbl.EMPLOYMENT_BENCHMARK__C AS VARCHAR(255)), '') AS EMPLOYMENT_BENCHMARK__C,
                CAST(NULLIF(NULLIF(CAST(tbl.ESAT_JCA_ASSESSMENT_DATE__C AS VARCHAR(255)), ''), 'NULL') AS DATE) AS ESAT_JCA_ASSESSMENT_DATE__C,
                CAST(NULLIF(NULLIF(CAST(tbl.EXIT_DATE__C AS VARCHAR(255)), ''), 'NULL') AS DATE) AS EXIT_DATE__C,
                NULLIF(CAST(tbl.FUNDING_LEVEL__C AS VARCHAR(255)), '') AS FUNDING_LEVEL__C,
                NULLIF(CAST(tbl.INDIGENOUS_INDICATOR__C AS VARCHAR(255)), '') AS INDIGENOUS_INDICATOR__C,
                NULLIF(CAST(tbl.JOB_PLAN_STATUS__C AS VARCHAR(255)), '') AS JOB_PLAN_STATUS__C,
                CAST(NULLIF(NULLIF(CAST(tbl.JSCI_ASSESSMENT_DATE__C AS VARCHAR(255)), ''), 'NULL') AS DATE) AS JSCI_ASSESSMENT_DATE__C,
                NULLIF(CAST(tbl.JSCI_STATUS__C AS VARCHAR(255)), '') AS JSCI_STATUS__C,
                CAST(NULLIF(NULLIF(CAST(tbl.LATEST_RESUME_DATE__C AS VARCHAR(255)), ''), 'NULL') AS DATE) AS LATEST_RESUME_DATE__C,
                NULLIF(CAST(tbl.LINKED_ACTIVITY__C AS VARCHAR(255)), '') AS LINKED_ACTIVITY__C,
                CAST(CAST(NULLIF(CAST(tbl.MATURE_AGE__C AS VARCHAR(255)), '') AS NUMERIC(18, 0)) AS INT) AS MATURE_AGE__C,
                NULLIF(CAST(tbl.NEIS_FLAG__C AS VARCHAR(255)), '') AS NEIS_FLAG__C,
                CAST(NULLIF(NULLIF(CAST(tbl.NEXT_APPOINTMENT_DATE__C AS VARCHAR(255)), ''), 'NULL') AS DATE) AS NEXT_APPOINTMENT_DATE__C,
                NULLIF(CAST(tbl.NEXT_SERVICE_DELIVERY__C AS VARCHAR(255)), '') AS NEXT_SERVICE_DELIVERY__C,
                CAST(NULLIF(NULLIF(CAST(tbl.ORIGINAL_COMMENCEMENT_DATE__C AS VARCHAR(255)), ''), 'NULL') AS DATE) AS ORIGINAL_COMMENCEMENT_DATE__C,
                NULLIF(CAST(tbl.OTHER_PERSONAL_ISSUES_BARRIERS__C AS VARCHAR(255)), '') AS OTHER_PERSONAL_ISSUES_BARRIERS__C,
                NULLIF(CAST(tbl.OWNERID__C AS VARCHAR(255)), '') AS OWNERID__C,
                NULLIF(CAST(tbl.PARTICIPATION_PLAN_STATUS__C AS VARCHAR(255)), '') AS PARTICIPATION_PLAN_STATUS__C,
                NULLIF(CAST(tbl.PERIOD_OF_SERVICE__C AS VARCHAR(255)), '') AS PERIOD_OF_SERVICE__C,
                NULLIF(CAST(tbl.POI_CHECKED__C AS VARCHAR(255)), '') AS POI_CHECKED__C,
                NULLIF(CAST(tbl.PRINCIPAL_PARENT_CARER__C AS VARCHAR(255)), '') AS PRINCIPAL_PARENT_CARER__C,
                NULLIF(CAST(tbl.ID AS VARCHAR(18)), '') AS ID,
                NULLIF(CAST(tbl.PROGRAM_NAME__C AS VARCHAR(255)), '') AS PROGRAM_NAME__C,
                CAST(NULLIF(NULLIF(CAST(tbl.REFERRAL_DATE__C AS VARCHAR(255)), ''), 'NULL') AS DATE) AS REFERRAL_DATE__C,
                NULLIF(CAST(tbl.REFERRAL_PHASE_CODE__C AS VARCHAR(255)), '') AS REFERRAL_PHASE_CODE__C,
                NULLIF(CAST(tbl.REFERRAL_STATUS_CODE__C AS VARCHAR(255)), '') AS REFERRAL_STATUS_CODE__C,
                NULLIF(CAST(tbl.REFUGEE_HUMANITARIAN_VISA__C AS VARCHAR(255)), '') AS REFUGEE_HUMANITARIAN_VISA__C,
                NULLIF(CAST(tbl.SERVICING_PLACEMENT_STATUS__C AS VARCHAR(255)), '') AS SERVICING_PLACEMENT_STATUS__C,
                NULLIF(CAST(tbl.SITE_CODE__C AS VARCHAR(255)), '') AS SITE_CODE__C,
                NULLIF(CAST(tbl.SSI_SITE__C AS VARCHAR(255)), '') AS SSI_SITE__C,
                NULLIF(CAST(tbl.SUSPENSION_REASON_DESCRIPTION__C AS VARCHAR(255)), '') AS SUSPENSION_REASON_DESCRIPTION__C,
                NULLIF(CAST(tbl.THINK_TIME_DAYS__C AS VARCHAR(255)), '') AS THINK_TIME_DAYS__C,
                NULLIF(CAST(tbl.THINK_TIME_ELECTED__C AS VARCHAR(255)), '') AS THINK_TIME_ELECTED__C,
                NULLIF(CAST(tbl.TRAFFIC_LIGHT__C AS VARCHAR(255)), '') AS TRAFFIC_LIGHT__C,
                CAST(CAST(tbl.ISDELETED AS CHAR) AS INT) AS ISDELETED,
                CAST(NULLIF(NULLIF(CAST(tbl.CREATEDDATE AS VARCHAR(255)), ''), 'NULL') AS DATETIME) AS CREATEDDATE,
                CAST(NULLIF(NULLIF(CAST(tbl.PMDM__APPLICATIONDATE__C AS VARCHAR(255)), ''), 'NULL') AS DATETIME) AS PMDM__APPLICATIONDATE__C,
                NULLIF(CAST(tbl.tbl.pmdm__Account__c AS VARCHAR(18)), '') AS PMDM__ACCOUNT__C
            FROM [$(Staging)].dbo.[SFICMS_PMDM__PROGRAMENGAGEMENT__C] AS tbl
        ) AS nst
    ) AS stm;
