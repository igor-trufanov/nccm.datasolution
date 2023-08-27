﻿CREATE PROCEDURE [bronze.NCCM].SP_PROGRAMENGAGEMENT
AS
BEGIN

    MERGE [bronze.HSP].PROGRAMENGAGEMENT AS trg
    USING [bronze.HSP].VW_PROGRAMENGAGEMENT AS src
    ON (trg.Id = src.Id)
    
    WHEN MATCHED AND trg.RowHashSum <> src.RowHashSum THEN
        UPDATE SET
            trg.[AGE_OF_YOUNGEST_CHILD__C] = src.[AGE_OF_YOUNGEST_CHILD__C],
            trg.[CALD__C] = src.[CALD__C],
            trg.[DFV_ABUSE_COERCIVE_CONTROL__C] = src.[DFV_ABUSE_COERCIVE_CONTROL__C],
            trg.[DFV_ABUSE_CULTURAL_SPIRITUAL__C] = src.[DFV_ABUSE_CULTURAL_SPIRITUAL__C],
            trg.[DFV_ABUSE_DOMESTIC_SERVITUDE__C] = src.[DFV_ABUSE_DOMESTIC_SERVITUDE__C],
            trg.[DFV_ABUSE_DOWRY__C] = src.[DFV_ABUSE_DOWRY__C],
            trg.[DFV_ABUSE_EMOTIONAL_PSYCHOLOGICAL__C] = src.[DFV_ABUSE_EMOTIONAL_PSYCHOLOGICAL__C],
            trg.[DFV_ABUSE_FINANCIAL__C] = src.[DFV_ABUSE_FINANCIAL__C],
            trg.[DFV_ABUSE_FORCED_MARRIAGE__C] = src.[DFV_ABUSE_FORCED_MARRIAGE__C],
            trg.[DFV_ABUSE_HARRASSMENT_STALKING__C] = src.[DFV_ABUSE_HARRASSMENT_STALKING__C],
            trg.[DFV_ABUSE_IMMIGRATION_FACILITATED__C] = src.[DFV_ABUSE_IMMIGRATION_FACILITATED__C],
            trg.[DFV_ABUSE_MULTIPERPETRATOR__C] = src.[DFV_ABUSE_MULTIPERPETRATOR__C],
            trg.[DFV_ABUSE_PHYSICAL__C] = src.[DFV_ABUSE_PHYSICAL__C],
            trg.[DFV_ABUSE_REPRODUCTIVE__C] = src.[DFV_ABUSE_REPRODUCTIVE__C],
            trg.[DFV_ABUSE_SEXUAL__C] = src.[DFV_ABUSE_SEXUAL__C],
            trg.[DFV_ABUSE_STRANGULATION__C] = src.[DFV_ABUSE_STRANGULATION__C],
            trg.[DFV_ABUSE_SYSTEMS__C] = src.[DFV_ABUSE_SYSTEMS__C],
            trg.[DFV_ABUSE_TECHNOLOGY_FACILITATED__C] = src.[DFV_ABUSE_TECHNOLOGY_FACILITATED__C],
            trg.[DFV_ABUSE_VERBAL__C] = src.[DFV_ABUSE_VERBAL__C],
            trg.[DFV_BARRIER_TO_DISCLOSURE_CULTURAL__C] = src.[DFV_BARRIER_TO_DISCLOSURE_CULTURAL__C],
            trg.[DFV_BARRIER_TO_DISCLOSURE_LANGUAGE__C] = src.[DFV_BARRIER_TO_DISCLOSURE_LANGUAGE__C],
            trg.[DFV_BARRIER_TO_DISCLOSURE_MISIDENTIFY__C] = src.[DFV_BARRIER_TO_DISCLOSURE_MISIDENTIFY__C],
            trg.[DFV_BARRIER_TO_DISCLOSURE_SYSTEM__C] = src.[DFV_BARRIER_TO_DISCLOSURE_SYSTEM__C],
            trg.[DFV_CASE_CONFERENCE_DATE__C] = src.[DFV_CASE_CONFERENCE_DATE__C],
            trg.[DFV_CHILDREN_UNDER_18__C] = src.[DFV_CHILDREN_UNDER_18__C],
            trg.[DISABILITY_TYPE__C] = src.[DISABILITY_TYPE__C],
            trg.[EDUCATION_LEVEL__C] = src.[EDUCATION_LEVEL__C],
            trg.[INTERPRETER_LANGUAGE__C] = src.[INTERPRETER_LANGUAGE__C],
            trg.[MENTAL_ILLNESS_FLAG__C] = src.[MENTAL_ILLNESS_FLAG__C],
            trg.[VULNERABLE_YOUTH__C] = src.[VULNERABLE_YOUTH__C],
            trg.[VULNERABLE_YOUTH_STUDENT__C] = src.[VULNERABLE_YOUTH_STUDENT__C],
            trg.[COMMENCEMENT_OF_SERVICING_DATE__C] = src.[COMMENCEMENT_OF_SERVICING_DATE__C],
            trg.[PMDM__CONTACT__C] = src.[PMDM__CONTACT__C],
            trg.[CONTRACT_REFERENCE_ID__C] = src.[CONTRACT_REFERENCE_ID__C],
            trg.[CONTRACT_REFERRAL_START_DATE__C] = src.[CONTRACT_REFERRAL_START_DATE__C],
            trg.[CONTRACT_REFERRAL_STATUS__C] = src.[CONTRACT_REFERRAL_STATUS__C],
            trg.[CRN__C] = src.[CRN__C],
            trg.[DES_PROGRAM_TYPE_CODE__C] = src.[DES_PROGRAM_TYPE_CODE__C],
            trg.[ECM_CASE_ID__C] = src.[ECM_CASE_ID__C],
            trg.[EMPLOYMENT_BENCHMARK__C] = src.[EMPLOYMENT_BENCHMARK__C],
            trg.[ESAT_JCA_ASSESSMENT_DATE__C] = src.[ESAT_JCA_ASSESSMENT_DATE__C],
            trg.[EXIT_DATE__C] = src.[EXIT_DATE__C],
            trg.[FUNDING_LEVEL__C] = src.[FUNDING_LEVEL__C],
            trg.[INDIGENOUS_INDICATOR__C] = src.[INDIGENOUS_INDICATOR__C],
            trg.[JOB_PLAN_STATUS__C] = src.[JOB_PLAN_STATUS__C],
            trg.[JSCI_ASSESSMENT_DATE__C] = src.[JSCI_ASSESSMENT_DATE__C],
            trg.[JSCI_STATUS__C] = src.[JSCI_STATUS__C],
            trg.[LATEST_RESUME_DATE__C] = src.[LATEST_RESUME_DATE__C],
            trg.[LINKED_ACTIVITY__C] = src.[LINKED_ACTIVITY__C],
            trg.[MATURE_AGE__C] = src.[MATURE_AGE__C],
            trg.[NEIS_FLAG__C] = src.[NEIS_FLAG__C],
            trg.[NEXT_APPOINTMENT_DATE__C] = src.[NEXT_APPOINTMENT_DATE__C],
            trg.[NEXT_SERVICE_DELIVERY__C] = src.[NEXT_SERVICE_DELIVERY__C],
            trg.[ORIGINAL_COMMENCEMENT_DATE__C] = src.[ORIGINAL_COMMENCEMENT_DATE__C],
            trg.[OTHER_PERSONAL_ISSUES_BARRIERS__C] = src.[OTHER_PERSONAL_ISSUES_BARRIERS__C],
            trg.[OWNERID__C] = src.[OWNERID__C],
            trg.[PARTICIPATION_PLAN_STATUS__C] = src.[PARTICIPATION_PLAN_STATUS__C],
            trg.[PERIOD_OF_SERVICE__C] = src.[PERIOD_OF_SERVICE__C],
            trg.[POI_CHECKED__C] = src.[POI_CHECKED__C],
            trg.[PRINCIPAL_PARENT_CARER__C] = src.[PRINCIPAL_PARENT_CARER__C],
            trg.[PROGRAM_NAME__C] = src.[PROGRAM_NAME__C],
            trg.[REFERRAL_DATE__C] = src.[REFERRAL_DATE__C],
            trg.[REFERRAL_PHASE_CODE__C] = src.[REFERRAL_PHASE_CODE__C],
            trg.[REFERRAL_STATUS_CODE__C] = src.[REFERRAL_STATUS_CODE__C],
            trg.[REFUGEE_HUMANITARIAN_VISA__C] = src.[REFUGEE_HUMANITARIAN_VISA__C],
            trg.[SERVICING_PLACEMENT_STATUS__C] = src.[SERVICING_PLACEMENT_STATUS__C],
            trg.[SITE_CODE__C] = src.[SITE_CODE__C],
            trg.[SSI_SITE__C] = src.[SSI_SITE__C],
            trg.[SUSPENSION_REASON_DESCRIPTION__C] = src.[SUSPENSION_REASON_DESCRIPTION__C],
            trg.[THINK_TIME_DAYS__C] = src.[THINK_TIME_DAYS__C],
            trg.[THINK_TIME_ELECTED__C] = src.[THINK_TIME_ELECTED__C],
            trg.[TRAFFIC_LIGHT__C] = src.[TRAFFIC_LIGHT__C],
            trg.[IsDeleted] = src.[IsDeleted],
            trg.[RowHashSum] = src.[RowHashSum]
    WHEN NOT MATCHED BY TARGET THEN 
        INSERT (
            [Id],
            [AGE_OF_YOUNGEST_CHILD__C],
            [CALD__C],
            [DFV_ABUSE_COERCIVE_CONTROL__C],
            [DFV_ABUSE_CULTURAL_SPIRITUAL__C],
            [DFV_ABUSE_DOMESTIC_SERVITUDE__C],
            [DFV_ABUSE_DOWRY__C],
            [DFV_ABUSE_EMOTIONAL_PSYCHOLOGICAL__C],
            [DFV_ABUSE_FINANCIAL__C],
            [DFV_ABUSE_FORCED_MARRIAGE__C],
            [DFV_ABUSE_HARRASSMENT_STALKING__C],
            [DFV_ABUSE_IMMIGRATION_FACILITATED__C],
            [DFV_ABUSE_MULTIPERPETRATOR__C],
            [DFV_ABUSE_PHYSICAL__C],
            [DFV_ABUSE_REPRODUCTIVE__C],
            [DFV_ABUSE_SEXUAL__C],
            [DFV_ABUSE_STRANGULATION__C],
            [DFV_ABUSE_SYSTEMS__C],
            [DFV_ABUSE_TECHNOLOGY_FACILITATED__C],
            [DFV_ABUSE_VERBAL__C],
            [DFV_BARRIER_TO_DISCLOSURE_CULTURAL__C],
            [DFV_BARRIER_TO_DISCLOSURE_LANGUAGE__C],
            [DFV_BARRIER_TO_DISCLOSURE_MISIDENTIFY__C],
            [DFV_BARRIER_TO_DISCLOSURE_SYSTEM__C],
            [DFV_CASE_CONFERENCE_DATE__C],
            [DFV_CHILDREN_UNDER_18__C],
            [DISABILITY_TYPE__C],
            [EDUCATION_LEVEL__C],
            [INTERPRETER_LANGUAGE__C],
            [MENTAL_ILLNESS_FLAG__C],
            [VULNERABLE_YOUTH__C],
            [VULNERABLE_YOUTH_STUDENT__C],
            [COMMENCEMENT_OF_SERVICING_DATE__C],
            [PMDM__CONTACT__C],
            [CONTRACT_REFERENCE_ID__C],
            [CONTRACT_REFERRAL_START_DATE__C],
            [CONTRACT_REFERRAL_STATUS__C],
            [CRN__C],
            [DES_PROGRAM_TYPE_CODE__C],
            [ECM_CASE_ID__C],
            [EMPLOYMENT_BENCHMARK__C],
            [ESAT_JCA_ASSESSMENT_DATE__C],
            [EXIT_DATE__C],
            [FUNDING_LEVEL__C],
            [INDIGENOUS_INDICATOR__C],
            [JOB_PLAN_STATUS__C],
            [JSCI_ASSESSMENT_DATE__C],
            [JSCI_STATUS__C],
            [LATEST_RESUME_DATE__C],
            [LINKED_ACTIVITY__C],
            [MATURE_AGE__C],
            [NEIS_FLAG__C],
            [NEXT_APPOINTMENT_DATE__C],
            [NEXT_SERVICE_DELIVERY__C],
            [ORIGINAL_COMMENCEMENT_DATE__C],
            [OTHER_PERSONAL_ISSUES_BARRIERS__C],
            [OWNERID__C],
            [PARTICIPATION_PLAN_STATUS__C],
            [PERIOD_OF_SERVICE__C],
            [POI_CHECKED__C],
            [PRINCIPAL_PARENT_CARER__C],
            [PROGRAM_NAME__C],
            [REFERRAL_DATE__C],
            [REFERRAL_PHASE_CODE__C],
            [REFERRAL_STATUS_CODE__C],
            [REFUGEE_HUMANITARIAN_VISA__C],
            [SERVICING_PLACEMENT_STATUS__C],
            [SITE_CODE__C],
            [SSI_SITE__C],
            [SUSPENSION_REASON_DESCRIPTION__C],
            [THINK_TIME_DAYS__C],
            [THINK_TIME_ELECTED__C],
            [TRAFFIC_LIGHT__C],
            [IsDeleted],
            [RowHashSum]
        ) VALUES (
            src.[Id],
            src.[AGE_OF_YOUNGEST_CHILD__C],
            src.[CALD__C],
            src.[DFV_ABUSE_COERCIVE_CONTROL__C],
            src.[DFV_ABUSE_CULTURAL_SPIRITUAL__C],
            src.[DFV_ABUSE_DOMESTIC_SERVITUDE__C],
            src.[DFV_ABUSE_DOWRY__C],
            src.[DFV_ABUSE_EMOTIONAL_PSYCHOLOGICAL__C],
            src.[DFV_ABUSE_FINANCIAL__C],
            src.[DFV_ABUSE_FORCED_MARRIAGE__C],
            src.[DFV_ABUSE_HARRASSMENT_STALKING__C],
            src.[DFV_ABUSE_IMMIGRATION_FACILITATED__C],
            src.[DFV_ABUSE_MULTIPERPETRATOR__C],
            src.[DFV_ABUSE_PHYSICAL__C],
            src.[DFV_ABUSE_REPRODUCTIVE__C],
            src.[DFV_ABUSE_SEXUAL__C],
            src.[DFV_ABUSE_STRANGULATION__C],
            src.[DFV_ABUSE_SYSTEMS__C],
            src.[DFV_ABUSE_TECHNOLOGY_FACILITATED__C],
            src.[DFV_ABUSE_VERBAL__C],
            src.[DFV_BARRIER_TO_DISCLOSURE_CULTURAL__C],
            src.[DFV_BARRIER_TO_DISCLOSURE_LANGUAGE__C],
            src.[DFV_BARRIER_TO_DISCLOSURE_MISIDENTIFY__C],
            src.[DFV_BARRIER_TO_DISCLOSURE_SYSTEM__C],
            src.[DFV_CASE_CONFERENCE_DATE__C],
            src.[DFV_CHILDREN_UNDER_18__C],
            src.[DISABILITY_TYPE__C],
            src.[EDUCATION_LEVEL__C],
            src.[INTERPRETER_LANGUAGE__C],
            src.[MENTAL_ILLNESS_FLAG__C],
            src.[VULNERABLE_YOUTH__C],
            src.[VULNERABLE_YOUTH_STUDENT__C],
            src.[COMMENCEMENT_OF_SERVICING_DATE__C],
            src.[PMDM__CONTACT__C],
            src.[CONTRACT_REFERENCE_ID__C],
            src.[CONTRACT_REFERRAL_START_DATE__C],
            src.[CONTRACT_REFERRAL_STATUS__C],
            src.[CRN__C],
            src.[DES_PROGRAM_TYPE_CODE__C],
            src.[ECM_CASE_ID__C],
            src.[EMPLOYMENT_BENCHMARK__C],
            src.[ESAT_JCA_ASSESSMENT_DATE__C],
            src.[EXIT_DATE__C],
            src.[FUNDING_LEVEL__C],
            src.[INDIGENOUS_INDICATOR__C],
            src.[JOB_PLAN_STATUS__C],
            src.[JSCI_ASSESSMENT_DATE__C],
            src.[JSCI_STATUS__C],
            src.[LATEST_RESUME_DATE__C],
            src.[LINKED_ACTIVITY__C],
            src.[MATURE_AGE__C],
            src.[NEIS_FLAG__C],
            src.[NEXT_APPOINTMENT_DATE__C],
            src.[NEXT_SERVICE_DELIVERY__C],
            src.[ORIGINAL_COMMENCEMENT_DATE__C],
            src.[OTHER_PERSONAL_ISSUES_BARRIERS__C],
            src.[OWNERID__C],
            src.[PARTICIPATION_PLAN_STATUS__C],
            src.[PERIOD_OF_SERVICE__C],
            src.[POI_CHECKED__C],
            src.[PRINCIPAL_PARENT_CARER__C],
            src.[PROGRAM_NAME__C],
            src.[REFERRAL_DATE__C],
            src.[REFERRAL_PHASE_CODE__C],
            src.[REFERRAL_STATUS_CODE__C],
            src.[REFUGEE_HUMANITARIAN_VISA__C],
            src.[SERVICING_PLACEMENT_STATUS__C],
            src.[SITE_CODE__C],
            src.[SSI_SITE__C],
            src.[SUSPENSION_REASON_DESCRIPTION__C],
            src.[THINK_TIME_DAYS__C],
            src.[THINK_TIME_ELECTED__C],
            src.[TRAFFIC_LIGHT__C],
            src.[IsDeleted],
            src.[RowHashSum]
        );

END;