﻿CREATE VIEW [meta.NCCM].VW_CSV_SOURCE_FLAT_FILES
AS
	SELECT 
		SOURCE_FILE_NAME,
		LIST_OF_COLUMNS_TO_SELECT,
		BATCH_SIZE_RECORDS,
		DELIMITER,
		CONCAT(SOURCE_FILE_NAME, CHUNKS_TARGET_FOLDER_NAME_SUFFIX) AS CHUNKS_TARGET_FOLDER_NAME,
		TARGET_TABLE_NAME
	FROM (
		SELECT 
			'Contact.csv' AS SOURCE_FILE_NAME,
			'ID,LASTNAME,FIRSTNAME,BIRTHDATE,GENDER__C,JOBSEEKERID__C,ACCOUNTID,ISDELETED,REGION__C' AS LIST_OF_COLUMNS_TO_SELECT,
			10000 AS BATCH_SIZE_RECORDS,
			',' AS DELIMITER,
			'[raw.NCCM].FFS_CONTACT' AS TARGET_TABLE_NAME
		UNION ALL
		SELECT 
			'PMDM__PROGRAMENGAGEMENT__C.csv',
			'AGE_OF_YOUNGEST_CHILD__C,CALD__C,DFV_ABUSE_COERCIVE_CONTROL__C,DFV_ABUSE_CULTURAL_SPIRITUAL__C,DFV_ABUSE_DOMESTIC_SERVITUDE__C,DFV_ABUSE_DOWRY__C,DFV_ABUSE_EMOTIONAL_PSYCHOLOGICAL__C,DFV_ABUSE_FINANCIAL__C,DFV_ABUSE_FORCED_MARRIAGE__C,DFV_ABUSE_HARRASSMENT_STALKING__C,DFV_ABUSE_IMMIGRATION_FACILITATED__C,DFV_ABUSE_MULTIPERPETRATOR__C,DFV_ABUSE_PHYSICAL__C,DFV_ABUSE_REPRODUCTIVE__C,DFV_ABUSE_SEXUAL__C,DFV_ABUSE_STRANGULATION__C,DFV_ABUSE_SYSTEMS__C,DFV_ABUSE_TECHNOLOGY_FACILITATED__C,DFV_ABUSE_VERBAL__C,DFV_BARRIER_TO_DISCLOSURE_CULTURAL__C,DFV_BARRIER_TO_DISCLOSURE_LANGUAGE__C,DFV_BARRIER_TO_DISCLOSURE_MISIDENTIFY__C,DFV_BARRIER_TO_DISCLOSURE_SYSTEM__C,DFV_CASE_CONFERENCE_DATE__C,DFV_CHILDREN_UNDER_18__C,DISABILITY_TYPE__C,EDUCATION_LEVEL__C,INTERPRETER_LANGUAGE__C,MENTAL_ILLNESS_FLAG__C,VULNERABLE_YOUTH__C,VULNERABLE_YOUTH_STUDENT__C,COMMENCEMENT_OF_SERVICING_DATE__C,PMDM__CONTACT__C,CONTRACT_REFERENCE_ID__C,CONTRACT_REFERRAL_START_DATE__C,CONTRACT_REFERRAL_STATUS__C,CRN__C,DES_PROGRAM_TYPE_CODE__C,ECM_CASE_ID__C,EMPLOYMENT_BENCHMARK__C,ESAT_JCA_ASSESSMENT_DATE__C,EXIT_DATE__C,FUNDING_LEVEL__C,INDIGENOUS_INDICATOR__C,JOB_PLAN_STATUS__C,JSCI_ASSESSMENT_DATE__C,JSCI_STATUS__C,LATEST_RESUME_DATE__C,LINKED_ACTIVITY__C,MATURE_AGE__C,NEIS_FLAG__C,NEXT_APPOINTMENT_DATE__C,NEXT_SERVICE_DELIVERY__C,ORIGINAL_COMMENCEMENT_DATE__C,OTHER_PERSONAL_ISSUES_BARRIERS__C,OWNERID__C,PARTICIPATION_PLAN_STATUS__C,PERIOD_OF_SERVICE__C,POI_CHECKED__C,PRINCIPAL_PARENT_CARER__C,ID,PROGRAM_NAME__C,REFERRAL_DATE__C,REFERRAL_PHASE_CODE__C,REFERRAL_STATUS_CODE__C,REFUGEE_HUMANITARIAN_VISA__C,SERVICING_PLACEMENT_STATUS__C,SITE_CODE__C,SSI_SITE__C,SUSPENSION_REASON_DESCRIPTION__C,THINK_TIME_DAYS__C,THINK_TIME_ELECTED__C,TRAFFIC_LIGHT__C,ISDELETED,CREATEDDATE,PMDM__APPLICATIONDATE__C,PMDM__ACCOUNT__C,PILOT_PROGRAM_PHASE__C,JOB_SEEKER_ID__C,PMDM__STARTDATE__C,PMDM__STAGE__C',
			10000,
			',',
			'[raw.NCCM].FFS_PMDM__PROGRAMENGAGEMENT__C'
		UNION ALL
		SELECT 
			'AS_CLAIM__c.csv',
			'AS_APPROVED_AMOUNT__C,AS_CLAIM_AMOUNT__C,AS_CLAIM_ID__C,AS_CLAIM_RATE_TYPE__C,AS_GST_AMOUNT__C,AS_JOBSEEKER_ID__C,AS_STATUS_DATE__C,AS_STATUS__C,ID,NAME,OUTCOME_TYPE__C,SITE_CODE__C,ISDELETED',
			10000,
			',',
			'[raw.NCCM].FFS_AS_CLAIMS'
		UNION ALL
		SELECT 
			'CLIENT_CASE__c.csv',
			'ID,CLIENT_CONTACT__C,FUNDING_PROGRAM__C,ISDELETED,STAGE__C,CLIENT_COMMENCEMENT_DATE__C,PROGRAM_ENGAGEMENT__C,CREATEDDATE,DATE_OF_EXIT__C,OWNERID,SUBURB__C,POSTCODE__C',
			10000,
			',',
			'[raw.NCCM].FFS_CLIENT_CASE'
		UNION ALL
		SELECT 
			'JOB_ORDER__c.csv',
			'ID,HOST_ORGANISATION__C,JOB_ORDER_TYPE__C,JOB_TITLE__C,JOB_TYPE__C,ISDELETED',
			10000,
			',',
			'[raw.NCCM].FFS_JOB_ORDERS'
		UNION ALL
		SELECT 
			'OUTCOMES__c.csv',
			'WRD_PHASE__C,RECORD_TYPE_NAME__C,ID,CLIENT_CASE__C,ISDELETED',
			10000,
			',',
			'[raw.NCCM].FFS_OUTCOME'
		UNION ALL
		SELECT 
			'Program_Engagement_Participant__c.csv',
			'ID,CONTACT__C,PROGRAM_ENGAGEMENT__C,ISDELETED',
			10000,
			',',
			'[raw.NCCM].FFS_PARTICIPANTS'
		UNION ALL
		SELECT 
			'SSI_Placement__c.csv',
			'ID,ANCHOR_DATE__C,CONTACT__C,END_DATE__C,INTERVIEW_DATE__C,PLACED_DATE__C,PLACEMENT_TYPE__C,REFERRED_TO_EMPLOYER_DATE__C,START_DATE__C,STATUS__C,VACANCY_ID__C,ISDELETED',
			10000,
			',',
			'[raw.NCCM].FFS_PLACEMENT'
		UNION ALL
		SELECT 
			'account.csv',
			'ID,EMPLOYER__C,ISDELETED',
			10000,
			',',
			'[raw.NCCM].FFS_ACCOUNT'

	) stm
	CROSS APPLY (SELECT '_json_chunks') AS ca(CHUNKS_TARGET_FOLDER_NAME_SUFFIX)