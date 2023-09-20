CREATE VIEW [meta.NCCM].VW_CSV_SOURCE_FLAT_FILES
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
			'Id,LastName,FirstName,Date_of_Arrival_in_Australia__c,Birthdate,Gender__c,Citizenship__c,Client_Status__c,HSP_Client_Id__c,HSP_Case_ID__c,Highest_Level_of_Education_Outside_AUS__c,Religion_Faith__c,ExpECM__Ethnicity__c,Visa_Sub_Class__c,First_Language__c,Client_Tier__c,MailingCity,MailingPostalCode,AccountId,IsDeleted' AS LIST_OF_COLUMNS_TO_SELECT,
			10000 AS BATCH_SIZE_RECORDS,
			',' AS DELIMITER,
			'[raw.NCCM].FFS_CONTACT' AS TARGET_TABLE_NAME
		UNION ALL
		SELECT 
			'PMDM__PROGRAMENGAGEMENT__C.csv',
			'Id,HSP_Client_Id__c,Name,Arrival_Date__c,HSP_Case_Id__c,HSP_Case_Manager__c,HSP_Created_Date_Time__c,HSP_Last_Updated_Date_Time__c,Settlement_Location__c,HSP_Case_Status__c,HSP_Case_Type__c,HSP_Refugee_Cohort__c,Referred_Date__c,pmdm__Program__c,pmdm__Stage__c,Case_SMT_Indicator__c,DIBP_File_Id__c,DIBP_Post__c,Entry_Expiry__c,HSP_Tier_3_Indicator__c,Externally_Referred__c,Preferred_Language__c,Visa_Sub_Class__c,HSP_Medical_Indicator__c,pmdm__Account__c,IsDeleted',
			10000,
			',',
			'[raw.NCCM].FFS_PMDM__PROGRAMENGAGEMENT__C'
	) stm
	CROSS APPLY (SELECT	'_json_chunks') AS ca(CHUNKS_TARGET_FOLDER_NAME_SUFFIX)