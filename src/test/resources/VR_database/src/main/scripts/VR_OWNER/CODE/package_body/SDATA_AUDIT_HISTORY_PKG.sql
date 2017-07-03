--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_BODY_SDATA_AUDIT_HISTORY_PKG runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE stripComments:FALSE


  CREATE OR REPLACE PACKAGE BODY "SDATA_AUDIT_HISTORY_PKG" 
IS
PROCEDURE SDATA_AUDIT_HISTORY_PROC(
   	p_audit_detail_arr 	IN 	SDATA_AUDIT_HISTORY_TYPE_TABLE
   	,p_widget_id 		IN 	Number
   	,p_action_type 		IN 	varchar2
   	,p_email_id 		IN 	varchar2
   	,p_approver_email_id	IN 	varchar2
)
AS 
  	action_Id  Number;
BEGIN
	action_id  := SDATA_AUDIT_HISTORY_SEQ.nextval;

	INSERT INTO SDATA_AUDIT_HISTORY a (ACTION_ID, WIDGET_ID , ACTION_TYPE , EMAIL_ID, APPROVER_EMAIL_ID ) VALUES (action_id, p_widget_id, p_action_type, p_email_id, p_approver_email_id);
	IF (p_audit_detail_arr IS NOT NULL AND p_audit_detail_arr.COUNT >0) THEN
		FOR i IN p_audit_detail_arr.FIRST .. p_audit_detail_arr.LAST LOOP
			insert into SDATA_AUDIT_HISTORY_DETAIL 
				(ACTION_ID
				, AUDIT_KEY
				, AUDIT_VALUE
				, MODIFIED_FROM_VALUE )
			values (action_id
				,p_audit_detail_arr(i).AUDIT_KEY
				,p_audit_detail_arr(i).AUDIT_VALUE
				,p_audit_detail_arr(i).MODIFIED_FROM_VALUE);
		END LOOP;
	END IF;
	
END SDATA_AUDIT_HISTORY_PROC;

END SDATA_AUDIT_HISTORY_PKG;
