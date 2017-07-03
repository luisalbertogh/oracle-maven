--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_BODY_PKG_PROH_PROCESS runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE stripComments:FALSE


  CREATE OR REPLACE PACKAGE BODY "PKG_PROH_PROCESS" 
AS


-----------------------------------------------------------------------------
-- is empty record
-----------------------------------------------------------------------------
FUNCTION F_IS_EMPTY_RECORD (p_excel_record IN csv_table_array, p_total_columns IN NUMBER) RETURN BOOLEAN
IS
  v_is_empty_record BOOLEAN := TRUE;
  v_cell_value VARCHAR2(4000);
  v_cell_value_size NUMBER;
BEGIN
  FOR i in 1 .. p_total_columns LOOP
    v_cell_value := F_FORMAT_COLUMN_CSV(p_excel_record(i));
    IF v_cell_value IS NOT NULL THEN
      v_cell_value_size := LENGTH(v_cell_value);
      FOR j in 1 .. v_cell_value_size LOOP
        IF ASCII(v_cell_value) NOT IN (9, 10, 13, 32) THEN --Tabulator, new line, carriage return, blank space
          v_is_empty_record := FALSE;
          EXIT;
        END IF;
      END LOOP; 
    END IF;
    
    IF v_is_empty_record THEN      
      EXIT;
    END IF;
  END LOOP;
  
  RETURN v_is_empty_record;
END F_IS_EMPTY_RECORD;

-- **********************************************************************
-- Procedure: P_SPLIT_CSV
-- **********************************************************************
PROCEDURE P_SPLIT_CSV_PROCESS (idUpload IN NUMBER)
IS
   csv                  REF_DATA_UI_UPLOAD.csv%TYPE;
   csv_line             LONG;
   csv_line_out         CLOB;
   csv_line_error       CLOB;
   csv_result           CLOB;
   csv_field            LONG;
   cont_x               INTEGER;
   cont_y               INTEGER;
   total_rows           INTEGER;
   v_prohibited_cp_data   PROHIBITED_COUNTERPARTIES%ROWTYPE;

   CURSOR c1 IS SELECT COLUMN_VALUE FROM TABLE (f_convert_rows (csv));
   CURSOR c2 IS SELECT COLUMN_VALUE FROM TABLE (f_convert_row (csv_line, ','));

   csv_table                        csv_table_type;   
   validation_msg                   VARCHAR2 (4000);
   aux_validation_msg               VARCHAR2 (4000);
   validation_code                  NUMBER;   
   cont_error                       INTEGER;   
   action_name						VARCHAR2(500);   
   b_action              			BOOLEAN;   
   validation_result     			VARCHAR2(10);
   upload_error          			VARCHAR2(4000);
   
   v_asofdate        					DATE;
   v_client_type         				VARCHAR2(200);
   v_contracting_party_id 			    VARCHAR2(200);
   v_contracting_party_name 			VARCHAR2(200);
   v_business				        	VARCHAR2(200);
   v_business_relationship_id        	VARCHAR2(200);
   v_business_relationship_name      	VARCHAR2(200);
   v_business_relationship_rt           VARCHAR2(200);
   v_underlying_principal_id 			VARCHAR2(200);
   v_underlying_principal_name 		    VARCHAR2(100);
   v_sub_product_name           	    VARCHAR2(200);
   v_system                             VARCHAR2(200);
   v_product                 			VARCHAR2(200);
   v_account_id      					VARCHAR2(200);
    status                NUMBER; 
   c_error_log                      CLOB;
   total_colum_valid       CONSTANT INTEGER := 17;
   pos_validation_msg      CONSTANT INTEGER := 18;
   pos_validation_result   CONSTANT INTEGER := 19;
   total_colum             CONSTANT INTEGER := 19;
   comments                VARCHAR2(4000);
   
 
   field_value             VARCHAR2 (4000);   
   C_EMPTY_LINE            CONSTANT VARCHAR2(200) := '[EMPTY_LINE]';
   cont_empty                       INTEGER := 0;
   b_csv_true                       BOOLEAN;
   l_clob CLOB;
      
    v_paragon_id                       VARCHAR2(200);
    v_aspen_id                         VARCHAR2(200);
    v_location                         VARCHAR2(200);
   
   	v_asofdate2        					DATE;
    v_duplicates_ct_different            VARCHAR2(4000);
    v_duplicates_ct_equal                VARCHAR2(4000);   
    v_client_type_uppercase            	 VARCHAR2(200);
    v_client_type2_uppercase           	 VARCHAR2(200);
    v_account_id2      					 VARCHAR2(200);
    v_client_type2						 VARCHAR2(200);
         
    v_output_line_csv clob;    
    v_cont_error_total  INTEGER;
    v_duplicates_equals    VARCHAR2(4000);
    v_validation_message   VARCHAR2(4000);
    -- GBSVR-34428: Start: 
    FIELDS_LESS_THAN_EXPECTED EXCEPTION;
    MORE_FIELDS_THAN_EXPECTED EXCEPTION; 
    -- GBSVR-34428: End: 
   
BEGIN
   -- control csv when the csv row has bad format    
   b_csv_true := TRUE;
   cont_error := 0;
   csv_table := csv_table_type ();

   SELECT csv INTO csv
     FROM REF_DATA_UI_UPLOAD REF_DATA_UI_UPLOAD
    WHERE REF_DATA_UI_UPLOAD.id = idUpload;

   -- count number of rows  
   cont_x := 0;
   l_clob:= csv || CHR(10);
  select length(l_clob) - length (replace(l_clob,CHR(10))) into cont_x from dual;
  
   csv_table.EXTEND (cont_x);
   

   cont_x := 0;

   OPEN c1;

   --1.2 read record and split the clob in rows within a bidimensional array
   LOOP
      FETCH c1 INTO csv_line;

      EXIT WHEN c1%NOTFOUND;
      cont_y := 0;
      cont_x := cont_x + 1;
      
      -- init 16 fields (17 template + 1 validation message + 1 validation result)
      csv_table(cont_x) := csv_table_array (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,NULL, NULL, NULL,NULL);
	  
      OPEN c2;

      LOOP
         FETCH c2 INTO csv_field;

         EXIT WHEN c2%NOTFOUND;
         cont_y := cont_y + 1;
         
         --If input excel file has 18 or more columns, force raising error (csv_table_array has 19 positions and won't be raised till number of columns >= 19)
         IF cont_y > total_colum_valid
         THEN
            RAISE MORE_FIELDS_THAN_EXPECTED;
         END IF;

         csv_table(cont_x)(cont_y) := csv_field;
      END LOOP;

      -- validate template should have 17 fields
      IF cont_y != total_colum_valid
      THEN
         -- GBSVR-34428: Start: 
         RAISE FIELDS_LESS_THAN_EXPECTED; 
         -- GBSVR-34428: End: 
      ELSE
         csv_table(cont_x)(pos_validation_result) := 'OK';
      END IF;

      CLOSE c2;
   END LOOP;

   CLOSE c1;

   total_rows := cont_x;
   v_cont_error_total := 0;
      
   FOR i IN 1 .. total_rows
   LOOP
      validation_msg := NULL;
      validation_code := NULL;
      cont_error := 0;
      b_csv_true := TRUE;
    
      --2 validate process
      -- validate format template is OK
      validation_result := csv_table(i)(pos_validation_result); 
          
      BEGIN
      --asofdate
      field_value:=  F_FORMAT_COLUMN_CSV(csv_table(i)(1));
      v_asofdate :=F_IS_DATE(field_value);      
      IF field_value IS NULL THEN
           validation_code:= 1003; 
           validation_msg := validation_msg ||F_GET_VALIDATION_MSG(validation_code);
           cont_error     := cont_error + 1;        
           b_csv_true := false;
       ELSIF  v_asofdate IS  NULL THEN  
      	validation_code:= 2001; 
        validation_msg := validation_msg ||F_GET_VALIDATION_MSG(validation_code);
        cont_error     := cont_error + 1;        
        b_csv_true := false;
        
      END IF;

     
      -- CLIENT_TYPE
      field_value:= F_FORMAT_COLUMN_CSV(csv_table(i)(2));      
      IF  field_value IS NOT NULL  THEN
          IF (UPPER(field_value) = 'CORE' OR UPPER(field_value) = 'NON-CORE' )THEN            
             v_client_type:=field_value;
          ELSE
             validation_code:= 2002; 
             validation_msg := validation_msg ||F_GET_VALIDATION_MSG(validation_code);
             cont_error     := cont_error + 1;
             v_client_type:=field_value;
             b_csv_true := false;
          END IF;
      ELSE
         validation_code:= 1002;  
         validation_msg := validation_msg ||F_GET_VALIDATION_MSG(validation_code);
         cont_error     := cont_error + 1;         
         v_client_type:=field_value;         
         b_csv_true := false;
      END IF;
      
      
      -- ACCOUNT_ID
      field_value:= F_FORMAT_COLUMN_CSV(csv_table(i)(17)); 
      IF  field_value IS NOT NULL  THEN
             v_account_id := field_value;
      ELSE
         validation_code:= 1001;  
         validation_msg := validation_msg || F_GET_VALIDATION_MSG(validation_code);
         cont_error     := cont_error + 1;         
         v_account_id:=field_value;         
         b_csv_true := false;
      END IF;
      v_contracting_party_id:=F_FORMAT_COLUMN_CSV(csv_table(i)(3));
      v_contracting_party_name:=F_FORMAT_COLUMN_CSV(csv_table(i)(4));   
      v_paragon_id:=F_FORMAT_COLUMN_CSV(csv_table(i)(5));
      v_aspen_id:=F_FORMAT_COLUMN_CSV(csv_table(i)(6));
      v_location:=F_FORMAT_COLUMN_CSV(csv_table(i)(7));
      v_business:=F_FORMAT_COLUMN_CSV(csv_table(i)(8));
      v_business_relationship_id:=F_FORMAT_COLUMN_CSV(csv_table(i)(9));
      v_business_relationship_name:= F_FORMAT_COLUMN_CSV(csv_table(i)(10));
      v_business_relationship_rt:=F_FORMAT_COLUMN_CSV(csv_table(i)(11));
      v_underlying_principal_id:=F_FORMAT_COLUMN_CSV(csv_table(i)(12));
      v_underlying_principal_name:=F_FORMAT_COLUMN_CSV(csv_table(i)(13));
      v_sub_product_name:=F_FORMAT_COLUMN_CSV(csv_table(i)(14));      
      v_system:=F_FORMAT_COLUMN_CSV(csv_table(i)(15));
      v_product:=F_FORMAT_COLUMN_CSV(csv_table(i)(16));
      
      
     
     END;  
    
      
   ----------------- END VALIDATION CSV--------------------
     
   IF validation_result = 'OK' THEN
       
         -- Validation OK
         IF cont_error = 0 THEN
          
            validation_code := 100;
            validation_msg := F_GET_VALIDATION_MSG (validation_code);
            csv_table(i)(pos_validation_result) := 'OK';
         ELSE   
  
            csv_table(i)(pos_validation_result) := 'KO';
         END IF;

         -- insert the validation message into to the array
         csv_table(i)(pos_validation_msg) := validation_msg;
         comments:=csv_table(i)(pos_validation_msg);
      END IF; -- end validatate error template


     
            select count(*) into v_duplicates_equals from  PROH_INTERMEDIARY 
                where  
                      UPLOAD_ID = idUpload
                        and 
                            (v_asofdate is not null and ASOFDATE = v_asofdate)
                          and    
                            ((UPPER(v_client_type) = 'NON-CORE' or UPPER(v_client_type) = 'CORE') and UPPER(CLIENT_TYPE) <> UPPER(v_client_type))                          
                              and 
                                (v_account_id is not null and ACCOUNT_ID = v_account_id);
              IF  v_duplicates_equals > 0 THEN
                              validation_code:= 2004;  
                              v_duplicates_ct_different := F_GET_VALIDATION_MSG(validation_code);
                              cont_error     := cont_error + 1 ;
                              b_csv_true := false;
                              csv_table(i)(pos_validation_msg) :=  v_duplicates_ct_different;
                          csv_table(i)(pos_validation_result) := 'KO';  
                          
                 UPDATE  PROH_INTERMEDIARY set VALIDATION_MESSAGE = v_duplicates_ct_different
                  where  
                    UPLOAD_ID = idUpload
                        and 
                            (v_asofdate is not null and ASOFDATE = v_asofdate)
                          and    
                            ((UPPER(CLIENT_TYPE) = 'NON-CORE' or UPPER(CLIENT_TYPE) = 'CORE') and UPPER(CLIENT_TYPE) <> UPPER(v_client_type))                                                    
                              and 
                                (v_account_id is not null and ACCOUNT_ID = v_account_id);
              END IF;
              comments:=csv_table(i)(pos_validation_msg);
          -- insert row
      INSERT INTO PROH_INTERMEDIARY (ID,
                                   UPLOAD_ID,
                                   ASOFDATE,
                                   CLIENT_TYPE,
                                   CONTRACTING_PARTY_ID,
                                   CONTRACTING_PARTY_NAME,
                                   BUSINESS,
                                   BUSINESS_RELATIONSHIP_ID,
                                   BUSINESS_RELATIONSHIP_NAME,
                                   BUSINESS_RELATIONSHIP_ROLETYPE,
                                   UNDERLYING_PRINCIPAL_ID,
                                   UNDERLYING_PRINCIPAL_NAME,
                                   SUB_PRODUCT_NAME,
                                   SYSTEM,
                                   PRODUCT,
                                   ACCOUNT_ID,
                                   VALIDATION_MESSAGE,
                                   PARAGON_ID,
                                   ASPEN_ID,
                                   LOCATION,
                                   CSV_LINE_ID                                   
                                  )
           VALUES (SEQ_PROH_INTERMEDIARY.NEXTVAL,
           		   idUpload                          /* UPLOAD_ID */,
                   v_asofdate                        /* ASOFDATE */,
                   v_client_type                     /* client_type */,
                   v_contracting_party_id            /* contracting_party_id */,
                   v_contracting_party_name          /* contracting_party_name */,
                   v_business              			 /* business */,
                   v_business_relationship_id          /* business_relationship_id */,
                   v_business_relationship_name        /* business_relationship_name */,
                   v_business_relationship_rt   /* business_relationship_role_type */,
                   v_underlying_principal_id           /* underlying_principal_id */,
                   v_underlying_principal_name         /* underlying_principal_name */,
                   v_sub_product_name           		 /* sub_product_name */,
                   v_system                            /* system */,
                   v_product                            /* prodcut */,
                   v_account_id                        /* account_id */,
                   comments                            /*comments*/,                   
                   v_paragon_id,
                   v_aspen_id,
                   v_location,
                   i                   
                 );
                
           v_cont_error_total := cont_error + v_cont_error_total;
   END LOOP;
   -- 3 Update Upload clob with adding validation message and and status such as from SUBMITTED to VALID / PENDING APPROVAL, or UPLOAD INVALID
   -- 3.1 build the csv output
   csv_line_out := NULL;
   csv_line_error := NULL;
   cont_error := 0;

   FOR i IN 1 .. total_rows
   LOOP
    select validation_message into v_validation_message from PROH_INTERMEDIARY where UPLOAD_ID = idUpload and CSV_LINE_ID= i;
       v_output_line_csv :=csv_table(i)(1) ||','
				      || csv_table(i)(2) ||','
				      || csv_table(i)(3) ||','
				      || csv_table(i)(4) ||','
				      || csv_table(i)(5) ||','
				      || csv_table(i)(6) ||','
				      || csv_table(i)(7) ||','
				      || csv_table(i)(8) ||','
				      || csv_table(i)(9) ||','
				      || csv_table(i)(10) ||','
				      || csv_table(i)(11) ||','
				      || csv_table(i)(12) ||','
				      || csv_table(i)(13) ||','
				      || csv_table(i)(14) ||','
				      || csv_table(i)(15) ||','
				      || csv_table(i)(16) ||',' 
				      || csv_table(i)(17) ||','
				      || v_validation_message||'';
      csv_result := csv_result || v_output_line_csv  || chr(13) || chr(10)  ;
   END LOOP;
     --Error: file with no records detected
   IF LENGTH(NVL(csv_result, '')) = 0 THEN
     v_cont_error_total := 1;
     --csv_result cannot be saved as null in ref_data_ui_upload. create dummy record
     csv_result := '';
     FOR pos IN 1 .. total_colum-1 LOOP
       csv_result := csv_result || ',';
     END LOOP;
     csv_result := csv_result || 'ERROR file. The uploaded file has no valid records.';    
   END IF;  
  
   --3.2 update table  REF_DATA_UI_UPLOAD
   dbms_output.put_line(
      ':::::::::::::::::init SUMMARY::::::::::::::::::::::::::');
   
   dbms_output.put_line('- TOTAL ERRORS:' || v_cont_error_total);
   
   dbms_output.put_line('- TOTAL EMPTY:' || cont_empty);
   dbms_output.put_line('- TOTAL ROWS:' || total_rows);

  
   IF v_cont_error_total > 0 THEN  
      -- UPLOADED INVALID     
      status := 3;
   ELSE
      status := 2;     
   -- VALID - PENDING APPROVAL
   END IF;

   IF status = 2 THEN  
     
      UPDATE REF_DATA_UI_UPLOAD
         SET status_id = status, csv = csv_result
       WHERE REF_DATA_UI_UPLOAD.id = idUpload;

      COMMIT;
   --Now we have to wait for approval to load in staging
   ELSE
      -- status  UPLOADED INVALID
      UPDATE REF_DATA_UI_UPLOAD
         SET status_id = status,
             csv = csv_result,
             comments = upload_error,
             error_log = c_error_log,
             rejected_by = 'admin_pl',
             rejected_on = SYSDATE
       WHERE REF_DATA_UI_UPLOAD.id = idUpload;

      COMMIT;
   END IF;
EXCEPTION
   WHEN NO_DATA_FOUND THEN
      upload_error := 'IdUpload NOT EXIST. ' || SQLERRM;
      ROLLBACK;      
      RAISE;
   WHEN MORE_FIELDS_THAN_EXPECTED THEN
      status := 3;
      --The uploaded spreadsheet has rows with more columns than allowed
      upload_error := F_GET_VALIDATION_MSG (5002);
      c_error_log := SQLERRM;
      ROLLBACK;
      
      BEGIN
         UPDATE REF_DATA_UI_UPLOAD
            SET status_id = status,
                comments = upload_error,
                error_log = c_error_log,
                rejected_by = 'admin_pl',
                rejected_on = SYSDATE
          WHERE REF_DATA_UI_UPLOAD.id = idUpload;

         COMMIT;
      EXCEPTION
         WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
      END;
   -- GBSVR-34428: Start: 
   WHEN FIELDS_LESS_THAN_EXPECTED THEN
      status := 3;
      --The uploaded spreadsheet has the incorrect number of columns 
      upload_error := F_GET_VALIDATION_MSG (101);
      c_error_log := SQLERRM;
      ROLLBACK;
      
      BEGIN
         UPDATE REF_DATA_UI_UPLOAD
            SET status_id = status,
                comments = upload_error,
                error_log = c_error_log,
                rejected_by = 'admin_pl',
                rejected_on = SYSDATE
          WHERE REF_DATA_UI_UPLOAD.id = idUpload;

         COMMIT;
      EXCEPTION
         WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
      END;
   -- GBSVR-34428: End: 
   WHEN OTHERS
   THEN
      status := 4;
      c_error_log := SQLERRM;
      ROLLBACK;
--      dbms_output.put_line(c_error_log);

      BEGIN
         -- UPLOAD ERROR, Technical error
         upload_error := F_GET_VALIDATION_MSG (5001);

         UPDATE REF_DATA_UI_UPLOAD
            SET status_id = status,
                comments = upload_error,
                error_log = c_error_log,
                rejected_by = 'admin_pl',
                rejected_on = SYSDATE
          WHERE REF_DATA_UI_UPLOAD.id = idUpload;

         COMMIT;
      EXCEPTION
         WHEN OTHERS
         THEN
            ROLLBACK;
            RAISE;
      END;
END P_SPLIT_CSV_PROCESS;


------------------------------------------------------------------------
--Allow to convert a column value to the right format. 
-------------------------------------------------------------------------

FUNCTION F_FORMAT_COLUMN_CSV(field IN VARCHAR) 
    RETURN  VARCHAR2
AS 
  out_field VARCHAR2(4000);
  C_REF_DATA_COMMA CONSTANT VARCHAR2(20):='[REF_DATA_COMMA]';
  C_REF_DATA_NEW_LINE CONSTANT VARCHAR2(20):='[REF_DATA_LF]';
  C_REF_DATA_QUOTE CONSTANT VARCHAR2(20):='[REF_DATA_QUOTE]';
BEGIN
  out_field:=field;
  IF field IS NOT NULL THEN
	  out_field:=REPLACE(out_field,C_REF_DATA_COMMA , ',');
	  out_field:=REPLACE(out_field,C_REF_DATA_NEW_LINE,' ');
	  out_field:=REPLACE(out_field,C_REF_DATA_QUOTE,'"');
	  out_field:=TRIM(out_field);
  END IF;
  RETURN out_field;
END F_FORMAT_COLUMN_CSV;

---------------------------------------------------------------------------
-- Allow to work with the rows of clob. 
---------------------------------------------------------------------------

FUNCTION F_CONVERT_ROWS(p_list IN CLOB)
  RETURN bh_clob_type PIPELINED
AS
  l_line_index PLS_INTEGER;
  l_index PLS_INTEGER := 1;
  l_clob CLOB:= p_list || CHR(10);    
BEGIN
  LOOP      
    l_line_index := dbms_lob.instr(l_clob, CHR(10), l_index);   
    EXIT WHEN l_line_index = 0;    
    PIPE ROW ( dbms_lob.substr(l_clob, l_line_index - l_index, l_index ) ); 
    l_index := l_line_index + 1;
  END LOOP;
  RETURN;
END F_CONVERT_ROWS;

-----------------------------------------------------------------------------
-- Allow to work with the columns of each row of csv file.
-----------------------------------------------------------------------------

FUNCTION F_CONVERT_ROW(
    p_list IN VARCHAR2,
    token  IN VARCHAR2)
  RETURN bh_varchar_type PIPELINED
AS
  l_string LONG := p_list || token;
  l_line_index PLS_INTEGER;
  l_index PLS_INTEGER := 1;
BEGIN
  LOOP
    l_line_index := INSTR(l_string, token, l_index);
    EXIT
  WHEN l_line_index = 0;
    PIPE ROW ( SUBSTR(l_string, l_index, l_line_index - l_index) );
    l_index := l_line_index                           + 1;
  END LOOP;
  RETURN;
END F_CONVERT_ROW;

-----------------------------------------------------------------------------
-- Accept Upload
-----------------------------------------------------------------------------

PROCEDURE P_ACCEPT_UPLOAD (IDUPLOAD IN NUMBER, IDUSER IN VARCHAR, COMMENTS IN VARCHAR)
IS
  v_uploaded_by REF_DATA_UI_UPLOAD.UPLOADED_BY%TYPE;
  v_status_id REF_DATA_UI_UPLOAD.STATUS_ID%TYPE;
BEGIN  

  SELECT uploaded_by INTO v_uploaded_by
    FROM REF_DATA_UI_UPLOAD
   WHERE id = idupload;
   
  IF v_uploaded_by = IDUSER THEN
    ROLLBACK;
    
    pkg_monitoring.pr_insert_log_jobs_qv('PROH', null, 'PROH', current_date, 'PKG_PROH_PROCESS.P_ACCEPT_UPLOAD', 'ERROR', 'LOGGING', 'Users cannot change status of their own uploaded files', '', 'PROH');
  ELSIF v_status_id <> 2 THEN --status must be "VALID - PENDING APPROVAL"
    ROLLBACK;
    
    pkg_monitoring.pr_insert_log_jobs_qv('PROH', null, 'PROH', current_date, 'PKG_PROH_PROCESS.P_ACCEPT_UPLOAD', 'ERROR', 'LOGGING', 'The status file is not ready for approval/reject', '', 'PROH');
  END IF;
  
  UPDATE REF_DATA_UI_UPLOAD
    SET approved_by = IDUSER, approved_on = systimestamp --we save who tried to approve the file and when
  WHERE id = IDUPLOAD;
  
  
  
  IF F_PROH_LOAD_FILE_IN_PROH_CP(idUpload) = TRUE THEN
      UPDATE REF_DATA_UI_UPLOAD
         SET status_id = 5 --Set status to USER APPROVED
       WHERE id = IDUPLOAD;
  
  ELSE
      UPDATE REF_DATA_UI_UPLOAD
         SET status_id = 7, --Set status to APPROVED INVALID
             rejected_by='admin_pl', rejected_on=SYSDATE,
             comments=F_GET_VALIDATION_MSG(5001)
       WHERE id = IDUPLOAD;
  
	   pkg_monitoring.pr_insert_log_jobs_qv('PROH',null,'PROH',current_date,'P_ACCEPT_UPLOAD','DEBUG', 'LOGGING', 'Set status to APPROVED INVALID', '', 'PROH');
  END IF;  
EXCEPTION
WHEN OTHERS THEN
  ROLLBACK;
  
  pkg_monitoring.pr_insert_log_jobs_qv('PROH',null,'PROH',current_date,'P_ACCEPT_UPLOAD','DEBUG', 'LOGGING', 'ROLLBACK', '', 'PROH');
  RAISE;
END P_ACCEPT_UPLOAD;

-----------------------------------------------------------------------------
-- Reject upload
-----------------------------------------------------------------------------
PROCEDURE P_REJECT_UPLOAD (P_IDUPLOAD IN NUMBER, P_IDUSER IN VARCHAR, P_COMMENTS IN VARCHAR)
IS
  v_uploaded_by REF_DATA_UI_UPLOAD.UPLOADED_BY%TYPE;
  v_status_id REF_DATA_UI_UPLOAD.STATUS_ID%TYPE;
BEGIN  

  SELECT uploaded_by, status_id INTO v_uploaded_by, v_status_id
    FROM REF_DATA_UI_UPLOAD
   WHERE id = P_IDUPLOAD;
   
  IF v_uploaded_by = P_IDUSER THEN
    ROLLBACK;    
    pkg_monitoring.pr_insert_log_jobs_qv('PROH', null, 'PROH', current_date, 'PKG_PROH_PROCESS.P_REJECT_UPLOAD', 'ERROR', 'LOGGING', 'Users cannot change status of their own uploaded files.', '', 'PROH');
  ELSIF v_status_id <> 2 THEN --status must be "VALID - PENDING APPROVAL"
    ROLLBACK;    
    pkg_monitoring.pr_insert_log_jobs_qv('PROH', null, 'PROH', current_date, 'PKG_PROH_PROCESS.P_REJECT_UPLOAD', 'ERROR', 'LOGGING', 'The status file is not ready for approval/reject', '', 'PROH');
  END IF;
  
  UPDATE REF_DATA_UI_UPLOAD
     SET rejected_by = P_IDUSER, rejected_on = systimestamp,
         status_id = 6, --User rejected
         comments = P_COMMENTS
   WHERE id = P_IDUPLOAD;
EXCEPTION
WHEN OTHERS THEN
  ROLLBACK;
  RAISE;
END P_REJECT_UPLOAD;

-----------------------------------------------------------------------------
-- load file into PROHIBITED_COUNTERPARTIES
-----------------------------------------------------------------------------
FUNCTION F_PROH_LOAD_FILE_IN_PROH_CP(P_UPLOAD_ID IN NUMBER) RETURN BOOLEAN
IS
  cursor c1 is
      SELECT *
      FROM PROH_INTERMEDIARY
      WHERE upload_id = P_UPLOAD_ID;
  success BOOLEAN := TRUE;
  v_validation_message VARCHAR2(4000);
  v_cont_error INTEGER;
  PROCESS_APPROVAL CONSTANT INT := 2;    
  v_upload_data   REF_DATA_UI_UPLOAD%ROWTYPE;
  
BEGIN
	

  select * into v_upload_data from REF_DATA_UI_UPLOAD
    WHERE id = P_UPLOAD_ID;



   delete from PROHIBITED_COUNTERPARTIES pc where pc.ASOFDATE IN (
      select distinct(ph.asofdate)       from PROH_INTERMEDIARY ph
        where ph.UPLOAD_ID = P_UPLOAD_ID and ph.asofdate is not null); 

  --Transform each row
  FOR v_intermediary in c1
    LOOP
       BEGIN
       

         success := F_PROH_INTERMEDIARY_TO_PROH_CP(v_intermediary.id, v_upload_data);

         EXCEPTION
           WHEN OTHERS THEN
            success := FALSE;
       END;
       IF success = FALSE THEN       
           pkg_monitoring.pr_insert_log_jobs_qv('PROH',null,'PROH',current_date,'F_PROH_LOAD_FILE_IN_PROHIBITED_COUNTERPARTIES','DEBUG', 'LOGGING', 'success false when calling F_PROH_LOAD_FILE_IN_PROHIBITED_COUNTERPARTIES', '', 'PROH');
      	   EXIT;
       END IF;
  
    END LOOP;
   
  RETURN success;
END F_PROH_LOAD_FILE_IN_PROH_CP;

-----------------------------------------------------------------------------
-- check date format
-----------------------------------------------------------------------------
FUNCTION F_IS_DATE(FIELD_DATE_VALUE IN VARCHAR2) RETURN DATE
IS

BEGIN
	
 RETURN TO_DATE(FIELD_DATE_VALUE, 'DD-Mon-YY');

EXCEPTION WHEN OTHERS THEN
   RETURN NULL;
END F_IS_DATE;

-----------------------------------------------------------------------------
-- From PROH_INTERMEDIARY TO PROHIBITED_COUNTERPARTIES
-----------------------------------------------------------------------------

FUNCTION F_PROH_INTERMEDIARY_TO_PROH_CP(P_ID NUMBER, p_upload_data  REF_DATA_UI_UPLOAD%ROWTYPE) RETURN BOOLEAN

IS
   v_intermediary_data PROH_INTERMEDIARY%ROWTYPE;   
   v_count NUMBER;
   
   v_expanded_data PROHIBITED_COUNTERPARTIES%ROWTYPE;
   v_asofdate DATE;
   v_rows_found INTEGER;
   
   
BEGIN
   SELECT * INTO v_intermediary_data
   FROM PROH_INTERMEDIARY b
   WHERE b.id = P_ID;   
      
      v_expanded_data.asofdate  := v_intermediary_data.asofdate;							                        
      v_expanded_data.client_type := v_intermediary_data.client_type;                     							
      v_expanded_data.contracting_party_id := v_intermediary_data.contracting_party_id;            				
      v_expanded_data.contracting_party_name := v_intermediary_data.contracting_party_name;          
      v_expanded_data.business := v_intermediary_data.business;              			 
      v_expanded_data.business_relationship_id := v_intermediary_data.business_relationship_id;         
      v_expanded_data.business_relationship_name := v_intermediary_data.business_relationship_name;     
      v_expanded_data.business_relationship_roletype := v_intermediary_data.business_relationship_roletype;  
      v_expanded_data.underlying_principal_id := v_intermediary_data.underlying_principal_id;         
      v_expanded_data.underlying_principal_name := v_intermediary_data.underlying_principal_name;     
      v_expanded_data.sub_product_name := v_intermediary_data.sub_product_name;           		
      v_expanded_data.system := v_intermediary_data.system;                          
      v_expanded_data.product := v_intermediary_data.product;
      v_expanded_data.account_id := v_intermediary_data.account_id;
      
      v_expanded_data.paragon_id := v_intermediary_data.paragon_id; 
      v_expanded_data.aspen_id := v_intermediary_data.aspen_id; 
      v_expanded_data.location := v_intermediary_data.location;
      v_expanded_data.create_user := p_upload_data.uploaded_by;
	  v_expanded_data.create_date := p_upload_data.uploaded_on;
	  v_expanded_data.last_modification_user := p_upload_data.uploaded_by;
	  v_expanded_data.last_modified_date := p_upload_data.uploaded_on;
	  v_expanded_data.APPROVER_USER := p_upload_data.approved_by;
	  v_expanded_data.APPROVAL_DATE := p_upload_data.approved_on;
	    
      --Insert in PROHIBITED_COUNTERPARTIESG
      INSERT INTO PROHIBITED_COUNTERPARTIES VALUES v_expanded_data;
     
   RETURN TRUE;
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    
    pkg_monitoring.pr_insert_log_jobs_qv('PROH', null, 'PROH', current_date,'PKG_PROH_PROCESS.F_PROH_INTERMEDIARY_TO_PROH_COUNTERP','ERROR', 'FATAL', 'Error:'||TO_CHAR(SQLCODE), SUBSTR(SQLERRM, 1, 2500), 'PROH');
    RAISE;
END F_PROH_INTERMEDIARY_TO_PROH_CP;

----------------------------------------------------------
--return the validation message depending on the code IN
----------------------------------------------------------
FUNCTION F_GET_VALIDATION_MSG(
    code IN NUMBER)
  RETURN VARCHAR2
IS
  return_msg VARCHAR2(1000);
BEGIN
  SELECT MESSAGE
  INTO return_msg
  FROM proh_validation_lookup
  WHERE proh_validation_lookup.return_code = code;
  RETURN return_msg || ' ';
EXCEPTION
WHEN NO_DATA_FOUND THEN
  RETURN '-1';
END F_GET_VALIDATION_MSG;
  
END PKG_PROH_PROCESS;
