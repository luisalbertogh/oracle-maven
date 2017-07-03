--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_BODY_PKG_VRI_LIMIT runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE stripComments:FALSE


  CREATE OR REPLACE PACKAGE BODY "PKG_VRI_LIMIT" AS

PROCEDURE P_MAIN_PROCEDURE_LIMIT(idUpload IN NUMBER)
IS
BEGIN
  P_SPLIT_CSV(idUpload);
END P_MAIN_PROCEDURE_LIMIT;

-- **********************************************************************
-- Procedure: P_SPLIT_CSV
-- **********************************************************************
PROCEDURE P_SPLIT_CSV(
    idUpload IN NUMBER)
IS
  csv CLOB;
  csv_line CLOB;
  csv_line_out CLOB;
  csv_line_error CLOB;
  csv_result CLOB;
  csv_field CLOB;
  cont_x            INTEGER;
  nummmm            INTEGER;
  cont_y            INTEGER;
  total_rows        INTEGER;
  CURSOR c1
  IS
    SELECT COLUMN_VALUE FROM TABLE(PKG_COMMON_UTILS.f_convert_rows(csv));
  CURSOR c2
  IS
    SELECT COLUMN_VALUE FROM TABLE(PKG_COMMON_UTILS.f_convert_row(csv_line,','));
    
TYPE csv_table_array IS varray(13) OF VARCHAR2(4000);
TYPE csv_table_type IS  TABLE OF csv_table_array;
  csv_table             csv_table_type; 
  validation_msg        VARCHAR2(4000);
  validation_code       NUMBER;
  b_validation          BOOLEAN;  
  cont_error            INTEGER; 
  cont_warning          INTEGER;
  desk_name        		  VARCHAR2(100);
  p_desk_id        		    VARCHAR2(100);
  p_asofdate              VARCHAR2(100);
  asofdate_spot         VARCHAR2(100);
  p_limit_method         	VARCHAR2(100);
  p_limit_risk_class    	VARCHAR2(100);
  p_limit_asset_all      	VARCHAR2(100);
  limit_size           	NUMBER(24,2);
  limit_size_aux        VARCHAR2(100);
  limit_exposure        NUMBER(24,2);
  limit_exposure_aux    VARCHAR2(100);
  limit_usage           NUMBER(24,2);
  limit_usage_aux       VARCHAR2(100);
  unit_measure          VARCHAR2(200);
  aux_date              CHAR;
  validation_result     VARCHAR(10);
  status                NUMBER;
  upload_error          VARCHAR(1000);
  c_error_log           CLOB;
  total_colum_valid     CONSTANT INTEGER:=10;
  pos_validation_msg    CONSTANT INTEGER:=11;
  pos_validation_result CONSTANT INTEGER:=12;
  total_colum           CONSTANT INTEGER:=12;
  csv_true              BOOLEAN;
  p_reporting_month       vri_exemp_limit_intermediary.reporting_month%type;
  allowed_rep_month     vri_exemp_limit_intermediary.reporting_month%type;
  C_SIZE_HUNDRED        CONSTANT INTEGER:=100;
  C_SIZE_MEASURE        CONSTANT INTEGER:=100;
  C_SIZE_NUMBER         CONSTANT INTEGER:=24;
  C_SIZE_COMMENTS       CONSTANT INTEGER:=300;
  field_value           VARCHAR2(4000);
  field_name            VARCHAR2(4000);  
  message_table         CONSTANT VARCHAR2(30) := 'REF_DATA_VALIDATION_LOOKUP';
  limit_usage_validate  NUMBER(24,2);
  asofdate_format_ok    int:=1;
  upper_limit_usage     NUMBER(24,2);
  lower_limit_usage     NUMBER(24,2);
  days                  VARCHAR2(4) := 'Days';
  eur_bp                VARCHAR2(10) := 'EUR/bp';
  currency_SYM          VARCHAR2(50) := 'Currency '||UNISTR('\20AC');
  currency_txt          VARCHAR2(15) := 'Currency EUR';
  v_boolean_blank       BOOLEAN;
  v_aux                 VARCHAR2(400 BYTE);
  v_blank_row           BOOLEAN;
BEGIN
  -- control csv when the csv row has bad format
  csv_true := true;
  v_boolean_blank := false;
  cont_error       :=0;
  cont_warning     :=0;
  csv_table        := csv_table_type();  
  csv_table.EXTEND(150000);
  
  SELECT csv INTO csv FROM REF_DATA_UI_UPLOAD ui_uploaded WHERE ui_uploaded.id = idUpload;
  
  cont_x :=0;
  OPEN c1;
  --1 read record and split the clob in rows within a bidimensional array
  LOOP
    v_blank_row:=false;
   
    FETCH c1 INTO csv_line;
      
     if PKG_COMMON_UTILS.F_IS_NUMBER(TRIM(replace (REPLACE(csv_line,' ','0'),',','0')))<>0 tHEN
           v_blank_row:=true;
       else
           v_blank_row:=false;
       END IF;
        
    EXIT
  WHEN c1%NOTFOUND;
  
  if v_blank_row = false then 
          
            cont_y :=0;
            cont_x := cont_x + 1;
            -- init 12 fields (11 template + 1 validation message + 1 validation result)
            csv_table(cont_x):=csv_table_array(NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
            
            OPEN c2;
            LOOP
              FETCH c2 INTO csv_field;
              EXIT
            WHEN c2%NOTFOUND;    
              cont_y                   := cont_y + 1;
              if(cont_y = 6) THEN   -- when the field limit_asset_all is blank we substitute for NONE   
                if (csv_field = '' or csv_field IS NULL) THEN
                    csv_field := 'NONE';
                END IF;
              END IF;
              
               if(cont_y = 10) and (csv_field='Currency €'  or csv_field='Currency ???' or csv_field='Currency ?'  or instr(convert('�','utf8','us7ascii'),csv_field) != 0)   then 
                csv_field:='Currency '||UNISTR('\20AC');
               end if   ;
              
              
              
              csv_table(cont_x)(cont_y):=csv_field;      
            END LOOP;

            -- validate template should have 12 fields
            IF cont_y != total_colum_valid THEN
              validation_code:= 101;
              
              validation_msg := PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(validation_code,'Template',message_table);
               
              -- insert the validation message into to the array
              csv_table(cont_x)(pos_validation_msg):= validation_msg;
              -- set error
              csv_table(cont_x)(pos_validation_result):= 'KO';
            ELSE 
               csv_table(cont_x)(pos_validation_result):= 'OK';
            END IF;
            CLOSE c2;
            
  end if;  
  END LOOP;
  CLOSE c1;
  total_rows:= cont_x;
  
  FOR i IN 1 .. total_rows
  LOOP
    validation_msg  := NULL;
    validation_code := NULL;
    cont_error      :=0;
    cont_warning     :=0;
    --2 validate process
    -- validate format template is OK
    validation_result    := csv_table(i)(pos_validation_result);
     
    --------------------- INIT VALIDATION CSV correct -----------------------------------------------------
	 --- validate length each column
     BEGIN
     
     if (csv_table(i)(1) IS null and csv_table(i)(2) IS null and csv_table(i)(3) IS null and csv_table(i)(4) IS null and csv_table(i)(5) IS null
        and csv_table(i)(6) IS null and csv_table(i)(7) IS null and csv_table(i)(8) IS null and csv_table(i)(9) IS null and csv_table(i)(10) IS null)
     THEN     
          v_boolean_blank := true;
     END IF;
      -- DESK_NAME
      field_value:=  PKG_COMMON_UTILS.F_FORMAT_COLUMN_CSV(csv_table(i)(1));
      field_name :='desk_name';
      PKG_COMMON_UTILS.P_CHECK_FORMAT_FIELD(field_name, field_value, C_SIZE_HUNDRED, cont_error, validation_msg);
      desk_name := field_value;
      
      
      -- DESK_ID
	    field_value:=  PKG_COMMON_UTILS.F_FORMAT_COLUMN_CSV(csv_table(i)(2));
      field_name :='desk_id';
      PKG_COMMON_UTILS.P_CHECK_FORMAT_FIELD(field_name, field_value, C_SIZE_HUNDRED, cont_error, validation_msg);
      p_desk_id := field_value;
      
     
      -- ASOFDATE
	   
      field_value:=  PKG_COMMON_UTILS.F_FORMAT_COLUMN_CSV(csv_table(i)(3));
	   aux_date :='N';
      IF  field_value IS NOT NULL THEN

          IF PKG_COMMON_UTILS.F_CHECK_DATE_FORMAT(field_value) =  'Y' THEN
          aux_date := PKG_COMMON_UTILS.F_IS_DATE_OK(field_value,'DD/MM/YYYY'); 
          ELSE 
          aux_date := 'N';
          end if;
          IF aux_date = 'N' THEN
            validation_code:= 1500;
            validation_msg := validation_msg || PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(validation_code,'asofdate',message_table);
            p_asofdate:=field_value;
            cont_error     := cont_error + 1;
            csv_true := false;
            p_reporting_month:='?';
            asofdate_format_ok:=0;            
          ELSE
            p_asofdate:=field_value;
            p_reporting_month:=To_char(TO_DATE(p_asofdate,'DD/MM/YYYY'),'MM-YY');  
            allowed_rep_month := TO_CHAR (ADD_MONTHS (SYSDATE, -1),'MM-YY');
            IF p_reporting_month != allowed_rep_month THEN
            validation_code:= 1177;
            validation_msg := validation_msg || PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(validation_code,'reporting_month',message_table);            
            cont_error     := cont_error + 1;
            csv_true := false;
            asofdate_format_ok:=0;
            END IF;
            
            					--FUNLIM04 � If ASOFDATE is part of a weekend, record will be ignored
            select To_CHAR(To_DATE(p_asofdate,'DD-MM-YYYY'),'DY') into v_aux from dual;
            IF v_aux = 'SUN' OR v_aux = 'SAT' THEN
              cont_warning := cont_warning + 1;
              validation_code := 2003;
              validation_msg := validation_msg || PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(validation_code,'FUNLIM04',message_table);
            END IF;
          END IF;
      ELSE
          -- Validate ASOFDATE  not null
          p_asofdate:=field_value;
          field_name:='asofdate';
          PKG_COMMON_UTILS.P_IS_MANDATORY_NULL(field_name,p_asofdate,cont_error,validation_msg,message_table);
          p_asofdate := '?';   
          p_reporting_month:='?';
          asofdate_format_ok:=0;          
      END IF;
      
      -- LIMIT_METHOD
	    field_value:=  PKG_COMMON_UTILS.F_FORMAT_COLUMN_CSV(csv_table(i)(4));
      field_name :='limit_method';
      PKG_COMMON_UTILS.P_CHECK_FORMAT_FIELD(field_name, field_value, C_SIZE_HUNDRED, cont_error, validation_msg);
	  --**********************************/
	  --New Validation LIMIT_METHOD should be one of this (MMI,UW,RMH, FX_MMI)
	  --GBSVR-35509: Accepting also limit method DGO
	  --
	  IF(field_value <> 'MMI' AND field_value <> 'UW' AND field_value <> 'RMH' AND field_value <> 'FX_MMI' AND field_value <> 'DGO') THEN
	   validation_code:= 11113;
	   validation_msg := validation_msg || PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(validation_code,'limit_method',message_table);
	   p_limit_method:=field_value;
       cont_error     := cont_error + 1;
       csv_true := false;
	  ELSE
		p_limit_method := field_value;
		ENd IF;
      
      
      -- LIMIT_RISK_CLASS
	    field_value:=  PKG_COMMON_UTILS.F_FORMAT_COLUMN_CSV(csv_table(i)(5));
      field_name :='limit_risk_class';
      PKG_COMMON_UTILS.P_CHECK_FORMAT_FIELD(field_name, field_value, C_SIZE_HUNDRED, cont_error, validation_msg);
     
      p_limit_risk_class := field_value;
      
       -- LIMIT_ASSET_ALL
	    field_value:=  PKG_COMMON_UTILS.F_FORMAT_COLUMN_CSV(csv_table(i)(6));
      field_name :='limit_asset_all';
      PKG_COMMON_UTILS.P_CHECK_FORMAT_FIELD(field_name, field_value, C_SIZE_HUNDRED, cont_error, validation_msg);
	  IF field_value is NULL or field_value = '' THEN
         p_limit_asset_all:='NONE';
    
    ELSE
        p_limit_asset_all := field_value;
	  END IF;       
      
       
       -- LIMIT_SIZE
      field_value:=  PKG_COMMON_UTILS.F_FORMAT_COLUMN_CSV(csv_table(i)(7));
      limit_size := '';
      field_name :='limit_size';
       IF PKG_COMMON_UTILS.F_IS_NUMBER(field_value) = 0 THEN
       -- we only accept NC, reject the rest of the strings
            IF field_value = 'NC' THEN
            
            limit_size_aux := field_value;
            ELSE
            
            validation_code:= 1155;
						validation_msg :=validation_msg || PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(validation_code,field_name,message_table);
						cont_error     := cont_error + 1;   
            limit_size_aux := field_value;
            csv_true := false;
            END IF;
      ELSE
      
      PKG_COMMON_UTILS.P_CHECK_FORMAT_FIELD(field_name, field_value, C_SIZE_NUMBER, cont_error, validation_msg);
      limit_size_aux := field_value;
      limit_size := TO_NUMBER(limit_size_aux);
      end if;
      
      
	   -- LIMIT_EXPOSURE
      field_value:=  PKG_COMMON_UTILS.F_FORMAT_COLUMN_CSV(csv_table(i)(8));
      limit_exposure := '';
      field_name :='limit_exposure';
      
      IF PKG_COMMON_UTILS.F_IS_NUMBER(field_value) = 0 THEN
             -- we only accept NC, reject the rest of the strings
            IF field_value = 'NC' THEN
           
            limit_exposure_aux := field_value;
            ELSE
            
            validation_code:= 1155;
						validation_msg :=validation_msg || PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(validation_code,field_name,message_table);
						cont_error     := cont_error + 1;   
            limit_exposure_aux := field_value;
            csv_true := false;
            END IF;
      ELSE
     
      PKG_COMMON_UTILS.P_CHECK_FORMAT_FIELD(field_name, field_value, C_SIZE_NUMBER, cont_error, validation_msg);
      limit_exposure_aux := field_value;
      limit_exposure := TO_NUMBER(limit_exposure_aux);
      END IF;
     
	  -- LIMIT_USAGE
      field_value:=  PKG_COMMON_UTILS.F_FORMAT_COLUMN_CSV(csv_table(i)(9));
      limit_usage := '';
      field_name :='limit_usage';
     
      IF PKG_COMMON_UTILS.F_IS_NUMBER(field_value) = 0 THEN
       -- we only accept NC, reject the rest of the strings
            IF field_value = 'NC' THEN
           
            limit_usage_aux := field_value;
            ELSE
           
            validation_code:= 1155;
						validation_msg :=validation_msg || PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(validation_code,field_name,message_table);
						cont_error     := cont_error + 1;   
            limit_usage_aux := field_value;
            csv_true := false;
            END IF;
      ELSE
     
      PKG_COMMON_UTILS.P_CHECK_FORMAT_FIELD(field_name, field_value, C_SIZE_NUMBER, cont_error, validation_msg);
      limit_usage_aux := field_value;
      limit_usage := TO_NUMBER(limit_usage_aux);
      END IF;
      
	  --UNIT_MEASURE
      field_value:=  PKG_COMMON_UTILS.F_FORMAT_COLUMN_CSV(csv_table(i)(10));       
      field_name :='unit_measure';
      PKG_COMMON_UTILS.P_CHECK_FORMAT_FIELD(field_name, field_value, C_SIZE_MEASURE, cont_error, validation_msg);
      unit_measure := field_value;
      IF INSTR(unit_measure, currency_txt) = 1 THEN
          unit_measure := 'Currency '||UNISTR('\20AC');
      END IF;
      IF unit_measure != currency_txt AND unit_measure != currency_SYM AND unit_measure != days AND unit_measure != eur_bp THEN
          validation_code:= 1188;
					validation_msg :=validation_msg || PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(validation_code,field_name,message_table);
					cont_error     := cont_error + 1;
          csv_true := false;
      END IF;

     END;

      --Starting validations for mandatory fields.(DESK_NAME,DESK_ID,REPORTING_MONTH,LIMIT_METHOD,LIMIT_RISK_CLASS)
      -- Validate  desk_name not null
      field_name:='desk_name';
      PKG_COMMON_UTILS.P_IS_MANDATORY_NULL(field_name,desk_name,cont_error,validation_msg,message_table);
    
    
      -- Validate  DESK_ID not null
      field_name:='desk_id';
      PKG_COMMON_UTILS.P_IS_MANDATORY_NULL(field_name,p_desk_id,cont_error,validation_msg,message_table);

	  	
      -- Validate  LIMIT_METHOD id not null
      field_name:='limit_method';
      PKG_COMMON_UTILS.P_IS_MANDATORY_NULL(field_name,p_limit_method,cont_error,validation_msg,message_table);
      
	  
	   -- Validate  LIMIT_RISK_CLASS id not null
	   field_name:='limit_risk_class';
      PKG_COMMON_UTILS.P_IS_MANDATORY_NULL(field_name,p_limit_risk_class,cont_error,validation_msg,message_table);
      
       field_name:='unit_measure';
      PKG_COMMON_UTILS.P_IS_MANDATORY_NULL(field_name,unit_measure,cont_error,validation_msg,message_table);
	  
	  --New validation: LIMIT_USAGE Calculation: Value of Usage/Limit Size * 100; For us LIMIT_EXPOSURE/LIMIT_SIZE *100
		IF (limit_exposure is not null AND limit_exposure != 0) AND (limit_size is not NULL AND limit_size != 0) THEN
		  --limit_usage_validate := trunc((limit_exposure/limit_size)*100,2);
      upper_limit_usage := trunc(((limit_exposure/limit_size)*101),2);
      lower_limit_usage := trunc(((limit_exposure/limit_size)*99),2);
      
		  IF limit_usage < lower_limit_usage or limit_usage > upper_limit_usage
			THEN
				validation_code:= 1111;
       
				validation_msg := validation_msg || PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(validation_code,'limit_usage',message_table);
				cont_warning := cont_warning + 1;
				csv_true := false;
		   END IF;
		END IF;
	

     ----------------- END VALIDATION CSV--------------------
     IF validation_result = 'OK' THEN
     
	 -- Validation OK
      IF cont_error = 0 AND cont_warning = 0 THEN
        validation_code                    := 100;
        validation_msg                     :=PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(validation_code,'',message_table);
        csv_table(i)(pos_validation_result):= 'OK';
      END IF;
      IF cont_error > 0 THEN
       csv_table(i)(pos_validation_result):= 'KO';       
      ELSE
       if cont_warning > 0 THEN
        csv_table(i)(pos_validation_result):= 'WA';
       END IF;
      END IF;
      -- insert the validation message into to the array
      csv_table(i)(pos_validation_msg):= validation_msg;
    END IF; -- end validatate error template
   
   --NEW VALIDATION: IF all fields are blank then do nothing (this skip functional validations)   
     if v_boolean_blank = false then
    -- Delete from intermediary table the contents of the file. We need to do this in case the user uploads the same file or updates the file with more info
    delete from VRI_EXEMP_LIMIT_INTERMEDIARY where DESK_ID=p_desk_id and ASOFDATE=p_asofdate and LIMIT_METHOD = p_limit_method AND LIMIT_RISK_CLASS = p_limit_risk_class
                                                  and LIMIT_ASSET_ALL = p_limit_asset_all and upload_id != idUpload;
    -- insert row(DESK_NAME,DESK_ID,REPORTING_MONTH,ASOFDATE,LIMIT_METHOD,LIMIT_RISK_CLASS,LIMIT_ASSET_ALL,LIMIT_SIZE,LIMIT_EXPOSURE,LIMIT_USAGE,UNIT_MEASURE)
    INSERT INTO VRI_EXEMP_LIMIT_INTERMEDIARY
      (
        ID,
        UPLOAD_ID,
        DESK_NAME,
        DESK_ID,
        REPORTING_MONTH,
        ASOFDATE,
        LIMIT_METHOD,
        LIMIT_RISK_CLASS,
        LIMIT_ASSET_ALL,
        LIMIT_SIZE,
        LIMIT_EXPOSURE,
        LIMIT_USAGE,
        UNIT_MEASURE,
        VALIDATION_MESSAGE
      )
      VALUES
      (
        SEQ_RD_LIM_INTERMEDIARY.NEXTVAL,
        idUpload,
        desk_name,
        p_desk_id,
        p_reporting_month,
        p_asofdate,
        p_limit_method,
        p_limit_risk_class,
        p_limit_asset_all,
        limit_size_aux,
        limit_exposure_aux,
        limit_usage_aux,
        unit_measure,
        validation_msg
      );
      
    ELSE
		--Empty line
   
      csv_table(i)(pos_validation_result):= 'EL';
	END IF; 
  
	END LOOP;  
  
  
  
  -- 4 Update Upload clob with adding validation message and and status such as from SUBMITTED to VALID / PENDING APPROVAL, or UPLOAD INVALID
  --4.1 build the csv output
  csv_line_out  := NULL;
  csv_line_error:=NULL;
  cont_error:=0;
  cont_warning :=0;
  
 FOR i IN 1 .. total_rows
  LOOP
  
   
    validation_result:= csv_table(i)(pos_validation_result);
    
    FOR j IN 1 .. total_colum
    LOOP        
      -- last field
      IF pos_validation_msg  = j THEN        
        IF validation_result = 'KO' THEN
          IF asofdate_format_ok = 0 THEN         
                 csv_line_error    := csv_line_error || csv_table(i)(j) || 'File Format Validation failure - File rejected and no functional validations will be performed until format is correct' || CHR(10);
                 cont_error:= cont_error + 1;
          ELSE
                 csv_line_error    := csv_line_error || csv_table(i)(j) || CHR(10);
                 cont_error:= cont_error + 1;
          END IF;
        END IF;
        IF validation_result = 'OK' THEN
           IF asofdate_format_ok = 0 THEN
              csv_line_out:= csv_line_out ||  'File Format Validation failure - File rejected and no functional validations will be performed until format is correct' || CHR(10);
           ELSE
              csv_line_out:= csv_line_out || csv_table(i)(j) || CHR(10);
          
           END IF;
        END IF;
        IF validation_result = 'WA' THEN
          
          IF asofdate_format_ok = 0 THEN
              csv_line_error    := csv_line_error || csv_table(i)(j) || 'File Format Validation failure - File rejected and no functional validations will be performed until format is correct' || CHR(10);
              cont_warning := cont_warning +1;                
          ELSE
              csv_line_error    := csv_line_error || csv_table(i)(j) || CHR(10);
              cont_warning := cont_warning +1;  
          END IF;
        END IF;
      ELSE
        IF pos_validation_msg  > j THEN
          IF validation_result = 'KO' OR validation_result = 'WA' THEN
            csv_line_error    := csv_line_error || csv_table(i)(j) || ',';
            
          ELSE
            IF validation_result = 'OK' THEN
            csv_line_out:= csv_line_out || csv_table(i)(j) || ',';
            
            END IF;
          END IF;
        END IF;
      END IF;
    END LOOP;
  END LOOP;
  csv_result:=csv_line_error || csv_line_out;  
  IF cont_error > 0 THEN
    -- UPLOADED INVALID
    status:= 3;
  ELSE
    if (cont_warning >0) THEN
       status := 9;
    else
       status:= 2;
    END IF ;-- VALID - PENDING APPROVAL  
  END IF;
  
      if csv_result is null or length(csv_result) =0 THEN   
      --pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'VRI_LIMITS','DEBUG', 'LOGGING', 'P_SPLIT_CSV', 'ENTRA ', 'bRDS'); 
        select csv into csv_result from ref_data_ui_upload where id = idUpload;      
        status := 3;
      END IF;
    
  IF (status = 2 OR status = 9) THEN
      UPDATE REF_DATA_UI_UPLOAD
        SET status_id         = status ,
            csv               = csv_result
           -- comments = 'LINES WITH ERRORS:'|| cont_error || ' in '|| total_rows || ' ROWS'
        WHERE REF_DATA_UI_UPLOAD.id = idUpload;
    COMMIT;
  ELSE
      -- status  UPLOADED INVALID      
          UPDATE REF_DATA_UI_UPLOAD
             SET status_id         = status,
                 csv               = csv_result,
                comments          = upload_error,
                 error_log         = c_error_log,
                 rejected_by       = 'admin_pl',
                 rejected_on       = SYSDATE
                 WHERE REF_DATA_UI_UPLOAD.id = idUpload;
          COMMIT;
 END IF;
 --If the asofdate format is correct
 IF asofdate_format_ok = 1 then 
  P_CHECK_FUNCIONAL_LIMITS(idUpload);
 END IF; 
 
EXCEPTION
WHEN NO_DATA_FOUND THEN    
    upload_error:= 'IdUpload NOT EXIST. ' || SQLERRM;
    ROLLBACK;
    DBMS_OUTPUT.put_line(upload_error);
    RAISE; 
WHEN SUBSCRIPT_OUTSIDE_LIMIT THEN   
   status:= 4;
   --The uploaded spreadsheet has rows with more columns than allowed
   upload_error:=PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(5002,'',message_table); 
   c_error_log:= SQLERRM;
   ROLLBACK;
  
   BEGIN
     UPDATE REF_DATA_UI_UPLOAD
         SET status_id         = status,
             comments          = upload_error,
             error_log         = c_error_log,
             rejected_by       = 'admin_pl',
             rejected_on       = SYSDATE
             WHERE REF_DATA_UI_UPLOAD.id = idUpload;
     COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      RAISE;
  END;
   
 WHEN OTHERS THEN
    status:= 4;
    c_error_log:= SQLERRM;
    ROLLBACK;
    
  BEGIN
    -- UPLOAD ERROR, Technical error
    upload_error:= PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(5001,'',message_table) || ' Unexpected error processing field: ' || field_name;
    UPDATE REF_DATA_UI_UPLOAD
    SET status_id       = status,
      comments          = upload_error,
      error_log         = c_error_log,
      rejected_by       = 'admin_pl',
      rejected_on       = SYSDATE
    WHERE REF_DATA_UI_UPLOAD.id = idUpload;
    COMMIT;
  EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    RAISE;
  END;
  
  --P_CHECK_FUNCIONAL_LIMITS(idUpload);
END P_SPLIT_CSV;

/************************************************************/
/*CHECK length for each column AND DATE FORMATS				*/
/************************************************************/


PROCEDURE P_CHECK_FORMAT_FIELD(field_name IN VARCHAR2, field_value IN OUT VARCHAR2, C_SIZE_PARAM IN NUMBER, cont_error IN OUT VARCHAR2, validation_msg IN OUT VARCHAR )
	IS
		C_SIZE_2HUNDRED CONSTANT INTEGER:=200;
		C_SIZE_HUNDRED CONSTANT INTEGER:=100;
		C_SIZE_FIFTY CONSTANT INTEGER:=50;
		C_SIZE_NUMBER CONSTANT INTEGER:=24;
		C_SIZE_MEASURE CONSTANT INTEGER:=100;
		C_SIZE_COMMENTS CONSTANT INTEGER:=300;
    validation_code VARCHAR2(5);
    message_table CONSTANT VARCHAR2(30) := 'REF_DATA_VALIDATION_LOOKUP';
		BEGIN
			case  
				WHEN C_SIZE_PARAM = C_SIZE_HUNDRED THEN
					IF field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_HUNDRED THEN
						field_value := SUBSTR(field_value, 1 , C_SIZE_HUNDRED);
						validation_code:= 1100;
						validation_msg :=validation_msg || PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(validation_code,field_name,message_table);
						cont_error     := cont_error + 1;
					END IF; 
				WHEN C_SIZE_PARAM = C_SIZE_FIFTY THEN
					IF field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_FIFTY THEN
						field_value := SUBSTR(field_value, 1 , C_SIZE_FIFTY);
						validation_code:= 1150;
						validation_msg :=validation_msg || PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(validation_code,field_name,message_table);
						cont_error     := cont_error + 1;
					END IF; 
				WHEN C_SIZE_PARAM = C_SIZE_NUMBER THEN
					IF field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_NUMBER THEN
						field_value := SUBSTR(field_value, 1 , C_SIZE_NUMBER);
						validation_code:= 1126;
						validation_msg :=validation_msg || PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(validation_code,field_name,message_table);
						cont_error     := cont_error + 1;
					END IF; 
				WHEN C_SIZE_PARAM = C_SIZE_MEASURE THEN
					IF field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_MEASURE THEN
						field_value := SUBSTR(field_value, 1 , C_SIZE_MEASURE);
						validation_code:= 1100;
						validation_msg :=validation_msg || PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(validation_code,field_name,message_table);
						cont_error     := cont_error + 1;
					END IF;
				WHEN C_SIZE_PARAM = C_SIZE_2HUNDRED THEN
					IF field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_2HUNDRED THEN
						field_value := SUBSTR(field_value, 1 , C_SIZE_2HUNDRED);
						validation_code:= 1120;
						validation_msg :=validation_msg || PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(validation_code,field_name,message_table);
						cont_error     := cont_error + 1;
					END IF;
       ELSE
          cont_error     := cont_error + 1;
          validation_msg :=validation_msg || 'Unknow length error';
      END Case;
END;

PROCEDURE P_CHECK_FUNCIONAL_LIMITS(idUpload IN NUMBER)
IS
	v_keys Number;
	v_DESK_NAME VARCHAR2(100 BYTE);
	v_DESK_ID VARCHAR2(100 BYTE);
	v_ASOFDATE VARCHAR2(25 BYTE);
	v_LIMIT_METHOD VARCHAR2(100 BYTE);
	v_LIMIT_RISK_CLASS VARCHAR2(100 BYTE);
	v_LIMIT_ASSET_ALL VARCHAR2(100 BYTE);
	v_LIMIT_SIZE VARCHAR2(100 BYTE);
	v_LIMIT_EXPOSURE VARCHAR2(100 BYTE);
	v_LIMIT_USAGE VARCHAR2(100 BYTE);
	v_msg VARCHAR2(4000 BYTE);
  f_msg VARCHAR2(4000 BYTE);
        v_unit_measure VARCHAR2(200 BYTE);
        v_ASOFDATE_aux VARCHAR2(25 BYTE);
	
	message_table CONSTANT VARCHAR2(30) := 'REF_DATA_VALIDATION_LOOKUP';
	validation_code NUMBER;
	n_errors NUMBER;
	v_aux VARCHAR2(400 BYTE);
	v_aux_mon VARCHAR2(400 BYTE);
  ready_for_submission NUMBER:=1;
  v_csv CLOB;
  p_value VARCHAR2(4000);
  f_errors NUMBER;
  f_errors_message NUMBER;
  p_message_ok REF_DATA_VALIDATION_LOOKUP.message%type;
  f_warnings NUMBER;  
  f_warnings_2 NUMBER; 
  f_warnings_3 NUMBER;
  f_warnings_4 NUMBER;  
  v_idUser              VARCHAR2(100);
	CURSOR c1
  IS
    SELECT DESK_NAME,DESK_ID,ASOFDATE,LIMIT_METHOD,LIMIT_RISK_CLASS,LIMIT_ASSET_ALL,LIMIT_SIZE,LIMIT_EXPOSURE,LIMIT_USAGE, VALIDATION_MESSAGE, UNIT_MEASURE FROM VRI_EXEMP_LIMIT_INTERMEDIARY WHERE UPLOAD_ID = idUpload;
	--
BEGIN
	n_errors := 0;
  select message into p_message_ok from REF_DATA_VALIDATION_LOOKUP where return_code=100; 
  
  p_message_ok := p_message_ok || ' : ' || '' || ' ; ';
  
  
	OPEN c1;
		LOOP
			FETCH c1 INTO v_DESK_NAME,v_DESK_ID,v_ASOFDATE,v_LIMIT_METHOD,v_LIMIT_RISK_CLASS,v_LIMIT_ASSET_ALL,v_LIMIT_SIZE,v_LIMIT_EXPOSURE,v_LIMIT_USAGE,v_msg, v_unit_measure;
			EXIT WHEN c1%NOTFOUND;
      
				--FUNLIM01 - No duplicates are allowed. Key to validate the duplicates is: DESK_NAME,DESK_ID,ASOFDATE,LIMIT_METHOD,LIMIT_RISK_CLASS,LIMIT_ASSET_ALL
				select count(1) into v_keys
				from VRI_EXEMP_LIMIT_INTERMEDIARY
				WHERE UPLOAD_ID = idUpload AND
					 DESK_NAME = v_DESK_NAME
					AND DESK_ID =  v_DESK_ID
					AND ASOFDATE = v_ASOFDATE
					AND LIMIT_METHOD = v_LIMIT_METHOD
					AND LIMIT_RISK_CLASS = v_LIMIT_RISK_CLASS
					AND LIMIT_ASSET_ALL = v_LIMIT_ASSET_ALL;
				IF v_keys > 1 THEN
					--ERROR, update REF_DATA_UI_UPLOAD, update row message
					n_errors := n_errors +1;
					validation_code := 2000;
					v_msg := v_msg || PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(validation_code,'FUNLIM01',message_table);
				END IF;
					v_keys:=0;
				--FUNLIM02 � If there are no duplicates within the file but there are duplicates with other rows already pre-existing in the VRI_EXEMPTION_LIMIT_STAGING, rows will be rejected
					select To_CHAR(To_DATE(v_ASOFDATE,'DD-MM-YYYY')) into v_aux from dual;
          -- Delete from STAGING table the contents of the file. We need to do this in case the user uploads the same file or updates the file with more info
         delete from VRI_EXEMPTION_LIMIT_STAGING where DESK_ID=v_DESK_ID and ASOFDATE=v_aux and LIMIT_METHOD = v_LIMIT_METHOD AND LIMIT_RISK_CLASS = v_LIMIT_RISK_CLASS
                                                  and LIMIT_ASSET_ALL = v_LIMIT_ASSET_ALL and id != idUpload;
					select count(1) into v_keys
					from  VRI_EXEMPTION_LIMIT_STAGING VRI
					WHERE --VRI.ID = idUpload AND
						 v_DESK_NAME = VRI.DESK_NAME
						AND v_DESK_ID = VRI.DESK_ID
						AND v_aux = VRI.ASOFDATE
						AND v_LIMIT_METHOD = VRI.LIMIT_METHOD
						AND v_LIMIT_RISK_CLASS = VRI.LIMIT_RISK_CLASS
						AND v_LIMIT_ASSET_ALL = LIMIT_ASSET_ALL;
					IF v_keys > 0 THEN
					--ERROR, update REF_DATA_UI_UPLOAD, update row message
						validation_code := 2001;
						n_errors := n_errors+1;
						v_msg := v_msg || PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(validation_code,'FUNLIM02',message_table);
					END IF;
					v_keys:=0;
					

					v_aux := '';
					v_aux_mon := '';
					v_keys:=0;
          v_ASOFDATE_aux:= UPPER(v_ASOFDATE);     
					--* FUNLIM05 � If DESK_ID is not present in vri_desk_mapping configuration table for that MMM-YYYY, record will be rejected.
						SELECT count(DESK_ID) into v_aux
						FROM vri_desk_mapping
           WHERE  desk_id = v_DESK_ID AND TO_CHAR(ASOFDATE,'MM-YYYY') = TO_CHAR(To_DATE(v_ASOFDATE_aux,'dd/mm/yyyy'),'MM-YYYY');
						IF v_aux = 0 then
							n_errors := n_errors +1;
							validation_code := 2004;
							v_msg := v_msg || PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(validation_code,'FUNLIM05',message_table);
						end IF;
					v_keys:=0;
					v_aux := '';
	

						--Update intermediary row with error
						IF n_errors > 0 THEN

							Update VRI_EXEMP_LIMIT_INTERMEDIARY set VALIDATION_MESSAGE = v_msg 
							WHERE UPLOAD_ID = idUpload
								AND DESK_NAME = v_DESK_NAME
								AND DESK_ID =  v_DESK_ID
								AND ASOFDATE = v_ASOFDATE
								AND LIMIT_METHOD = v_LIMIT_METHOD
								AND LIMIT_RISK_CLASS = v_LIMIT_RISK_CLASS
								AND LIMIT_ASSET_ALL = v_LIMIT_ASSET_ALL
                AND LIMIT_SIZE = v_LIMIT_SIZE
                AND LIMIT_EXPOSURE = v_LIMIT_EXPOSURE
                AND LIMIT_USAGE = v_LIMIT_USAGE
                AND unit_measure = v_unit_measure;
							COMMIT;            
						 END IF;
        --Create csv with errors Desk Name	Desk Id	Reporting Month	As of Date	Limit Method	Limit Risk Class	Limit Asset All	Limit Size	Limit Exposure	Limit Usage	Unit Measure
         
        p_value:=v_DESK_NAME|| ',' ||v_DESK_ID|| ',' || v_ASOFDATE|| ',' ||v_LIMIT_METHOD|| ',' ||v_LIMIT_RISK_CLASS|| ',' ||v_LIMIT_ASSET_ALL|| ',' ||v_LIMIT_SIZE|| ',' ||v_LIMIT_EXPOSURE|| ',' ||v_LIMIT_USAGE|| ',' || v_unit_measure;
        
        v_csv := v_csv || p_value || ', ' || v_msg || CHR(10);
        v_msg :='';
        -------
        END LOOP;
        --If errors then update file status to invalid
		
			IF n_errors > 0 THEN
				-- Upload REF_DATA_UI_UPLOAD. The status 3 --> UPLOADED INVALID
				UPDATE REF_DATA_UI_UPLOAD SET REF_DATA_UI_UPLOAD.status_id=3,
				csv        = v_csv,
				rejected_by       = 'admin_pl',
				rejected_on       = SYSDATE
				WHERE REF_DATA_UI_UPLOAD.ID = idUpload;
				COMMIT;
			END IF;   
	
		       
      --If there are not errors, check format errors (status in ui_upload)
      
      select count(1) into f_errors FROM REF_DATA_UI_UPLOAD where status_id = 3 and REF_DATA_UI_UPLOAD.ID = idUpload; 
      select message into p_message_ok from REF_DATA_VALIDATION_LOOKUP where return_code=100;  
      select count(1) into f_warnings FROM VRI_EXEMP_LIMIT_INTERMEDIARY where UPLOAD_ID = idUpload AND (VALIDATION_MESSAGE = 'Warning: LIMIT_USAGE Calculation: Value of Usage/Limit Size * 100 : limit_usage ; ');      
      select count(1) into f_warnings_2 FROM VRI_EXEMP_LIMIT_INTERMEDIARY where UPLOAD_ID = idUpload AND (VALIDATION_MESSAGE = 'Warning - as of date is part of a weekend - row will be ignored and not exported to the Regulators : FUNLIM04 ; ');      
      select count(1) into f_warnings_3 FROM VRI_EXEMP_LIMIT_INTERMEDIARY where UPLOAD_ID = idUpload AND (VALIDATION_MESSAGE = 'Warning - as of date is part of a weekend - row will be ignored and not exported to the Regulators : FUNLIM04 ; Warning: LIMIT_USAGE Calculation: Value of Usage/Limit Size * 100 : limit_usage ; ');
      SELECT COUNT(1) into f_warnings_4 FROM VRI_EXEMP_LIMIT_INTERMEDIARY where UPLOAD_ID = idUpload AND (VALIDATION_MESSAGE = 'Warning: DESK_ID and LIMIT_METHOD exist in RD_DESK_LIMIT_CONFIG : desk_id,limit_method ; ');
      select count(1) into f_errors_message FROM VRI_EXEMP_LIMIT_INTERMEDIARY where UPLOAD_ID = idUpload AND (VALIDATION_MESSAGE <> p_message_ok || ' :  ; ' AND VALIDATION_MESSAGE <> 'Warning: LIMIT_USAGE Calculation: Value of Usage/Limit Size * 100 : limit_usage ; ' 
            AND VALIDATION_MESSAGE <> 'Warning - as of date is part of a weekend - row will be ignored and not exported to the Regulators : FUNLIM04 ; '
            AND VALIDATION_MESSAGE <> 'Warning - as of date is part of a weekend - row will be ignored and not exported to the Regulators : FUNLIM04 ; Warning: LIMIT_USAGE Calculation: Value of Usage/Limit Size * 100 : limit_usage ; '
            AND VALIDATION_MESSAGE <> 'Warning: DESK_ID and LIMIT_METHOD exist in RD_DESK_LIMIT_CONFIG : desk_id,limit_method ; '); 
         IF n_errors = 0 AND f_errors=0 AND f_errors_message=0 THEN
            --Insert into VRI_EXEMPTION_LIMIT_STAGING
            INSERT INTO VRI_EXEMPTION_LIMIT_STAGING (ID,DESK_NAME,DESK_ID,REPORTING_MONTH,ASOFDATE,LIMIT_METHOD,LIMIT_RISK_CLASS,LIMIT_ASSET_ALL,LIMIT_SIZE,LIMIT_EXPOSURE,LIMIT_USAGE,UNIT_MEASURE,STATUS)
            --VALUES (idUpload,v_DESK_NAME,v_DESK_ID,TO_DATE(v_REPORTING_MONTH,'MON-YY'),TO_DATE(v_ASOFDATE,'DD-MON-YY'),v_LIMIT_METHOD,v_LIMIT_RISK_CLASS,v_LIMIT_ASSET_ALL,v_LIMIT_SIZE,v_LIMIT_EXPOSURE,v_LIMIT_USAGE,v_unit_measure,ready_for_submission);
			SELECT UPLOAD_ID,DESK_NAME,
					DESK_ID,
					TO_DATE(REPORTING_MONTH,'MM-YY'),
					TO_DATE(ASOFDATE,'DD/MM/YYYY'),
					LIMIT_METHOD,
					LIMIT_RISK_CLASS,
					LIMIT_ASSET_ALL,
					LIMIT_SIZE,
					LIMIT_EXPOSURE,
					LIMIT_USAGE,
					UNIT_MEASURE,
					ready_for_submission
			FROM VRI_EXEMP_LIMIT_INTERMEDIARY
			WHERE UPLOAD_ID = idUpload;
           COMMIT;
		   --Upload REF_DATA_UI_UPLOAD. The status 2 --> VALID - PENDING APPROVAL
       IF (f_warnings != 0 or f_warnings_2 != 0 or f_warnings_3 !=0 or f_warnings_4 !=0) THEN
          UPDATE REF_DATA_UI_UPLOAD SET REF_DATA_UI_UPLOAD.status_id=9, comments = '' WHERE REF_DATA_UI_UPLOAD.ID = idUpload;
              --COMMIT;             
              select uploaded_by into v_idUser from ref_data_ui_upload where id = idUpload;
              P_ACCEPT_UPLOAD(idUpload, v_idUser, 'w');
       ELSE
          UPDATE REF_DATA_UI_UPLOAD SET REF_DATA_UI_UPLOAD.status_id=2, comments = '' WHERE REF_DATA_UI_UPLOAD.ID = idUpload;
              --COMMIT; 
              select uploaded_by into v_idUser from ref_data_ui_upload where id = idUpload;
              P_ACCEPT_UPLOAD(idUpload, v_idUser, '');
       END IF;
       END IF;
		
    
END P_CHECK_FUNCIONAL_LIMITS;


PROCEDURE P_ACCEPT_UPLOAD (IDUPLOAD IN NUMBER, IDUSER IN VARCHAR, COMMENTS IN VARCHAR)
IS
  v_uploaded_by REF_DATA_UI_UPLOAD.UPLOADED_BY%TYPE;
  v_status_id REF_DATA_UI_UPLOAD.STATUS_ID%TYPE;
  v_staging_row VRI_EXEMPTION_LIMIT_STAGING%ROWTYPE;
  status NUMBER;
  
  found_staging NUMBER:=0;
  found_upload NUMBER:=0;
  found_desk_limit  NUMBER:=0;
  CURSOR c_staging_rows IS
      SELECT * FROM VRI_EXEMPTION_LIMIT_STAGING WHERE id = IDUPLOAD;
BEGIN
  SELECT STATUS_ID INTO v_status_id
      FROM REF_DATA_UI_UPLOAD
     WHERE id = idupload;

  SELECT count(*) INTO found_upload
      FROM REF_DATA_UI_UPLOAD
     WHERE id = idupload;
  
  IF found_upload > 0 THEN
    SELECT uploaded_by INTO v_uploaded_by
      FROM REF_DATA_UI_UPLOAD
     WHERE id = idupload;
     
    IF v_status_id <> 2 and v_status_id <> 9 THEN --status must be "VALID - PENDING APPROVAL" or with warnings
      ROLLBACK;
      raise_application_error(-20001, 'The status file is not ready for approval/reject');
    ELSE

      SELECT count(*) INTO found_staging
        FROM VRI_EXEMPTION_LIMIT_STAGING
        WHERE id = IDUPLOAD;
      
      IF found_staging > 0 THEN
       
        FOR v_staging_row in c_staging_rows
        LOOP
          
          IF v_staging_row.id IS NOT NULL AND v_staging_row.id = IDUPLOAD THEN
            --if desk_id and limit_method are in RD_DESK_LIMIT_CONFIG table, reject with warnings
            SELECT COUNT(1) into found_desk_limit FROM VRI_EXEMP_LIMIT_INTERMEDIARY 
			where UPLOAD_ID = idUpload and desk_id = v_staging_row.DESK_ID and limit_method = v_staging_row.LIMIT_METHOD and limit_risk_class = v_staging_row.LIMIT_RISK_CLASS
			AND (VALIDATION_MESSAGE = 'Warning: DESK_ID and LIMIT_METHOD exist in RD_DESK_LIMIT_CONFIG : desk_id,limit_method ; ');

            IF (found_desk_limit = 0) THEN
              -- Delete from FINAL table the contents of the file. We need to do this in case the user uploads the same file or updates the file with more info
              delete from VRI_EXEMPTION_LIMIT where DESK_ID = v_staging_row.DESK_ID and asofdate =v_staging_row.ASOFDATE and limit_method = v_staging_row.LIMIT_METHOD 
              and limit_risk_class = v_staging_row.LIMIT_RISK_CLASS and limit_asset_all = v_staging_row.LIMIT_ASSET_ALL;  
            
              INSERT INTO VRI_EXEMPTION_LIMIT
              VALUES(v_staging_row.DESK_NAME,
                 v_staging_row.DESK_ID,
                 v_staging_row.REPORTING_MONTH,
                 v_staging_row.ASOFDATE,
                 v_staging_row.LIMIT_METHOD,
                 v_staging_row.LIMIT_RISK_CLASS,
                 v_staging_row.LIMIT_ASSET_ALL,
                 v_staging_row.LIMIT_SIZE,
                 v_staging_row.LIMIT_EXPOSURE,
                 v_staging_row.LIMIT_USAGE,
                 v_staging_row.UNIT_MEASURE);
          
              UPDATE VRI_EXEMPTION_LIMIT_STAGING
              SET STATUS = 2 -- Set status to SUBMITTED
              WHERE id = IDUPLOAD;
            
              -- warnings?
              if (COMMENTS = 'w') THEN
              status := 10;
              else
              status := 5;
              end if;
              UPDATE REF_DATA_UI_UPLOAD
              SET status_id = status, --Set status to USER APPROVED or APRROVED WITH WARNINGS
                approved_by = IDUSER, 
                approved_on = systimestamp
              WHERE id = IDUPLOAD;
            END IF;
          END IF; 
        END LOOP;
      END IF;
    END IF;
  END IF;
END P_ACCEPT_UPLOAD;

PROCEDURE P_REJECT_UPLOAD (IDUPLOAD IN NUMBER, IDUSER IN VARCHAR, COMMENTS IN VARCHAR)
IS
  v_uploaded_by REF_DATA_UI_UPLOAD.UPLOADED_BY%TYPE;
  v_status_id REF_DATA_UI_UPLOAD.STATUS_ID%TYPE;
  v_staging_row VRI_EXEMPTION_LIMIT_STAGING%ROWTYPE;
  p_comments VARCHAR(4000) := COMMENTS;
  
  found_staging NUMBER:=0;
  found_upload NUMBER:=0;
BEGIN
  SELECT count(*) INTO found_upload
        FROM REF_DATA_UI_UPLOAD
       WHERE id = idupload;
  IF found_upload > 0 THEN
      SELECT uploaded_by INTO v_uploaded_by
        FROM REF_DATA_UI_UPLOAD
       WHERE id = idupload;
       
      IF v_uploaded_by = IDUSER THEN
        ROLLBACK;
        raise_application_error(-20001, 'Users cannot change status of their own uploaded files.');
      ELSIF v_status_id <> 2 and v_status_id <> 9 THEN --status must be "VALID - PENDING APPROVAL" or with warnings
        ROLLBACK;
        raise_application_error(-20001, 'The status file is not ready for approval/reject');
      ELSE
        
        UPDATE REF_DATA_UI_UPLOAD
        SET status_id = 6, --User rejected
            rejected_by = IDUSER, 
            rejected_on = systimestamp,
            comments = p_comments
        WHERE id = IDUPLOAD;
        
        delete from VRI_EXEMPTION_LIMIT_STAGING where id = IDUPLOAD;
           
      END IF;
  END IF;
  
END P_REJECT_UPLOAD;

END PKG_VRI_LIMIT;


--End Jira:GBSVR-30352 Developer: beldraq Date:06/09/2016
