--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_BODY_PKG_COMMON_UTILS runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE stripComments:FALSE


  CREATE OR REPLACE PACKAGE BODY "PKG_COMMON_UTILS" 
AS

FUNCTION F_CONVERT_ROWS(
    p_list IN CLOB)
  RETURN clob_type PIPELINED
AS
  l_string CLOB := p_list || CHR(10);
  l_line_index PLS_INTEGER;
  l_index PLS_INTEGER := 1;
BEGIN
  LOOP
    l_line_index := INSTR(l_string, CHR(10), l_index);    
    EXIT
  WHEN l_line_index = 0;
    PIPE ROW ( SUBSTR(l_string, l_index, l_line_index - l_index) );
    l_index := l_line_index + 1;
  END LOOP;
  RETURN;
END F_CONVERT_ROWS;

--function is_number would return 1 if the value is numeric and 0 if the value is NOT numeric
FUNCTION F_IS_NUMBER (p_string IN VARCHAR2)
   RETURN INT
IS
   v_new_num NUMBER;
BEGIN
   v_new_num := TO_NUMBER(p_string);
   RETURN 1;
EXCEPTION
WHEN VALUE_ERROR THEN
   RETURN 0;
END;

FUNCTION F_CONVERT_ROW(
    p_list IN VARCHAR2,
    token  IN VARCHAR2)
  RETURN varchar_type PIPELINED
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

-- pattern should be MMM-YYYY
FUNCTION F_IS_DATE_OK(
    p_date VARCHAR2,
	p_format VARCHAR2)
  RETURN CHAR
IS
  v_date        DATE;
  v_return_date CHAR := 'Y';
BEGIN
    v_date := TO_DATE (p_date, p_format);
    --DBMS_OUTPUT.put_line('v_date: '|| v_date);
	RETURN v_return_date;
  EXCEPTION
  WHEN OTHERS THEN
    v_return_date :=  'N';  
    RETURN v_return_date;
END F_IS_DATE_OK;

FUNCTION F_FORMAT_COLUMN_CSV(field IN VARCHAR) 
    RETURN  VARCHAR2
AS 
  out_field VARCHAR2(4000);
  C_BH_COMMA CONSTANT VARCHAR2(10):='[BH_COMMA]';
  C_BH_NEW_LINE CONSTANT VARCHAR2(10):='[BH_LF]';
  C_BH_QUOTE CONSTANT VARCHAR2(10):='[BH_QUOTE]';
  C_DOUBLE_QUOTES CONSTANT VARCHAR(16):='[REF_DATA_QUOTE]';
BEGIN
  out_field:=field;
  IF field IS NOT NULL THEN
	  out_field:=REPLACE(out_field,C_BH_COMMA , ',');
	  out_field:=REPLACE(out_field,C_BH_NEW_LINE,' ');
	  out_field:=REPLACE(out_field,C_BH_QUOTE,'"');
    out_field:=REPLACE(out_field,C_DOUBLE_QUOTES);
	  --out_field:=TRIM(out_field);
  END IF;
  RETURN out_field;
END;

          
FUNCTION F_CHECK_DATE_FORMAT(p_checkdate IN VARCHAR2)
    RETURN VARCHAR2 IS RESULT VARCHAR2(10);
  BEGIN
    SELECT CASE
             WHEN regexp_like(p_checkdate, '\d{2}\/\d{2}\/\d{4}\', 'i') THEN
              'Y'
             WHEN regexp_like(p_checkdate, '\d{1}\/\d{1}\/\d{4}\', 'i') THEN
             'Y'
             WHEN regexp_like(p_checkdate, '\d{2}\/\d{1}\/\d{4}\', 'i') THEN
             'Y'
             WHEN regexp_like(p_checkdate, '\d{1}\/\d{2}\/\d{4}\', 'i') THEN
             'Y'
             ELSE
              'N'
           END into RESULT
      FROM dual;
    RETURN(RESULT);
  END F_CHECK_DATE_FORMAT;

----------------------------------------------------------
--return the validation message depending on the code IN
----------------------------------------------------------
FUNCTION F_GET_VALIDATION_MSG(
    code IN NUMBER,
	field IN VARCHAR2,
	ref_table IN VARCHAR2)
  RETURN VARCHAR2
IS
  return_msg VARCHAR2(1000);
  sql_stmt    VARCHAR2(200);
BEGIN
 SELECT MESSAGE INTO return_msg FROM REF_DATA_VALIDATION_LOOKUP WHERE REF_DATA_VALIDATION_LOOKUP.return_code = code;
  RETURN return_msg || ' : ' || field || ' ; ';
EXCEPTION
WHEN NO_DATA_FOUND THEN
  RETURN '-1';
END F_GET_VALIDATION_MSG;


PROCEDURE P_IS_MANDATORY_NULL (field_name IN VARCHAR2,field IN OUT VARCHAR2, cont_error IN OUT NUMBER, validation_msg IN OUT VARCHAR2, ref_table IN VARCHAR2)
IS 
  validation_code varchar2(4);
BEGIN
	IF field    IS NULL THEN
       field    := '?';
       validation_code:= 1200;
       validation_msg := validation_msg || PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(validation_code,field_name,ref_table);
       cont_error     := cont_error + 1;
    END IF;
END P_IS_MANDATORY_NULL;

PROCEDURE P_IS_MANDATORY_BLANK (field_name IN VARCHAR2,field IN OUT VARCHAR2, cont_error IN OUT NUMBER, validation_msg IN OUT VARCHAR2, ref_table IN VARCHAR2)
IS 
  validation_code varchar2(4);
  length_field number;
BEGIN
	IF field IS NULL THEN
       field    := '?';
       validation_code:= 1201;       
       validation_msg := validation_msg || PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(validation_code,field_name,ref_table);       
       cont_error     := cont_error + 1;       
    END IF;
END P_IS_MANDATORY_BLANK;

/************************************************************/
/*CHECK length for each column AND DATE FORMATS				*/
/************************************************************/


PROCEDURE P_CHECK_FORMAT_FIELD(field_name IN VARCHAR2, field_value IN OUT VARCHAR2, C_SIZE_PARAM IN NUMBER, cont_error IN OUT VARCHAR2, validation_msg IN OUT VARCHAR2)
	IS
		C_SIZE_2HUNDRED CONSTANT INTEGER:=200;
		C_SIZE_HUNDRED CONSTANT INTEGER:=100;
		C_SIZE_FIFTY CONSTANT INTEGER:=50;
		C_SIZE_NUMBER CONSTANT INTEGER:=24;
		C_SIZE_MEASURE CONSTANT INTEGER:=25;
		C_SIZE_COMMENTS CONSTANT INTEGER:=300;
    validation_code VARCHAR2(5);
    field_value_aux VARCHAR2(50);
    message_table CONSTANT VARCHAR2(30) := 'CRDS_OVERRIDE_VALID_LOOKUP';
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
                        
            if instr(field_value,'.')=0 and length(field_value)>24 then  
            field_value := SUBSTR(field_value,24);            
						validation_code:= 1126;
						validation_msg :=validation_msg || PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(validation_code,field_name,message_table);
						cont_error     := cont_error + 1;
            else
             field_value_aux:=SUBSTR(field_value,1,instr(field_value,'.'));
             if length(field_value_aux)>24 then
              field_value := SUBSTR(field_value_aux,24);
              validation_code:= 1126;
              validation_msg :=validation_msg || PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(validation_code,field_name,message_table);
              cont_error     := cont_error + 1;
              end if;            
					END IF; 
         END IF; 
				WHEN C_SIZE_PARAM = C_SIZE_MEASURE THEN
					IF field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_MEASURE THEN
						field_value := SUBSTR(field_value, 1 , C_SIZE_MEASURE);
						validation_code:= 1125;
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


END PKG_COMMON_UTILS;
