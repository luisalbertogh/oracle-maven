--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_BODY_PKG_NON_VTD_PROCESS runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE stripComments:FALSE


CREATE OR REPLACE PACKAGE BODY PKG_NON_VTD_PROCESS AS




-- **********************************************************************************************************************************
-- Function: f_is_number
-- **********************************************************************************************************************************



function f_is_number(str in varchar2) return BOOLEAN
is
dummy  number;

begin

  dummy := TO_NUMBER(str);
  return TRUE;

  EXCEPTION WHEN OTHERS then
  return FALSE;

end f_is_number;




-- **********************************************************************************************************************************
-- Function: f_get_validation_msg
-- **********************************************************************************************************************************



function f_get_validation_msg(
    code IN NUMBER )
  RETURN VARCHAR2
IS
  return_msg VARCHAR2(1000);
begin

  select  message
  into    return_msg
  from    non_vtd_validation_lookup
  where   non_vtd_validation_lookup.return_code = code;

  return  return_msg || ' ';

EXCEPTION
WHEN NO_DATA_FOUND THEN
  RETURN '-1';

end f_get_validation_msg;




-- **********************************************************************************************************************************
-- Function: f_is_empty_record
-- **********************************************************************************************************************************


function f_is_empty_record (p_excel_record IN csv_table_array, p_total_columns IN NUMBER) RETURN BOOLEAN
IS
  v_is_empty_record BOOLEAN := TRUE;
  v_cell_value VARCHAR2(4000);
  v_is_empty_cell BOOLEAN := TRUE;
  v_cell_value_size NUMBER;
  v_count_empty  NUMBER :=0;
begin
 FOR i in 1 .. p_total_columns LOOP
  	v_is_empty_cell:= TRUE;
    v_cell_value := pkg_bh_process.f_format_column_csv(p_excel_record(i));
    if v_cell_value IS NOT NULL THEN
      v_cell_value_size := LENGTH(v_cell_value);
      FOR j in 1 .. v_cell_value_size LOOP
        if ASCII(substr(v_cell_value,j,j+1)) NOT IN (9, 10, 13, 32) THEN --Tabulator, new line, carriage return, blank space
          v_is_empty_cell := FALSE;
          EXIT;
        end if;
      end LOOP;
    else
    	v_is_empty_cell := TRUE;
    end if;

    v_is_empty_record := (v_is_empty_record and v_is_empty_cell );

    if not v_is_empty_record THEN
      EXIT;
    end if;
  end LOOP;
  RETURN v_is_empty_record;

end f_is_empty_record;




-- **********************************************************************************************************************************
-- Procedure: p_split_csv_process
-- **********************************************************************************************************************************


procedure p_split_csv_process (idUpload IN NUMBER)
IS
  csv                           REF_DATA_UI_UPLOAD.csv%TYPE;
  csv_line                      LONG;
  csv_line_out                  CLOB;
  csv_line_error                CLOB;
  csv_result                    CLOB;
  csv_field                     LONG;
  cont_x                        INTEGER;
  cont_y                        INTEGER;
  total_rows                    INTEGER;
  v_non_vtd_intermediary        non_vtd_intermediary%ROWTYPE;

  CURSOR c1 IS select COLUMN_VALUE from TABLE (PKG_BH_PROCESS.f_convert_rows (csv));                -- *** DM: Cursor around all rows
  CURSOR c2 IS select COLUMN_VALUE from TABLE (PKG_BH_PROCESS.f_convert_row (csv_line, ','));       -- *** DM: Cursor around all elements in a single row

  td_tc			                    VARCHAR2(500);
  division                      VARCHAR2(500);
  product_volcker_forum         VARCHAR2(500);
  business                      VARCHAR2(500);
  ubr_label                     VARCHAR2(500);
  ubr_id                        VARCHAR2(500);
  ubr_or_desk                   VARCHAR2(500);
  ubr_level                     VARCHAR2(500);
  for_strats                    VARCHAR2(500);
  excluded_control              VARCHAR2(500);
  excluded_control_detail       VARCHAR2(500);
  nvtd_rpl_code                 VARCHAR2(500);
  rpl_label                     VARCHAR2(500);
  source_systems                VARCHAR2(500);
  tbs                           VARCHAR2(500);
  bbs                           VARCHAR2(500);
  comments                      VARCHAR2(1000);
  item_type                     VARCHAR2(500);
  vpath                         VARCHAR2(500);
  exclusion                     VARCHAR2(500);
  asofdate                      DATE;
  csv_table                     csv_table_type;
  validation_msg                VARCHAR2 (4000);
  aux_validation_msg            VARCHAR2 (4000);
  validation_code               NUMBER;
  b_validation                  BOOLEAN;
  cont_error_lines              INTEGER := 0; --Number of lines with errores
  cont_error                    INTEGER; --Number of errors of a single line
  aux_cont_error                INTEGER; --Number of errors for validation in PKHG_BH_COMMONS.P_VALIDATION_LOAD_APPROVE
  status                        NUMBER;
  validation_result             VARCHAR2(10);
  upload_error                  VARCHAR2(4000);
  c_error_log                   CLOB;
  total_colum_valid             CONSTANT INTEGER := 20;
  pos_validation_msg            CONSTANT INTEGER := 21;
  pos_validation_result         CONSTANT INTEGER := 22;
  total_colum                   CONSTANT INTEGER := 22;
  b_csv_true                    BOOLEAN;
  C_SIZE_TD_TC                  CONSTANT INTEGER:=100;
  C_SIZE_TD_TC_ERROR            CONSTANT INTEGER:=200;
  C_SIZE_DIVISION               CONSTANT INTEGER:=100;
  C_SIZE_DIVISION_ERROR         CONSTANT INTEGER:=200;
  C_SIZE_PRODUCT_VOLCKER_FORUM  CONSTANT INTEGER:=100;
  C_SIZE_PROD_VOLCKER_FORUM_ERR CONSTANT INTEGER:=200;
  C_SIZE_UBR_LABEL              CONSTANT INTEGER:=100;
  C_SIZE_UBR_LABEL_ERROR        CONSTANT INTEGER:=200;
  C_SIZE_UBR_ID                 CONSTANT INTEGER:=100;
  C_SIZE_UBR_ID_ERROR           CONSTANT INTEGER:=200;
  C_SIZE_UBR_OR_DESK            CONSTANT INTEGER:=100;
  C_SIZE_UBR_OR_DESK_ERROR      CONSTANT INTEGER:=200;
  C_SIZE_UBR_LEVEL              CONSTANT INTEGER:=100;
  C_SIZE_UBR_LEVEL_ERROR        CONSTANT INTEGER:=200;
  C_SIZE_FOR_STRATS             CONSTANT INTEGER:=100;
  C_SIZE_FOR_STRATS_ERROR       CONSTANT INTEGER:=200;
  -- GBSVR-32773: Start 1:
  C_SIZE_EXCLUDED_CONTROL       CONSTANT INTEGER:=50;
  -- GBSVR-32773: End 1:
  C_SIZE_EXCLUDED_CONTROL_ERROR CONSTANT INTEGER:=200;
  C_SIZE_EXC_CON_DETAIL         CONSTANT INTEGER:=100;
  C_SIZE_EXC_CON_DETAIL_ERROR   CONSTANT INTEGER:=200;
  C_SIZE_NVTD_RPL_CODE          CONSTANT INTEGER:=100;
  C_SIZE_NVTD_RPL_CODE_ERROR    CONSTANT INTEGER:=200;
  C_SIZE_RPL_LABEL              CONSTANT INTEGER:=100;
  C_SIZE_RPL_LABEL_ERROR        CONSTANT INTEGER:=200;
  C_SIZE_SOURCE_SYSTEMS         CONSTANT INTEGER:=300;
  C_SIZE_SOURCE_SYSTEMS_ERROR   CONSTANT INTEGER:=400;
  C_SIZE_TBS                    CONSTANT INTEGER:=100;
  C_SIZE_TBS_ERROR              CONSTANT INTEGER:=200;
  C_SIZE_BBS                    CONSTANT INTEGER:=100;
  C_SIZE_BBS_ERROR              CONSTANT INTEGER:=200;
  C_SIZE_COMMENTS               CONSTANT INTEGER:=500;
  C_SIZE_COMMENTS_ERROR         CONSTANT INTEGER:=600;
  C_SIZE_ITEM_TYPE              CONSTANT INTEGER:=100;
  C_SIZE_ITEM_TYPE_ERROR        CONSTANT INTEGER:=200;
  C_SIZE_PATH                   CONSTANT INTEGER:=100;
  C_SIZE_PATH_ERROR             CONSTANT INTEGER:=200;
  C_SIZE_BUSINESS               CONSTANT INTEGER:=100;
  C_SIZE_BUSINESS_ERROR         CONSTANT INTEGER:=200;
  C_SIZE_EXCLUSION              CONSTANT INTEGER:=100;
  C_SIZE_EXCLUSION_ERROR        CONSTANT INTEGER:=200;


  field_value                   VARCHAR2 (4000);
  PROCESS_UPLOAD                CONSTANT INT := 1;
  C_EMPTY_LINE                  CONSTANT VARCHAR2(20) := '[EMPTY_LINE]';
  cont_empty                    INTEGER := 0;
  l_clob							          CLOB;
  FIELDS_LESS_THAN_EXPECTED     EXCEPTION;
  v_validation_message          VARCHAR2(4000);
  v_output_line_csv             clob;
  v_status_processing_validation CONSTANT INTEGER := 11;
  debug_flag                    INTEGER := 0;
  MORE_FIELDS_THAN_EXPECTED		EXCEPTION;

BEGIN

   if ( debug_flag != 0 ) then
      DBMS_OUTPUT.put_line( 'DEBUG 1: idUpload: ' || idUpload);
   end if;

   if PKG_BH_PROCESS.P_PROCESS_ETL_STATUS(IDUPLOAD, 1) = FALSE THEN
     RETURN;
   end if;

   update REF_DATA_UI_UPLOAD
      set STATUS_ID = v_status_processing_validation,
          COMMENTS = 'Processing validation'
   where ID = IDUPLOAD;
   if SQL%ROWCOUNT = 0 THEN
      RAISE NO_DATA_FOUND;
   end if;
   COMMIT;


   -- control csv when the csv row has bad format
   b_csv_true := TRUE;
   cont_error := 0;
   csv_table := csv_table_type ();

   select csv INTO csv
     from REF_DATA_UI_UPLOAD REF_DATA_UI_UPLOAD
    where REF_DATA_UI_UPLOAD.id = idUpload;

   if ( debug_flag != 0 ) then
    DBMS_OUTPUT.put_line( 'DEBUG 2: csv: ' || csv);
   end if;

   -- count number of rows
   cont_x := 0;

   --1 read record and split the clob in rows within a bidimensional array
   -- 1.1- inizialitation number of lines
   l_clob := csv||CHR(10);

   select length(l_clob) - length (replace(l_clob,CHR(10))) into cont_x from dual;
   --DBMS_OUTPUT.put_line( 'number of rows: '|| cont_x);
   csv_table.EXTEND (cont_x);

   cont_x := 0;

   OPEN c1;

   --1.2 read record and split the clob in rows within a bidimensional array
   LOOP
      FETCH c1 INTO csv_line;

      EXIT WHEN c1%NOTFOUND;

      if ( debug_flag != 0 ) then
        DBMS_OUTPUT.put_line( 'DEBUG 3: csv_line: ' || csv_line);
      end if;

      cont_y := 0;
      cont_x := cont_x + 1;
      -- init 20 fields (20 template + 1 validation message + 1 validation result)
      csv_table(cont_x) := csv_table_array (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL );

      OPEN c2;

      LOOP
         FETCH c2 INTO csv_field;

         EXIT WHEN c2%NOTFOUND;
         cont_y := cont_y + 1;

         if ( debug_flag != 0 ) then
          DBMS_OUTPUT.put_line( 'DEBUG 4: csv_field: ' || csv_field);
         end if;

         --If input excel file has 20 or more columns, force raising error (csv_table_array has 20 positions and won't be raised till number of columns >= 21)
         if cont_y > total_colum_valid
         THEN
            RAISE MORE_FIELDS_THAN_EXPECTED;
         end if;

         csv_table(cont_x)(cont_y) := csv_field;
      end LOOP;


      if ( debug_flag != 0 ) then
        DBMS_OUTPUT.put_line( 'DEBUG 5: cont_y: ' || cont_y);
        DBMS_OUTPUT.put_line( 'DEBUG 5: total_colum_valid: ' || total_colum_valid);
      end if;



      -- validate template should have 20 fields
      if cont_y != total_colum_valid
      THEN
        RAISE FIELDS_LESS_THAN_EXPECTED;
       else
         csv_table(cont_x)(pos_validation_result) := 'OK';
      end if;

      CLOSE c2;
   END LOOP;

   CLOSE c1;

   total_rows := cont_x;

    if ( debug_flag != 0 ) then
      DBMS_OUTPUT.put_line( 'DEBUG 6: total_rows: ' || total_rows);
    end if;



   FOR i IN 1 .. total_rows
   LOOP
      validation_msg := NULL;
      validation_code := NULL;
      cont_error := 0;
      b_csv_true := TRUE;
      --2 validate process
      -- validate format template is OK
      validation_result := csv_table(i)(pos_validation_result);

      if ( debug_flag != 0 ) then
        DBMS_OUTPUT.put_line( 'TD_TC: '|| csv_table(i)(1));
        DBMS_OUTPUT.put_line( 'DIVISION: '|| csv_table(i)(2));
        DBMS_OUTPUT.put_line( 'PRODUCT_VOLCKER_FORUM: ' || csv_table(i)(3));
        DBMS_OUTPUT.put_line( 'BUSINESS: ' || csv_table(i)(4));
        DBMS_OUTPUT.put_line( 'UBR_LABEL: ' || csv_table(i)(5));
        DBMS_OUTPUT.put_line( 'UBR_ID: ' || csv_table(i)(6));
        DBMS_OUTPUT.put_line( 'UBR_OR_DESK: ' || csv_table(i)(7));
        DBMS_OUTPUT.put_line( 'UBR_LEVEL: ' || csv_table(i)(8));
        DBMS_OUTPUT.put_line( 'FOR_STRATS: ' || csv_table(i)(9));
        DBMS_OUTPUT.put_line( 'EXCLUDED_CONTROL: ' || csv_table(i)(10));
        DBMS_OUTPUT.put_line( 'EXCLUDED_CONTROL_DETAIL: ' || csv_table(i)(11));
        DBMS_OUTPUT.put_line( 'NVTD_RPL_CODE: ' || csv_table(i)(12));
        DBMS_OUTPUT.put_line( 'EXCLUSION: ' || csv_table(i)(13));
        DBMS_OUTPUT.put_line( 'RPL_LABEL: ' || csv_table(i)(14));
        DBMS_OUTPUT.put_line( 'SOURCE_SYSTEMS: ' || csv_table(i)(15));
        DBMS_OUTPUT.put_line( 'TBS: ' || csv_table(i)(16));
        DBMS_OUTPUT.put_line( 'BBS: ' || csv_table(i)(17));
        DBMS_OUTPUT.put_line( 'COMMENTS: ' || csv_table(i)(18));
        DBMS_OUTPUT.put_line( 'ITEM_TYPE: ' || csv_table(i)(19));
        DBMS_OUTPUT.put_line( 'PATH: ' || csv_table(i)(20));
      end if;

      --Mark as discarded and continue if it is an empty record
      if f_is_empty_record(csv_table(i), total_colum_valid) THEN
        csv_table(i)(pos_validation_msg) := C_EMPTY_LINE;
        cont_empty := cont_empty + 1;
        CONTINUE;
      end if;



-- **********************************************************************************************************************************
-- INITIAL VALIDATION: Validate the length of each column

    BEGIN

    -- Column 1: td_tc
    field_value:=  pkg_bh_process.f_format_column_csv(csv_table(i)(1));
    if  field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_TD_TC THEN
        if   LENGTH( field_value ) > C_SIZE_TD_TC_ERROR THEN
          td_tc := SUBSTR(field_value, 1 , C_SIZE_TD_TC_ERROR);
        else
          td_tc := field_value;
        end if;
        validation_code:= 110;
        validation_msg := validation_msg || f_get_validation_msg(validation_code);
        cont_error     := cont_error + 1;
        b_csv_true := false;
    else
        td_tc := field_value;
    end if;

    -- Column 2: division
    field_value:= pkg_bh_process.f_format_column_csv(csv_table(i)(2));
    if  field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_DIVISION THEN
        if   LENGTH( field_value ) > C_SIZE_DIVISION_ERROR THEN
          division := SUBSTR(field_value, 1 , C_SIZE_DIVISION_ERROR);
        else
          division := field_value;
        end if;
        validation_code:= 111;
        validation_msg := validation_msg || f_get_validation_msg(validation_code);
        cont_error     := cont_error + 1;
        b_csv_true := false;
	  else
        division := field_value;
    end if;

    -- Column 3: product_volcker_forum
    field_value := pkg_bh_process.f_format_column_csv(csv_table(i)(3));
    if field_value IS NOT NULL AND LENGTH (field_value) > C_SIZE_PRODUCT_VOLCKER_FORUM THEN
        if field_value IS NOT NULL AND LENGTH (field_value) > C_SIZE_PROD_VOLCKER_FORUM_ERR THEN
          -- length max bh_intermediary tables and so avoid tec. error
          product_volcker_forum := SUBSTR (field_value, 1, C_SIZE_PROD_VOLCKER_FORUM_ERR);
        else
          product_volcker_forum := field_value;
        end if;
        validation_code := 112;
        validation_msg := validation_msg || f_get_validation_msg (validation_code);
        cont_error := cont_error + 1;
        b_csv_true := FALSE;
     else
        product_volcker_forum := field_value;
     end if;

    -- Column 4: business
    field_value := pkg_bh_process.f_format_column_csv(csv_table(i)(4));
    if field_value IS NOT NULL AND LENGTH (field_value) > C_SIZE_BUSINESS THEN
        if field_value IS NOT NULL AND LENGTH (field_value) > C_SIZE_BUSINESS_ERROR THEN
          -- length max bh_intermediary tables and so avoid tec. error
          business := SUBSTR (field_value, 1, C_SIZE_BUSINESS_ERROR);
        else
          business := field_value;
        end if;
        validation_code := 113;
        validation_msg := validation_msg || f_get_validation_msg (validation_code);
        cont_error := cont_error + 1;
        b_csv_true := FALSE;
     else
        business := field_value;
     end if;


    -- Column 5: ubr_label
    field_value:= pkg_bh_process.f_format_column_csv(csv_table(i)(5));
    if  field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_UBR_LABEL THEN
      if   LENGTH( field_value ) > C_SIZE_UBR_LABEL_ERROR THEN
        ubr_label := SUBSTR(field_value, 1 , C_SIZE_UBR_LABEL_ERROR);
      else
		    ubr_label := field_value;
      end if;
      validation_code:= 114;
      validation_msg := validation_msg || f_get_validation_msg(validation_code);
      cont_error     := cont_error + 1;
      b_csv_true := false;
	  else
      ubr_label := field_value;
    end if;


    -- Column 6: ubr_id
    field_value:= pkg_bh_process.f_format_column_csv(csv_table(i)(6));
    if  field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_UBR_ID THEN
      if   LENGTH( field_value ) > C_SIZE_UBR_ID_ERROR THEN
        ubr_id := SUBSTR(field_value, 1 , C_SIZE_UBR_ID_ERROR);
      else
		    ubr_id := field_value;
      end if;
      validation_code:= 115;
      validation_msg := validation_msg || f_get_validation_msg(validation_code);
      cont_error     := cont_error + 1;
      b_csv_true := false;
	  else
      ubr_id := field_value;
    end if;



    -- Column 7: ubr_or_desk
    field_value:= pkg_bh_process.f_format_column_csv(csv_table(i)(7));
    if  field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_UBR_OR_DESK THEN
      if   LENGTH( field_value ) > C_SIZE_UBR_OR_DESK_ERROR THEN
        ubr_or_desk := SUBSTR(field_value, 1 , C_SIZE_UBR_OR_DESK_ERROR);
      else
		    ubr_or_desk := field_value;
      end if;
      validation_code:= 116;
      validation_msg := validation_msg || f_get_validation_msg(validation_code);
      cont_error     := cont_error + 1;
      b_csv_true := false;
	  else
      ubr_or_desk := field_value;
    end if;


    -- Column 8: ubr_level
    field_value:= pkg_bh_process.f_format_column_csv(csv_table(i)(8));
    if  field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_UBR_LEVEL THEN
      if   LENGTH( field_value ) > C_SIZE_UBR_LEVEL_ERROR THEN
        ubr_level := SUBSTR(field_value, 1 , C_SIZE_UBR_LEVEL_ERROR);
      else
		    ubr_level := field_value;
      end if;
      validation_code:= 117;
      validation_msg := validation_msg || f_get_validation_msg(validation_code);
      cont_error     := cont_error + 1;
      b_csv_true := false;
	  else
      ubr_level := field_value;
    end if;


    if ( ( field_value IS NOT NULL ) and ( NOT f_is_number( field_value ) ) ) then
      validation_code:= 128;
      validation_msg := validation_msg || f_get_validation_msg(validation_code);
      cont_error     := cont_error + 1;
      b_csv_true     := false;
    end if;


    -- Column 9: for_strats
    field_value:= pkg_bh_process.f_format_column_csv(csv_table(i)(9));
    if  field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_FOR_STRATS THEN
      if   LENGTH( field_value ) > C_SIZE_FOR_STRATS_ERROR THEN
        for_strats := SUBSTR(field_value, 1 , C_SIZE_FOR_STRATS_ERROR);
      else
		    for_strats := field_value;
      end if;
      validation_code:= 118;
      validation_msg := validation_msg || f_get_validation_msg(validation_code);
      cont_error     := cont_error + 1;
      b_csv_true := false;
	  else
      for_strats := field_value;
    end if;


    -- Column 10: excluded_control
    field_value:= pkg_bh_process.f_format_column_csv(csv_table(i)(10));
    if  field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_EXCLUDED_CONTROL THEN
      if   LENGTH( field_value ) > C_SIZE_EXCLUDED_CONTROL_ERROR THEN
        excluded_control := SUBSTR(field_value, 1 , C_SIZE_EXCLUDED_CONTROL_ERROR);
      else
		    excluded_control := field_value;
      end if;
      validation_code:= 119;
      validation_msg := validation_msg || f_get_validation_msg(validation_code);
      cont_error     := cont_error + 1;
      b_csv_true := false;
	  else
      excluded_control := field_value;
    end if;


    -- Column 11: excluded_control_detail
    field_value:= pkg_bh_process.f_format_column_csv(csv_table(i)(11));
    if  field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_EXC_CON_DETAIL THEN
      if   LENGTH( field_value ) > C_SIZE_EXC_CON_DETAIL_ERROR THEN
        excluded_control_detail := SUBSTR(field_value, 1 , C_SIZE_EXC_CON_DETAIL_ERROR);
      else
		    excluded_control_detail := field_value;
      end if;
      validation_code:= 120;
      validation_msg := validation_msg || f_get_validation_msg(validation_code);
      cont_error     := cont_error + 1;
      b_csv_true := false;
	  else
      excluded_control_detail := field_value;
    end if;


    -- Column 12: nvtd_rpl_code
    field_value:= pkg_bh_process.f_format_column_csv(csv_table(i)(12));
    if  field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_NVTD_RPL_CODE THEN
      if   LENGTH( field_value ) > C_SIZE_NVTD_RPL_CODE_ERROR THEN
        nvtd_rpl_code := SUBSTR(field_value, 1 , C_SIZE_NVTD_RPL_CODE_ERROR);
      else
		    nvtd_rpl_code := field_value;
      end if;
      validation_code:= 121;
      validation_msg := validation_msg || f_get_validation_msg(validation_code);
      cont_error     := cont_error + 1;
      b_csv_true := false;
	  else
      nvtd_rpl_code := field_value;
    end if;





    -- Column 13: exclusion
    field_value:= pkg_bh_process.f_format_column_csv(csv_table(i)(13));
    if  field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_EXCLUSION THEN
      if   LENGTH( field_value ) > C_SIZE_EXCLUSION_ERROR THEN
        exclusion := SUBSTR(field_value, 1 , C_SIZE_EXCLUSION_ERROR);
      else
		    exclusion := field_value;
      end if;
      validation_code:= 122;
      validation_msg := validation_msg || f_get_validation_msg(validation_code);
      cont_error     := cont_error + 1;
      b_csv_true := false;
	  else
      exclusion := field_value;
    end if;



    -- Column 14: rpl_label
    field_value:= pkg_bh_process.f_format_column_csv(csv_table(i)(14));
    if  field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_RPL_LABEL THEN
      if   LENGTH( field_value ) > C_SIZE_RPL_LABEL_ERROR THEN
        rpl_label := SUBSTR(field_value, 1 , C_SIZE_RPL_LABEL_ERROR);
      else
		    rpl_label := field_value;
      end if;
      validation_code:= 123;
      validation_msg := validation_msg || f_get_validation_msg(validation_code);
      cont_error     := cont_error + 1;
      b_csv_true := false;
	  else
      rpl_label := field_value;
    end if;


    -- Column 15: source_systems
    field_value:= pkg_bh_process.f_format_column_csv(csv_table(i)(15));
    if  field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_SOURCE_SYSTEMS THEN
      if   LENGTH( field_value ) > C_SIZE_SOURCE_SYSTEMS_ERROR THEN
        source_systems := SUBSTR(field_value, 1 , C_SIZE_SOURCE_SYSTEMS_ERROR);
      else
		    source_systems := field_value;
      end if;
      validation_code:= 124;
      validation_msg := validation_msg || f_get_validation_msg(validation_code);
      cont_error     := cont_error + 1;
      b_csv_true := false;
	  else
      source_systems := field_value;
    end if;



    -- Column 16: tbs
    field_value:= pkg_bh_process.f_format_column_csv(csv_table(i)(16));
    if  field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_TBS THEN
      if   LENGTH( field_value ) > C_SIZE_TBS_ERROR THEN
        tbs := SUBSTR(field_value, 1 , C_SIZE_TBS_ERROR);
      else
		    tbs := field_value;
      end if;
      validation_code:= 125;
      validation_msg := validation_msg || f_get_validation_msg(validation_code);
      cont_error     := cont_error + 1;
      b_csv_true := false;
	  else
      tbs := field_value;
    end if;


    -- Column 17: bbs
    field_value:= pkg_bh_process.f_format_column_csv(csv_table(i)(17));
    if  field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_BBS THEN
      if   LENGTH( field_value ) > C_SIZE_BBS_ERROR THEN
        bbs := SUBSTR(field_value, 1, C_SIZE_BBS_ERROR);
      else
		    bbs := field_value;
      end if;
      validation_code:= 126;
      validation_msg := validation_msg || f_get_validation_msg(validation_code);
      cont_error     := cont_error + 1;
      b_csv_true := false;
	  else
      bbs := field_value;
    end if;



    -- Column 18: comments
    field_value:= pkg_bh_process.f_format_column_csv(csv_table(i)(18));
    if  field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_COMMENTS THEN
      if   LENGTH( field_value ) > C_SIZE_COMMENTS_ERROR THEN
        comments := SUBSTR(field_value, 1, C_SIZE_COMMENTS_ERROR);
      else
		    comments := field_value;
      end if;
      validation_code:= 127;
      validation_msg := validation_msg || f_get_validation_msg(validation_code);
      cont_error     := cont_error + 1;
      b_csv_true := false;
	  else
      comments := field_value;
    end if;



    -- Column 19: item_type
    field_value:= pkg_bh_process.f_format_column_csv(csv_table(i)(19));
    if  field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_ITEM_TYPE THEN
      if   LENGTH( field_value ) > C_SIZE_ITEM_TYPE_ERROR THEN
        item_type := SUBSTR(field_value, 1 , C_SIZE_ITEM_TYPE_ERROR);
      else
		    item_type := field_value;
      end if;
      validation_code:= 129;
      validation_msg := validation_msg || f_get_validation_msg(validation_code);
      cont_error     := cont_error + 1;
      b_csv_true := false;
	  else
      item_type := field_value;
    end if;


    -- Column 20: path
    field_value:= pkg_bh_process.f_format_column_csv(csv_table(i)(20));
    if  field_value IS NOT NULL AND LENGTH( field_value ) > C_SIZE_PATH THEN
      if   LENGTH( field_value ) > C_SIZE_PATH_ERROR THEN
        vpath := SUBSTR(field_value, 1 , C_SIZE_PATH_ERROR);
      else
		    vpath := field_value;
      end if;
      validation_code:= 130;
      validation_msg := validation_msg || f_get_validation_msg(validation_code);
      cont_error     := cont_error + 1;
      b_csv_true := false;
	  else
      vpath := field_value;
    end if;



    end;
      --- end validation length


    if ( debug_flag != 0 ) then
      DBMS_OUTPUT.put_line( 'DEBUG 7: validation_result: ' || validation_result);
      DBMS_OUTPUT.put_line( 'DEBUG 7: validation_msg: ' || validation_msg);
      DBMS_OUTPUT.put_line( 'DEBUG 7: cont_error: ' || cont_error);
    end if;



-- **********************************************************************************************************************************
-- validation if the action is correct



      ----------------- END VALIDATION CSV--------------------
      if validation_result = 'OK' THEN
         if b_csv_true THEN
            v_non_vtd_intermediary.td_tc := td_tc;
            v_non_vtd_intermediary.division := division;
            v_non_vtd_intermediary.product_volcker_forum := product_volcker_forum;
            v_non_vtd_intermediary.business := business;
            v_non_vtd_intermediary.ubr_label := ubr_label;
            v_non_vtd_intermediary.ubr_id := ubr_id;
            v_non_vtd_intermediary.ubr_or_desk := ubr_or_desk;
            v_non_vtd_intermediary.ubr_level := ubr_level;
            v_non_vtd_intermediary.for_strats := for_strats;
            v_non_vtd_intermediary.excluded_control := excluded_control;
            v_non_vtd_intermediary.excluded_control_detail := excluded_control_detail;
            v_non_vtd_intermediary.nvtd_rpl_code := nvtd_rpl_code;
            v_non_vtd_intermediary.exclusion := exclusion;
            v_non_vtd_intermediary.rpl_label := rpl_label;
            v_non_vtd_intermediary.source_systems := source_systems;
            v_non_vtd_intermediary.tbs := tbs;
            v_non_vtd_intermediary.bbs := bbs;
            v_non_vtd_intermediary.comments := comments;
            v_non_vtd_intermediary.item_type := item_type;
            v_non_vtd_intermediary.path := vpath;

-- DM: Comment out for now:
-- We may need to do some validation but can leave this out until unit testing:
-- Looks like this is not relevant: Validations on action type, book ids, rpl codes, etc - Only relevant to book uploads.
--            PKG_BH_COMMONS.P_VALIDATION_LOAD_APPROVE (v_intermediary_data, PROCESS_UPLOAD, aux_validation_msg, aux_cont_error);

            if aux_cont_error IS NULL THEN
               aux_cont_error := 0;
            end if;

            cont_error := cont_error + aux_cont_error;
            validation_msg := validation_msg || aux_validation_msg;
         end if;

         -----------------------------END VALIDATIONS ------------------------------------------------------
         -- Validation OK
         if cont_error = 0 THEN
            validation_code := 100;
            validation_msg := f_get_validation_msg (validation_code);
            csv_table(i)(pos_validation_result) := 'OK';
         else
            cont_error_lines := cont_error_lines + 1;
            csv_table(i)(pos_validation_result) := 'KO';
         end if;

         -- insert the validation message into to the array
         csv_table(i)(pos_validation_msg) := validation_msg;
      end if; -- end validatate error template


      select to_date(to_char(last_day(current_date) + 1, 'DD-MON-YYYY')) into asofdate from dual;


      insert into non_vtd_intermediary (
          ID,
          UPLOAD_ID,
          CSV_LINE_ID,
          ASOFDATE,
          TD_TC,
          DIVISION,
          PRODUCT_VOLCKER_FORUM,
          BUSINESS,
          UBR_LABEL,
          UBR_ID,
          UBR_OR_DESK,
          UBR_LEVEL,
          FOR_STRATS,
          EXCLUDED_CONTROL,
          EXCLUDED_CONTROL_DETAIL,
          NVTD_RPL_CODE,
          EXCLUSION,
          RPL_LABEL,
          SOURCE_SYSTEMS,
          TBS,
          BBS,
          COMMENTS,
          ITEM_TYPE,
          PATH,
          VALIDATION_MESSAGE )
      values (
          SEQ_NON_VTD_INTERMEDIARY.NEXTVAL,
          idUpload,
          i,
          asofdate,
          td_tc,
          division,
          product_volcker_forum,
          business,
          ubr_label,
          ubr_id,
          ubr_or_desk,
          ubr_level,
          for_strats,
          excluded_control,
          excluded_control_detail,
          nvtd_rpl_code,
          exclusion,
          rpl_label,
          source_systems,
          tbs,
          bbs,
          comments,
          item_type,
          vpath,
          validation_msg );

          if ( debug_flag != 0 ) then
            DBMS_OUTPUT.put_line( 'DEBUG 8: Inserted to non_vtd_intermediary (no commit)');
          end if;


   END LOOP;

   -- 4 Update Upload clob with adding validation message and and status such as from SUBMITTED to VALID / PENDING APPROVAL, or UPLOAD INVALID
   --4.1 build the csv output
   csv_line_out := NULL;
   csv_line_error := NULL;


if ( debug_flag != 0 ) then
  DBMS_OUTPUT.put_line( 'DEBUG 9: total_rows: ' || total_rows);
end if;


 FOR i IN 1 .. total_rows
   LOOP
    select validation_message into v_validation_message from non_vtd_intermediary where UPLOAD_ID = idUpload and CSV_LINE_ID= i;
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
					    || csv_table(i)(18) ||','
              || csv_table(i)(19) ||','
              || csv_table(i)(20) ||','
				      || v_validation_message||'';
      csv_result := csv_result || v_output_line_csv  || chr(13) || chr(10)  ;
   END LOOP;

     --Error: file with no records detected

   if csv_result is null THEN
     cont_error_lines := 1;

     --csv_result cannot be saved as null in ref_data_ui_upload. create dummy record
     csv_result := '';
     FOR pos IN 1 .. total_colum-1 LOOP
       csv_result := csv_result || ',';
     END LOOP;
     csv_result := csv_result || 'ERROR file. The uploaded file has no valid records.';
   end if;

    if ( debug_flag != 0 ) then
      dbms_output.put_line('- TOTAL ERRORS:' || cont_error_lines);
      dbms_output.put_line('- TOTAL EMPTY:' || cont_empty);
      dbms_output.put_line('- TOTAL ROWS:' || total_rows);
   end if;

   if cont_error_lines > 0 THEN

      -- UPLOADED INVALID
      status := 3;
   else
      status := 2;
   -- VALID - PENDING APPROVAL
   end if;


  if ( debug_flag != 0 ) then
    DBMS_OUTPUT.put_line( 'DEBUG 10: cont_error_lines: ' || cont_error_lines);
    DBMS_OUTPUT.put_line( 'DEBUG 10: status: ' || status);
  end if;




   if status = 2 THEN
      update REF_DATA_UI_UPLOAD
         set status_id = status, csv = csv_result
             , comments = null
       where REF_DATA_UI_UPLOAD.id = idUpload;

      COMMIT;
   --Now we have to wait for approval to load in staging
   else
      -- status  UPLOADED INVALID
      update REF_DATA_UI_UPLOAD
         set status_id = status,
             csv = csv_result,
             comments = upload_error,
             error_log = c_error_log,
             rejected_by = 'admin_pl',
             rejected_on = SYSDATE
       where REF_DATA_UI_UPLOAD.id = idUpload;

      COMMIT;
   end if;
EXCEPTION
   WHEN NO_DATA_FOUND THEN
      upload_error := 'IdUpload NOT EXIST. ' || SQLERRM;
	  pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'pkg_non_vtd_process.p_split_csv_process','ERROR', 'FATAL', 'Error NO_DATA_FOUND -> IdUpload NOT EXIST.' , substr(upload_error,1,2500), 'bRDS');
      ROLLBACK;
      --dbms_output.put_line(upload_error);
      RAISE;
   WHEN MORE_FIELDS_THAN_EXPECTED THEN
      status := 3;
      --The uploaded spreadsheet has rows with more columns than allowed
      upload_error := f_get_validation_msg (102);
      c_error_log := SQLERRM;
	  pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'pkg_non_vtd_process.p_split_csv_process','ERROR', 'FATAL', 'Error MORE_FIELDS_THAN_EXPECTED ' || upload_error, substr(c_error_log,1,2500), 'bRDS');
      ROLLBACK;

      -- DBMS_OUTPUT.put_line(upload_error);
      BEGIN
         update REF_DATA_UI_UPLOAD
            set status_id = status,
                comments = upload_error,
                error_log = c_error_log,
                rejected_by = 'admin_pl',
                rejected_on = SYSDATE
          where REF_DATA_UI_UPLOAD.id = idUpload;

         COMMIT;
      EXCEPTION
         WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
      END;
      WHEN FIELDS_LESS_THAN_EXPECTED THEN
      status := 3;
      --The uploaded spreadsheet has rows with less columns than allowed
      upload_error := f_get_validation_msg (101);
      c_error_log := SQLERRM;
	  pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'pkg_non_vtd_process.p_split_csv_process','ERROR', 'FATAL', 'Error FIELDS_LESS_THAN_EXPECTED ' || upload_error, substr(c_error_log,1,2500), 'bRDS');
      ROLLBACK;

      -- DBMS_OUTPUT.put_line(upload_error);
      BEGIN
         update REF_DATA_UI_UPLOAD
            set status_id = status,
                comments = upload_error,
                error_log = c_error_log,
                rejected_by = 'admin_pl',
                rejected_on = SYSDATE
          where REF_DATA_UI_UPLOAD.id = idUpload;

         COMMIT;
      EXCEPTION
         WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
      END;
   WHEN OTHERS
   THEN
      status := 4;
      c_error_log := SQLERRM;
	  pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'pkg_non_vtd_process.p_split_csv_process','ERROR', 'FATAL', 'Error OTHERS', substr(c_error_log,1,2500), 'bRDS');
      ROLLBACK;
      --dbms_output.put_line(c_error_log);

      BEGIN
         -- UPLOAD ERROR, Technical error
         upload_error := f_get_validation_msg (200);

         update REF_DATA_UI_UPLOAD
            set status_id = status,
                comments = upload_error,
                error_log = c_error_log,
                rejected_by = 'admin_pl',
                rejected_on = SYSDATE
          where REF_DATA_UI_UPLOAD.id = idUpload;

         COMMIT;
      EXCEPTION
         WHEN OTHERS
         THEN
            ROLLBACK;
            RAISE;
      END;
END p_split_csv_process;





-- **********************************************************************************************************************************
-- Procedure: p_update_staging
-- **********************************************************************************************************************************



procedure p_update_staging (idUpload IN NUMBER)
as
v_non_vtd_asofdate  varchar2(20);

begin
pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'Non VTD UI','DEBUG', 'LOGGING', 'pkg_non_vtd_process.p_update_staging', 'Step 1 Start', 'bRDS');


-- bRDS book processing:

update  bh_ubr_desk_hierarchy
set     non_vtd = 'N';

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'Non VTD UI','DEBUG', 'LOGGING', 'pkg_non_vtd_process.p_update_staging', 'Step 2', 'bRDS');


delete
from    bh_workflow
where   workflow_type_id = 11;

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'Non VTD UI','DEBUG', 'LOGGING', 'pkg_non_vtd_process.p_update_staging', 'Step 3', 'bRDS');


update  bh_staging_intermed
set     non_vtd_code            = NULL,
        non_vtd_name            = NULL,
        non_vtd_rpl_code        = NULL,
        non_vtd_exclusion_type  = NULL,
        non_vtd_division        = NULL,
        non_vtd_pvf             = NULL,
        non_vtd_business        = NULL;

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'Non VTD UI','DEBUG', 'LOGGING', 'pkg_non_vtd_process.p_update_staging', 'Step 4', 'bRDS');


PKG_BRDS_BH_RPL.p_brds_etl_non_vtd;

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'Non VTD UI','DEBUG', 'LOGGING', 'pkg_non_vtd_process.p_update_staging', 'Step 5', 'bRDS');


PKG_BRDS_BH_RPL.p_brds_etl_apply_deltas;

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'Non VTD UI','DEBUG', 'LOGGING', 'pkg_non_vtd_process.p_update_staging', 'Step 6', 'bRDS');


-- MANUAL book processing:

update  bh_staging
set     non_vtd_code            = NULL,
        non_vtd_name            = NULL,
        -- GBSVR-32773 Start/End 1: Remove reset of non_vtd_rpl_code to NULL
        non_vtd_exclusion_type  = NULL,
        non_vtd_division        = NULL,
        non_vtd_pvf             = NULL,
        non_vtd_business        = NULL
where   data_source = 'MANUAL';

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'Non VTD UI','DEBUG', 'LOGGING', 'pkg_non_vtd_process.p_update_staging', 'Step 7', 'bRDS');



select  max(asofdate)
into    v_non_vtd_asofdate
from    bh_non_vtd
where   asofdate <= (last_day(current_date) + 1);

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'Non VTD UI','DEBUG', 'LOGGING', 'pkg_non_vtd_process.p_update_staging', 'Step 8', 'bRDS');


update bh_staging s
	  set ( non_vtd_code,
          non_vtd_name,
          non_vtd_rpl_code,
          non_vtd_exclusion_type,
          non_vtd_division,
          non_vtd_pvf,
          non_vtd_business ) =  ( select  nvtd.non_vtd_code,
	                                  			nvtd.non_vtd_rpl_name,
	                                  			nvtd.non_vtd_rpl_code,
	                                  			nvtd.non_vtd_exclusion_type,
	                                  			nvtd.non_vtd_division,
                                          nvtd.non_vtd_pvf,
                                          nvtd.non_vtd_business
	                            			from  bh_non_vtd nvtd
	                            			where s.non_vtd_rpl_code = nvtd.non_vtd_rpl_code
	                            			and   nvtd.asofdate = v_non_vtd_asofdate )
where   s.data_source like 'MANUAL'
and     s.active_flag = 'Y';

pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'Non VTD UI','DEBUG', 'LOGGING', 'pkg_non_vtd_process.p_update_staging', 'Step 9 End', 'bRDS');


end p_update_staging;





-- **********************************************************************************************************************************
-- Procedure: p_accept_upload
-- **********************************************************************************************************************************



-- Currently no change from original copied from pkg_bh_process:
procedure p_accept_upload (IDUPLOAD IN NUMBER, IDUSER IN VARCHAR, COMMENTS IN VARCHAR)
IS
  v_uploaded_by REF_DATA_UI_UPLOAD.UPLOADED_BY%TYPE;
  v_status_id REF_DATA_UI_UPLOAD.STATUS_ID%TYPE;
  v_count_active_approvals integer;
  v_status_processing_approval CONSTANT INT := 12;
  v_asofdate date;
BEGIN
  if PKG_BH_PROCESS.P_PROCESS_ETL_STATUS(IDUPLOAD, 2) = FALSE THEN
    RETURN;
  END if;

  --Check that there is not another approval being processed
  select NVL(COUNT(*), 0) INTO v_count_active_approvals
    from REF_DATA_UI_UPLOAD
   where status_id = v_status_processing_approval
   AND   file_type_id in ( 1, 11 );


  if v_count_active_approvals > 0 THEN
    update REF_DATA_UI_UPLOAD
       set comments = 'Another file is currently being approved, please wait some minutes before trying to approve again this file.'
     where id = idupload;
     RETURN;
  END if;

  --Set status to PROCESSING_APPROVAL
  update REF_DATA_UI_UPLOAD
     set STATUS_ID = v_status_processing_approval,
         COMMENTS = 'Processing approval'
  where ID = IDUPLOAD;
  COMMIT;

  select uploaded_by INTO v_uploaded_by
    from REF_DATA_UI_UPLOAD
   where id = idupload;

  if v_uploaded_by = IDUSER THEN
    ROLLBACK;
    pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'pkg_non_vtd_process.p_accept_upload', 'ERROR', 'LOGGING', 'Users cannot change status of their own uploaded files', '', 'bRDS');
  elsif v_status_id <> 2 THEN --status must be "VALID - PENDING APPROVAL"
    ROLLBACK;
    pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'pkg_non_vtd_process.p_accept_upload', 'ERROR', 'LOGGING', 'The status file is not ready for approval/reject', '', 'bRDS');
  else
  -- Code to remove any bh_non_vtd entries for same asofdate; insert new entries from non_vtd_intermediary into bh_nono_vtd; update ref_data_ui_upload:

        -- Calculate the current working asofdate:
        select to_date(to_char(last_day(current_date) + 1, 'DD-MON-YYYY')) into v_asofdate from dual;

        -- Remove any entries in bh_non_vtd for this asofdate:

        delete
        from    bh_non_vtd
        where   asofdate = v_asofdate;


        insert into bh_non_vtd (
                asofdate,
                non_vtd_code,
                non_vtd_type,
                non_vtd_level,
                non_vtd_exclusion_type,
                non_vtd_rpl_code,
                non_vtd_rpl_name,
                non_vtd_division,
                non_vtd_pvf,
                non_vtd_business,
                -- GBSVR-32558: Start 1:
                non_vtd_td_tc,
                non_vtd_ubr_label,
                non_vtd_for_strats,
                non_vtd_exclusion_type_detail,
                non_vtd_exclusion,
                non_vtd_source_systems,
                non_vtd_tbs,
                non_vtd_bbs,
                non_vtd_comments,
                non_vtd_item_type,
                non_vtd_path )
                -- GBSVR-32558: End 1:
        select  v_asofdate,
                ubr_id,
                ubr_or_desk,
                ubr_level,
                excluded_control,
                nvtd_rpl_code,
                rpl_label,
                division,
                product_volcker_forum,
                business,
                -- GBSVR-32558: Start 2:
                td_tc,
                ubr_label,
                for_strats,
                excluded_control_detail,
                exclusion,
                source_systems,
                tbs,
                bbs,
                comments,
                item_type,
                path
                -- GBSVR-32558: End 2:
        from    non_vtd_intermediary
        where   upload_id = IDUPLOAD;


        update  REF_DATA_UI_UPLOAD
           set  status_id = 5,      --Set status to USER APPROVED and comments empty (removing 'Processing approval' message)
                comments = NULL,
                approved_by = IDUSER,
                approved_on = systimestamp
         where id = IDUPLOAD;

         commit;

         PKG_NON_VTD_PROCESS.p_update_staging(IDUPLOAD);


  END if;
EXCEPTION
WHEN OTHERS THEN
	pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'pkg_non_vtd_process.p_accept_upload','ERROR', 'FATAL', 'Error OTHERS', substr(SQLERRM,1,2500), 'bRDS');
  ROLLBACK;
  update REF_DATA_UI_UPLOAD
     set status_id = 7, --Set status to APPROVED INVALID
         rejected_by='admin_pl', rejected_on=SYSDATE,
         comments=pkg_non_vtd_process.f_get_validation_msg(200)
   where id = IDUPLOAD;

  commit;
-- GBSVR-32524: End 13:

  RAISE;
END p_accept_upload;




-- **********************************************************************************************************************************
-- Procedure: p_reject_upload
-- **********************************************************************************************************************************



-- Currently no change from original copied from pkg_bh_process:
procedure p_reject_upload ( P_IDUPLOAD IN NUMBER, P_IDUSER IN VARCHAR, P_COMMENTS IN VARCHAR )
IS
  v_uploaded_by REF_DATA_UI_UPLOAD.UPLOADED_BY%TYPE;
  v_status_id REF_DATA_UI_UPLOAD.STATUS_ID%TYPE;
BEGIN

  select uploaded_by, status_id INTO v_uploaded_by, v_status_id
    from REF_DATA_UI_UPLOAD
   where id = P_IDUPLOAD;

  if v_uploaded_by = P_IDUSER THEN
    ROLLBACK;
    pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'pkg_non_vtd_process.p_reject_upload', 'ERROR', 'LOGGING', 'Users cannot change status of their own uploaded files.', '', 'bRDS');
  elsif v_status_id <> 2 THEN --status must be "VALID - PENDING APPROVAL"
    ROLLBACK;
    pkg_monitoring.pr_insert_log_jobs_qv('bRDS', null, 'bRDS', current_date, 'pkg_non_vtd_process.p_reject_upload', 'ERROR', 'LOGGING', 'The status file is not ready for approval/reject', '', 'bRDS');
  END if;

  update REF_DATA_UI_UPLOAD
     set rejected_by = P_IDUSER, rejected_on = systimestamp,
         status_id = 6, --User rejected
         comments = P_COMMENTS
   where id = P_IDUPLOAD;

EXCEPTION
WHEN OTHERS THEN
	pkg_monitoring.pr_insert_log_jobs_qv('bRDS',null,'bRDS',current_date,'pkg_non_vtd_process.p_reject_upload','ERROR', 'FATAL', 'Error OTHERS', substr(SQLERRM,1,2500), 'bRDS');
  ROLLBACK;
  RAISE;
END p_reject_upload;





END PKG_NON_VTD_PROCESS;