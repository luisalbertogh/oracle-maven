--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_BODY_PKG_CRDS_OVERRIDE runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE stripComments:FALSE


  CREATE OR REPLACE PACKAGE BODY "PKG_CRDS_OVERRIDE" 
AS
  -- **********************************************************************
  -- Procedure: P_SPLIT_CSV
  -- **********************************************************************
  PROCEDURE P_SPLIT_CSV_CRDS(
      idUpload IN NUMBER)
  IS
    csv CLOB;
    csv_line CLOB;
    csv_line_out CLOB;
    csv_line_error CLOB;
    csv_result CLOB;
    csv_field CLOB;
    cont_x     INTEGER;
    cont_y     INTEGER;
    total_rows INTEGER;
    CURSOR c1
    IS
      SELECT COLUMN_VALUE FROM TABLE(PKG_COMMON_UTILS.f_convert_rows(csv));
    CURSOR c2
    IS
      SELECT COLUMN_VALUE FROM TABLE(PKG_COMMON_UTILS.f_convert_row(csv_line,','));
  TYPE csv_table_array IS varray(13) OF VARCHAR2(4000);-- 11 template + 2 validations
TYPE csv_table_type
IS
  TABLE OF csv_table_array;
  csv_table csv_table_type;
  validation_msg  VARCHAR2(4000);
  validation_code NUMBER;
  b_validation    BOOLEAN;
  cont_error      INTEGER;
  asofdate crds_override_intermediary.asofdate%type;
  source_system_crds crds_override_intermediary.source_system_crds%type;
  counter_party_id crds_override_intermediary.counter_party_id%type;
  counter_party_name crds_override_intermediary.counter_party_name%type;
  volcker_trading_desk crds_override_intermediary.volcker_trading_desk%type;
  book_id crds_override_intermediary.book_id%type;
  classification crds_override_intermediary.classification%type;
  reason crds_override_intermediary.reason%type;
  position_id crds_override_intermediary.position_id%type;
  trade_id crds_override_intermediary.trade_id%type;
  action crds_override_intermediary.action%type;
  aux_date          CHAR;
  validation_result VARCHAR(10);
  status            NUMBER;
  upload_error      VARCHAR(1000);
  c_error_log CLOB;
  total_colum_valid     CONSTANT INTEGER:=11;
  pos_validation_msg    CONSTANT INTEGER:=12;
  pos_validation_result CONSTANT INTEGER:=13;
  total_colum           CONSTANT INTEGER:=13;
  csv_true              BOOLEAN;
  C_SIZE_2HUNDRED       CONSTANT INTEGER     :=200;
  C_SIZE_FIFTY          CONSTANT INTEGER     :=50;
  C_SIZE_COMMENTS       CONSTANT INTEGER     :=300;
  C_TABLE_NAME          CONSTANT VARCHAR2(30):='REF_DATA_VALIDATION_LOOKUP';
  field_value           VARCHAR2(4000);
  field_name            VARCHAR2(4000);
  asofdate_ok           INT:=0;
BEGIN
  -- control csv when the csv row has bad format
  csv_true   := true;
  cont_error :=0;
  csv_table  := csv_table_type();
  -- iniziality with aproach size ????todo
  csv_table.EXTEND(150000);
  asofdate_ok:=0;
  --DBMS_OUTPUT.put_line('Entra');
  SELECT csv
  INTO csv
  FROM ref_data_ui_upload ui_uploaded
  WHERE ui_uploaded.id = idUpload;
  cont_x              :=0;
  OPEN c1;
  --1 read record and split the clob in rows within a bidimensional array
  LOOP
    FETCH c1 INTO csv_line;
    --DBMS_OUTPUT.put_line('csv_line: ' || csv_line);
    EXIT
  WHEN c1%NOTFOUND;
    cont_y :=0;
    cont_x := cont_x + 1;
    -- init 13 fields (11 template + 1 validation message + 1 validation result)
    csv_table(cont_x):=csv_table_array(NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
    OPEN c2;
    LOOP
      FETCH c2 INTO csv_field;
      --DBMS_OUTPUT.put_line('csv_field: ' || csv_field);
      EXIT
    WHEN c2%NOTFOUND;
      cont_y                   := cont_y + 1;
      csv_table(cont_x)(cont_y):=csv_field;
    END LOOP;
    -- validate template should have 11 fields
    IF cont_y        != total_colum_valid THEN
      validation_code:= 101;
      validation_msg := PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(validation_code,'Template',C_TABLE_NAME);
      -- insert the validation message into to the array
      csv_table(cont_x)(pos_validation_msg):= validation_msg;
      -- set error
      csv_table(cont_x)(pos_validation_result):= 'KO';
    ELSE
      csv_table(cont_x)(pos_validation_result):= 'OK';
    END IF;
    CLOSE c2;
  END LOOP;
CLOSE c1;
total_rows:= cont_x;
FOR i IN 1 .. total_rows
LOOP
  validation_msg  := NULL;
  validation_code := NULL;
  cont_error      :=0;
  --2 validate process
  -- validate format template is OK
  validation_result := csv_table(i)(pos_validation_result);
  --DBMS_OUTPUT.put_line('validation_result:' || validation_result);
  --------------------- INIT VALIDATION CSV correct -----------------------------------------------------
  --- validate length each column
  BEGIN
    -- ASOFDATE
    field_value        := PKG_COMMON_UTILS.F_FORMAT_COLUMN_CSV(csv_table(i)(1));
    aux_date           :='N';
    IF field_value     IS NOT NULL THEN
      aux_date         := PKG_COMMON_UTILS.F_IS_DATE_OK(field_value,'DD-MON-YYYY');
      IF aux_date       = 'N' THEN
        validation_code:= 1500;
        validation_msg := validation_msg || PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(validation_code,'asofdate',C_TABLE_NAME);
        cont_error     := cont_error + 1;
        csv_true       := false;
        asofdate       :=field_value;
        asofdate_ok    :=0;
      ELSE
        asofdate   :=field_value;
        asofdate_ok:=1;
      END IF;
    ELSE
      -- Validate ASOFDATE  not null
      asofdate  :=field_value;
      field_name:='asofdate';
      PKG_COMMON_UTILS.P_IS_MANDATORY_NULL(field_name,asofdate,cont_error,validation_msg,C_TABLE_NAME);
      asofdate   := '?';
      asofdate_ok:=0;
    END IF;
    -- SOURCE_SYSTEM_CRDS
    field_value:= PKG_COMMON_UTILS.F_FORMAT_COLUMN_CSV(csv_table(i)(2));
    field_name :='source_system_crds';
    PKG_COMMON_UTILS.P_CHECK_FORMAT_FIELD(field_name, field_value, C_SIZE_FIFTY, cont_error, validation_msg);
    source_system_crds := field_value;
    -- COUNTER_PARTY_ID
    field_value:= PKG_COMMON_UTILS.F_FORMAT_COLUMN_CSV(csv_table(i)(3));
    field_name :='counter_party_id';
    PKG_COMMON_UTILS.P_CHECK_FORMAT_FIELD(field_name, field_value, C_SIZE_FIFTY, cont_error, validation_msg);
    counter_party_id := field_value;
    -- COUNTER_PARTY_NAME
    field_value:= PKG_COMMON_UTILS.F_FORMAT_COLUMN_CSV(csv_table(i)(4));
    field_name :='counter_party_name';
    PKG_COMMON_UTILS.P_CHECK_FORMAT_FIELD(field_name, field_value, C_SIZE_2HUNDRED, cont_error, validation_msg);
    counter_party_name := field_value;
    -- VOLCKER_TRADING_DESK
    field_value:= PKG_COMMON_UTILS.F_FORMAT_COLUMN_CSV(csv_table(i)(5));
    field_name :='volcker_trading_desk';
    PKG_COMMON_UTILS.P_CHECK_FORMAT_FIELD(field_name, field_value, C_SIZE_FIFTY, cont_error, validation_msg);
    volcker_trading_desk := field_value;
    -- BOOK_ID
    field_value:= PKG_COMMON_UTILS.F_FORMAT_COLUMN_CSV(csv_table(i)(6));
    field_name :='book_id';
    PKG_COMMON_UTILS.P_CHECK_FORMAT_FIELD(field_name, field_value, C_SIZE_FIFTY, cont_error, validation_msg);
    book_id := field_value;
    -- CLASSIFICATION
    field_value:= PKG_COMMON_UTILS.F_FORMAT_COLUMN_CSV(csv_table(i)(7));
    field_name :='classification';
    PKG_COMMON_UTILS.P_CHECK_FORMAT_FIELD(field_name, field_value, C_SIZE_FIFTY, cont_error, validation_msg);
    classification := field_value;
    -- REASON
    field_value:= PKG_COMMON_UTILS.F_FORMAT_COLUMN_CSV(csv_table(i)(8));
    field_name :='reason';
    PKG_COMMON_UTILS.P_CHECK_FORMAT_FIELD(field_name, field_value, C_SIZE_2HUNDRED, cont_error, validation_msg);
    reason := field_value;
    -- POSITION_ID
    field_value:= PKG_COMMON_UTILS.F_FORMAT_COLUMN_CSV(csv_table(i)(9));
    field_name :='position_id';
    PKG_COMMON_UTILS.P_CHECK_FORMAT_FIELD(field_name, field_value, C_SIZE_FIFTY, cont_error, validation_msg);
    position_id := field_value;
    -- TRADE_ID
    field_value:= PKG_COMMON_UTILS.F_FORMAT_COLUMN_CSV(csv_table(i)(10));
    field_name :='trade_id';
    PKG_COMMON_UTILS.P_CHECK_FORMAT_FIELD(field_name, field_value, C_SIZE_FIFTY, cont_error, validation_msg);
    trade_id := field_value;
    -- ACTION
    field_value:= PKG_COMMON_UTILS.F_FORMAT_COLUMN_CSV(csv_table(i)(11));
    field_name :='action';
    PKG_COMMON_UTILS.P_CHECK_FORMAT_FIELD(field_name, field_value, C_SIZE_FIFTY, cont_error, validation_msg);
    action := field_value;
  END;
  --Starting validations for mandatory fields.(ASOFDATE,SOURCE_SYSTEM_CRDS,COUNTER_PARTY_ID,CLASSIFICATION,ACTION)
  -- Validate  SOURCE_SYSTEM_CRDS not null
  field_name:='source_system_crds';
  PKG_COMMON_UTILS.P_IS_MANDATORY_NULL(field_name,source_system_crds,cont_error,validation_msg,C_TABLE_NAME);
  source_system_crds:=TRIM(source_system_crds);
  -- Validate  counter_party_id not null
  field_name:='counter_party_id';
  PKG_COMMON_UTILS.P_IS_MANDATORY_NULL(field_name,counter_party_id,cont_error,validation_msg,C_TABLE_NAME);
  counter_party_id    :=TRIM(counter_party_id);
  IF counter_party_id IS NULL THEN
    counter_party_id  := '?';
  END IF;
  -- Validate  CLASSIFICATION id not null
  field_name:='classification';
  PKG_COMMON_UTILS.P_IS_MANDATORY_NULL(field_name,classification,cont_error,validation_msg,C_TABLE_NAME);
  classification:=TRIM(classification);
  -- Validate CLASSIFICATION is Customer, Non-Customer OR Internal
  PKG_CRDS_OVERRIDE.P_CHECK_CUSTUMER_CLASSIF_VALUE(field_name,classification,cont_error,validation_msg,C_TABLE_NAME);
  -- Validate  ACTION id not null
  field_name:='action';
  PKG_COMMON_UTILS.P_IS_MANDATORY_NULL(field_name,action,cont_error,validation_msg,C_TABLE_NAME);
  action:=TRIM(action);
  -- Validate ACTION is INSERT, UPDATE OR DELETE
  PKG_CRDS_OVERRIDE.P_CHECK_ACTION_VALUE(field_name,action,cont_error,validation_msg,C_TABLE_NAME);
  -- Validate VOLCKER_TRADING_DESK is empty
  field_name          :='volcker_trading_desk';
  volcker_trading_desk:=TRIM(volcker_trading_desk);
  PKG_CRDS_OVERRIDE.P_IS_EMPTY_VALUE(field_name,volcker_trading_desk);
  -- Validate BOOK_ID is empty
  field_name:='book_id';
  book_id   :=TRIM(book_id);
  PKG_CRDS_OVERRIDE.P_IS_EMPTY_VALUE(field_name,book_id);
  -- Validate POSITION_ID is empty
  field_name :='position_id';
  position_id:=TRIM(position_id);
  PKG_CRDS_OVERRIDE.P_IS_EMPTY_VALUE(field_name,position_id);
  -- Validate TRADE_ID is empty
  field_name:='trade_id';
  trade_id  :=TRIM(trade_id);
  PKG_CRDS_OVERRIDE.P_IS_EMPTY_VALUE(field_name,trade_id);
  --Validate SOURCE_SYSTEM_CRDS is in source_system table
  field_name:='source_system_crds';
  PKG_CRDS_OVERRIDE.P_CHECK_SOURCE_SYSTEM_VALUE(field_name,source_system_crds,cont_error,validation_msg,C_TABLE_NAME);
  IF asofdate_ok = 1 THEN
    --Validate No duplicate entries in CRDS_OVERRIDE table
    PKG_CRDS_OVERRIDE.P_CHECK_ASOFDATE_CRDS_OVERRIDE(asofdate,cont_error,validation_msg,C_TABLE_NAME,action,source_system_crds,counter_party_id,volcker_trading_desk,book_id,position_id,trade_id);
  END IF;
  ----------------- END VALIDATION CSV--------------------
  IF validation_result = 'OK' THEN
    -- Validation OK
    IF cont_error      = 0 THEN
      validation_code := 100;
      --validation_msg                     :=PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(validation_code,'',C_TABLE_NAME);
      SELECT MESSAGE
      INTO validation_msg
      FROM REF_DATA_VALIDATION_LOOKUP
      WHERE REF_DATA_VALIDATION_LOOKUP.return_code = validation_code;
      csv_table(i)(pos_validation_result)         := 'OK';
    ELSE
      csv_table(i)(pos_validation_result):= 'KO';
    END IF;
    -- insert the validation message into to the array
    csv_table(i)(pos_validation_msg):= validation_msg;
  END IF; -- end validatate error template
  -- insert row(ID,UPLOAD_ID,ASOFDATE,SOURCE_SYSTEM_CRDS,COUNTER_PARTY_ID,COUNTER_PARTY_NAME,VOLCKER_TRADING_DESK,BOOK_ID,CLASSIFICATION,REASON,POSITION_ID,TRADE_ID,ACTION,VALIDATION_MESSAGE)
  INSERT
  INTO CRDS_OVERRIDE_INTERMEDIARY
    (
      ID,
      UPLOAD_ID,
      ASOFDATE,
      SOURCE_SYSTEM_CRDS,
      COUNTER_PARTY_ID,
      COUNTER_PARTY_NAME,
      VOLCKER_TRADING_DESK,
      BOOK_ID,
      CLASSIFICATION,
      REASON,
      POSITION_ID,
      TRADE_ID,
      ACTION,
      VALIDATION_MESSAGE
    )
    VALUES
    (
      SEQ_CRDS_OVERRIDE_INTERMEDIARY.NEXTVAL,
      idUpload,
      asofdate,
      source_system_crds,
      counter_party_id,
      counter_party_name,
      volcker_trading_desk,
      book_id,
      classification,
      reason,
      position_id,
      trade_id,
      action,
      validation_msg
    );
END LOOP;
-- 4 Update Upload clob with adding validation message and and status such as from SUBMITTED to VALID / PENDING APPROVAL, or UPLOAD INVALID
--4.1 build the csv output
csv_line_out  := NULL;
csv_line_error:=NULL;
cont_error    :=0;
FOR i IN 1 .. total_rows
LOOP
  validation_result:= csv_table
  (
    i
  )
  (
    pos_validation_result
  )
  ;
  FOR j IN 1 .. total_colum
  LOOP
    -- last field
    IF pos_validation_msg  = j THEN
      IF validation_result = 'KO' THEN
        csv_line_error    := csv_line_error || csv_table
        (
          i
        )
        (
          j
        )
        || CHR
        (
          10
        )
        ;
        cont_error:= cont_error + 1;
      ELSE
        csv_line_out:= csv_line_out || csv_table(i)(j) || CHR(10);
      END IF;
    ELSE
      IF pos_validation_msg  > j THEN
        IF validation_result = 'KO' THEN
          csv_line_error    := csv_line_error || csv_table(i)(j) || ',';
        ELSE
          csv_line_out:= csv_line_out || csv_table(i)(j) || ',';
        END IF;
      END IF;
    END IF;
  END LOOP;
END LOOP;
csv_result:=csv_line_error || csv_line_out;
--4.2 update table  ui_uploaded
IF cont_error > 0 THEN
  -- UPLOADED INVALID
  status:= 3;
ELSE
  status:= 2;
  -- VALID - PENDING APPROVAL
END IF;
IF status = 2 THEN
  UPDATE ref_data_ui_upload
  SET status_id               = status,
    csv                       = csv_result
  WHERE ref_data_ui_upload.id = idUpload;
  COMMIT;
ELSE
  -- status  UPLOADED INVALID
  UPDATE ref_data_ui_upload
  SET status_id               = status,
    csv                       = csv_result,
    comments                  = upload_error,
    error_log                 = c_error_log,
    rejected_by               = 'admin_pl',
    rejected_on               = SYSDATE
  WHERE ref_data_ui_upload.id = idUpload;
  COMMIT;
END IF;
EXCEPTION
WHEN NO_DATA_FOUND THEN
  upload_error:= 'IdUpload NOT EXIST. ' || SQLERRM;
  ROLLBACK;
  --DBMS_OUTPUT.put_line(upload_error);
  RAISE;
WHEN SUBSCRIPT_OUTSIDE_LIMIT THEN
  status:= 4;
  --The uploaded spreadsheet has rows with more columns than allowed
  upload_error:=PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(5002,'',C_TABLE_NAME);
  c_error_log := SQLERRM;
  ROLLBACK;
  -- --DBMS_OUTPUT.put_line(upload_error);
  BEGIN
    UPDATE ref_data_ui_upload
    SET status_id               = status,
      comments                  = upload_error,
      error_log                 = c_error_log,
      rejected_by               = 'admin_pl',
      rejected_on               = SYSDATE
    WHERE ref_data_ui_upload.id = idUpload;
    COMMIT;
  EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    RAISE;
  END;
WHEN OTHERS THEN
  status     := 4;
  c_error_log:= SQLERRM;
  ROLLBACK;
  --DBMS_OUTPUT.put_line(c_error_log);
  BEGIN
    -- UPLOAD ERROR, Technical error
    upload_error:= PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(5001,'',C_TABLE_NAME);
    UPDATE ref_data_ui_upload
    SET status_id               = status,
      comments                  = upload_error,
      error_log                 = c_error_log,
      rejected_by               = 'admin_pl',
      rejected_on               = SYSDATE
    WHERE ref_data_ui_upload.id = idUpload;
    COMMIT;
  EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    RAISE;
  END;
END P_SPLIT_CSV_CRDS;
PROCEDURE P_CHECK_ACTION_VALUE(
    field_name     IN VARCHAR2,
    field          IN OUT VARCHAR2,
    cont_error     IN OUT NUMBER,
    validation_msg IN OUT VARCHAR2,
    ref_table      IN VARCHAR2)
IS
  validation_code VARCHAR2(4);
BEGIN
  IF field         <> 'INSERT' AND field <> 'UPDATE' AND field <> 'DELETE' THEN
    validation_code:= 1202;
    validation_msg := validation_msg || PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(validation_code,field_name,ref_table);
    cont_error     := cont_error + 1;
  END IF;
END P_CHECK_ACTION_VALUE;
PROCEDURE P_CHECK_CUSTUMER_CLASSIF_VALUE(
    field_name     IN VARCHAR2,
    field          IN OUT VARCHAR2,
    cont_error     IN OUT NUMBER,
    validation_msg IN OUT VARCHAR2,
    ref_table      IN VARCHAR2)
IS
  validation_code VARCHAR2(4);
BEGIN
  IF field         <> 'Customer' AND field <> 'Non-Customer' AND field <> 'Internal' THEN
    validation_code:= 1203;
    validation_msg := validation_msg || PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(validation_code,field_name,ref_table);
    cont_error     := cont_error + 1;
  END IF;
END P_CHECK_CUSTUMER_CLASSIF_VALUE;
PROCEDURE P_IS_EMPTY_VALUE(
    field_name IN VARCHAR2,
    field      IN OUT VARCHAR2)
IS
BEGIN
  IF field IS NULL THEN
    field  := '*';
  END IF;
END P_IS_EMPTY_VALUE;
PROCEDURE P_CHECK_SOURCE_SYSTEM_VALUE(
    field_name     IN VARCHAR2,
    field          IN OUT VARCHAR2,
    cont_error     IN OUT NUMBER,
    validation_msg IN OUT VARCHAR2,
    ref_table      IN VARCHAR2)
IS
  validation_code VARCHAR2(4);
  p_is_ok         NUMBER;
BEGIN
  SELECT COUNT(source_system_crds_name)
  INTO p_is_ok
  FROM SOURCE_SYSTEM
  WHERE source_system_crds_name=field;
  IF p_is_ok                   = 0 THEN
    validation_code           := 1204;
    validation_msg            := validation_msg || PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(validation_code,field_name,ref_table);
    cont_error                := cont_error + 1;
  END IF;
END P_CHECK_SOURCE_SYSTEM_VALUE;
PROCEDURE P_CHECK_NO_DUPLICATES_INT(
    idUpload IN NUMBER)
IS
  C_CODE_MESSAGE CONSTANT INTEGER     :=1206;
  C_TABLE_NAME   CONSTANT VARCHAR2(30):='REF_DATA_VALIDATION_LOOKUP';
  p_asofdate crds_override_intermediary.asofdate%type;
  p_source_system_crds crds_override_intermediary.source_system_crds%type;
  p_counter_party_id crds_override_intermediary.counter_party_id%type;
  p_volcker_trading_desk crds_override_intermediary.volcker_trading_desk%type;
  p_book_id crds_override_intermediary.book_id%type;
  p_position_id crds_override_intermediary.position_id%type;
  p_trade_id crds_override_intermediary.trade_id%type;
  p_id crds_override_intermediary.id%type;
  p_validation_message crds_override_intermediary.validation_message%type;
  p_message_ok REF_DATA_VALIDATION_LOOKUP.message%type;
  p_field_value VARCHAR2(4000);
  p_counter_party_name crds_override_intermediary.counter_party_name%type;
  p_classification crds_override_intermediary.classification%type;
  p_reason crds_override_intermediary.reason%type;
  p_action crds_override_intermediary.action%type;
  p_value VARCHAR2(4000);
  p_csv CLOB;
  CURSOR c_duplicates is
    select asofdate,source_system_crds,counter_party_id, volcker_trading_desk,book_id,position_id,trade_id,count(*) 
    from crds_override_intermediary 
    where upload_id=idUpload
    group by asofdate,source_system_crds,counter_party_id, volcker_trading_desk,book_id,position_id,trade_id
    having count(*) >1;
  CURSOR c_data_duplicate is
    select id,validation_message,counter_party_name,classification,reason,action 
    from crds_override_intermediary 
    where asofdate=p_asofdate 
    and source_system_crds=p_source_system_crds
    and counter_party_id=p_counter_party_id
    and volcker_trading_desk=p_volcker_trading_desk
    and book_id=p_book_id
    and position_id=p_position_id
    and trade_id=p_trade_id
    and upload_id=idUpload;
BEGIN
  select message into p_message_ok from REF_DATA_VALIDATION_LOOKUP where return_code=100;
  p_field_value:= 'asofdate-source_system_crds-counter_party_id-volcker_trading_desk-book_id-position_id-trade_id';
  FOR duplicate in c_duplicates
   LOOP
    p_asofdate := duplicate.asofdate;
    p_source_system_crds := duplicate.source_system_crds;
    p_counter_party_id := duplicate.counter_party_id;
    p_volcker_trading_desk := duplicate.volcker_trading_desk;
    p_book_id := duplicate.book_id;
    p_position_id := duplicate.position_id;
    p_trade_id := duplicate.trade_id;
      FOR data_duplicate in c_data_duplicate
       LOOP
        p_id := data_duplicate.id;
        p_validation_message := data_duplicate.validation_message;
        if(p_validation_message = p_message_ok) then
          p_validation_message := PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(C_CODE_MESSAGE,p_field_value,C_TABLE_NAME);          
        else
          p_validation_message := p_validation_message || PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(C_CODE_MESSAGE,p_field_value,C_TABLE_NAME);    
        end if;
        update crds_override_intermediary set validation_message=p_validation_message where id=p_id;
        commit;
        p_counter_party_name := data_duplicate.counter_party_name;
        p_classification := data_duplicate.classification;
        p_reason := data_duplicate.reason;
        p_action := data_duplicate.action;
        p_value:=p_asofdate || ',' || p_source_system_crds || ',' || p_counter_party_id || ',' || p_counter_party_name || ',' || p_volcker_trading_desk || ',' || p_book_id || ',' || p_classification || ',' || p_reason || ',' || p_position_id || ',' || p_trade_id || ',' || p_action;
        p_csv := p_csv || CHR(10) || p_value || ', ' || p_validation_message;
       END LOOP;
       UPDATE ref_data_ui_upload
        SET status_id         = 3,
            csv               = p_csv
        WHERE ref_data_ui_upload.id = idUpload;
        commit;
   END LOOP;   
END P_CHECK_NO_DUPLICATES_INT;



-- **********************************************************************
-- Procedure: P_IS_SAME_ASOFDATE -> The asofdate for each entry in the posted file should be the same date
-- **********************************************************************
PROCEDURE P_IS_SAME_ASOFDATE(
    P_UPLOAD_ID IN NUMBER)
IS
  v_number        NUMBER;
  validation_code VARCHAR2(1000);
  validation_msg  VARCHAR2(1000);
  C_TABLE_NAME    CONSTANT VARCHAR2(30):='REF_DATA_VALIDATION_LOOKUP';
  p_asofdate crds_override_intermediary.asofdate%type;
  p_source_system_crds crds_override_intermediary.source_system_crds%type;
  p_counter_party_id crds_override_intermediary.counter_party_id%type;
  p_volcker_trading_desk crds_override_intermediary.volcker_trading_desk%type;
  p_book_id crds_override_intermediary.book_id%type;
  p_position_id crds_override_intermediary.position_id%type;
  p_trade_id crds_override_intermediary.trade_id%type;
  p_id crds_override_intermediary.id%type;
  p_validation_message crds_override_intermediary.validation_message%type;
  p_message_ok REF_DATA_VALIDATION_LOOKUP.message%type;
  p_field_value VARCHAR2(4000);
  p_counter_party_name crds_override_intermediary.counter_party_name%type;
  p_classification crds_override_intermediary.classification%type;
  p_reason crds_override_intermediary.reason%type;
  p_action crds_override_intermediary.action%type;
  p_validation_message2 crds_override_intermediary.validation_message%type;
  p_value VARCHAR2(4000);
  p_csv CLOB;
  CURSOR c1
  IS
    SELECT asofdate,
      source_system_crds,
      counter_party_id,
      counter_party_name,
      volcker_trading_desk,
      book_id,
      position_id,
      trade_id,
      classification,
      reason,
      action,
      validation_message
    FROM crds_override_intermediary
    WHERE upload_id = P_UPLOAD_ID;
BEGIN
  SELECT COUNT(DISTINCT coi.ASOFDATE)
  INTO v_number
  FROM crds_override_intermediary coi
  WHERE coi.UPLOAD_ID = P_UPLOAD_ID;
  IF v_number         > 1 THEN
    validation_code  := 1205;
    validation_msg   := PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(validation_code,'asofdate',C_TABLE_NAME);
    --UPDATE ROWS WITH VALIDATION_MESSAGE NOT LIKE VALIDATION OK
    UPDATE crds_override_intermediary
    SET crds_override_intermediary.VALIDATION_MESSAGE = crds_override_intermediary.VALIDATION_MESSAGE
      || validation_msg
    WHERE crds_override_intermediary.UPLOAD_ID = P_UPLOAD_ID
    AND crds_override_intermediary.validation_message NOT LIKE '%VALIDATION OK%';
    --UPDATE ROWS WITH VALIDATION_MESSAGE LIKE VALIDATION OK
    UPDATE crds_override_intermediary
    SET crds_override_intermediary.VALIDATION_MESSAGE = validation_msg
    WHERE crds_override_intermediary.UPLOAD_ID        = P_UPLOAD_ID
    AND crds_override_intermediary.validation_message LIKE '%VALIDATION OK%';
    COMMIT;
    FOR info IN c1
    LOOP
      p_asofdate             := info.asofdate;
      p_source_system_crds   := info.source_system_crds;
      p_counter_party_id     := info.counter_party_id;
      p_counter_party_name   := info.counter_party_name;
      p_volcker_trading_desk := info.volcker_trading_desk;
      p_book_id              := info.book_id;
      p_position_id          := info.position_id;
      p_trade_id             := info.trade_id;
      p_classification       := info.classification;
      p_reason               := info.reason;
      p_action               := info.action;
      p_validation_message2  := info.validation_message;
      p_value                :=p_asofdate || ',' || p_source_system_crds || ',' || p_counter_party_id || ',' || p_counter_party_name || ',' || p_volcker_trading_desk || ',' || p_book_id || ',' || p_classification || ',' || p_reason || ',' || p_position_id || ',' || p_trade_id || ',' || p_action;
      p_csv                  := p_csv || CHR(10) || p_value || ', ' || p_validation_message2;
    END LOOP;
    UPDATE ref_data_ui_upload
    SET status_id               = 3,
      csv                       = p_csv
    WHERE ref_data_ui_upload.id = P_UPLOAD_ID;
    COMMIT;
  END IF;
END P_IS_SAME_ASOFDATE;
PROCEDURE P_CHECK_ASOFDATE_CRDS_OVERRIDE(
    asofdate               IN VARCHAR2,
    cont_error             IN OUT NUMBER,
    validation_msg         IN OUT VARCHAR2,
    ref_table              IN VARCHAR2,
    p_status               IN VARCHAR2,
    p_source_system_crds   IN VARCHAR2,
    p_counter_party_id     IN VARCHAR2,
    p_volcker_trading_desk IN VARCHAR2,
    p_book_id              IN VARCHAR2,
    p_position_id          IN VARCHAR2,
    p_trade_id             IN VARCHAR2)
IS
  validation_code VARCHAR2(4);
  p_asofdate crds_override_intermediary.asofdate%type;
  p_exist_same_date   NUMBER:=0;
  p_exist_before_date NUMBER:=0;
  p_field_name        VARCHAR2(4000);
BEGIN
  IF p_status   = 'INSERT' THEN
    p_asofdate := to_date(asofdate,'DD-MM-YY') ;
    SELECT COUNT(*)
    INTO p_exist_same_date
    FROM CRDS_OVERRIDE crds
    WHERE crds.ASOFDATE          =to_date(p_asofdate,'DD-MM-YY')
    AND crds.source_system_crds  =p_source_system_crds
    AND crds.counter_party_id    =p_counter_party_id
    AND crds.volcker_trading_desk=p_volcker_trading_desk
    AND crds.book_id             = p_book_id
    AND crds.positionid          =p_position_id
    AND crds.tradeid             =p_trade_id;
    IF p_exist_same_date         > 0 THEN
      p_field_name              := 'asofdate-source_system_crds-counter_party_id-volcker_trading_desk-book_id-position_id-trade_id';
      validation_code           := 1207;
      validation_msg            := validation_msg || PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(validation_code,p_field_name,ref_table);
      cont_error                := cont_error + 1;
    ELSE
      SELECT COUNT(*)
      INTO p_exist_before_date
      FROM CRDS_OVERRIDE crds
      WHERE crds.ASOFDATE=
        (SELECT MAX(asofdate)
        FROM CRDS_OVERRIDE
        WHERE asofdate < to_date(p_asofdate,'dd/mm/yy')
        )
      AND crds.source_system_crds  =p_source_system_crds
      AND crds.counter_party_id    =p_counter_party_id
      AND crds.volcker_trading_desk=p_volcker_trading_desk
      AND crds.book_id             = p_book_id
      AND crds.positionid          =p_position_id
      AND crds.tradeid             =p_trade_id;
      IF p_exist_before_date       > 0 THEN
        p_field_name              := 'source_system_crds-counter_party_id-volcker_trading_desk-book_id-position_id-trade_id';
        validation_code           := 1207;
        validation_msg            := validation_msg || PKG_COMMON_UTILS.F_GET_VALIDATION_MSG(validation_code,p_field_name,ref_table);
        cont_error                := cont_error + 1;
      END IF;
    END IF;
  END IF;
END P_CHECK_ASOFDATE_CRDS_OVERRIDE;
PROCEDURE P_MAIN_PROCEDURE(
    P_UPLOAD_ID IN NUMBER)
IS
BEGIN
  p_split_csv_crds(P_UPLOAD_ID);
  p_is_same_asofdate(P_UPLOAD_ID);
  p_check_no_duplicates_int(P_UPLOAD_ID);
  p_check_validation(P_UPLOAD_ID);
END P_MAIN_PROCEDURE;
FUNCTION F_INTERMEDIARY_TO_STAGING(
    idUpload NUMBER)
  RETURN BOOLEAN
IS
BEGIN
  -- pkg_monitoring.pr_insert_log_jobs_qv('RD_CRDS_OVERRIDE_EXTRACT',null,'RD_CRDS_OVERRIDE_EXTRACT',current_date,'RD_CRDS_OVERRIDE_EXTRACT ETL','DEBUG', 'LOGGING', 'F_INTERMEDIARY_TO_STAGING', 'Inicio ', 'RENTD');
  INSERT
  INTO crds_override_staging
    (
      ID,
      ASOFDATE,
      SOURCE_SYSTEM_CRDS,
      COUNTER_PARTY_ID,
      COUNTER_PARTY_NAME,
      VOLCKER_TRADING_DESK,
      BOOK_ID,
      CLASSIFICATION,
      REASON,
      POSITION_ID,
      TRADE_ID,
      ACTION,
      STATUS_ID
    )
  SELECT UPLOAD_ID,
    to_date(ASOFDATE,'dd/mm/yy'),
    SOURCE_SYSTEM_CRDS,
    COUNTER_PARTY_ID,
    COUNTER_PARTY_NAME,
    VOLCKER_TRADING_DESK,
    BOOK_ID,
    CLASSIFICATION,
    REASON,
    POSITION_ID,
    TRADE_ID,
    ACTION,
    1
  FROM crds_override_intermediary
  WHERE crds_override_intermediary.UPLOAD_ID = idUpload;
  COMMIT;
  -- pkg_monitoring.pr_insert_log_jobs_qv('RD_CRDS_OVERRIDE_EXTRACT',null,'RD_CRDS_OVERRIDE_EXTRACT',current_date,'RD_CRDS_OVERRIDE_EXTRACT ETL','DEBUG', 'LOGGING', 'F_INTERMEDIARY_TO_STAGING', 'Fin ', 'RENTD');
  RETURN true;
EXCEPTION
WHEN OTHERS THEN
  ROLLBACK;
  RAISE;
END F_INTERMEDIARY_TO_STAGING;
PROCEDURE P_CHECK_VALIDATION(
    idUpload IN NUMBER)
IS
  p_validation_message crds_override_intermediary.validation_message%type;
  p_message_ok REF_DATA_VALIDATION_LOOKUP.message%type;
  p_field_value VARCHAR2(4000);
  p_value       VARCHAR2(4000);
  p_csv CLOB;
  p_result     BOOLEAN;
  p_validation BOOLEAN:=true;
  p_status_id ref_data_ui_upload.status_id%TYPE;
  CURSOR c_validation
  IS
    SELECT DISTINCT validation_message
    FROM crds_override_intermediary
    WHERE upload_id=idUpload;
BEGIN
  SELECT MESSAGE
  INTO p_message_ok
  FROM REF_DATA_VALIDATION_LOOKUP
  WHERE return_code=100;
  --pkg_monitoring.pr_insert_log_jobs_qv('RD_CRDS_OVERRIDE_EXTRACT',null,'RD_CRDS_OVERRIDE_EXTRACT',current_date,'RD_CRDS_OVERRIDE_EXTRACT ETL','DEBUG', 'LOGGING', 'P_CHECK_VALIDATION', 'p_message_ok: '||p_message_ok, 'RENTD');
  FOR data_validation IN c_validation
  LOOP
    p_validation_message := data_validation.validation_message;
    --pkg_monitoring.pr_insert_log_jobs_qv('RD_CRDS_OVERRIDE_EXTRACT',null,'RD_CRDS_OVERRIDE_EXTRACT',current_date,'RD_CRDS_OVERRIDE_EXTRACT ETL','DEBUG', 'LOGGING', 'P_CHECK_VALIDATION', 'p_validation_message: '||p_validation_message, 'RENTD');
    IF(p_validation_message <> p_message_ok) THEN
      p_validation          :=false;
      --pkg_monitoring.pr_insert_log_jobs_qv('RD_CRDS_OVERRIDE_EXTRACT',null,'RD_CRDS_OVERRIDE_EXTRACT',current_date,'RD_CRDS_OVERRIDE_EXTRACT ETL','DEBUG', 'LOGGING', 'P_CHECK_VALIDATION', 'p_validation:FALSE ', 'RENTD');
    END IF;
  END LOOP;
  SELECT status_id INTO p_status_id FROM ref_data_ui_upload WHERE id = idUpload;
  IF p_validation = true AND p_status_id=2 THEN
    -- pkg_monitoring.pr_insert_log_jobs_qv('RD_CRDS_OVERRIDE_EXTRACT',null,'RD_CRDS_OVERRIDE_EXTRACT',current_date,'RD_CRDS_OVERRIDE_EXTRACT ETL','DEBUG', 'LOGGING', 'P_CHECK_VALIDATION', 'p_validation:TRUE ', 'RENTD');
    p_result := F_INTERMEDIARY_TO_STAGING(idUpload);
    UPDATE ref_data_ui_upload
    SET status_id               = 2
    WHERE ref_data_ui_upload.id = idUpload;
    COMMIT;
  END IF;
END P_CHECK_VALIDATION;
PROCEDURE P_ACCEPT_UPLOAD(
    IDUPLOAD IN NUMBER,
    IDUSER   IN VARCHAR,
    COMMENTS IN VARCHAR)
IS
  v_uploaded_by REF_DATA_UI_UPLOAD.UPLOADED_BY%TYPE;
  v_row_to_update CRDS_OVERRIDE_STAGING%ROWTYPE;
  v_status_id REF_DATA_UI_UPLOAD.STATUS_ID%TYPE;
  p_asofdate crds_override_staging.asofdate%type;
  p_exist_same_date     NUMBER;

p_exist_to_update     NUMBER;
  p_exist_previous_data NUMBER;
  p_contador            NUMBER:=0;
  CURSOR c_staging_rows
  IS
    SELECT * FROM CRDS_OVERRIDE_STAGING WHERE id = IDUPLOAD;
BEGIN
  SELECT uploaded_by
  INTO v_uploaded_by
  FROM REF_DATA_UI_UPLOAD
  WHERE id = idupload;
  SELECT status_id INTO v_status_id FROM REF_DATA_UI_UPLOAD WHERE id = idupload;
  IF v_uploaded_by = IDUSER THEN
    ROLLBACK;
    raise_application_error(-20001, 'Users cannot change status of their own uploaded files.');
  ELSIF v_status_id <> 2 THEN --status must be "VALID - PENDING APPROVAL"
    ROLLBACK;
    raise_application_error(-20001, 'The status file is not ready for approval/reject');
  ELSE
    FOR v_staging_row IN c_staging_rows
    LOOP
      p_asofdate :=to_date(v_staging_row.ASOFDATE,'DD-MM-YY');
      SELECT COUNT(*)
      INTO p_exist_same_date
      FROM CRDS_OVERRIDE crds
      WHERE crds.ASOFDATE       =to_date(p_asofdate,'DD-MM-YY');
      IF p_exist_same_date      > 0 THEN
        IF v_staging_row.action = 'INSERT' THEN
          IF v_staging_row.id  IS NOT NULL AND v_staging_row.id = IDUPLOAD THEN
            INSERT
            INTO CRDS_OVERRIDE VALUES
              (
                v_staging_row.ASOFDATE,
                v_staging_row.SOURCE_SYSTEM_CRDS,
                v_staging_row.COUNTER_PARTY_ID,
                v_staging_row.COUNTER_PARTY_NAME,
                v_staging_row.VOLCKER_TRADING_DESK,
                v_staging_row.BOOK_ID,
                v_staging_row.CLASSIFICATION,
                v_staging_row.REASON,
                NULL,
                NULL,
                NULL,
                NULL,
                v_staging_row.ACTION,
                v_staging_row.POSITION_ID,
                v_staging_row.TRADE_ID
              );
            COMMIT;
          END IF;
        ELSE --v_staging_row.status = 'UPDATE' or 'DELETE'
          SELECT COUNT(*)
          INTO p_exist_to_update
          FROM CRDS_OVERRIDE crds
          WHERE crds.ASOFDATE          =to_date(p_asofdate,'DD-MM-YY')
          AND crds.source_system_crds  =v_staging_row.source_system_crds
          AND crds.counter_party_id    =v_staging_row.counter_party_id
          AND crds.volcker_trading_desk=v_staging_row.volcker_trading_desk
          AND crds.book_id             = v_staging_row.book_id
          AND crds.positionid          =v_staging_row.position_id
          AND crds.tradeid             =v_staging_row.trade_id;
          IF p_exist_to_update         > 0 THEN-- UPDATE FOUND ENTRY
            UPDATE CRDS_OVERRIDE
            SET asofdate            =v_staging_row.ASOFDATE,
              source_system_crds    =v_staging_row.source_system_crds,
              counter_party_id      =v_staging_row.counter_party_id,
              counter_party_name    =v_staging_row.COUNTER_PARTY_NAME,
              volcker_trading_desk  =v_staging_row.volcker_trading_desk,
              book_id               = v_staging_row.book_id,
              classification        =v_staging_row.classification,
              reason                =v_staging_row.reason,
              action                =v_staging_row.action,
              positionid            =v_staging_row.position_id,
              tradeid               =v_staging_row.trade_id
            WHERE ASOFDATE          =to_date(p_asofdate,'DD-MM-YY')
            AND source_system_crds  =v_staging_row.source_system_crds
            AND counter_party_id    =v_staging_row.counter_party_id
            AND volcker_trading_desk=v_staging_row.volcker_trading_desk
            AND book_id             = v_staging_row.book_id
            AND positionid          =v_staging_row.position_id
            AND tradeid             =v_staging_row.trade_id;
            COMMIT;
          ELSE -- UPDATE NOT FOUND AN ENTRY
            INSERT
            INTO CRDS_OVERRIDE VALUES
              (
                v_staging_row.ASOFDATE,
                v_staging_row.SOURCE_SYSTEM_CRDS,
                v_staging_row.COUNTER_PARTY_ID,
                v_staging_row.COUNTER_PARTY_NAME,
                v_staging_row.VOLCKER_TRADING_DESK,
                v_staging_row.BOOK_ID,
                v_staging_row.CLASSIFICATION,
                v_staging_row.REASON,
                NULL,
                NULL,
                NULL,
                NULL,
                v_staging_row.ACTION,
                v_staging_row.POSITION_ID,
                v_staging_row.TRADE_ID
              );
            COMMIT;
          END IF;
        END IF;
      ELSE --  If the asofdate value for the posted file has not been published earlier in CRDS_OVERRIDE
        IF p_contador = 0 THEN
          SELECT COUNT(*)
          INTO p_exist_previous_data
          FROM CRDS_OVERRIDE
          WHERE ASOFDATE=
            (SELECT MAX(asofdate)
            FROM CRDS_OVERRIDE
            WHERE asofdate < to_date(p_asofdate,'DD-MM-YY')
            AND (ACTION    = 'INSERT'
            OR ACTION      = 'UPDATE')
            );
          IF p_exist_previous_data > 0 THEN
            p_contador            := 1;
            INSERT
            INTO CRDS_OVERRIDE
              (
                ASOFDATE,
                SOURCE_SYSTEM_CRDS,
                COUNTER_PARTY_ID,
                COUNTER_PARTY_NAME,
                VOLCKER_TRADING_DESK,
                BOOK_ID,
                CLASSIFICATION,
                REASON,
                CREATE_USER,
                CREATE_DATE,
                MODIFY_USER,
                MODIFY_DATE,
                ACTION,
                POSITIONID,
                TRADEID
              )
            SELECT v_staging_row.ASOFDATE,
              source_system_crds,
              counter_party_id,
              counter_party_name,
              volcker_trading_desk,
              book_id,
              classification,
              reason,
              create_user,
              create_date,
              modify_user,
              modify_date,
              action,
              positionid,
              tradeid
            FROM CRDS_OVERRIDE
            WHERE ASOFDATE=
              (SELECT MAX(asofdate)
              FROM CRDS_OVERRIDE
              WHERE asofdate < to_date(p_asofdate,'DD-MM-YY')
              )
            AND (ACTION = 'INSERT'
            OR ACTION   = 'UPDATE');
            COMMIT;
          END IF;
        END IF;
        IF v_staging_row.ACTION = 'INSERT' THEN
          IF v_staging_row.id  IS NOT NULL AND v_staging_row.id = IDUPLOAD THEN
            INSERT
            INTO CRDS_OVERRIDE VALUES
              (
                v_staging_row.ASOFDATE,
                v_staging_row.SOURCE_SYSTEM_CRDS,
                v_staging_row.COUNTER_PARTY_ID,
                v_staging_row.COUNTER_PARTY_NAME,
                v_staging_row.VOLCKER_TRADING_DESK,
                v_staging_row.BOOK_ID,
                v_staging_row.CLASSIFICATION,
                v_staging_row.REASON,
                NULL,
                NULL,
                NULL,
                NULL,
                v_staging_row.ACTION,
                v_staging_row.POSITION_ID,
                v_staging_row.TRADE_ID
              );
            COMMIT;
          END IF;
        ELSE
          SELECT COUNT(*)
          INTO p_exist_to_update
          FROM CRDS_OVERRIDE crds
          WHERE crds.ASOFDATE          =to_date(p_asofdate,'DD-MM-YY')
          AND crds.source_system_crds  =v_staging_row.source_system_crds
          AND crds.counter_party_id    =v_staging_row.counter_party_id
          AND crds.volcker_trading_desk=v_staging_row.volcker_trading_desk
          AND crds.book_id             = v_staging_row.book_id
          AND crds.positionid          =v_staging_row.position_id
          AND crds.tradeid             =v_staging_row.trade_id;
          IF p_exist_to_update         > 0 THEN -- UPDATE FOUND ENTRY
            UPDATE CRDS_OVERRIDE
            SET asofdate            =v_staging_row.ASOFDATE,
              source_system_crds    =v_staging_row.source_system_crds,
              counter_party_id      =v_staging_row.counter_party_id,
              counter_party_name    =v_staging_row.COUNTER_PARTY_NAME,
              volcker_trading_desk  =v_staging_row.volcker_trading_desk,
              book_id               = v_staging_row.book_id,
              classification        =v_staging_row.classification,
              reason                =v_staging_row.reason,
              action                =v_staging_row.action,
              positionid            =v_staging_row.position_id,
              tradeid               =v_staging_row.trade_id
            WHERE ASOFDATE          =to_date(p_asofdate,'DD-MM-YY')
            AND source_system_crds  =v_staging_row.source_system_crds
            AND counter_party_id    =v_staging_row.counter_party_id
            AND volcker_trading_desk=v_staging_row.volcker_trading_desk
            AND book_id             = v_staging_row.book_id
            AND positionid          =v_staging_row.position_id
            AND tradeid             =v_staging_row.trade_id;
            COMMIT;
          ELSE -- UPDATE -> DID NOT FIND AN ENTRY
            INSERT
            INTO CRDS_OVERRIDE VALUES
              (
                v_staging_row.ASOFDATE,
                v_staging_row.SOURCE_SYSTEM_CRDS,
                v_staging_row.COUNTER_PARTY_ID,
                v_staging_row.COUNTER_PARTY_NAME,
                v_staging_row.VOLCKER_TRADING_DESK,
                v_staging_row.BOOK_ID,
                v_staging_row.CLASSIFICATION,
                v_staging_row.REASON,
                NULL,
                NULL,
                NULL,
                NULL,
                v_staging_row.ACTION,
                v_staging_row.POSITION_ID,
                v_staging_row.TRADE_ID
              );
            COMMIT;
          END IF;
        END IF;
      END IF;
    END LOOP;
    UPDATE CRDS_OVERRIDE_STAGING
    SET STATUS_ID = 2 -- Set status to SUBMITTED
    WHERE id      = IDUPLOAD;
    COMMIT;
    UPDATE REF_DATA_UI_UPLOAD
    SET status_id = 5, --Set status to USER APPROVED
      approved_by = IDUSER,
      approved_on = systimestamp
    WHERE id      = IDUPLOAD;
    COMMIT;
  END IF;
END P_ACCEPT_UPLOAD;
PROCEDURE P_REJECT_UPLOAD(
    IDUPLOAD IN NUMBER,
    IDUSER   IN VARCHAR,
    COMMENTS IN VARCHAR)
IS
  v_uploaded_by REF_DATA_UI_UPLOAD.UPLOADED_BY%TYPE;
  v_status_id REF_DATA_UI_UPLOAD.STATUS_ID%TYPE;
  v_staging_row CRDS_OVERRIDE_STAGING%ROWTYPE;
  p_comments VARCHAR(4000) := COMMENTS;
BEGIN
  SELECT uploaded_by
  INTO v_uploaded_by
  FROM REF_DATA_UI_UPLOAD
  WHERE id         = idupload;
  IF v_uploaded_by = IDUSER THEN
    ROLLBACK;
    raise_application_error(-20001, 'Users cannot change status of their own uploaded files.');
  ELSIF v_status_id <> 2 THEN --status must be "VALID - PENDING APPROVAL"
    ROLLBACK;
    raise_application_error(-20001, 'The status file is not ready for approval/reject');
  ELSE
    UPDATE REF_DATA_UI_UPLOAD rdu
    SET rdu.status_id = 6, --User rejected
      rdu.rejected_by = IDUSER,
      rdu.rejected_on = systimestamp,
      rdu.comments    = p_comments
    WHERE rdu.id      = IDUPLOAD;
    COMMIT;
  END IF;
END P_REJECT_UPLOAD;

END PKG_CRDS_OVERRIDE;

