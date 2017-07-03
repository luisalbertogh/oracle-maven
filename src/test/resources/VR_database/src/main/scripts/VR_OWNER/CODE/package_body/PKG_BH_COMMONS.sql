--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_BODY_PKG_BH_COMMONS runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE stripComments:FALSE


  CREATE OR REPLACE PACKAGE BODY "PKG_BH_COMMONS" AS
  -----------------------------------------------------------
  -- Get the date depending on the flag 'true' or 'false'
  ----------------------------------------------------------
FUNCTION F_GET_DATE_EMERGENCY_FLAG(
    isEmergency IN BOOLEAN)
  RETURN DATE
IS
  first_day DATE;
BEGIN
  IF isEmergency THEN
    SELECT TRUNC(sysdate, 'mm') INTO first_day FROM dual;
  ELSE
    SELECT TRUNC(last_day(sysdate))+1 INTO first_day FROM dual;
  END IF;
  RETURN first_day;
END F_GET_DATE_EMERGENCY_FLAG;
------------------------------------------
-- Exist book Id in the table "BOOK_HIERARCHY_RPL"
-------------------------------------------
FUNCTION F_EXIST_RPL_BOOK(
    p_bookId   IN VARCHAR2,
    p_asofdate IN DATE)
  RETURN BOOLEAN
IS
  num_total NUMBER;
BEGIN
  SELECT COUNT(*)
  INTO num_total
  FROM book_hierarchy_rpl
  WHERE book_hierarchy_rpl.book_id      = p_bookId
  AND book_hierarchy_rpl.asofdate       = p_asofdate
  AND book_hierarchy_rpl.source_system IS NULL;
  IF num_total                          > 0 THEN
    RETURN true;
  ELSE
    RETURN false;
  END IF;
END F_EXIST_RPL_BOOK;
--------------------------------------------------------------------
-- Exist book Id and Source System  in the table "BOOK_HIERARCHY_RPL"
--------------------------------------------------------------------
FUNCTION F_EXIST_RPL_BOOK_AND_SS(
    p_bookId       IN VARCHAR2,
    p_sourceSystem IN VARCHAR2,
    p_asofdate     IN DATE)
  RETURN BOOLEAN
IS
  num_total NUMBER;
BEGIN
  SELECT COUNT(*)
  INTO num_total
  FROM book_hierarchy_rpl
  WHERE book_hierarchy_rpl.book_id     = p_bookId
  AND book_hierarchy_rpl.source_system = p_sourceSystem
  AND book_hierarchy_rpl.asofdate      = p_asofdate;
  IF num_total                         > 0 THEN
    RETURN true;
  ELSE
    RETURN false;
  END IF;
END F_EXIST_RPL_BOOK_AND_SS;
-------------------------------------------
-- Exist book Id in the table "bh_staging"
-------------------------------------------
FUNCTION F_EXIST_STA_BOOK(
    p_bookId   IN VARCHAR2,
    p_asofdate IN DATE)
  RETURN BOOLEAN
IS
  num_total NUMBER;
BEGIN
  SELECT COUNT(*)
  INTO num_total
  FROM bh_staging
  WHERE bh_staging.book_id      = p_bookId
  AND bh_staging.asofdate       = p_asofdate
  AND bh_staging.source_system IS NULL
  AND bh_staging.active_flag    = 'Y';
  IF num_total                  > 0 THEN
    RETURN true;
  ELSE
    RETURN false;
  END IF;
END F_EXIST_STA_BOOK;
------------------------------------------------------------------
-- Exist book Id and Source System  in the table "bh_staging"
------------------------------------------------------------------
FUNCTION F_EXIST_STA_BOOK_AND_SS(
    p_bookId       IN VARCHAR2,
    p_sourceSystem IN VARCHAR2,
    p_asofdate     IN DATE)
  RETURN BOOLEAN
IS
  num_total NUMBER;
BEGIN
  SELECT COUNT(*)
  INTO num_total
  FROM bh_staging
  WHERE bh_staging.book_id     = p_bookId
  AND bh_staging.source_system = p_sourceSystem
  AND bh_staging.asofdate      = p_asofdate
  AND bh_staging.active_flag   = 'Y';
  IF num_total                 > 0 THEN
    RETURN true;
  ELSE
    RETURN false;
  END IF;
END F_EXIST_STA_BOOK_AND_SS;
----------------------------------------
-- Global Trader Book Id exists  in the table "bh_staging"
----------------------------------------
FUNCTION F_EXIST_STA_GTB(
    p_gtb      IN VARCHAR2,
    p_asofdate IN DATE)
  RETURN BOOLEAN
IS
  num_total NUMBER;
BEGIN
  SELECT COUNT(*)
  INTO num_total
  FROM bh_staging
  WHERE bh_staging.global_trader_book_id = p_gtb
  AND bh_staging.asofdate                = p_asofdate
  AND bh_staging.active_flag             = 'Y';
  IF num_total                           > 0 THEN
    RETURN true;
  ELSE
    RETURN false;
  END IF;
END F_EXIST_STA_GTB;
----------------------------------------
-- Global Trader Book Id exists  in the table "BRDS_VW-BOOK"
----------------------------------------
FUNCTION F_EXIST_BRDS_GTB(
    p_gtb IN VARCHAR2)
  RETURN BOOLEAN
IS
  num_total NUMBER;
BEGIN
  SELECT COUNT(*)
  INTO num_total
  FROM brds_vw_book
  WHERE brds_vw_book.globaltraderbookid = p_gtb;
  IF num_total                          > 0 THEN
    RETURN true;
  ELSE
    RETURN false;
  END IF;
END F_EXIST_BRDS_GTB;
----------------------------------------
-- Global Trader Book Id/Book id exists  in the table "BRDS_VW-BOOK" and source system must be Null
----------------------------------------
FUNCTION F_EXIST_BRDS_GTB_BOOK_SS(
    p_gtb          IN VARCHAR2,
    p_sourceSystem IN VARCHAR2)
  RETURN BOOLEAN
IS
  num_total NUMBER;
BEGIN
  IF ( p_gtb IS NOT NULL AND p_sourceSystem IS NOT NULL ) THEN
    RETURN false;
  ELSE
    RETURN true;
  END IF;
END F_EXIST_BRDS_GTB_BOOK_SS;
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
  FROM bh_validation_lookup
  WHERE bh_validation_lookup.return_code = code;
  RETURN return_msg || ' ';
EXCEPTION
WHEN NO_DATA_FOUND THEN
  RETURN '-1';
END F_GET_VALIDATION_MSG;
-- pattern should be yyyy-mm-dd
FUNCTION F_IS_DATE_OK(
    p_date VARCHAR2)
  RETURN CHAR
IS
  v_date        DATE;
  v_return_date CHAR := 'Y';
BEGIN
  BEGIN
    v_date := TO_DATE (p_date, 'YYYY-MM-DD');
  EXCEPTION
  WHEN OTHERS THEN
    v_return_date := 'N';
  END;
  RETURN v_return_date;
END F_IS_DATE_OK;
------------------------------------------------------------------------
--Depending on the date return if emergegy or not, or validation error
------------------------------------------------------------------------
FUNCTION IS_EMERGENCY_FLAG(
    v_date VARCHAR2)
  RETURN VARCHAR2
IS
  first_day  DATE;
  next_month DATE;
BEGIN
  SELECT TRUNC(sysdate, 'mm') INTO first_day FROM dual;
  SELECT TRUNC(last_day(sysdate))+1 INTO next_month FROM dual;
  IF to_date(v_date,'YYYY-MM-DD') = first_day THEN
    RETURN 'Y';
  ELSE
    IF to_date(v_date,'YYYY-MM-DD') = next_month THEN
      RETURN 'N';
    ELSE
      RETURN 'VALIDATION KO';
    END IF;
  END IF;
END IS_EMERGENCY_FLAG;
------------------------------------
-- VTD: GTB
-- p_process = (1 = upload),(2 = approval), ...
------------------------------------
FUNCTION F_IS_VALID_GTB_VTD(
    global_trader_book_id IN VARCHAR2,
    volcker_trading_desk  IN VARCHAR2,
    p_process             IN INT)
  RETURN INT
IS
  num_vtd INT;
  num_manual_vtd INT;
  v_result BOOLEAN;
  v_book_function_code CHAR(1);
  PROCESS_UPLOAD   CONSTANT INT := 1;
  PROCESS_APPROVAL CONSTANT INT := 2;
BEGIN
  -- A global_trader_book_id and volcker_trading_desk are entered as parameters
  -- MANUAL VTD's
  SELECT COUNT(*)
  INTO num_manual_vtd
  FROM BH_MANUAL_HIERARCHY_ELEMS h
  WHERE h.rplCode = volcker_trading_desk
  AND h.element_type = 'VTD';
  
  IF num_manual_vtd != 0 THEN
    RETURN 4;
  END IF;
  
  -- 1. volcker_trading_desk must exist as a valid rplCode on the brds_vw_hierarchy table:
  SELECT COUNT(*)
  INTO num_vtd
  FROM brds_vw_hierarchy h
  WHERE h.rplCode = volcker_trading_desk;
  IF num_vtd      = 0 THEN
    RETURN 1;
  END IF;
  
  -- Passed all validations:
  RETURN 0;
END F_IS_VALID_GTB_VTD;
------------------------------------
-- VTD: BOOK
------------------------------------
FUNCTION F_IS_VALID_BOOK_VTD(
    book_id              IN VARCHAR2,
    volcker_trading_desk IN VARCHAR2,
    p_process            IN INT)
  RETURN INT
IS
  num_vtd INT;
  num_manual_vtd INT;
  v_result BOOLEAN;
  v_book_function_code CHAR(1);
  PROCESS_UPLOAD   CONSTANT INT := 1;
  PROCESS_APPROVAL CONSTANT INT := 2;
BEGIN
  -- A book_id and volcker_trading_desk are entered as parameters
  -- MANUAL VTD's
  SELECT COUNT(*)
  INTO num_manual_vtd
  FROM BH_MANUAL_HIERARCHY_ELEMS mh
  WHERE mh.rplCode = volcker_trading_desk
  AND mh.element_type = 'VTD';
  IF num_manual_vtd != 0 THEN
    RETURN 4;
  END IF;
  
  -- 1. volcker_trading_desk must exist as a valid rplCode on the brds_vw_hierarchy table:
  SELECT COUNT(*)
  INTO num_vtd
  FROM brds_vw_hierarchy h
  WHERE h.rplCode = volcker_trading_desk;
  IF num_vtd      = 0 THEN
    RETURN 1;
  END IF;
  
  -- Passed all validations:
  RETURN 0;
END F_IS_VALID_BOOK_VTD;
------------------------------------
-- CRU: GTB
------------------------------------
FUNCTION F_IS_VALID_GTB_CRU(
    global_trader_book_id IN VARCHAR2,
    charge_reporting_unit IN VARCHAR2,
    p_process             IN INT)
  RETURN INT
IS
  num_cru INT;
  num_manual_cru INT;
  v_result         BOOLEAN;
  PROCESS_UPLOAD   CONSTANT INT := 1;
  PROCESS_APPROVAL CONSTANT INT := 2;
BEGIN
  -- MANUAL CRUs
  SELECT COUNT(*)
  INTO num_manual_cru
  FROM BH_MANUAL_HIERARCHY_ELEMS mh
  WHERE mh.rplCode   = charge_reporting_unit;
  IF num_manual_cru != 0 THEN
    RETURN 4;
  END IF;
  
  -- 1. change reporting unit must exist as a valid rplCode on the brds_vw_hierarchy table:
  SELECT COUNT(*)
  INTO num_cru
  FROM brds_vw_hierarchy h
  WHERE h.rplCode = charge_reporting_unit;
  IF num_cru      = 0 THEN
    RETURN 1;
  END IF;
  
  -- 2. gtb must exist as a node on the brds_vw_hierarchy table:
  v_result:= true;
  SELECT COUNT(*)
  INTO num_cru
  FROM brds_vw_hierarchy h
  WHERE h.nodeid        = global_trader_book_id
  AND upper(h.nodeType) = 'BOOK';
  IF num_cru            = 0 THEN
    --return 2;  -- this case never happens now
    v_result:= false;
  END IF;
  -- if exist book id on BRDs
  IF v_result AND p_process != PROCESS_UPLOAD AND p_process != PROCESS_APPROVAL THEN
    -- A global_trader_book_id and charge_reporting_unit are entered as parameters
    -- Generate the hierarchy for the book.
    -- Ensure that the CRU passed in occurs only once in the hierarchy and is flagged as a CRU.
    SELECT COUNT(*)
    INTO num_cru
    FROM
      (SELECT h.rplCode,
        h.chargeReportingUnit
      FROM brds_vw_hierarchy h
        START WITH h.nodeId           = global_trader_book_id
        CONNECT BY prior parentNodeId = nodeId
      ) h
    WHERE h.rplCode = charge_reporting_unit;
    IF num_cru     != 1 THEN
      RETURN 2;
    END IF;
  END IF;
  
  -- check uniqueness
  -- 4. Other nodes on the hierarchy of the other books must NOT be a CRU:
  SELECT COUNT(*)
  INTO num_cru
  FROM
    ( SELECT DISTINCT h.rplCode,
      h.nodeid
    FROM brds_vw_hierarchy h
      START WITH h.nodeid IN
      (
      /*list of all gtb nodes of change_reporting_unit, global_trader_book_id*/
      SELECT nodeid
      FROM
        (SELECT h.rplcode,
          h.nodeid,
          h.nodetype,
          CONNECT_BY_ISLEAF isleaf,
          level
        FROM brds_vw_hierarchy h
          CONNECT BY prior nodeId = parentNodeId
          START WITH h.rplcode    = charge_reporting_unit
          /*new cru*/
        )
      WHERE isleaf        = 1
      AND upper(nodetype) = 'BOOK'
      AND nodeid         != global_trader_book_id
        /*global_trader_book_id*/
      )
      CONNECT BY prior parentNodeId = nodeId
    ) h1,
    brds_vw_cru v
  WHERE h1.rplCode != charge_reporting_unit
    /*new cru*/
  AND h1.nodeId = v.chargeReportingUnit;
  IF num_cru   != 0 THEN
    RETURN 3;
  END IF;
  
  -- Passed all validations:
  RETURN 0;
END F_IS_VALID_GTB_CRU;
------------------------------------
-- CRU: BOOK
------------------------------------
FUNCTION F_IS_VALID_BOOK_CRU(
    book_id               IN VARCHAR2,
    charge_reporting_unit IN VARCHAR2,
    p_process             IN INT)
  RETURN INT
IS
  num_cru INT;
  num_manual_cru INT;
  v_result         BOOLEAN;
  PROCESS_UPLOAD   CONSTANT INT := 1;
  PROCESS_APPROVAL CONSTANT INT := 2;
BEGIN
  -- MANUAL CRU's
  SELECT COUNT(*)
  INTO num_manual_cru
  FROM BH_MANUAL_HIERARCHY_ELEMS mh
  WHERE mh.rplCode   = charge_reporting_unit;
  IF num_manual_cru != 0 THEN
    RETURN 4;
  END IF;
  
  -- 1. change reporting unit must exist as a valid rplCode on the brds_vw_hierarchy table:
  SELECT COUNT(*)
  INTO num_cru
  FROM brds_vw_hierarchy h
  WHERE h.rplCode = charge_reporting_unit;
  IF num_cru      = 0 THEN
    RETURN 1;
  END IF;
  
  -- 2. book id must exist as a node on the brds_vw_hierarchy table:
  v_result:= true;
  SELECT COUNT(*)
  INTO num_cru
  FROM brds_vw_hierarchy h
  WHERE h.nodename      = book_id
  AND upper(h.nodeType) = 'BOOK';
  IF num_cru            = 0 THEN
    --return 2;  -- this case never happens now
    v_result:= false;
  END IF;
  -- Other nodes on the hierarchy of the same book must NOT be a CRU:
  -- case when exist book id on bRDS
  IF v_result AND p_process != PROCESS_UPLOAD AND p_process != PROCESS_APPROVAL THEN
    SELECT COUNT(*)
    INTO num_cru
    FROM
      (SELECT h.rplCode,
        h.chargeReportingUnit
      FROM brds_vw_hierarchy h
        START WITH h.nodeId           = book_id
        CONNECT BY prior parentNodeId = nodeId
      ) h,
      brds_vw_cru u
    WHERE h.rplCode != charge_reporting_unit
    AND h.rplCode    = u.chargeReportingUnit;
    IF ( num_cru    != 0 ) THEN
      RETURN 2;
    END IF;
  END IF;
  
  -- check uniqueness
  -- 4. Other nodes on the hierarchy of the other books must NOT be a CRU:
  SELECT COUNT(*)
  INTO num_cru
  FROM
    ( SELECT DISTINCT h.rplCode,
      h.nodeid
    FROM brds_vw_hierarchy h
      START WITH h.nodeName IN
      (
      /*list of all books nodes of volcker_trading_desk, less the book_id */
      SELECT nodename
      FROM
        (SELECT h.rplcode,
          h.nodename,
          h.nodetype,
          CONNECT_BY_ISLEAF isleaf,
          level
        FROM brds_vw_hierarchy h
          CONNECT BY prior nodeId = parentNodeId
          START WITH h.rplcode    = charge_reporting_unit
        )
      WHERE isleaf        = 1
      AND upper(nodetype) = 'BOOK'
      AND nodename       != book_id
      )
      CONNECT BY prior parentNodeId = nodeId
    ) h1,
    brds_vw_cru v
  WHERE h1.rplCode != charge_reporting_unit
  AND --h1.rplCode = v.volckerTradingDesk;
    h1.nodeId = v.chargeReportingUnit;
  -- Error when exists someone
  IF num_cru != 0 THEN
    RETURN 3;
  END IF;
  
  -- Passed all validations:
  RETURN 0;
END F_IS_VALID_BOOK_CRU;
------------------------------------
-- CRP: GTB
------------------------------------
FUNCTION F_IS_VALID_GTB_CRP(
    global_trader_book_id   IN VARCHAR2,
    charge_reporting_parent IN VARCHAR2,
    p_process               IN INT)
  RETURN INT
IS
  num_crp INT;
  num_manual_crp INT;
  v_result         BOOLEAN;
  PROCESS_UPLOAD   CONSTANT INT := 1;
  PROCESS_APPROVAL CONSTANT INT := 2;
BEGIN
  -- MANUAL CRP's
  SELECT COUNT(*)
  INTO num_manual_crp
  FROM BH_MANUAL_HIERARCHY_ELEMS mh
  WHERE mh.rplCode   = charge_reporting_parent;
  IF num_manual_crp != 0 THEN
    RETURN 4;
  END IF;
  
  -- 1. change reporting parent must exist as a valid rplCode on the brds_vw_hierarchy table:
  SELECT COUNT(*)
  INTO num_crp
  FROM brds_vw_hierarchy h
  WHERE h.rplCode = charge_reporting_parent;
  IF num_crp      = 0 THEN
    RETURN 1;
  END IF;
  
  -- 2. gtb must exist as a node on the brds_vw_hierarchy table:
  v_result:= true;
  SELECT COUNT(*)
  INTO num_crp
  FROM brds_vw_hierarchy h
  WHERE h.nodeid        = global_trader_book_id
  AND upper(h.nodeType) = 'BOOK';
  IF num_crp            = 0 THEN
    --return 2;  -- this case never happens now
    v_result:= false;
  END IF;
  -- if exist book id on BRDs
  IF v_result AND p_process != PROCESS_UPLOAD AND p_process != PROCESS_APPROVAL THEN
    -- Ensure that the CRP passed in occurs only once in the hierarchy and is flagged as a CRP.
    SELECT COUNT(*)
    INTO num_crp
    FROM
      (SELECT h.rplCode,
        h.chargeReportingParent
      FROM brds_vw_hierarchy h
        START WITH h.nodeId           = global_trader_book_id
        CONNECT BY prior parentNodeId = nodeId
      ) h
    WHERE h.rplCode = charge_reporting_parent;
    IF num_crp     != 1 THEN
      RETURN 2;
    END IF;
  END IF;
  
  -- check uniqueness
  -- 4. Other nodes on the hierarchy of the other books must NOT be a CRP:
  SELECT COUNT(*)
  INTO num_crp
  FROM
    ( SELECT DISTINCT h.rplCode,
      h.nodeid
    FROM brds_vw_hierarchy h
      START WITH h.nodeid IN
      (
      /*list of all gtb nodes of change_reporting_parent, global_trader_book_id*/
      SELECT nodeid
      FROM
        (SELECT h.rplcode,
          h.nodeid,
          h.nodetype,
          CONNECT_BY_ISLEAF isleaf,
          level
        FROM brds_vw_hierarchy h
          CONNECT BY prior nodeId = parentNodeId
          START WITH h.rplcode    = charge_reporting_parent
          /*new cru*/
        )
      WHERE isleaf        = 1
      AND upper(nodetype) = 'BOOK'
      AND nodeid         != global_trader_book_id
        /*global_trader_book_id*/
      )
      CONNECT BY prior parentNodeId = nodeId
    ) h1,
    brds_vw_crp v
  WHERE h1.rplCode != charge_reporting_parent
    /*new crp*/
  AND h1.nodeId = v.chargeReportingParent;
  IF num_crp   != 0 THEN
    RETURN 3;
  END IF;
  
  -- Passed all validations:
  RETURN 0;
END F_IS_VALID_GTB_CRP;
------------------------------------
-- CRP: BOOK
------------------------------------
FUNCTION F_IS_VALID_BOOK_CRP(
    book_id                 IN VARCHAR2,
    charge_reporting_parent IN VARCHAR2,
    p_process               IN INT)
  RETURN INT
IS
  num_crp INT;
  num_manual_crp INT;
  v_result         BOOLEAN;
  PROCESS_UPLOAD   CONSTANT INT := 1;
  PROCESS_APPROVAL CONSTANT INT := 2;
BEGIN
  -- MANUAL CRP's
  SELECT COUNT(*)
  INTO num_manual_crp
  FROM BH_MANUAL_HIERARCHY_ELEMS mh
  WHERE mh.rplCode   = charge_reporting_parent;
  IF num_manual_crp != 0 THEN
    RETURN 4;
  END IF;
  
  -- 1. change reporting parent must exist as a valid rplCode on the brds_vw_hierarchy table:
  SELECT COUNT(*)
  INTO num_crp
  FROM brds_vw_hierarchy h
  WHERE h.rplCode = charge_reporting_parent;
  IF num_crp      = 0 THEN
    RETURN 1;
  END IF;
  
  -- 2. book id must exist as a node on the brds_vw_hierarchy table:
  v_result:= true;
  SELECT COUNT(*)
  INTO num_crp
  FROM brds_vw_hierarchy h
  WHERE h.nodeName      = book_id
  AND upper(h.nodeType) = 'BOOK';
  IF num_crp            = 0 THEN
    --return 2;  -- this case never happens now
    v_result:= false;
  END IF;
  -- if exist book id on BRDs
  IF v_result AND p_process != PROCESS_UPLOAD AND p_process != PROCESS_APPROVAL THEN
    -- Ensure that the CRP passed in occurs only once in the hierarchy
    SELECT COUNT(*)
    INTO num_crp
    FROM
      (SELECT h.rplCode,
        h.chargeReportingParent
      FROM brds_vw_hierarchy h
        START WITH h.nodeId           = book_id
        CONNECT BY prior parentNodeId = nodeId
      ) h
    WHERE h.rplCode = charge_reporting_parent;
    IF num_crp     != 1 THEN
      RETURN 2;
    END IF;
  END IF;
  
  -- check uniqueness
  -- 4. Other nodes on the hierarchy of the other books must NOT be a CRP:
  SELECT COUNT(*)
  INTO num_crp
  FROM
    ( SELECT DISTINCT h.rplCode,
      h.nodeid
    FROM brds_vw_hierarchy h
      START WITH h.nodeName IN
      (
      /*list of all books nodes of change_reporting_parent, less the book_id */
      SELECT nodename
      FROM
        (SELECT h.rplcode,
          h.nodename,
          h.nodetype,
          CONNECT_BY_ISLEAF isleaf,
          level
        FROM brds_vw_hierarchy h
          CONNECT BY prior nodeId = parentNodeId
          START WITH h.rplcode    = charge_reporting_parent
        )
      WHERE isleaf        = 1
      AND upper(nodetype) = 'BOOK'
      AND nodename       != book_id
      )
      CONNECT BY prior parentNodeId = nodeId
    ) h1,
    brds_vw_crp v
  WHERE h1.rplCode != charge_reporting_parent
  AND --h1.rplCode = v.volckerTradingDesk;
    h1.nodeId = v.chargeReportingParent;
  -- Error when exists someone
  IF num_crp != 0 THEN
    RETURN 3;
  END IF;
  
  -- Passed all validations:
  RETURN 0;
END F_IS_VALID_BOOK_CRP;

-- GBSVR-33754: Start 1: Remove redundant functions: f_is_valid_gtb_cfbu, f_is_valid_book_cfbu
-- GBSVR-33754: End 1: 

FUNCTION F_EXIST_BRDS_RPLCODE(
    p_rpl_code IN VARCHAR2)
  RETURN BOOLEAN
IS
  num_total NUMBER;
BEGIN
  SELECT COUNT(*)
  INTO num_total
  FROM brds_vw_hierarchy
  WHERE rplcode = p_rpl_code;
  IF num_total  > 0 THEN
    RETURN true;
  ELSE
    RETURN false;
  END IF;
END F_EXIST_BRDS_RPLCODE;

FUNCTION F_IS_EQUAL_BRDS_BOOK(
    p_gtb     IN VARCHAR2,
    p_book_id IN VARCHAR2)
  RETURN BOOLEAN
IS
  num_total NUMBER;
BEGIN
  SELECT COUNT(*)
  INTO num_total
  FROM brds_vw_book
  WHERE brds_vw_book.globaltraderbookid = p_gtb
  AND brds_vw_book.bookname             = p_book_id;
  IF num_total                          > 0 THEN
    RETURN true;
  ELSE
    RETURN false;
  END IF;
END F_IS_EQUAL_BRDS_BOOK;

PROCEDURE P_VALIDATION_LOAD_APPROVE(
    p_bh_intermediary IN BH_INTERMEDIARY%ROWTYPE,
    p_process         IN INT,
    p_validation_msg OUT VARCHAR2,
    p_cont_error OUT INTEGER)
IS
  validation_code      NUMBER;
  asofdate_staging     DATE;
  asofdate_rpl         DATE;
  b_validation_staging BOOLEAN;
  b_validation_rpl     BOOLEAN;
  b_validation         BOOLEAN;
  b_emergency_flag     BOOLEAN;
BEGIN
  p_cont_error                                  := 0;
  IF UPPER (p_bh_intermediary.emergency_flag)    = 'N' THEN
    b_emergency_flag                            := FALSE;
  ELSIF UPPER (p_bh_intermediary.emergency_flag) = 'Y' THEN
    b_emergency_flag                            := TRUE;
  END IF;
  BEGIN
    asofdate_rpl     := F_GET_DATE_EMERGENCY_FLAG(TRUE);
    asofdate_staging := F_GET_DATE_EMERGENCY_FLAG(FALSE);
    -------------- VALIDATION ACTIONS: ADD, DELETE, MODIFY  ------------------------------
    validation_code := NULL;
    -- validations for book not null and source system null into BOOK_HIERARCHY_RPL and BH_STAGING
    IF p_bh_intermediary.book_id IS NOT NULL AND p_bh_intermediary.source_system_id IS NULL THEN
      --start GBSVR-23237
      IF p_bh_intermediary.action_id IN (1, 2) THEN
        b_validation_rpl     := F_EXIST_RPL_BOOK (p_bh_intermediary.book_id, asofdate_rpl);
        b_validation_staging := F_EXIST_STA_BOOK (p_bh_intermediary.book_id, asofdate_staging);
      ELSE
        b_validation_rpl     := F_EXIST_RPL_BOOK_MANUAL(p_bh_intermediary.book_id, asofdate_rpl);
        b_validation_staging := F_EXIST_STA_BOOK_MANUAL(p_bh_intermediary.book_id, asofdate_staging);
      END IF;
      --end GBSVR-23237
      -- action ADD
      IF b_validation_rpl AND b_emergency_flag AND p_bh_intermediary.action_id                                 = 1 THEN
        validation_code                                                                                       := 2004;
      ELSIF b_validation_rpl AND b_validation_staging AND NOT b_emergency_flag AND p_bh_intermediary.action_id = 1 THEN
        validation_code                                                                                       := 2033;
        -- action MODIFY
        --missing only bh_rpl
      ELSIF NOT b_validation_rpl AND b_validation_staging AND p_bh_intermediary.action_id = 2 THEN
        validation_code                                                                  := 2003;
        --missing only bh_staging
      ELSIF b_validation_rpl AND NOT b_validation_staging AND p_bh_intermediary.action_id = 2 THEN
        validation_code                                                                  := 2013;
        --missing both bh_rpl and bh_staging
      ELSIF NOT b_validation_rpl AND NOT b_validation_staging AND p_bh_intermediary.action_id = 2 THEN
        validation_code                                                                      := 2031;
        -- action DELETE
      ELSIF NOT b_validation_rpl AND b_emergency_flag AND p_bh_intermediary.action_id                                  = 3 THEN
        validation_code                                                                                               := 2040;
      ELSIF NOT b_validation_rpl AND NOT b_validation_staging AND NOT b_emergency_flag AND p_bh_intermediary.action_id = 3 THEN
        validation_code                                                                                               := 2041;
      END IF;
      -- validations for both book and source system not null into BOOK_HIERARCHY_RPL and BH_STAGING
    ELSIF p_bh_intermediary.book_id IS NOT NULL AND p_bh_intermediary.source_system_id IS NOT NULL THEN
      IF p_bh_intermediary.action_id IN (1, 2) THEN
        b_validation_rpl     := F_EXIST_RPL_BOOK_AND_SS(p_bh_intermediary.book_id, p_bh_intermediary.source_system_id, asofdate_rpl);
        b_validation_staging := F_EXIST_STA_BOOK_AND_SS(p_bh_intermediary.book_id, p_bh_intermediary.source_system_id, asofdate_staging);
      ELSE
        b_validation_rpl     := F_EXIST_RPL_BOOK_AND_SS_MANUAL(p_bh_intermediary.book_id, p_bh_intermediary.source_system_id, asofdate_rpl);
        b_validation_staging := F_EXIST_STA_BOOK_AND_SS_MANUAL(p_bh_intermediary.book_id, p_bh_intermediary.source_system_id, asofdate_staging);
      END IF;
      -- action ADD
      IF b_validation_rpl AND b_emergency_flag AND p_bh_intermediary.action_id                                 = 1 THEN
        validation_code                                                                                       := 2002;
      ELSIF b_validation_rpl AND b_validation_staging AND NOT b_emergency_flag AND p_bh_intermediary.action_id = 1 THEN
        validation_code                                                                                       := 2034;
        -- action MODIFY
        --missing only bh_rpl
      ELSIF NOT b_validation_rpl AND b_validation_staging AND p_bh_intermediary.action_id = 2 THEN
        validation_code                                                                  := 2001;
        --missing only bh_staging
      ELSIF b_validation_rpl AND NOT b_validation_staging AND p_bh_intermediary.action_id = 2 THEN
        validation_code                                                                  := 2015;
        --missing both bh_rpl and bh_staging
      ELSIF NOT b_validation_rpl AND NOT b_validation_staging AND p_bh_intermediary.action_id = 2 THEN
        validation_code                                                                      := 2032;
        -- action DELETE
      ELSIF NOT b_validation_rpl AND b_emergency_flag AND p_bh_intermediary.action_id                                  = 3 THEN

validation_code                                                                                               := 2042;
      ELSIF NOT b_validation_rpl AND NOT b_validation_staging AND NOT b_emergency_flag AND p_bh_intermediary.action_id = 3 THEN
        validation_code                                                                                               := 2043;
      END IF;
    END IF;
    IF validation_code IS NOT NULL THEN
      p_validation_msg := p_validation_msg || F_GET_VALIDATION_MSG( validation_code);
      p_cont_error     := p_cont_error + 1;
    END IF;
  END;
  -------------------- END validate book and source system BOOK_HIERARCHY_RPL and BH_STAGING
  -----INIT FULL MATCH VALIDATION------------------------------------------------------------------------------
  IF p_bh_intermediary.action_id IN (1,2) THEN
    IF p_bh_intermediary.emergency_flag = 'N' THEN
      b_validation                     := F_IS_FULL_MATCH_STAGING(p_bh_intermediary, TRUE);
      IF b_validation THEN
        -- Error FULL MATCH
        validation_code  := 2038;
        p_validation_msg := p_validation_msg || F_GET_VALIDATION_MSG(validation_code);
        p_cont_error     := p_cont_error + 1;
      END IF;
    END IF;
  END IF;
  ------END FULL MATCH VALIDATION---------------------------------------------------------------------
  BEGIN
    --- ADD action and  Exist or not GTB:
    IF p_bh_intermediary.global_trader_book_id IS NOT NULL THEN -- ALL actions
      -- all action: error if gtb not exist in bRDS
      b_validation := F_EXIST_BRDS_GTB(p_bh_intermediary.global_trader_book_id);
      IF NOT b_validation THEN
        validation_code  := 2005;
        p_validation_msg := p_validation_msg || F_GET_VALIDATION_MSG( validation_code);
        p_cont_error     := p_cont_error + 1;
      END IF;
    END IF;
    -- Error if both gtbId and source_system_id are entered:
    b_validation := F_EXIST_BRDS_GTB_BOOK_SS(p_bh_intermediary.global_trader_book_id, p_bh_intermediary.source_system_id);
    IF NOT b_validation THEN
      validation_code  := 2044;
      p_validation_msg := p_validation_msg || PKG_BH_COMMONS.F_GET_VALIDATION_MSG(validation_code);
      p_cont_error     := p_cont_error + 1;
    END IF;
    IF p_bh_intermediary.global_trader_book_id IS NOT NULL AND p_bh_intermediary.action_id IN (1, 2) THEN --ADD, MODIFY
      -- The manual book should be equal than bRDS book
      b_validation := F_IS_EQUAL_BRDS_BOOK(p_bh_intermediary.global_trader_book_id, p_bh_intermediary.book_id);
      IF NOT b_validation THEN
        validation_code  := 2030;
        p_validation_msg := p_validation_msg || F_GET_VALIDATION_MSG( validation_code);
        p_cont_error     := p_cont_error + 1;
      END IF;
    END IF;
  END;
  -- end GTB
  ------------------------- BEGIN VTD---------------------------
  -- Validation Volcker Trader Desk: first taking GTB, if not Book id
  BEGIN
    -- action add and modify
    IF p_bh_intermediary.action_id IN (1, 2) AND (p_bh_intermediary.volcker_trading_desk IS NOT NULL AND (upper(p_bh_intermediary.volcker_trading_desk) != '00_EXCLUDE' AND upper(p_bh_intermediary.volcker_trading_desk) != '00_NO_MATCH')) THEN
      ------------------------ Valid Volcker Trading Desk
      IF p_bh_intermediary.global_trader_book_id IS NOT NULL THEN
        -- first step search by gtb
        CASE f_is_valid_gtb_vtd (p_bh_intermediary.global_trader_book_id, p_bh_intermediary.volcker_trading_desk, p_process)
        WHEN 1 THEN
          -- Error VTD must exist on bRDS.
          validation_code  := 2025;
          p_validation_msg := p_validation_msg || F_GET_VALIDATION_MSG(validation_code);
          p_cont_error     := p_cont_error + 1;
        ELSE
          NULL;
        END CASE;
      ELSE
        -- checking second case using book id when gtb is null
        CASE f_is_valid_book_vtd (p_bh_intermediary.book_id, p_bh_intermediary.volcker_trading_desk, p_process)
        WHEN 1 THEN
          -- Error VTD must exist on bRDS.
          validation_code  := 2025;
          p_validation_msg := p_validation_msg || F_GET_VALIDATION_MSG(validation_code);
          p_cont_error     := p_cont_error + 1;
        ELSE
          NULL;
        END CASE;
      END IF;
    END IF;
  END;
  -------------------------END BEGIN VTD---------------------------
  -- start 1: GBSVR-25923
  /*
  ---------------------------  Valid Change Reporting Unit
  BEGIN
  -- action add or modify
  IF p_bh_intermediary.action_id IN (1, 2) THEN
  -- start GBSVR-23353
  IF p_bh_intermediary.charge_reporting_unit_code IS NOT NULL AND (upper(p_bh_intermediary.charge_reporting_unit_code) != '00_EXCLUDE'
  AND upper(p_bh_intermediary.charge_reporting_unit_code) != '00_NO_MATCH') THEN
  -- end GBSVR-23353
  IF p_bh_intermediary.global_trader_book_id IS NOT NULL THEN
  CASE f_is_valid_gtb_cru(p_bh_intermediary.global_trader_book_id, p_bh_intermediary.charge_reporting_unit_code, p_process)
  WHEN 1 THEN
  --CRU must exist on brds_vw_cru
  validation_code := 2019;
  p_validation_msg := p_validation_msg || F_GET_VALIDATION_MSG(validation_code);
  p_cont_error := p_cont_error + 1;
  WHEN 2 THEN
  -- CRU must occur once and only once in the hierarchy
  validation_code := 2007;
  p_validation_msg := p_validation_msg || F_GET_VALIDATION_MSG(validation_code);
  p_cont_error := P_cont_error + 1;
  WHEN 3 THEN
  --Other nodes on the hierarchy of the other books must NOT have this CRU
  validation_code := 2020;
  p_validation_msg := p_validation_msg || F_GET_VALIDATION_MSG(validation_code);
  p_cont_error := p_cont_error + 1;
  ELSE
  NULL;
  END CASE;
  ELSE -- end gtb
  -- init book
  CASE f_is_valid_book_cru (p_bh_intermediary.book_id, p_bh_intermediary.charge_reporting_unit_code, p_process)
  WHEN 1 THEN
  --CRU must exist on brds_vw_cru
  validation_code := 2019;
  p_validation_msg := p_validation_msg || F_GET_VALIDATION_MSG(validation_code);
  p_cont_error := p_cont_error + 1;
  WHEN 2 THEN
  -- CRU must occur once and only once in the hierarchy
  validation_code := 2007;
  p_validation_msg := p_validation_msg || F_GET_VALIDATION_MSG(validation_code);
  p_cont_error := p_cont_error + 1;
  WHEN 3 THEN
  --Other nodes on the hierarchy of the other books must NOT have this CRU.
  validation_code := 2020;
  p_validation_msg := p_validation_msg || F_GET_VALIDATION_MSG(validation_code);
  p_cont_error := p_cont_error + 1;
  ELSE
  NULL;
  END CASE;
  END IF; --end if gtb
  END IF;    --end if cru
  END IF;       --end csv_true
  END;
  ---------------------------  Valid Change Reporting Parent
  BEGIN
  -- action add or modify
  IF p_bh_intermediary.action_id IN (1, 2) THEN
  -- start GBSVR-23353
  IF p_bh_intermediary.charge_reporting_parent_code IS NOT NULL AND (upper(p_bh_intermediary.charge_reporting_parent_code) != '00_EXCLUDE'
  AND upper(p_bh_intermediary.charge_reporting_parent_code) != '00_NO_MATCH') THEN
  -- end GBSVR-23353
  IF p_bh_intermediary.global_trader_book_id IS NOT NULL THEN
  CASE f_is_valid_gtb_crp(p_bh_intermediary.global_trader_book_id, p_bh_intermediary.charge_reporting_parent_code, p_process)
  WHEN 1 THEN
  --CRP must exist on brds
  validation_code := 2021;
  p_validation_msg := p_validation_msg || F_GET_VALIDATION_MSG( validation_code);
  p_cont_error := p_cont_error + 1;
  WHEN 2 THEN
  -- CRP must occur once and only once in the hierarchy
  validation_code := 2008;
  p_validation_msg := p_validation_msg || F_GET_VALIDATION_MSG(validation_code);
  p_cont_error := p_cont_error + 1;
  WHEN 3 THEN
  --Other nodes on the hierarchy of the other books must NOT have this CRP.
  validation_code := 2022;
  p_validation_msg := p_validation_msg || F_GET_VALIDATION_MSG(validation_code);
  p_cont_error := p_cont_error + 1;
  ELSE
  NULL;
  END CASE;
  ELSE  -- end crp
  -- init book
  CASE f_is_valid_book_crp(p_bh_intermediary.book_id, p_bh_intermediary.charge_reporting_parent_code, p_process)
  WHEN 1 THEN
  --CRP must exist on brds_vw_cru
  validation_code := 2021;
  p_validation_msg := p_validation_msg || F_GET_VALIDATION_MSG(validation_code);
  p_cont_error := p_cont_error + 1;
  WHEN 2 THEN
  -- CRP must occur once and only once in the hierarchy
  validation_code := 2008;
  p_validation_msg := p_validation_msg || F_GET_VALIDATION_MSG(validation_code);
  p_cont_error := p_cont_error + 1;
  WHEN 3 THEN
  --Other nodes on the hierarchy of the other books must NOT have this CRU.
  validation_code := 2022;
  p_validation_msg := p_validation_msg || F_GET_VALIDATION_MSG(validation_code);
  p_cont_error := p_cont_error + 1;
  ELSE
  NULL;
  END CASE;
  END IF;
  END IF;
  END IF;  -- end crp
  END;
  --------------------------  Valid Covered Fund business Unit
  BEGIN
  -- action add or modify
  IF p_bh_intermediary.action_id IN (1, 2) THEN
  -- start GBSVR-23353
  IF p_bh_intermediary.covered_funds_units IS NOT NULL AND (upper(p_bh_intermediary.covered_funds_units) != '00_EXCLUDE'
  AND upper(p_bh_intermediary.covered_funds_units) != '00_NO_MATCH') THEN
  -- end GBSVR-23353
  IF p_bh_intermediary.global_trader_book_id IS NOT NULL THEN
  CASE f_is_valid_gtb_cfbu(p_bh_intermediary.global_trader_book_id, p_bh_intermediary.covered_funds_units, p_process)
  WHEN 1 THEN
  --CFBU must exist on brds
  validation_code := 2021;
  p_validation_msg := p_validation_msg || F_GET_VALIDATION_MSG(validation_code);
  p_cont_error := p_cont_error + 1;
  WHEN 2 THEN
  -- CFBU must occur once and only once in the hierarchy
  validation_code := 2008;
  p_validation_msg := p_validation_msg || F_GET_VALIDATION_MSG(validation_code);
  p_cont_error := p_cont_error + 1;
  WHEN 3 THEN
  --Other nodes on the hierarchy of the other books must NOT have this CFBU.
  validation_code := 2022;
  p_validation_msg := p_validation_msg || F_GET_VALIDATION_MSG(validation_code);
  p_cont_error := p_cont_error + 1;
  ELSE
  NULL;
  END CASE;
  ELSE  -- end cfbu with gtb
  -- init with book
  CASE f_is_valid_book_cfbu(p_bh_intermediary.book_id, p_bh_intermediary.covered_funds_units, p_process)
  WHEN 1 THEN
  --CFBU must exist on brds_vw_cfbu
  validation_code := 2035;
  p_validation_msg := p_validation_msg || F_GET_VALIDATION_MSG(validation_code);
  p_cont_error := p_cont_error + 1;
  WHEN 2 THEN
  -- CFBU must occur once and only once in the hierarchy
  validation_code := 2036;
  p_validation_msg := p_validation_msg || F_GET_VALIDATION_MSG(validation_code);
  p_cont_error := p_cont_error + 1;
  WHEN 3 THEN
  --Other nodes on the hierarchy of the other books must NOT have this CFBU.
  validation_code := 2037;
  p_validation_msg := p_validation_msg || F_GET_VALIDATION_MSG(validation_code);
  p_cont_error := p_cont_error + 1;
  ELSE
  NULL;
  END CASE;
  END IF;
  END IF;
  END IF; -- end cfbu
  --------- validation exists business and sub-business
  -- action add or modify
  IF p_bh_intermediary.business IS NOT NULL AND p_bh_intermediary.action_id IN (1, 2) THEN
  -- all action: error if gtb not exist in bRDS
  b_validation:=F_EXIST_RPL_CODE_AND_IS_UBR(p_bh_intermediary.business); --- start / end GBSVR-21061
  IF NOT b_validation THEN
  -- Error Business must exist on bRDS.
  validation_code := 2023;
  p_validation_msg := p_validation_msg || F_GET_VALIDATION_MSG(validation_code);
  p_cont_error := p_cont_error + 1;
  END IF;
  b_validation:=F_EXIST_RPL_CODE_AND_IS_UBR(p_bh_intermediary.sub_business); --- start / end GBSVR-21061
  IF NOT b_validation THEN
  -- Error Sub-Business must exist on bRDS.
  validation_code := 2024;
  p_validation_msg := p_validation_msg || F_GET_VALIDATION_MSG(validation_code);
  p_cont_error := p_cont_error + 1;
  END IF;
  END IF;
  END;
  -- GBSVR-22716: Start 2:
  ------------------------- BEGIN Non-book Hierarchy dups validation ---------------------------
  BEGIN
  -- action add and modify
  IF p_bh_intermediary.action_id IN (1, 2) THEN
  IF p_bh_intermediary.global_trader_book_id IS NOT NULL THEN
  -- first step search by gtb
  CASE f_hierarchy_dups_gtb ( p_bh_intermediary.global_trader_book_id )
  WHEN 1 THEN
  -- Error dups detected:
  validation_code := 2045;
  p_validation_msg := p_validation_msg || F_GET_VALIDATION_MSG(validation_code);
  p_cont_error := p_cont_error + 1;
  ELSE
  NULL;
  END CASE;
  ELSE
  -- checking second case using book id when gtb is null
  CASE f_hierarchy_dups_book ( p_bh_intermediary.book_id )
  WHEN 1 THEN
  -- Error dups detected:
  validation_code := 2045;
  p_validation_msg := p_validation_msg || F_GET_VALIDATION_MSG(validation_code);
  p_cont_error := p_cont_error + 1;
  ELSE
  NULL;
  END CASE;
  END IF;
  END IF;
  END;
  ------------------------- END Non-book Hierarchy dups validation ---------------------------
  -- GBSVR-22716: End 2:
  */
  -- end 1: GBSVR-25923
END P_VALIDATION_LOAD_APPROVE;

FUNCTION F_IS_BRDS(DATA_SOURCE IN VARCHAR2) RETURN NUMBER
IS
BEGIN
  RETURN CASE WHEN F_IS_MANUAL(DATA_SOURCE) = 1 THEN
    0
  ELSE
    1
  END;
END F_IS_BRDS;

FUNCTION F_IS_MANUAL(DATA_SOURCE IN VARCHAR2) RETURN NUMBER
IS
BEGIN
  RETURN CASE WHEN DATA_SOURCE IS NULL OR UPPER(DATA_SOURCE) <> 'BRDS' THEN
    0
  ELSE
    1
  END;
END F_IS_MANUAL;

FUNCTION F_GET_BRDS_BOOK(p_gtb IN VARCHAR2) RETURN VARCHAR2
IS
  v_book_id VARCHAR2(255);
BEGIN
  SELECT BOOKNAME
  INTO v_book_id
  FROM brds_vw_book
  WHERE GLOBALTRADERBOOKID = p_gtb;
  RETURN v_book_id;
EXCEPTION
WHEN NO_DATA_FOUND THEN
  RETURN NULL;
END F_GET_BRDS_BOOK;

FUNCTION F_GET_MAX_ASOFDATE_BH_RPL RETURN DATE IS
  v_max_Asofdate DATE;
BEGIN
  SELECT MAX(asofdate) INTO v_max_asofdate FROM BOOK_HIERARCHY_RPL;
  RETURN v_max_asofdate;
END F_GET_MAX_ASOFDATE_BH_RPL;

FUNCTION F_IS_FULL_MATCH_RPL(
    p_intermediary_data BH_INTERMEDIARY%ROWTYPE)
  RETURN BOOLEAN
IS
  num_total NUMBER;
  p_intermediary_data_brds BH_INTERMEDIARY%ROWTYPE;
  v_asofdate DATE;
  v_book_id VARCHAR2(500);
BEGIN
  num_total:= 0;
  -- get current month
  v_asofdate := F_GET_DATE_EMERGENCY_FLAG(true);
  -- complete the necessaries data where are "null"
  IF p_intermediary_data.global_trader_book_id IS NOT NULL AND p_intermediary_data.book_id IS NULL THEN
    v_book_id                                  := F_GET_BRDS_BOOK(p_intermediary_data.global_trader_book_id);
  ELSE
    v_book_id:= p_intermediary_data.book_id;
  END IF;
  SELECT COUNT(*)
  INTO num_total
  FROM BOOK_HIERARCHY_RPL
  WHERE ASOFDATE                      = v_asofdate
  AND BOOK_ID                         = v_book_id
  AND (NVL(VOLCKER_TRADING_DESK, ' ') = NVL(p_intermediary_data.volcker_trading_desk, ' ')); -- it's mandatory
  IF num_total = 1 THEN
    RETURN true;
  ELSE
    RETURN false;
  END IF;
END F_IS_FULL_MATCH_RPL;

--start GBSVR-30255
FUNCTION F_IS_FULL_MATCH_STAGING(p_intermediary_data BH_INTERMEDIARY%ROWTYPE, p_use_active_flag IN BOOLEAN) RETURN BOOLEAN
IS
BEGIN
  RETURN F_IS_FULL_MATCH_STAGING(p_intermediary_data.book_id, p_intermediary_data.global_trader_book_id, p_intermediary_data.volcker_trading_desk, p_use_active_flag);
END F_IS_FULL_MATCH_STAGING;

FUNCTION F_IS_FULL_MATCH_STAGING(p_book_id IN VARCHAR2, p_global_trader_book_id IN VARCHAR2, p_volcker_trading_desk IN VARCHAR2, p_use_active_flag IN BOOLEAN) RETURN BOOLEAN
IS
  num_total NUMBER;
  p_intermediary_data_brds BH_INTERMEDIARY%ROWTYPE;
  v_asofdate DATE;
  v_book_id VARCHAR2(500);
BEGIN
  num_total:= 0;
  -- get next month
  v_asofdate := F_GET_DATE_EMERGENCY_FLAG(false);
  -- complete the necessaries data where are "null"
  IF p_global_trader_book_id IS NOT NULL AND p_book_id IS NULL THEN
    v_book_id                                  := F_GET_BRDS_BOOK(p_global_trader_book_id);
  ELSE
    v_book_id:= p_book_id;
  END IF;
  
  IF p_use_active_flag THEN
	  SELECT COUNT(*)
	  INTO num_total
	  FROM BH_STAGING
	  WHERE ASOFDATE                      = v_asofdate
	  AND BOOK_ID                         = v_book_id
	  AND (NVL(VOLCKER_TRADING_DESK, ' ') = NVL(p_volcker_trading_desk, ' ')) -- it's mandatory
	  AND UPPER(DATA_SOURCE) ='BRDS'
	  AND ACTIVE_FLAG        = 'Y';
	  IF num_total           = 1 THEN
	    RETURN true;
	  ELSE
	    RETURN false;
	  END IF;
  ELSE
	  SELECT COUNT(*)
	  INTO num_total
	  FROM BH_STAGING
	  WHERE ASOFDATE                      = v_asofdate
	  AND BOOK_ID                         = v_book_id
	  AND (NVL(VOLCKER_TRADING_DESK, ' ') = NVL(p_volcker_trading_desk, ' ')) -- it's mandatory
	  AND UPPER(DATA_SOURCE) ='BRDS';
	  IF num_total           = 1 THEN
	    RETURN true;
	  ELSE
	    RETURN false;
	  END IF;
  
  END IF;
END F_IS_FULL_MATCH_STAGING;
--end GBSVR-30255

FUNCTION F_CONVERT_REGION_AMER(p_region VARCHAR2) RETURN VARCHAR2
IS
  v_region VARCHAR2(255);
BEGIN
  IF UPPER(NVL(p_region, ' ')) != 'AMER' THEN
    v_region:=p_region;
  ELSE
    v_region:='AMERICAS';
  END IF;
  RETURN v_region;
END F_CONVERT_REGION_AMER;

FUNCTION F_CONVERT_TO_NULL(
    p_string_null VARCHAR2)
  RETURN VARCHAR2
IS
BEGIN
  IF UPPER(p_string_null) = 'NULL' THEN
    RETURN NULL;
  END IF;
  RETURN p_string_null;
END F_CONVERT_TO_NULL;

FUNCTION F_EXIST_RPL_BOOK_MANUAL(p_bookId   IN VARCHAR2, p_asofdate IN DATE) RETURN BOOLEAN
IS
  num_total NUMBER;
BEGIN
  SELECT COUNT(*)
  INTO num_total
  FROM book_hierarchy_rpl
  WHERE book_id                = p_bookId
  AND asofdate                 = p_asofdate
  AND source_system           IS NULL
  AND F_IS_MANUAL(data_source) = 0;
  IF num_total                 > 0 THEN
    RETURN true;
  ELSE
    RETURN false;
  END IF;
END F_EXIST_RPL_BOOK_MANUAL;

FUNCTION F_EXIST_RPL_BOOK_AND_SS_MANUAL(p_bookId IN VARCHAR2, p_sourceSystem IN VARCHAR2, p_asofdate IN DATE) RETURN BOOLEAN
IS
  num_total NUMBER;
BEGIN
  SELECT COUNT(*)
  INTO num_total
  FROM book_hierarchy_rpl
  WHERE book_id                = p_bookId
  AND asofdate                 = p_asofdate
  AND source_system            = p_sourceSystem
  AND F_IS_MANUAL(data_source) = 0;
  IF num_total                 > 0 THEN
    RETURN true;
  ELSE
    RETURN false;
  END IF;
END F_EXIST_RPL_BOOK_AND_SS_MANUAL;

FUNCTION F_EXIST_STA_BOOK_MANUAL(p_bookId IN VARCHAR2, p_asofdate IN DATE) RETURN BOOLEAN
IS
  num_total NUMBER;
BEGIN
  SELECT COUNT(*)
  INTO num_total
  FROM bh_staging
  WHERE bh_staging.book_id      = p_bookId
  AND bh_staging.asofdate       = p_asofdate
  AND bh_staging.source_system IS NULL
  -- GBSVR-31214: Start 1: 
  --AND bh_staging.active_flag    = 'Y'
  -- GBSVR-31214: End 1: 
  AND F_IS_MANUAL(data_source)  = 0;
  IF num_total                  > 0 THEN
    RETURN true;
  ELSE
    RETURN false;
  END IF;
END F_EXIST_STA_BOOK_MANUAL;

FUNCTION F_EXIST_STA_BOOK_AND_SS_MANUAL(p_bookId IN VARCHAR2, p_sourceSystem IN VARCHAR2, p_asofdate IN DATE) RETURN BOOLEAN
IS
  num_total NUMBER;
BEGIN
  SELECT COUNT(*)
  INTO num_total
  FROM bh_staging
  WHERE bh_staging.book_id     = p_bookId
  AND bh_staging.source_system = p_sourceSystem
  AND bh_staging.asofdate      = p_asofdate
  -- GBSVR-31214: Start 2: 
  --AND bh_staging.active_flag    = 'Y'
  -- GBSVR-31214: End 2:
  AND F_IS_MANUAL(data_source) = 0;
  IF num_total                 > 0 THEN
    RETURN true;
  ELSE
    RETURN false;
  END IF;
END F_EXIST_STA_BOOK_AND_SS_MANUAL;

FUNCTION F_EXIST_RPL_CODE_AND_IS_UBR(p_rpl_code IN VARCHAR2) RETURN BOOLEAN
IS
  num_total NUMBER;
BEGIN
  SELECT COUNT(*)
  INTO num_total
  FROM brds_vw_hierarchy
  WHERE rplcode = p_rpl_code
  AND NODETYPE  = 'UBR';
  IF num_total  > 0 THEN
    RETURN true;
  ELSE
    RETURN false;
  END IF;
END F_EXIST_RPL_CODE_AND_IS_UBR;

FUNCTION f_hierarchy_dups_gtb(p_gtb IN VARCHAR2)
  RETURN INT
IS
  num_total NUMBER;
BEGIN
  SELECT COUNT(*)
  INTO num_total
  FROM
    (SELECT h.nodeType,
      connect_by_root h.nodeId book,
      h.nodeId,
      h.nodeName,
      h.rplCode,
      level ubr_desk_level,
      0 num_desks,
      0 num_ubrs,
      h.volckerTradingDesk,
      h.chargeReportingUnit,
      h.chargeReportingParent
      -- GBSVR-33754: Start: CFBU decomissioning
      -- GBSVR-33754: End:   CFBU decomissioning
    FROM brds_vw_hierarchy h
      CONNECT BY prior parentNodeId = nodeId
      START WITH h.nodeId           = p_gtb
    ) h1
  WHERE h1.nodeId != p_gtb
  AND h1.nodeId   IN
    (SELECT vh.nodeId nodeId -- Duplicate nodeIds in the hierarchy that have different books
    FROM brds_vw_hierarchy vh
    WHERE nodeType != 'BOOK'
    GROUP BY vh.nodeId
    HAVING COUNT(DISTINCT NVL(vh.rplCode, ' ')) > 1
    );
  IF num_total > 1 THEN
    RETURN 1;
  ELSE
    RETURN 0;
  END IF;
END f_hierarchy_dups_gtb;

FUNCTION f_hierarchy_dups_book(p_book_id IN VARCHAR2) RETURN INT
IS
  num_total NUMBER;
BEGIN
  SELECT COUNT(*)
  INTO num_total
  FROM
    (SELECT h.nodeType,
      connect_by_root h.nodeId book,
      h.nodeId,
      h.nodeName,
      h.rplCode,
      level ubr_desk_level,
      0 num_desks,
      0 num_ubrs,
      h.volckerTradingDesk,
      h.chargeReportingUnit,
      h.chargeReportingParent
      -- GBSVR-33754: Start: CFBU decomissioning
      -- GBSVR-33754: End:   CFBU decomissioning
    FROM brds_vw_hierarchy h
      CONNECT BY prior parentNodeId = nodeId
      START WITH h.nodeName         = p_book_id
    ) h1
  WHERE h1.nodeName != p_book_id
  AND h1.nodeId     IN
    (SELECT vh.nodeId nodeId -- Duplicate nodeIds in the hierarchy that have different books
    FROM brds_vw_hierarchy vh
    WHERE nodeType != 'BOOK'
    GROUP BY vh.nodeId
    HAVING COUNT(DISTINCT NVL(vh.rplCode, ' ')) > 1
    );
  IF num_total > 1 THEN
    RETURN 1;
  ELSE
    RETURN 0;
  END IF;
END f_hierarchy_dups_book;

FUNCTION F_IS_BRDS_INTEGRATION_ACTIVE RETURN BOOLEAN
IS
  v_brds_activation NUMBER;
BEGIN
  SELECT activation
  INTO v_brds_activation
  FROM BH_BRDS_ACTIVATION
  WHERE ROWNUM         = 1;
  IF v_brds_activation = 1 THEN
    RETURN TRUE;
  ELSE
    RETURN FALSE;
  END IF;
END F_IS_BRDS_INTEGRATION_ACTIVE;


END PKG_BH_COMMONS;

