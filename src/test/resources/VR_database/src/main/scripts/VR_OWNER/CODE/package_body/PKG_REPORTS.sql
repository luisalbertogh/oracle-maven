--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_BODY_PKG_REPORTS runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE stripComments:FALSE


  CREATE OR REPLACE PACKAGE BODY "PKG_REPORTS" 
AS
  -----------------------------------------------------------------------------
  -- Functionality: Get conversion rate for CRP
  -- Used:
  ------------------------------------------------------------------------------
  FUNCTION F_CONVERT_RATE_CRP(
      A_NUMBER IN NUMBER,
      A_DATE   IN FX_RATES.ASOFDATE%TYPE,
      A_CRP    IN BOOK_HIERARCHY_RPL.CHARGE_REPORTING_PARENT_CODE%TYPE)
    RETURN NUMBER
  IS
    v_spot NUMBER(30,6);
    v_curr VARCHAR2(3);
    v_reg  VARCHAR2(10);
  BEGIN
    BEGIN
      IF (A_NUMBER IS NULL OR A_NUMBER = 0 ) THEN
        RETURN 0;
      END IF;
      SELECT currency,region INTO v_curr,v_reg FROM REPORT_CRP WHERE CRP=A_CRP;
      SELECT spot
      INTO v_spot
      FROM fx_rates
      WHERE asofdate =A_DATE
      AND region_id  =v_reg
      AND currency_id=v_curr;
      RETURN A_NUMBER/v_spot;
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
      SELECT value INTO v_curr FROM REPORT_PARAM WHERE ID='CRP_CURR';
      SELECT value INTO v_reg FROM REPORT_PARAM WHERE ID='CRP_REG';
      SELECT spot
      INTO v_spot
      FROM fx_rates
      WHERE asofdate =A_DATE
      AND region_id  =v_reg
      AND currency_id=v_curr;
      RETURN A_NUMBER/v_spot;
    WHEN OTHERS THEN
      RETURN 0;
      RAISE;
    END;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN 0;
    RAISE;
  END F_CONVERT_RATE_CRP;
-----------------------------------------------------------------------------
-- Functionality: Get conversion rate for CRP in thousands
-- Used:
------------------------------------------------------------------------------
  FUNCTION F_C_RATE_CRPT(
      A_NUMBER IN NUMBER,
      A_DATE   IN FX_RATES.ASOFDATE%TYPE,
      A_CRP    IN BOOK_HIERARCHY_RPL.CHARGE_REPORTING_PARENT_CODE%TYPE)
    RETURN NUMBER
  IS
  BEGIN
    RETURN (F_CONVERT_RATE_CRP(A_NUMBER,A_DATE,A_CRP))/1000;
  END F_C_RATE_CRPT;
-----------------------------------------------------------------------------
-- Functionality: Get conversion rate for CRP in millions
-- Used:
------------------------------------------------------------------------------
  FUNCTION F_C_RATE_CRPM(
      A_NUMBER IN NUMBER,
      A_DATE   IN FX_RATES.ASOFDATE%TYPE,
      A_CRP    IN BOOK_HIERARCHY_RPL.CHARGE_REPORTING_PARENT_CODE%TYPE)
    RETURN NUMBER
  IS
  BEGIN
    RETURN F_CONVERT_RATE_CRP(A_NUMBER,A_DATE,A_CRP)/1000000;
  END F_C_RATE_CRPM;
----------------
-----------------------------------------------------------------------------
-- Functionality: Get conversion rate for VTD
-- Used:
------------------------------------------------------------------------------
  FUNCTION F_CONVERT_RATE_VTD(
      A_NUMBER IN NUMBER,
      A_DATE   IN FX_RATES.ASOFDATE%TYPE)
    RETURN NUMBER
  IS
    v_spot NUMBER(30,6);
    v_curr VARCHAR2(3);
    v_reg  VARCHAR2(10);
  BEGIN
    IF (A_NUMBER IS NULL OR A_NUMBER = 0 ) THEN
      RETURN 0;
    END IF;
    SELECT value INTO v_curr FROM REPORT_PARAM WHERE ID='VTD_CURR';
    SELECT value INTO v_reg FROM REPORT_PARAM WHERE ID='VTD_REG';
    SELECT spot
    INTO v_spot
    FROM fx_rates
    WHERE asofdate =A_DATE
    AND region_id  =v_reg
    AND currency_id=v_curr;
    RETURN A_NUMBER/v_spot;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN 0;
    RAISE;
  END F_CONVERT_RATE_VTD;
-----------------------------------------------------------------------------
-- Functionality: Get conversion rate for VTD
-- Used:
------------------------------------------------------------------------------
  FUNCTION F_CONVERT_RATE_VTD(
      A_NUMBER IN NUMBER,
      A_DATE   IN FX_RATES.ASOFDATE%TYPE,
      A_REGION IN FX_RATES.REGION_ID%TYPE)
    RETURN NUMBER
  IS
    v_spot NUMBER(30,6);
    v_curr VARCHAR2(3);
    --v_reg  VARCHAR2(10);
  BEGIN
    IF (A_NUMBER IS NULL OR A_NUMBER = 0 ) THEN
      RETURN 0;
    END IF;
    SELECT value INTO v_curr FROM REPORT_PARAM WHERE ID='VTD_CURR';
    --SELECT value INTO v_reg FROM REPORT_PARAM WHERE ID='VTD_REG';
    SELECT spot
    INTO v_spot
    FROM fx_rates
    WHERE asofdate =A_DATE
    AND region_id  =A_REGION
    AND currency_id=v_curr;
    RETURN A_NUMBER/v_spot;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN 0;
    RAISE;
  END F_CONVERT_RATE_VTD;
-----------------------------------------------------------------------------
-- Functionality: Get conversion rate for VTD in thousands
-- Used:
------------------------------------------------------------------------------
  FUNCTION F_C_RATE_VTDT(
      A_NUMBER IN NUMBER,
      A_DATE   IN FX_RATES.ASOFDATE%TYPE)
    RETURN NUMBER
  IS
  BEGIN
    RETURN (F_CONVERT_RATE_VTD(A_NUMBER,A_DATE))/1000;
  END F_C_RATE_VTDT;
-----------------------------------------------------------------------------
-- Functionality: Get conversion rate for VTD in thousands
-- Used:
------------------------------------------------------------------------------
  FUNCTION F_C_RATE_VTDT(
      A_NUMBER IN NUMBER,
      A_DATE   IN FX_RATES.ASOFDATE%TYPE,
      A_REGION IN FX_RATES.REGION_ID%TYPE)
    RETURN NUMBER
  IS
  BEGIN
    RETURN (F_CONVERT_RATE_VTD(A_NUMBER,A_DATE,A_REGION))/1000;
  END F_C_RATE_VTDT;
-----------------------------------------------------------------------------
-- Functionality: Get conversion rate for VTD in million
-- Used:
------------------------------------------------------------------------------
  FUNCTION F_C_RATE_VTDM(
      A_NUMBER IN NUMBER,
      A_DATE   IN FX_RATES.ASOFDATE%TYPE)
    RETURN NUMBER
  IS
  BEGIN
    RETURN (F_CONVERT_RATE_VTD(A_NUMBER,A_DATE))/1000000;
  END F_C_RATE_VTDM;
-----------------------------------------------------------------------------
-- Functionality: Get conversion rate for VTD in million
-- Used:
------------------------------------------------------------------------------
  FUNCTION F_C_RATE_VTDM(
      A_NUMBER IN NUMBER,
      A_DATE   IN FX_RATES.ASOFDATE%TYPE,
      A_REGION IN FX_RATES.REGION_ID%TYPE)
    RETURN NUMBER
  IS
  BEGIN
    RETURN (F_CONVERT_RATE_VTD(A_NUMBER,A_DATE,A_REGION))/1000000;
  END F_C_RATE_VTDM;
-----------------------------------------------------------------------------
-- Functionality: Get currency for CRP
-- Used:
------------------------------------------------------------------------------
  FUNCTION F_GET_CURRENCY_CRP(
      A_CRP IN BOOK_HIERARCHY_RPL.CHARGE_REPORTING_PARENT_CODE%TYPE)
    RETURN VARCHAR2
  IS
    v_curr VARCHAR2(3);
  BEGIN
    BEGIN
      SELECT currency INTO v_curr FROM REPORT_CRP WHERE CRP=A_CRP;
      RETURN v_curr;
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
      SELECT value INTO v_curr FROM REPORT_PARAM WHERE ID='CRP_CURR';
      RETURN v_curr;
    WHEN OTHERS THEN
      RETURN 'ERR';
      RAISE;
    END;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN 'ERR';
    RAISE;
  END F_GET_CURRENCY_CRP;
-----------------------------------------------------------------------------
-- Functionality: Get currency for VTD
-- Used:
------------------------------------------------------------------------------
  FUNCTION F_GET_CURRENCY_VTD
    RETURN VARCHAR2
  IS
    v_curr VARCHAR2(3);
  BEGIN
    SELECT value INTO v_curr FROM REPORT_PARAM WHERE ID='VTD_CURR';
    RETURN v_curr;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN 'ERR';
    RAISE;
  END F_GET_CURRENCY_VTD;
------------------------------------------------------------------------
-- Functionality: Get the ASOFDATE
-- DEPRECATED
------------------------------------------------------------------------
  FUNCTION PR_GET_ASOFDATE
    RETURN VARCHAR2
  IS
    p_age_date source_system.age_hadoop_lastload%type;
    p_date VARCHAR2(25);
  BEGIN
    SELECT value INTO p_date FROM REPORT_PARAM WHERE id='MANUAL';
    IF(p_date IS NOT NULL) THEN
      RETURN p_date;
      --p_age_date:=to_date(p_date,'yyyyMMdd');
    ELSE
      SELECT sysdate INTO p_age_date FROM dual;
      p_date:=TO_CHAR(p_age_date,'yyyyMMdd');
    END IF;
    RETURN p_date;
  EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.put_line ('DATE: ' || p_date);
    RAISE;
  END PR_GET_ASOFDATE;
------------------------------------------------------------------------
-- Functionality: Get the ASOFDATE
------------------------------------------------------------------------
  FUNCTION F_MIN_HADOOP_LASTDATE(
      A_METRIC_ID IN SOURCE_SYSTEM_METRIC.METRIC_ID%TYPE)
    RETURN DATE
  IS
    p_min_hadoop_lastdate source_system_metric.age_hadoop_lastload%type;
    p_ss VARCHAR2(25);
  BEGIN
    SELECT MIN(age_hadoop_lastload)
    INTO p_min_hadoop_lastdate
    FROM SOURCE_SYSTEM_METRIC
    WHERE metric_id           =A_METRIC_ID
    AND source_system_id NOT IN
      (SELECT NVL(exempt_ss,'NONE')
      FROM
        ( SELECT DISTINCT regexp_substr(value,'[^,]+', 1, level) AS exempt_ss
        FROM report_param
        WHERE id                                              ='EXEMPT_SS'
          CONNECT BY regexp_substr(value, '[^,]+', 1, level) IS NOT NULL
        )
      );
    RETURN p_min_hadoop_lastdate;
  EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.put_line ('DATE: ' || p_min_hadoop_lastdate);
    RAISE;
  END F_MIN_HADOOP_LASTDATE;
-----------------------------------------------------------------------------
-- Functionality: Get date monthly
-- Used:
------------------------------------------------------------------------------
  FUNCTION F_GET_COMPARISON_MONTH(
      A_ASOFDATE      IN DATE,
      A_COMP          IN NUMBER,
      A_NUMBER_MONTHS IN NUMBER)
    RETURN DATE
  IS
    p_age_date source_system.age_hadoop_lastload%type;
    p_date_month source_system.age_hadoop_lastload%type;
    p_asofdate source_system.age_hadoop_lastload%type;
    p_end_day source_system.age_hadoop_lastload%type;
    p_day_of_week              NUMBER;
    p_s_comparison_oldest_date VARCHAR2(8);
    p_comparison_oldest_date source_system.age_hadoop_lastload%type;
  BEGIN
    p_date_month:=add_months(A_ASOFDATE, A_NUMBER_MONTHS);
    --Get the last day from the month
    SELECT last_day(p_date_month)
    INTO p_end_day
    FROM dual;
    -- Get the day of week
    SELECT TO_CHAR(p_end_day, 'D')
    INTO p_day_of_week
    FROM dual;
    IF(p_day_of_week = 6) THEN
      --If the day is Saturday, substract 1 day
      SELECT TRUNC(p_end_day - 1)
      INTO p_asofdate
      FROM dual;
    ELSIF(p_day_of_week = 7) THEN
      --If the day is Sunday, substract 2 days
      SELECT TRUNC(p_end_day - 2)
      INTO p_asofdate
      FROM dual;
    ELSE
      p_asofdate:=p_end_day;
    END IF;
    --Get comparison Limit
    SELECT value
    INTO p_s_comparison_oldest_date
    FROM REPORT_PARAM
    WHERE ID                        ='COMPARISON_OLD_DATE';
    IF (p_s_comparison_oldest_date IS NOT NULL) THEN
      p_comparison_oldest_date     :=to_date(p_s_comparison_oldest_date,'yyyyMMdd');
    ELSE
      IF (A_COMP = 1) THEN
        RETURN NULL;
      ELSE
        RETURN p_asofdate;
      END IF;
    END IF;
    --set asofdate to null if we are checking age_comparison_der and asofdate > threshold
    IF (A_COMP   = 1 AND p_asofdate > p_comparison_oldest_date) THEN
      p_asofdate:=NULL;
    END IF;
    --set asofdate to null if we are checking age and asofdate < threshold
    IF (A_COMP   = 0 AND p_asofdate < p_comparison_oldest_date) THEN
      p_asofdate:=NULL;
    END IF;
    RETURN p_asofdate;
  EXCEPTION
  WHEN NO_DATA_FOUND THEN
    IF (A_COMP = 1) THEN
      RETURN NULL;
    ELSE
      RETURN p_asofdate;
    END IF;
  WHEN OTHERS THEN
    DBMS_OUTPUT.put_line ('NUMBER MONTHS: ' || A_NUMBER_MONTHS);
    RAISE;
  END F_GET_COMPARISON_MONTH;
-----------------------------------------------------------------------------
-- Functionality: Get fx_rates CONVERSION
-- Used:
------------------------------------------------------------------------------
  FUNCTION F_GET_FX_RATE(
      A_FROM_CCY IN FX_RATES.CURRENCY_ID%TYPE,
      A_TO_CCY   IN FX_RATES.CURRENCY_ID%TYPE,
      A_REGION   IN FX_RATES.REGION_ID%TYPE,
      A_ASOFDATE IN FX_RATES.ASOFDATE%TYPE)
    RETURN NUMBER
  IS
    p_from_usd NUMBER;
    p_to_usd   NUMBER;
  BEGIN
    SELECT spot
    INTO p_from_usd
    FROM fx_rates
    WHERE currency_id=A_FROM_CCY
    AND region_id    =A_REGION
    AND asofdate     =A_ASOFDATE;
    SELECT spot
    INTO p_to_usd
    FROM fx_rates
    WHERE currency_id=A_TO_CCY
    AND region_id    =A_REGION
    AND asofdate     =A_ASOFDATE;
    RETURN ROUND(p_from_usd/p_to_usd,6);
  EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.put_line ('A_FROM_CCY: ' || A_FROM_CCY);
    RAISE;
  END F_GET_FX_RATE;
-----------------------------------------------------------------------------
-- Functionality: Get region for CRP
-- Used:
------------------------------------------------------------------------------
  FUNCTION F_GET_REGION_CRP(
      A_CRP IN BOOK_HIERARCHY_RPL.CHARGE_REPORTING_PARENT%TYPE)
    RETURN VARCHAR2
  IS
    v_reg VARCHAR2(10);
  BEGIN
    BEGIN
      SELECT region INTO v_reg FROM REPORT_CRP WHERE CRP=A_CRP;
      RETURN v_reg;
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
      SELECT value INTO v_reg FROM REPORT_PARAM WHERE ID='CRP_REG';
      RETURN v_reg;
    WHEN OTHERS THEN
      RETURN 'ERR';
      RAISE;
    END;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN 'ERR';
    RAISE;
  END F_GET_REGION_CRP;
-----------------------------------------------------------------------------
-- Functionality: Get region for VTD
-- Used:
------------------------------------------------------------------------------
  FUNCTION F_GET_REGION_VTD
    RETURN VARCHAR2
  IS
    v_reg VARCHAR2(10);
  BEGIN
    SELECT value INTO v_reg FROM REPORT_PARAM WHERE ID='VTD_REG';
    RETURN v_reg;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN 'ERR';
    RAISE;
  END F_GET_REGION_VTD;
  PROCEDURE PR_GET_FXRATE_COMP_CASH
  IS
    A_PRICE age_comparison_der.PRICE%TYPE;
    A_QUANTITY age_comparison_der.QUANTITY%TYPE;
    A_MARKET_VALUE age_comparison_der.MARKET_VALUE%TYPE;
    A_B0TO30_QTY age_comparison_der.B0TO30_QTY%TYPE;
    A_B31TO60_QTY age_comparison_der.B31TO60_QTY%TYPE;
    A_B61TO90_QTY age_comparison_der.B61TO90_QTY%TYPE;
    A_B91TO180_QTY age_comparison_der.B91TO180_QTY%TYPE;
    A_B181TO360_QTY age_comparison_der.B181TO360_QTY%TYPE;
    A_B361_QTY age_comparison_der.B361_QTY%TYPE;
    A_B0TO30_MV age_comparison_der.B0TO30_MV%TYPE;
    A_B31TO60_MV age_comparison_der.B31TO60_MV%TYPE;
    A_B61TO90_MV age_comparison_der.B61TO90_MV%TYPE;
    A_B91TO180_MV age_comparison_der.B91TO180_MV%TYPE;
    A_B181TO360_MV age_comparison_der.B181TO360_MV%TYPE;
    A_B361_MV age_comparison_der.B361_MV%TYPE;
    A_CHRG_B0TO30 age_comparison_der.CHRG_B0TO30%TYPE;
    A_CHRG_B31TO60 age_comparison_der.CHRG_B31TO60%TYPE;
    A_CHRG_B61TO90 age_comparison_der.CHRG_B61TO90%TYPE;
    A_CHRG_B91TO180 age_comparison_der.CHRG_B91TO180%TYPE;
    A_CHRG_B181TO360 age_comparison_der.CHRG_B181TO360%TYPE;
    A_CHRG_B361 age_comparison_der.CHRG_B361%TYPE;
    A_EXMP_B0TO30 age_comparison_der.EXMP_B0TO30%TYPE;
    A_EXMP_B31TO60 age_comparison_der.EXMP_B31TO60%TYPE;
    A_EXMP_B61TO90 age_comparison_der.EXMP_B61TO90%TYPE;
    A_EXMP_B91TO180 age_comparison_der.EXMP_B91TO180%TYPE;
    A_EXMP_B181TO360 age_comparison_der.EXMP_B181TO360%TYPE;
    A_EXMP_B361 age_comparison_der.EXMP_B361%TYPE;
    A_FINAL_B0TO30 age_comparison_der.FINAL_B0TO30%TYPE;
    A_FINAL_B31TO60 age_comparison_der.FINAL_B31TO60%TYPE;
    A_FINAL_B61TO90 age_comparison_der.FINAL_B61TO90%TYPE;
    A_FINAL_B91TO180 age_comparison_der.FINAL_B91TO180%TYPE;
    A_FINAL_B181TO360 age_comparison_der.FINAL_B181TO360%TYPE;
    A_FINAL_B361 age_comparison_der.FINAL_B361%TYPE;
    A_CLEAN_PRICE age_comparison_der.CLEAN_PRICE%TYPE;
    A_CLEAN_MARKET_VALUE age_comparison_der.CLEAN_MARKET_VALUE%TYPE;
    A_ACCRUED_INTEREST age_comparison_der.ACCRUED_INTEREST%TYPE;
    p_FX_RATES NUMBER;
    p_cont     NUMBER:=0;
    CURSOR age_comparison_cash_t
    IS
      SELECT ag.ASOFDATE,
        ag.SOURCE_SYSTEM_ID,
        ag.INSTRUMENT_ID,
        ag.INSTRUMENT_TYPE,
        ag.BOOK_ID,
        ag.PRICE,
        ag.QUANTITY,
        ag.FACTOR,
        ag.FX_CURRENCY_ID,
        ag.NOTIONAL_CURRENCY_ID,
        ag.MARKET_VALUE,
        ag.MARKET_VALUE_USD,
        ag.LONG_SHORT,
        ag.PRODUCT_TYPE,
        ag.LEGAL_ENTITY_ID,
        ag.B0TO30_QTY,
        ag.B31TO60_QTY,
        ag.B61TO90_QTY,
        ag.B91TO180_QTY,
        ag.B181TO360_QTY,
        ag.B361_QTY,
        ag.B0TO30_MV,
        ag.B31TO60_MV,
        ag.B61TO90_MV,
        ag.B91TO180_MV,
        ag.B181TO360_MV,
        ag.B361_MV,
        ag.INSTRUMENT_ID_LEVEL2,
        ag.INSTRUMENT_TYPE_LEVEL2,
        ag.INSTRUMENT_ID_LEVEL3,
        ag.INSTRUMENT_TYPE_LEVEL3,
        ag.INSTRUMENT_ID_LEVEL4,
        ag.INSTRUMENT_TYPE_LEVEL4,
        ag.INSTRUMENT_ID_LEVEL5,
        ag.INSTRUMENT_TYPE_LEVEL5,
        ag.EXEC_MP_FLG,
        ag.EXEC_CA_FLG,
        ag.EXEC_DB_FGL,
        ag.CLEAN_PRICE,
        ag.CLEAN_MARKET_VALUE,
        ag.ACCRUED_INTEREST,
        ag.HAS_CP_FLG,
        ag.HAS_CMV_FLG,
        ag.HAS_AI_FLG,
        ag.INSTRUMENT_DESCRIPTION,
        ag.CHRG_ID,
        ag.CHRG_B0TO30,
        ag.CHRG_B31TO60,
        ag.CHRG_B61TO90,
        ag.CHRG_B91TO180,
        ag.CHRG_B181TO360,
        ag.CHRG_B361,
        ag.EXMP_ID,
        ag.EXMP_B0TO30,
        ag.EXMP_B31TO60,
        ag.EXMP_B61TO90,
        ag.EXMP_B91TO180,
        ag.EXMP_B181TO360,
        ag.EXMP_B361,
        ag.FINAL_B0TO30,
        ag.FINAL_B31TO60,
        ag.FINAL_B61TO90,
        ag.FINAL_B91TO180,
        ag.FINAL_B181TO360,
        ag.FINAL_B361,
        ag.ORPHAN_FLG,
        ag.DUPLICATED_FLG,
        ag.CHARGE_THRESHOLD_FLG,
        ag.NUM_TRADES,
        ss.region_id
      FROM age_comparison_cash_t ag,
        source_system ss
      WHERE ag.source_system_id = ss.source_system_id;
  BEGIN
    FOR age_comparison_index IN age_comparison_cash_t
    LOOP
      IF age_comparison_index.notional_currency_id = 'USD' THEN
        INSERT
        INTO age_comparison_der VALUES
          (
            age_comparison_index.ASOFDATE,
            age_comparison_index.SOURCE_SYSTEM_ID,
            age_comparison_index.INSTRUMENT_ID,
            age_comparison_index.INSTRUMENT_TYPE,
            age_comparison_index.BOOK_ID,
            age_comparison_index.PRICE,
            age_comparison_index.QUANTITY,
            age_comparison_index.FACTOR,
            age_comparison_index.FX_CURRENCY_ID,
            age_comparison_index.NOTIONAL_CURRENCY_ID,
            age_comparison_index.MARKET_VALUE,
            age_comparison_index.MARKET_VALUE_USD,
            age_comparison_index.LONG_SHORT,
            age_comparison_index.PRODUCT_TYPE,
            age_comparison_index.LEGAL_ENTITY_ID,
            age_comparison_index.B0TO30_QTY,
            age_comparison_index.B31TO60_QTY,
            age_comparison_index.B61TO90_QTY,
            age_comparison_index.B91TO180_QTY,
            age_comparison_index.B181TO360_QTY,
            age_comparison_index.B361_QTY,
            age_comparison_index.B0TO30_MV,
            age_comparison_index.B31TO60_MV,
            age_comparison_index.B61TO90_MV,
            age_comparison_index.B91TO180_MV,
            age_comparison_index.B181TO360_MV,
            age_comparison_index.B361_MV,
            age_comparison_index.INSTRUMENT_ID_LEVEL2,
            age_comparison_index.INSTRUMENT_TYPE_LEVEL2,
            age_comparison_index.INSTRUMENT_ID_LEVEL3,
            age_comparison_index.INSTRUMENT_TYPE_LEVEL3,
            age_comparison_index.INSTRUMENT_ID_LEVEL4,
            age_comparison_index.INSTRUMENT_TYPE_LEVEL4,
            age_comparison_index.INSTRUMENT_ID_LEVEL5,
            age_comparison_index.INSTRUMENT_TYPE_LEVEL5,
            age_comparison_index.EXEC_MP_FLG,
            age_comparison_index.EXEC_CA_FLG,
            age_comparison_index.EXEC_DB_FGL,
            age_comparison_index.CLEAN_PRICE,
            age_comparison_index.CLEAN_MARKET_VALUE,
            age_comparison_index.ACCRUED_INTEREST,
            age_comparison_index.HAS_CP_FLG,
            age_comparison_index.HAS_CMV_FLG,
            age_comparison_index.HAS_AI_FLG,
            age_comparison_index.INSTRUMENT_DESCRIPTION,
            age_comparison_index.CHRG_ID,
            age_comparison_index.CHRG_B0TO30,
            age_comparison_index.CHRG_B31TO60,
            age_comparison_index.CHRG_B61TO90,
            age_comparison_index.CHRG_B91TO180,
            age_comparison_index.CHRG_B181TO360,
            age_comparison_index.CHRG_B361,
            age_comparison_index.EXMP_ID,
            age_comparison_index.EXMP_B0TO30,
            age_comparison_index.EXMP_B31TO60,
            age_comparison_index.EXMP_B61TO90,
            age_comparison_index.EXMP_B91TO180,
            age_comparison_index.EXMP_B181TO360,
            age_comparison_index.EXMP_B361,
            age_comparison_index.FINAL_B0TO30,
            age_comparison_index.FINAL_B31TO60,
            age_comparison_index.FINAL_B61TO90,
            age_comparison_index.FINAL_B91TO180,
            age_comparison_index.FINAL_B181TO360,
            age_comparison_index.FINAL_B361,
            age_comparison_index.ORPHAN_FLG,
            age_comparison_index.DUPLICATED_FLG,
            age_comparison_index.CHARGE_THRESHOLD_FLG,
            age_comparison_index.NUM_TRADES,
            NULL,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            NULL,
            0,
            NULL,
            NULL,
            0,
            0,
            NULL,
            NULL,
            NULL,
            1,
            NULL
          );
      ELSE
        SELECT PKG_REPORTS.F_GET_FX_RATE(age_comparison_index.notional_currency_id,age_comparison_index.fx_currency_id,age_comparison_index.region_id,age_comparison_index.asofdate)
        INTO p_FX_RATES
        FROM dual;
        A_PRICE             :=age_comparison_index.PRICE;
        A_QUANTITY          :=age_comparison_index.QUANTITY;
        A_MARKET_VALUE      :=ROUND(age_comparison_index.MARKET_VALUE    *p_FX_RATES,6);
        A_B0TO30_QTY        :=ROUND(age_comparison_index.B0TO30_QTY      *p_FX_RATES,6);
        A_B31TO60_QTY       :=ROUND(age_comparison_index.B31TO60_QTY     *p_FX_RATES,6);
        A_B61TO90_QTY       :=ROUND(age_comparison_index.B61TO90_QTY     *p_FX_RATES,6);
        A_B91TO180_QTY      :=ROUND(age_comparison_index.B91TO180_QTY    *p_FX_RATES,6);
        A_B181TO360_QTY     :=ROUND(age_comparison_index.B181TO360_QTY   *p_FX_RATES,6);
        A_B361_QTY          :=ROUND(age_comparison_index.B361_QTY        *p_FX_RATES,6);
        A_B0TO30_MV         :=ROUND(age_comparison_index.B0TO30_MV       *p_FX_RATES,6);
        A_B31TO60_MV        :=ROUND(age_comparison_index.B31TO60_MV      *p_FX_RATES,6);
        A_B61TO90_MV        :=ROUND(age_comparison_index.B61TO90_MV      *p_FX_RATES,6);
        A_B91TO180_MV       :=ROUND(age_comparison_index.B91TO180_MV     *p_FX_RATES,6);
        A_B181TO360_MV      :=ROUND(age_comparison_index.B181TO360_MV    *p_FX_RATES,6);
        A_B361_MV           :=ROUND(age_comparison_index.B361_MV         *p_FX_RATES,6);
        A_CHRG_B0TO30       :=ROUND(age_comparison_index.CHRG_B0TO30     *p_FX_RATES,6);
        A_CHRG_B31TO60      :=ROUND(age_comparison_index.CHRG_B31TO60    *p_FX_RATES,6);
        A_CHRG_B61TO90      :=ROUND(age_comparison_index.CHRG_B61TO90    *p_FX_RATES,6);
        A_CHRG_B91TO180     :=ROUND(age_comparison_index.CHRG_B91TO180   *p_FX_RATES,6);
        A_CHRG_B181TO360    :=ROUND(age_comparison_index.CHRG_B181TO360  *p_FX_RATES,6);
        A_CHRG_B361         :=ROUND(age_comparison_index.CHRG_B361       *p_FX_RATES,6);
        A_EXMP_B0TO30       :=ROUND(age_comparison_index.EXMP_B0TO30     *p_FX_RATES,6);
        A_EXMP_B31TO60      :=ROUND(age_comparison_index.EXMP_B31TO60    *p_FX_RATES,6);
        A_EXMP_B61TO90      :=ROUND(age_comparison_index.EXMP_B61TO90    *p_FX_RATES,6);
        A_EXMP_B91TO180     :=ROUND(age_comparison_index.EXMP_B91TO180   *p_FX_RATES,6);
        A_EXMP_B181TO360    :=ROUND(age_comparison_index.EXMP_B181TO360  *p_FX_RATES,6);
        A_EXMP_B361         :=ROUND(age_comparison_index.EXMP_B361       *p_FX_RATES,6);
        A_FINAL_B0TO30      :=ROUND(age_comparison_index.FINAL_B0TO30    *p_FX_RATES,6);
        A_FINAL_B31TO60     :=ROUND(age_comparison_index.FINAL_B31TO60   *p_FX_RATES,6);
        A_FINAL_B61TO90     :=ROUND(age_comparison_index.FINAL_B61TO90   *p_FX_RATES,6);
        A_FINAL_B91TO180    :=ROUND(age_comparison_index.FINAL_B91TO180  *p_FX_RATES,6);
        A_FINAL_B181TO360   :=ROUND(age_comparison_index.FINAL_B181TO360 *p_FX_RATES,6);
        A_FINAL_B361        :=ROUND(age_comparison_index.FINAL_B361      *p_FX_RATES,6);
        A_CLEAN_PRICE       :=age_comparison_index.CLEAN_PRICE;
        A_CLEAN_MARKET_VALUE:=age_comparison_index.CLEAN_MARKET_VALUE;
        A_ACCRUED_INTEREST  :=age_comparison_index.ACCRUED_INTEREST;
        INSERT
        INTO age_comparison_der VALUES
          (
            age_comparison_index.ASOFDATE,
            age_comparison_index.SOURCE_SYSTEM_ID,
            age_comparison_index.INSTRUMENT_ID,
            age_comparison_index.INSTRUMENT_TYPE,
            age_comparison_index.BOOK_ID,
            A_PRICE,
            A_QUANTITY,
            age_comparison_index.FACTOR,
            age_comparison_index.FX_CURRENCY_ID,
            age_comparison_index.NOTIONAL_CURRENCY_ID,
            A_MARKET_VALUE,
            age_comparison_index.MARKET_VALUE_USD,
            age_comparison_index.LONG_SHORT,
            age_comparison_index.PRODUCT_TYPE,
            age_comparison_index.LEGAL_ENTITY_ID,
            A_B0TO30_QTY,
            A_B31TO60_QTY,
            A_B61TO90_QTY,
            A_B91TO180_QTY,
            A_B181TO360_QTY,
            A_B361_QTY,
            A_B0TO30_MV,
            A_B31TO60_MV,
            A_B61TO90_MV,
            A_B91TO180_MV,
            A_B181TO360_MV,
            A_B361_MV,
            age_comparison_index.INSTRUMENT_ID_LEVEL2,
            age_comparison_index.INSTRUMENT_TYPE_LEVEL2,
            age_comparison_index.INSTRUMENT_ID_LEVEL3,
            age_comparison_index.INSTRUMENT_TYPE_LEVEL3,
            age_comparison_index.INSTRUMENT_ID_LEVEL4,
            age_comparison_index.INSTRUMENT_TYPE_LEVEL4,
            age_comparison_index.INSTRUMENT_ID_LEVEL5,
            age_comparison_index.INSTRUMENT_TYPE_LEVEL5,
            age_comparison_index.EXEC_MP_FLG,
            age_comparison_index.EXEC_CA_FLG,
            age_comparison_index.EXEC_DB_FGL,
            A_CLEAN_PRICE,
            A_CLEAN_MARKET_VALUE,
            A_ACCRUED_INTEREST,
            age_comparison_index.HAS_CP_FLG,
            age_comparison_index.HAS_CMV_FLG,
            age_comparison_index.HAS_AI_FLG,
            age_comparison_index.INSTRUMENT_DESCRIPTION,
            age_comparison_index.CHRG_ID,
            A_CHRG_B0TO30,
            A_CHRG_B31TO60,
            A_CHRG_B61TO90,
            A_CHRG_B91TO180,
            A_CHRG_B181TO360,
            A_CHRG_B361,
            age_comparison_index.EXMP_ID,
            A_EXMP_B0TO30,
            A_EXMP_B31TO60,
            A_EXMP_B61TO90,
            A_EXMP_B91TO180,
            A_EXMP_B181TO360,
            A_EXMP_B361,
            A_FINAL_B0TO30,
            A_FINAL_B31TO60,
            A_FINAL_B61TO90,
            A_FINAL_B91TO180,
            A_FINAL_B181TO360,
            A_FINAL_B361,
            age_comparison_index.ORPHAN_FLG,
            age_comparison_index.DUPLICATED_FLG,
            age_comparison_index.CHARGE_THRESHOLD_FLG,
            age_comparison_index.NUM_TRADES,
            NULL,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            NULL,
            0,
            NULL,
            NULL,
            0,
            0,
            NULL,
            NULL,
            NULL,
            1,
            NULL
          );
      END IF;
      p_cont   :=p_cont+1;
      IF p_cont = 10000 THEN
        COMMIT;
        p_cont:=0;
      END IF;
    END LOOP;
    DELETE FROM age_comparison_cash_t;
    COMMIT;
  END PR_GET_FXRATE_COMP_CASH;
-----------------------------------------------------------------------------
-- Functionality: Return currency for regions
-- Used:
------------------------------------------------------------------------------
  FUNCTION F_GET_CURRENCY_REG(
      A_REGION IN VARCHAR2 )
    RETURN VARCHAR2
  IS
    v_curr VARCHAR2(3);
  BEGIN
    IF (A_REGION IS NULL ) THEN
      RETURN 'USD';
    END IF;
    IF (A_REGION = 'Americas' OR A_REGION = 'AMERICAS' OR A_REGION = 'NEWYORK') THEN
      v_curr    := 'USD';
    ELSE
      v_curr := 'EUR';
    END IF;
    RETURN v_curr;
  END F_GET_CURRENCY_REG;
-----------------------------------------------------------------------------
-- Functionality: Get conversion rate for Regions
-- Used:
------------------------------------------------------------------------------
  FUNCTION F_CONVERT_RATE_REG(
      A_NUMBER   IN NUMBER,
      A_DATE     IN FX_RATES.ASOFDATE%TYPE,
      A_REGIONSS IN BOOK_HIERARCHY_RPL.REGION%TYPE,
      A_REGION   IN SOURCE_SYSTEM.REGION_ID%TYPE)
    RETURN NUMBER
  IS
    v_spot NUMBER(30,6);
    v_curr VARCHAR2(3);
    v_reg  VARCHAR2(10);
  BEGIN
    IF (A_NUMBER IS NULL OR A_NUMBER = 0 ) THEN
      RETURN 0;
    END IF;
    IF(A_REGIONSS = 'AMERICAS' OR A_REGIONSS = 'Americas') THEN
      v_reg      := 'NEWYORK';
    ELSE
      v_reg := A_REGION;
    END IF;
    v_curr := F_GET_CURRENCY_REG(A_REGIONSS);
    SELECT spot
    INTO v_spot
    FROM fx_rates
    WHERE asofdate =A_DATE
    AND region_id  =v_reg
    AND currency_id=v_curr;
    RETURN A_NUMBER/v_spot;
  EXCEPTION
  WHEN OTHERS THEN
    RETURN 0;
    RAISE;
  END F_CONVERT_RATE_REG;
-----------------------------------------------------------------------------
-- Functionality: Get conversion rate for REGION in thousands
-- Used:
------------------------------------------------------------------------------
  FUNCTION F_C_RATE_REGT(
      A_NUMBER   IN NUMBER,
      A_DATE     IN FX_RATES.ASOFDATE%TYPE,
      A_REGIONSS IN BOOK_HIERARCHY_RPL.REGION%TYPE,
      A_REGION   IN SOURCE_SYSTEM.REGION_ID%TYPE)
    RETURN NUMBER
  IS
  BEGIN
    RETURN (F_CONVERT_RATE_REG(A_NUMBER,A_DATE,A_REGIONSS,A_REGION))/1000;
  END F_C_RATE_REGT;
-----------------------------------------------------------------------------
-- Functionality: Get conversion rate for REGION in millions
-- Used:
------------------------------------------------------------------------------
  FUNCTION F_C_RATE_REGM(
      A_NUMBER   IN NUMBER,
      A_DATE     IN FX_RATES.ASOFDATE%TYPE,
      A_REGIONSS IN BOOK_HIERARCHY_RPL.REGION%TYPE,
      A_REGION   IN SOURCE_SYSTEM.REGION_ID%TYPE)
    RETURN NUMBER
  IS
  BEGIN
    RETURN F_CONVERT_RATE_REG(A_NUMBER,A_DATE,A_REGIONSS,A_REGION)/1000000;
  END F_C_RATE_REGM;
  PROCEDURE PR_GET_FXRATE_COMP_DER
  IS
    A_PRICE age_comparison_der.PRICE%TYPE;
    A_QUANTITY age_comparison_der.QUANTITY%TYPE;
    A_MARKET_VALUE age_comparison_der.MARKET_VALUE%TYPE;
    A_B0TO30_QTY age_comparison_der.B0TO30_QTY%TYPE;
    A_B31TO60_QTY age_comparison_der.B31TO60_QTY%TYPE;
    A_B61TO90_QTY age_comparison_der.B61TO90_QTY%TYPE;
    A_B91TO180_QTY age_comparison_der.B91TO180_QTY%TYPE;
    A_B181TO360_QTY age_comparison_der.B181TO360_QTY%TYPE;
    A_B361_QTY age_comparison_der.B361_QTY%TYPE;
    A_B0TO30_MV age_comparison_der.B0TO30_MV%TYPE;
    A_B31TO60_MV age_comparison_der.B31TO60_MV%TYPE;
    A_B61TO90_MV age_comparison_der.B61TO90_MV%TYPE;
    A_B91TO180_MV age_comparison_der.B91TO180_MV%TYPE;
    A_B181TO360_MV age_comparison_der.B181TO360_MV%TYPE;
    A_B361_MV age_comparison_der.B361_MV%TYPE;
    A_CHRG_B0TO30 age_comparison_der.CHRG_B0TO30%TYPE;
    A_CHRG_B31TO60 age_comparison_der.CHRG_B31TO60%TYPE;
    A_CHRG_B61TO90 age_comparison_der.CHRG_B61TO90%TYPE;
    A_CHRG_B91TO180 age_comparison_der.CHRG_B91TO180%TYPE;
    A_CHRG_B181TO360 age_comparison_der.CHRG_B181TO360%TYPE;
    A_CHRG_B361 age_comparison_der.CHRG_B361%TYPE;
    A_EXMP_B0TO30 age_comparison_der.EXMP_B0TO30%TYPE;

A_EXMP_B31TO60 age_comparison_der.EXMP_B31TO60%TYPE;
    A_EXMP_B61TO90 age_comparison_der.EXMP_B61TO90%TYPE;
    A_EXMP_B91TO180 age_comparison_der.EXMP_B91TO180%TYPE;
    A_EXMP_B181TO360 age_comparison_der.EXMP_B181TO360%TYPE;
    A_EXMP_B361 age_comparison_der.EXMP_B361%TYPE;
    A_FINAL_B0TO30 age_comparison_der.FINAL_B0TO30%TYPE;
    A_FINAL_B31TO60 age_comparison_der.FINAL_B31TO60%TYPE;
    A_FINAL_B61TO90 age_comparison_der.FINAL_B61TO90%TYPE;
    A_FINAL_B91TO180 age_comparison_der.FINAL_B91TO180%TYPE;
    A_FINAL_B181TO360 age_comparison_der.FINAL_B181TO360%TYPE;
    A_FINAL_B361 age_comparison_der.FINAL_B361%TYPE;
    A_CLEAN_PRICE age_comparison_der.CLEAN_PRICE%TYPE;
    A_CLEAN_MARKET_VALUE age_comparison_der.CLEAN_MARKET_VALUE%TYPE;
    A_ACCRUED_INTEREST age_comparison_der.ACCRUED_INTEREST%TYPE;
    A_NOTIONAL age_comparison_der.NOTIONAL%TYPE;
    A_DELTA age_comparison_der.DELTA%TYPE;
    A_PV01 age_comparison_der.PV01%TYPE;
    A_DV01 age_comparison_der.DV01%TYPE;
    p_FX_RATES NUMBER;
    p_cont     NUMBER:=0;
    CURSOR age_comparison_der_t
    IS
      SELECT ag.ASOFDATE,
        ag.SOURCE_SYSTEM_ID,
        ag.INSTRUMENT_ID,
        ag.INSTRUMENT_TYPE,
        ag.BOOK_ID,
        ag.PRICE,
        ag.QUANTITY,
        ag.FACTOR,
        ag.FX_CURRENCY_ID,
        ag.NOTIONAL_CURRENCY_ID,
        ag.MARKET_VALUE,
        ag.MARKET_VALUE_USD,
        ag.LONG_SHORT,
        ag.PRODUCT_TYPE,
        ag.LEGAL_ENTITY_ID,
        ag.B0TO30_QTY,
        ag.B31TO60_QTY,
        ag.B61TO90_QTY,
        ag.B91TO180_QTY,
        ag.B181TO360_QTY,
        ag.B361_QTY,
        ag.B0TO30_MV,
        ag.B31TO60_MV,
        ag.B61TO90_MV,
        ag.B91TO180_MV,
        ag.B181TO360_MV,
        ag.B361_MV,
        ag.INSTRUMENT_ID_LEVEL2,
        ag.INSTRUMENT_TYPE_LEVEL2,
        ag.INSTRUMENT_ID_LEVEL3,
        ag.INSTRUMENT_TYPE_LEVEL3,
        ag.INSTRUMENT_ID_LEVEL4,
        ag.INSTRUMENT_TYPE_LEVEL4,
        ag.INSTRUMENT_ID_LEVEL5,
        ag.INSTRUMENT_TYPE_LEVEL5,
        ag.EXEC_MP_FLG,
        ag.EXEC_CA_FLG,
        ag.EXEC_DB_FGL,
        ag.CLEAN_PRICE,
        ag.CLEAN_MARKET_VALUE,
        ag.ACCRUED_INTEREST,
        ag.HAS_CP_FLG,
        ag.HAS_CMV_FLG,
        ag.HAS_AI_FLG,
        ag.INSTRUMENT_DESCRIPTION,
        ag.CHRG_ID,
        ag.CHRG_B0TO30,
        ag.CHRG_B31TO60,
        ag.CHRG_B61TO90,
        ag.CHRG_B91TO180,
        ag.CHRG_B181TO360,
        ag.CHRG_B361,
        ag.EXMP_ID,
        ag.EXMP_B0TO30,
        ag.EXMP_B31TO60,
        ag.EXMP_B61TO90,
        ag.EXMP_B91TO180,
        ag.EXMP_B181TO360,
        ag.EXMP_B361,
        ag.FINAL_B0TO30,
        ag.FINAL_B31TO60,
        ag.FINAL_B61TO90,
        ag.FINAL_B91TO180,
        ag.FINAL_B181TO360,
        ag.FINAL_B361,
        ag.ORPHAN_FLG,
        ag.DUPLICATED_FLG,
        ag.CHARGE_THRESHOLD_FLG,
        ag.NUM_TRADES,
        ag.QV_INSTRUMENT_ID,
        ag.NOTIONAL,
        ag.DELTA,
        ag.PV01,
        ag.DV01,
        ag.RISK_RATIO,
        ag.NPV_PL,
        ag.MTM_PV,
        ag.ASSET_LIABILITY,
        ag.CONTRACT_SIZE,
        ag.UNDERLYING,
        ag.UNDERLYING_CCY,
        ag.SPOT_PRICE,
        ag.STRIKE_PRICE,
        ag.INSTRUMENT_ID_UNDERLYING,
        ag.EXPIRY_MATURITY_DATE,
        ag.RISK_INFORMATION_FLG,
        ag.PRODUCT_ID,
        ag.RATES_INFORMATION_FLG,
        ss.region_id
      FROM age_comparison_der_t ag,
        source_system ss
      WHERE ag.source_system_id = ss.source_system_id;
  BEGIN
    FOR age_comparison_index IN age_comparison_der_t
    LOOP
      IF age_comparison_index.notional_currency_id = 'USD' THEN
        INSERT
        INTO age_comparison_der VALUES
          (
            age_comparison_index.ASOFDATE,
            age_comparison_index.SOURCE_SYSTEM_ID,
            age_comparison_index.INSTRUMENT_ID,
            age_comparison_index.INSTRUMENT_TYPE,
            age_comparison_index.BOOK_ID,
            age_comparison_index.PRICE,
            age_comparison_index.QUANTITY,
            age_comparison_index.FACTOR,
            age_comparison_index.FX_CURRENCY_ID,
            age_comparison_index.NOTIONAL_CURRENCY_ID,
            age_comparison_index.MARKET_VALUE,
            age_comparison_index.MARKET_VALUE_USD,
            age_comparison_index.LONG_SHORT,
            age_comparison_index.PRODUCT_TYPE,
            age_comparison_index.LEGAL_ENTITY_ID,
            age_comparison_index.B0TO30_QTY,
            age_comparison_index.B31TO60_QTY,
            age_comparison_index.B61TO90_QTY,
            age_comparison_index.B91TO180_QTY,
            age_comparison_index.B181TO360_QTY,
            age_comparison_index.B361_QTY,
            age_comparison_index.B0TO30_MV,
            age_comparison_index.B31TO60_MV,
            age_comparison_index.B61TO90_MV,
            age_comparison_index.B91TO180_MV,
            age_comparison_index.B181TO360_MV,
            age_comparison_index.B361_MV,
            age_comparison_index.INSTRUMENT_ID_LEVEL2,
            age_comparison_index.INSTRUMENT_TYPE_LEVEL2,
            age_comparison_index.INSTRUMENT_ID_LEVEL3,
            age_comparison_index.INSTRUMENT_TYPE_LEVEL3,
            age_comparison_index.INSTRUMENT_ID_LEVEL4,
            age_comparison_index.INSTRUMENT_TYPE_LEVEL4,
            age_comparison_index.INSTRUMENT_ID_LEVEL5,
            age_comparison_index.INSTRUMENT_TYPE_LEVEL5,
            age_comparison_index.EXEC_MP_FLG,
            age_comparison_index.EXEC_CA_FLG,
            age_comparison_index.EXEC_DB_FGL,
            age_comparison_index.CLEAN_PRICE,
            age_comparison_index.CLEAN_MARKET_VALUE,
            age_comparison_index.ACCRUED_INTEREST,
            age_comparison_index.HAS_CP_FLG,
            age_comparison_index.HAS_CMV_FLG,
            age_comparison_index.HAS_AI_FLG,
            age_comparison_index.INSTRUMENT_DESCRIPTION,
            age_comparison_index.CHRG_ID,
            age_comparison_index.CHRG_B0TO30,
            age_comparison_index.CHRG_B31TO60,
            age_comparison_index.CHRG_B61TO90,
            age_comparison_index.CHRG_B91TO180,
            age_comparison_index.CHRG_B181TO360,
            age_comparison_index.CHRG_B361,
            age_comparison_index.EXMP_ID,
            age_comparison_index.EXMP_B0TO30,
            age_comparison_index.EXMP_B31TO60,
            age_comparison_index.EXMP_B61TO90,
            age_comparison_index.EXMP_B91TO180,
            age_comparison_index.EXMP_B181TO360,
            age_comparison_index.EXMP_B361,
            age_comparison_index.FINAL_B0TO30,
            age_comparison_index.FINAL_B31TO60,
            age_comparison_index.FINAL_B61TO90,
            age_comparison_index.FINAL_B91TO180,
            age_comparison_index.FINAL_B181TO360,
            age_comparison_index.FINAL_B361,
            age_comparison_index.ORPHAN_FLG,
            age_comparison_index.DUPLICATED_FLG,
            age_comparison_index.CHARGE_THRESHOLD_FLG,
            age_comparison_index.NUM_TRADES,
            age_comparison_index.QV_INSTRUMENT_ID,
            age_comparison_index.NOTIONAL,
            age_comparison_index.DELTA,
            age_comparison_index.PV01,
            age_comparison_index.DV01,
            age_comparison_index.RISK_RATIO,
            age_comparison_index.NPV_PL,
            age_comparison_index.MTM_PV,
            age_comparison_index.ASSET_LIABILITY,
            age_comparison_index.CONTRACT_SIZE,
            age_comparison_index.UNDERLYING,
            age_comparison_index.UNDERLYING_CCY,
            age_comparison_index.SPOT_PRICE,
            age_comparison_index.STRIKE_PRICE,
            age_comparison_index.INSTRUMENT_ID_UNDERLYING,
            age_comparison_index.EXPIRY_MATURITY_DATE,
            age_comparison_index.RISK_INFORMATION_FLG,
            age_comparison_index.PRODUCT_ID,
            age_comparison_index.RATES_INFORMATION_FLG
          );
      ELSE
        SELECT PKG_REPORTS.F_GET_FX_RATE(age_comparison_index.notional_currency_id,age_comparison_index.fx_currency_id,age_comparison_index.region_id,age_comparison_index.asofdate)
        INTO p_FX_RATES
        FROM dual;
        A_PRICE             :=age_comparison_index.PRICE;
        A_QUANTITY          :=age_comparison_index.QUANTITY;
        A_MARKET_VALUE      :=ROUND(age_comparison_index.MARKET_VALUE    *p_FX_RATES,6);
        A_B0TO30_QTY        :=ROUND(age_comparison_index.B0TO30_QTY      *p_FX_RATES,6);
        A_B31TO60_QTY       :=ROUND(age_comparison_index.B31TO60_QTY     *p_FX_RATES,6);
        A_B61TO90_QTY       :=ROUND(age_comparison_index.B61TO90_QTY     *p_FX_RATES,6);
        A_B91TO180_QTY      :=ROUND(age_comparison_index.B91TO180_QTY    *p_FX_RATES,6);
        A_B181TO360_QTY     :=ROUND(age_comparison_index.B181TO360_QTY   *p_FX_RATES,6);
        A_B361_QTY          :=ROUND(age_comparison_index.B361_QTY        *p_FX_RATES,6);
        A_B0TO30_MV         :=ROUND(age_comparison_index.B0TO30_MV       *p_FX_RATES,6);
        A_B31TO60_MV        :=ROUND(age_comparison_index.B31TO60_MV      *p_FX_RATES,6);
        A_B61TO90_MV        :=ROUND(age_comparison_index.B61TO90_MV      *p_FX_RATES,6);
        A_B91TO180_MV       :=ROUND(age_comparison_index.B91TO180_MV     *p_FX_RATES,6);
        A_B181TO360_MV      :=ROUND(age_comparison_index.B181TO360_MV    *p_FX_RATES,6);
        A_B361_MV           :=ROUND(age_comparison_index.B361_MV         *p_FX_RATES,6);
        A_CHRG_B0TO30       :=ROUND(age_comparison_index.CHRG_B0TO30     *p_FX_RATES,6);
        A_CHRG_B31TO60      :=ROUND(age_comparison_index.CHRG_B31TO60    *p_FX_RATES,6);
        A_CHRG_B61TO90      :=ROUND(age_comparison_index.CHRG_B61TO90    *p_FX_RATES,6);
        A_CHRG_B91TO180     :=ROUND(age_comparison_index.CHRG_B91TO180   *p_FX_RATES,6);
        A_CHRG_B181TO360    :=ROUND(age_comparison_index.CHRG_B181TO360  *p_FX_RATES,6);
        A_CHRG_B361         :=ROUND(age_comparison_index.CHRG_B361       *p_FX_RATES,6);
        A_EXMP_B0TO30       :=ROUND(age_comparison_index.EXMP_B0TO30     *p_FX_RATES,6);
        A_EXMP_B31TO60      :=ROUND(age_comparison_index.EXMP_B31TO60    *p_FX_RATES,6);
        A_EXMP_B61TO90      :=ROUND(age_comparison_index.EXMP_B61TO90    *p_FX_RATES,6);
        A_EXMP_B91TO180     :=ROUND(age_comparison_index.EXMP_B91TO180   *p_FX_RATES,6);
        A_EXMP_B181TO360    :=ROUND(age_comparison_index.EXMP_B181TO360  *p_FX_RATES,6);
        A_EXMP_B361         :=ROUND(age_comparison_index.EXMP_B361       *p_FX_RATES,6);
        A_FINAL_B0TO30      :=ROUND(age_comparison_index.FINAL_B0TO30    *p_FX_RATES,6);
        A_FINAL_B31TO60     :=ROUND(age_comparison_index.FINAL_B31TO60   *p_FX_RATES,6);
        A_FINAL_B61TO90     :=ROUND(age_comparison_index.FINAL_B61TO90   *p_FX_RATES,6);
        A_FINAL_B91TO180    :=ROUND(age_comparison_index.FINAL_B91TO180  *p_FX_RATES,6);
        A_FINAL_B181TO360   :=ROUND(age_comparison_index.FINAL_B181TO360 *p_FX_RATES,6);
        A_FINAL_B361        :=ROUND(age_comparison_index.FINAL_B361      *p_FX_RATES,6);
        A_CLEAN_PRICE       :=age_comparison_index.CLEAN_PRICE;
        A_CLEAN_MARKET_VALUE:=age_comparison_index.CLEAN_MARKET_VALUE;
        A_ACCRUED_INTEREST  :=age_comparison_index.ACCRUED_INTEREST;
        A_NOTIONAL          :=age_comparison_index.NOTIONAL;
        A_DELTA             :=age_comparison_index.DELTA;
        A_PV01              :=age_comparison_index.PV01;
        A_DV01              :=age_comparison_index.DV01;
        INSERT
        INTO age_comparison_der VALUES
          (
            age_comparison_index.ASOFDATE,
            age_comparison_index.SOURCE_SYSTEM_ID,
            age_comparison_index.INSTRUMENT_ID,
            age_comparison_index.INSTRUMENT_TYPE,
            age_comparison_index.BOOK_ID,
            A_PRICE,
            A_QUANTITY,
            age_comparison_index.FACTOR,
            age_comparison_index.FX_CURRENCY_ID,
            age_comparison_index.NOTIONAL_CURRENCY_ID,
            A_MARKET_VALUE,
            age_comparison_index.MARKET_VALUE_USD,
            age_comparison_index.LONG_SHORT,
            age_comparison_index.PRODUCT_TYPE,
            age_comparison_index.LEGAL_ENTITY_ID,
            A_B0TO30_QTY,
            A_B31TO60_QTY,
            A_B61TO90_QTY,
            A_B91TO180_QTY,
            A_B181TO360_QTY,
            A_B361_QTY,
            A_B0TO30_MV,
            A_B31TO60_MV,
            A_B61TO90_MV,
            A_B91TO180_MV,
            A_B181TO360_MV,
            A_B361_MV,
            age_comparison_index.INSTRUMENT_ID_LEVEL2,
            age_comparison_index.INSTRUMENT_TYPE_LEVEL2,
            age_comparison_index.INSTRUMENT_ID_LEVEL3,
            age_comparison_index.INSTRUMENT_TYPE_LEVEL3,
            age_comparison_index.INSTRUMENT_ID_LEVEL4,
            age_comparison_index.INSTRUMENT_TYPE_LEVEL4,
            age_comparison_index.INSTRUMENT_ID_LEVEL5,
            age_comparison_index.INSTRUMENT_TYPE_LEVEL5,
            age_comparison_index.EXEC_MP_FLG,
            age_comparison_index.EXEC_CA_FLG,
            age_comparison_index.EXEC_DB_FGL,
            A_CLEAN_PRICE,
            A_CLEAN_MARKET_VALUE,
            A_ACCRUED_INTEREST,
            age_comparison_index.HAS_CP_FLG,
            age_comparison_index.HAS_CMV_FLG,
            age_comparison_index.HAS_AI_FLG,
            age_comparison_index.INSTRUMENT_DESCRIPTION,
            age_comparison_index.CHRG_ID,
            A_CHRG_B0TO30,
            A_CHRG_B31TO60,
            A_CHRG_B61TO90,
            A_CHRG_B91TO180,
            A_CHRG_B181TO360,
            A_CHRG_B361,
            age_comparison_index.EXMP_ID,
            A_EXMP_B0TO30,
            A_EXMP_B31TO60,
            A_EXMP_B61TO90,
            A_EXMP_B91TO180,
            A_EXMP_B181TO360,
            A_EXMP_B361,
            A_FINAL_B0TO30,
            A_FINAL_B31TO60,
            A_FINAL_B61TO90,
            A_FINAL_B91TO180,
            A_FINAL_B181TO360,
            A_FINAL_B361,
            age_comparison_index.ORPHAN_FLG,
            age_comparison_index.DUPLICATED_FLG,
            age_comparison_index.CHARGE_THRESHOLD_FLG,
            age_comparison_index.NUM_TRADES,
            age_comparison_index.QV_INSTRUMENT_ID,
            A_NOTIONAL,
            A_DELTA,
            A_PV01,
            A_DV01,
            age_comparison_index.RISK_RATIO,
            age_comparison_index.NPV_PL,
            age_comparison_index.MTM_PV,
            age_comparison_index.ASSET_LIABILITY,
            age_comparison_index.CONTRACT_SIZE,
            age_comparison_index.UNDERLYING,
            age_comparison_index.UNDERLYING_CCY,
            age_comparison_index.SPOT_PRICE,
            age_comparison_index.STRIKE_PRICE,
            age_comparison_index.INSTRUMENT_ID_UNDERLYING,
            age_comparison_index.EXPIRY_MATURITY_DATE,
            age_comparison_index.RISK_INFORMATION_FLG,
            age_comparison_index.PRODUCT_ID,
            age_comparison_index.RATES_INFORMATION_FLG
          );
      END IF;
      p_cont   :=p_cont+1;
      IF p_cont = 10000 THEN
        COMMIT;
        p_cont:=0;
      END IF;
    END LOOP;
    DELETE FROM age_comparison_der_t;
    COMMIT;
  END PR_GET_FXRATE_COMP_DER;
  PROCEDURE PR_GET_FX_RATE(
      p_source_system_id IN SOURCE_SYSTEM.SOURCE_SYSTEM_ID%TYPE)
  IS
    p_FX_RATES  NUMBER;
    p_metric_id VARCHAR2(4);
    p_date      DATE;
    CURSOR age_rates
    IS
      SELECT ag.ASOFDATE,
        ag.SOURCE_SYSTEM_ID,
        ss.region_id ,
        ag.FX_CURRENCY_ID,
        ag.NOTIONAL_CURRENCY_ID
      FROM age ag,
        VW_SOURCE_SYSTEM ss
      WHERE ag.source_system_id = ss.source_system_id
      AND ag.asofdate           =ss.next_load
      AND ss.source_system_id   =p_source_system_id
      AND ss.metric_id          ='IA'
      GROUP BY ag.source_system_id,
        ag.asofdate,
        ag.FX_CURRENCY_ID,
        ag.NOTIONAL_CURRENCY_ID,
        ss.region_id;
  BEGIN
    p_metric_id:= 'IA';
    SELECT age_hadoop_nextload
    INTO p_date
    FROM source_system_metric
    WHERE source_system_id=p_source_system_id
    AND metric_id         =p_metric_id;
    FOR age_rates_index IN age_rates
    LOOP
      SELECT PKG_REPORTS.F_GET_FX_RATE(age_rates_index.notional_currency_id,age_rates_index.fx_currency_id,age_rates_index.region_id,age_rates_index.asofdate)
      INTO p_FX_RATES
      FROM dual;
      dbms_output.put_line('valor px_rate:' || p_FX_RATES);
      dbms_output.put_line('Cursor values: notional_currency_id: ' || age_rates_index.notional_currency_id ||' fx_currency_id: ' || age_rates_index.fx_currency_id ||' region_id: ' || age_rates_index.region_id ||' asofdate: ' ||age_rates_index.asofdate );
      IF age_rates_index.notional_currency_id <> age_rates_index.fx_currency_id THEN
        UPDATE AGE ag
        SET ag.FX_RATE                           = p_FX_RATES
        WHERE ag.source_system_id                = age_rates_index.source_system_id
        AND ag.asofdate                          =age_rates_index.asofdate
        AND age_rates_index.notional_currency_id = ag.NOTIONAL_CURRENCY_ID
        AND age_rates_index.fx_currency_id       = ag.FX_CURRENCY_ID;
      ELSE
        UPDATE AGE ag
        SET ag.FX_RATE                           = 1
        WHERE ag.source_system_id                = age_rates_index.source_system_id
        AND ag.asofdate                          =age_rates_index.asofdate
        AND age_rates_index.notional_currency_id = ag.NOTIONAL_CURRENCY_ID
        AND age_rates_index.fx_currency_id       = ag.FX_CURRENCY_ID;
      END IF;
    END LOOP;
    COMMIT;
  END PR_GET_FX_RATE;
------------------------------------------------------------
------ GEt old dates for comparison reports ----------------
------------------------------------------------------------
  FUNCTION F_GET_COMPARISON_MONTH_R(
      A_ASOFDATE      IN DATE,
      A_NUMBER_MONTHS IN NUMBER)
    RETURN DATE
  IS
    p_age_date source_system.age_hadoop_lastload%type;
    p_date_month source_system.age_hadoop_lastload%type;
    p_asofdate source_system.age_hadoop_lastload%type;
    p_end_day source_system.age_hadoop_lastload%type;
    p_day_of_week NUMBER;
    p_comparison_oldest_date source_system.age_hadoop_lastload%type;
  BEGIN
    p_date_month:=add_months(A_ASOFDATE, A_NUMBER_MONTHS);
    --Get the last day from the month
    SELECT last_day(p_date_month)
    INTO p_end_day
    FROM dual;
    -- Get the day of week
    SELECT TO_CHAR(p_end_day, 'D')
    INTO p_day_of_week
    FROM dual;
    IF(p_day_of_week = 6) THEN
      --If the day is Saturday, substract 1 day
      SELECT TRUNC(p_end_day - 1)
      INTO p_asofdate
      FROM dual;
    ELSIF(p_day_of_week = 7) THEN
      --If the day is Sunday, substract 2 days
      SELECT TRUNC(p_end_day - 2)
      INTO p_asofdate
      FROM dual;
    ELSE
      p_asofdate:=p_end_day;
    END IF;
    RETURN p_asofdate;
  END F_GET_COMPARISON_MONTH_R;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Functionality: Load one year of month-end book hierarchies expanded into BOOK_HIERARCHY_RPL_EXPANDED
-- Used:
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  PROCEDURE UPDATE_YEAR_BH_RPL_EXPANDED(
      IDATE DATE)
  AS
    C_ASOFDATE BOOK_HIERARCHY_RPL.ASOFDATE%TYPE;
    CURSOR C_DATES_TO_REFRESH
    IS
      SELECT IDATE CANDIDATE FROM DUAL
      UNION
      SELECT CANDIDATE - GREATEST (0, TO_CHAR (CANDIDATE, 'D') - 5)
        FROM (    SELECT LAST_DAY (ADD_MONTHS (TRUNC (IDATE), -ROWNUM))
                            AS CANDIDATE
                    FROM DUAL
              CONNECT BY ROWNUM <= 11);
  BEGIN
    OPEN C_DATES_TO_REFRESH;
    LOOP
      FETCH C_DATES_TO_REFRESH INTO C_ASOFDATE;
      EXIT
    WHEN C_DATES_TO_REFRESH%NOTFOUND;
      UPDATE_BH_RPL_EXPANDED (C_ASOFDATE);
    END LOOP;
    CLOSE C_DATES_TO_REFRESH;
    COMMIT;
  EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
  END;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Functionality: Call REFRESH_BH_RPL_EXPANDED for the given date and for the previous 11 month-end dates
-- Used:
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  PROCEDURE UPDATE_BH_RPL_EXPANDED(
      IDATE DATE)
  AS
  BEGIN
    REFRESH_BH_RPL_EXPANDED(
      IDATE,
      f_get_comparison_month_r (IDATE, -1),
      f_get_comparison_month_r (IDATE, -2),
      f_get_comparison_month_r (IDATE, -3),
      f_get_comparison_month_r (IDATE, -4),
      f_get_comparison_month_r (IDATE, -5),
      f_get_comparison_month_r (IDATE, -6),
      f_get_comparison_month_r (IDATE, -7),
      f_get_comparison_month_r (IDATE, -8),
      f_get_comparison_month_r (IDATE, -9),
      f_get_comparison_month_r (IDATE, -10),
      f_get_comparison_month_r (IDATE, -11));
    COMMIT;
  EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
  END;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Functionality: refresh one date of data in BOOK_HIERARCHY_RPL_EXPANDED with data from BOOK_HIERARCHY_RPL
-- Used:
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  PROCEDURE REFRESH_BH_RPL_EXPANDED(
      IDATE DATE,
      IMONTHEND01 DATE,
      IMONTHEND02 DATE,
      IMONTHEND03 DATE,
      IMONTHEND04 DATE,
      IMONTHEND05 DATE,
      IMONTHEND06 DATE,
      IMONTHEND07 DATE,
      IMONTHEND08 DATE,
      IMONTHEND09 DATE,
      IMONTHEND10 DATE,
      IMONTHEND11 DATE)
  AS
    BH_DATE DATE;
  BEGIN
    SELECT MAX (ASOFDATE)
      INTO BH_DATE
      FROM BOOK_HIERARCHY_RPL
     WHERE ASOFDATE <= IDATE; 
    
    --REMOVE PREVIOUS LOADED DATA
    DELETE BOOK_HIERARCHY_RPL_EXPANDED
    WHERE ASOFDATE = BH_DATE;
    
    --INSERT THE BOOK HIERARCHY ROWS THAT DO HAVE SOURCE_SYSTEM INFORMED
    INSERT
    INTO BOOK_HIERARCHY_RPL_EXPANDED
      (
        ASOFDATE,
        BOOK_ID,
        VOLCKER_TRADING_DESK,
        VOLCKER_TRADING_DESK_FULL,
        LOWEST_LEVEL_RPL_CODE,
        LOWEST_LEVEL_RPL_FULL_NAME,
        LOWEST_LEVEL_RPL,
        SOURCE_SYSTEM,
        LEGAL_ENTITY,
        GLOBAL_TRADER_BOOK_ID,
        PROFIT_CENTER_ID,
        COMMENTS,
        DATA_SOURCE,
        CREATE_DATE,
        LAST_MODIFIED_DATE,
        CHARGE_REPORTING_UNIT_CODE,
        CHARGE_REPORTING_UNIT,
        CHARGE_REPORTING_PARENT_CODE,
        CHARGE_REPORTING_PARENT,
        MI_LOCATION,
        UBR_LEVEL_1_ID,
        UBR_LEVEL_1_NAME,
        UBR_LEVEL_1_RPL_CODE,
        UBR_LEVEL_2_ID,
        UBR_LEVEL_2_NAME,
        UBR_LEVEL_2_RPL_CODE,
        UBR_LEVEL_3_ID,
        UBR_LEVEL_3_NAME,
        UBR_LEVEL_3_RPL_CODE,
        UBR_LEVEL_4_ID,
        UBR_LEVEL_4_NAME,
        UBR_LEVEL_4_RPL_CODE,
        UBR_LEVEL_5_ID,
        UBR_LEVEL_5_NAME,
        UBR_LEVEL_5_RPL_CODE,
        UBR_LEVEL_6_ID,
        UBR_LEVEL_6_NAME,
        UBR_LEVEL_6_RPL_CODE,
        UBR_LEVEL_7_ID,
        UBR_LEVEL_7_NAME,
        UBR_LEVEL_7_RPL_CODE,
        UBR_LEVEL_8_ID,
        UBR_LEVEL_8_NAME,
        UBR_LEVEL_8_RPL_CODE,
        UBR_LEVEL_9_ID,
        UBR_LEVEL_9_NAME,
        UBR_LEVEL_9_RPL_CODE,
        UBR_LEVEL_10_ID,
        UBR_LEVEL_10_NAME,
        UBR_LEVEL_10_RPL_CODE,
        UBR_LEVEL_11_ID,
        UBR_LEVEL_11_NAME,
        UBR_LEVEL_11_RPL_CODE,
        UBR_LEVEL_12_ID,
        UBR_LEVEL_12_NAME,
        UBR_LEVEL_12_RPL_CODE,
        UBR_LEVEL_13_ID,
        UBR_LEVEL_13_NAME,
        UBR_LEVEL_13_RPL_CODE,
        UBR_LEVEL_14_ID,
        UBR_LEVEL_14_NAME,
        UBR_LEVEL_14_RPL_CODE,
        DESK_LEVEL_1_ID,
        DESK_LEVEL_1_NAME,
        DESK_LEVEL_1_RPL_CODE,
        DESK_LEVEL_2_ID,
        DESK_LEVEL_2_NAME,
        DESK_LEVEL_2_RPL_CODE,
        DESK_LEVEL_3_ID,
        DESK_LEVEL_3_NAME,
        DESK_LEVEL_3_RPL_CODE,
        DESK_LEVEL_4_ID,
        DESK_LEVEL_4_NAME,
        DESK_LEVEL_4_RPL_CODE,
        DESK_LEVEL_5_ID,
        DESK_LEVEL_5_NAME,
        DESK_LEVEL_5_RPL_CODE,
        PORTFOLIO_ID,
        PORTFOLIO_NAME,
        PORTFOLIO_RPL_CODE,
        BUSINESS,
        SUB_BUSINESS,
        CREATE_USER,
        LAST_MODIFICATION_USER,
        REGION,
        SUBREGION,
        APPROVER_USER,
        APPROVAL_DATE,
        COVERED_FUND_BUS_UNIT_RPL_CODE,
        COVERED_FUND_BUS_UNIT_NAME
      )
    SELECT ASOFDATE,
      BOOK_ID,
      VOLCKER_TRADING_DESK,
      VOLCKER_TRADING_DESK_FULL,
      LOWEST_LEVEL_RPL_CODE,
      LOWEST_LEVEL_RPL_FULL_NAME,
      LOWEST_LEVEL_RPL,
      SOURCE_SYSTEM,
      LEGAL_ENTITY,
      GLOBAL_TRADER_BOOK_ID,
      PROFIT_CENTER_ID,
      COMMENTS,
      DATA_SOURCE,
      CREATE_DATE,
      LAST_MODIFIED_DATE,
      CHARGE_REPORTING_UNIT_CODE,
      CHARGE_REPORTING_UNIT,
      CHARGE_REPORTING_PARENT_CODE,
      CHARGE_REPORTING_PARENT,
      MI_LOCATION,
      UBR_LEVEL_1_ID,
      UBR_LEVEL_1_NAME,
      UBR_LEVEL_1_RPL_CODE,
      UBR_LEVEL_2_ID,
      UBR_LEVEL_2_NAME,
      UBR_LEVEL_2_RPL_CODE,
      UBR_LEVEL_3_ID,
      UBR_LEVEL_3_NAME,
      UBR_LEVEL_3_RPL_CODE,
      UBR_LEVEL_4_ID,
      UBR_LEVEL_4_NAME,
      UBR_LEVEL_4_RPL_CODE,
      UBR_LEVEL_5_ID,
      UBR_LEVEL_5_NAME,
      UBR_LEVEL_5_RPL_CODE,
      UBR_LEVEL_6_ID,
      UBR_LEVEL_6_NAME,
      UBR_LEVEL_6_RPL_CODE,
      UBR_LEVEL_7_ID,
      UBR_LEVEL_7_NAME,
      UBR_LEVEL_7_RPL_CODE,
      UBR_LEVEL_8_ID,
      UBR_LEVEL_8_NAME,
      UBR_LEVEL_8_RPL_CODE,
      UBR_LEVEL_9_ID,
      UBR_LEVEL_9_NAME,
      UBR_LEVEL_9_RPL_CODE,
      UBR_LEVEL_10_ID,
      UBR_LEVEL_10_NAME,
      UBR_LEVEL_10_RPL_CODE,
      UBR_LEVEL_11_ID,
      UBR_LEVEL_11_NAME,
      UBR_LEVEL_11_RPL_CODE,
      UBR_LEVEL_12_ID,
      UBR_LEVEL_12_NAME,
      UBR_LEVEL_12_RPL_CODE,
      UBR_LEVEL_13_ID,
      UBR_LEVEL_13_NAME,
      UBR_LEVEL_13_RPL_CODE,
      UBR_LEVEL_14_ID,
      UBR_LEVEL_14_NAME,
      UBR_LEVEL_14_RPL_CODE,
      DESK_LEVEL_1_ID,
      DESK_LEVEL_1_NAME,
      DESK_LEVEL_1_RPL_CODE,
      DESK_LEVEL_2_ID,
      DESK_LEVEL_2_NAME,
      DESK_LEVEL_2_RPL_CODE,
      DESK_LEVEL_3_ID,
      DESK_LEVEL_3_NAME,
      DESK_LEVEL_3_RPL_CODE,
      DESK_LEVEL_4_ID,
      DESK_LEVEL_4_NAME,
      DESK_LEVEL_4_RPL_CODE,
      DESK_LEVEL_5_ID,
      DESK_LEVEL_5_NAME,
      DESK_LEVEL_5_RPL_CODE,
      PORTFOLIO_ID,
      PORTFOLIO_NAME,
      PORTFOLIO_RPL_CODE,
      BUSINESS,
      SUB_BUSINESS,
      CREATE_USER,
      LAST_MODIFICATION_USER,
      REGION,
      SUBREGION,
      APPROVER_USER,
      APPROVAL_DATE,
      COVERED_FUND_BUS_UNIT_RPL_CODE,
      COVERED_FUND_BUS_UNIT_NAME
    FROM BOOK_HIERARCHY_RPL
    WHERE ASOFDATE = BH_DATE
    AND SOURCE_SYSTEM IS NOT NULL;
    
    --INSERT THE BOOK HIERARCHY ROWS THAT DON'T HAVE SOURCE_SYSTEM INFORMED
    INSERT
    INTO BOOK_HIERARCHY_RPL_EXPANDED
      (
        ASOFDATE,
        BOOK_ID,
        VOLCKER_TRADING_DESK,
        VOLCKER_TRADING_DESK_FULL,
        LOWEST_LEVEL_RPL_CODE,
        LOWEST_LEVEL_RPL_FULL_NAME,
        LOWEST_LEVEL_RPL,
        SOURCE_SYSTEM,
        LEGAL_ENTITY,
        GLOBAL_TRADER_BOOK_ID,
        PROFIT_CENTER_ID,
        COMMENTS,
        DATA_SOURCE,
        CREATE_DATE,
        LAST_MODIFIED_DATE,
        CHARGE_REPORTING_UNIT_CODE,
        CHARGE_REPORTING_UNIT,
        CHARGE_REPORTING_PARENT_CODE,
        CHARGE_REPORTING_PARENT,
        MI_LOCATION,
        UBR_LEVEL_1_ID,
        UBR_LEVEL_1_NAME,
        UBR_LEVEL_1_RPL_CODE,
        UBR_LEVEL_2_ID,
        UBR_LEVEL_2_NAME,
        UBR_LEVEL_2_RPL_CODE,
        UBR_LEVEL_3_ID,
        UBR_LEVEL_3_NAME,
        UBR_LEVEL_3_RPL_CODE,
        UBR_LEVEL_4_ID,
        UBR_LEVEL_4_NAME,
        UBR_LEVEL_4_RPL_CODE,
        UBR_LEVEL_5_ID,
        UBR_LEVEL_5_NAME,
        UBR_LEVEL_5_RPL_CODE,
        UBR_LEVEL_6_ID,
        UBR_LEVEL_6_NAME,
        UBR_LEVEL_6_RPL_CODE,
        UBR_LEVEL_7_ID,
        UBR_LEVEL_7_NAME,
        UBR_LEVEL_7_RPL_CODE,
        UBR_LEVEL_8_ID,
        UBR_LEVEL_8_NAME,
        UBR_LEVEL_8_RPL_CODE,
        UBR_LEVEL_9_ID,
        UBR_LEVEL_9_NAME,
        UBR_LEVEL_9_RPL_CODE,
        UBR_LEVEL_10_ID,
        UBR_LEVEL_10_NAME,
        UBR_LEVEL_10_RPL_CODE,
        UBR_LEVEL_11_ID,
        UBR_LEVEL_11_NAME,
        UBR_LEVEL_11_RPL_CODE,
        UBR_LEVEL_12_ID,
        UBR_LEVEL_12_NAME,
        UBR_LEVEL_12_RPL_CODE,
        UBR_LEVEL_13_ID,
        UBR_LEVEL_13_NAME,
        UBR_LEVEL_13_RPL_CODE,
        UBR_LEVEL_14_ID,
        UBR_LEVEL_14_NAME,
        UBR_LEVEL_14_RPL_CODE,
        DESK_LEVEL_1_ID,
        DESK_LEVEL_1_NAME,
        DESK_LEVEL_1_RPL_CODE,
        DESK_LEVEL_2_ID,
        DESK_LEVEL_2_NAME,
        DESK_LEVEL_2_RPL_CODE,
        DESK_LEVEL_3_ID,
        DESK_LEVEL_3_NAME,
        DESK_LEVEL_3_RPL_CODE,
        DESK_LEVEL_4_ID,
        DESK_LEVEL_4_NAME,
        DESK_LEVEL_4_RPL_CODE,
        DESK_LEVEL_5_ID,
        DESK_LEVEL_5_NAME,
        DESK_LEVEL_5_RPL_CODE,
        PORTFOLIO_ID,
        PORTFOLIO_NAME,
        PORTFOLIO_RPL_CODE,
        BUSINESS,
        SUB_BUSINESS,
        CREATE_USER,
        LAST_MODIFICATION_USER,
        REGION,
        SUBREGION,
        APPROVER_USER,
        APPROVAL_DATE,
        COVERED_FUND_BUS_UNIT_RPL_CODE,
        COVERED_FUND_BUS_UNIT_NAME
      )
    SELECT ASOFDATE,
      BHR.BOOK_ID,
      VOLCKER_TRADING_DESK,
      VOLCKER_TRADING_DESK_FULL,
      LOWEST_LEVEL_RPL_CODE,
      LOWEST_LEVEL_RPL_FULL_NAME,
      LOWEST_LEVEL_RPL,
      SOURCE_SYSTEM_ID,
      LEGAL_ENTITY,
      GLOBAL_TRADER_BOOK_ID,
      PROFIT_CENTER_ID,
      COMMENTS,
      DATA_SOURCE,
      CREATE_DATE,
      LAST_MODIFIED_DATE,
      CHARGE_REPORTING_UNIT_CODE,
      CHARGE_REPORTING_UNIT,
      CHARGE_REPORTING_PARENT_CODE,
      CHARGE_REPORTING_PARENT,
      MI_LOCATION,
      UBR_LEVEL_1_ID,
      UBR_LEVEL_1_NAME,
      UBR_LEVEL_1_RPL_CODE,
      UBR_LEVEL_2_ID,
      UBR_LEVEL_2_NAME,
      UBR_LEVEL_2_RPL_CODE,
      UBR_LEVEL_3_ID,
      UBR_LEVEL_3_NAME,
      UBR_LEVEL_3_RPL_CODE,
      UBR_LEVEL_4_ID,
      UBR_LEVEL_4_NAME,
      UBR_LEVEL_4_RPL_CODE,
      UBR_LEVEL_5_ID,
      UBR_LEVEL_5_NAME,
      UBR_LEVEL_5_RPL_CODE,
      UBR_LEVEL_6_ID,
      UBR_LEVEL_6_NAME,
      UBR_LEVEL_6_RPL_CODE,
      UBR_LEVEL_7_ID,
      UBR_LEVEL_7_NAME,
      UBR_LEVEL_7_RPL_CODE,
      UBR_LEVEL_8_ID,
      UBR_LEVEL_8_NAME,
      UBR_LEVEL_8_RPL_CODE,
      UBR_LEVEL_9_ID,
      UBR_LEVEL_9_NAME,
      UBR_LEVEL_9_RPL_CODE,
      UBR_LEVEL_10_ID,
      UBR_LEVEL_10_NAME,
      UBR_LEVEL_10_RPL_CODE,
      UBR_LEVEL_11_ID,
      UBR_LEVEL_11_NAME,
      UBR_LEVEL_11_RPL_CODE,
      UBR_LEVEL_12_ID,
      UBR_LEVEL_12_NAME,
      UBR_LEVEL_12_RPL_CODE,
      UBR_LEVEL_13_ID,
      UBR_LEVEL_13_NAME,
      UBR_LEVEL_13_RPL_CODE,
      UBR_LEVEL_14_ID,
      UBR_LEVEL_14_NAME,
      UBR_LEVEL_14_RPL_CODE,
      DESK_LEVEL_1_ID,
      DESK_LEVEL_1_NAME,
      DESK_LEVEL_1_RPL_CODE,
      DESK_LEVEL_2_ID,
      DESK_LEVEL_2_NAME,
      DESK_LEVEL_2_RPL_CODE,
      DESK_LEVEL_3_ID,
      DESK_LEVEL_3_NAME,
      DESK_LEVEL_3_RPL_CODE,
      DESK_LEVEL_4_ID,
      DESK_LEVEL_4_NAME,
      DESK_LEVEL_4_RPL_CODE,
      DESK_LEVEL_5_ID,
      DESK_LEVEL_5_NAME,
      DESK_LEVEL_5_RPL_CODE,
      PORTFOLIO_ID,
      PORTFOLIO_NAME,
      PORTFOLIO_RPL_CODE,
      BUSINESS,
      SUB_BUSINESS,
      CREATE_USER,
      LAST_MODIFICATION_USER,
      REGION,
      SUBREGION,
      APPROVER_USER,
      APPROVAL_DATE,
      COVERED_FUND_BUS_UNIT_RPL_CODE,
      COVERED_FUND_BUS_UNIT_NAME
    FROM BOOK_HIERARCHY_RPL BHR,
      ((SELECT DISTINCT SOURCE_SYSTEM_ID, BOOK_ID
          FROM AGE

WHERE ASOFDATE = IDATE
            OR ASOFDATE = IMONTHEND01
            OR ASOFDATE = IMONTHEND02
            OR ASOFDATE = IMONTHEND03
            OR ASOFDATE = IMONTHEND04
            OR ASOFDATE = IMONTHEND05
            OR ASOFDATE = IMONTHEND06
            OR ASOFDATE = IMONTHEND07
            OR ASOFDATE = IMONTHEND08
            OR ASOFDATE = IMONTHEND09
            OR ASOFDATE = IMONTHEND10
            OR ASOFDATE = IMONTHEND11)
      MINUS
       SELECT DISTINCT SOURCE_SYSTEM, BOOK_ID
         FROM BOOK_HIERARCHY_RPL BHR2
        WHERE ASOFDATE = BH_DATE) SS
    WHERE BHR.ASOFDATE = BH_DATE
      AND BHR.SOURCE_SYSTEM IS NULL
      AND BHR.BOOK_ID = SS.BOOK_ID;
    
    COMMIT;
  
  EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
  END;

  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  -- Functionality: refresh one date of data in BOOK_HIERARCHY_RPL_QV_EXPANDED with data from BOOK_HIERARCHY_RPL
  -- Used:
  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  PROCEDURE REFRESH_BH_RPL_QV_EXPANDED(IDATE DATE)  
  IS  
  BEGIN
    
    --REMOVE PREVIOUS LOADED DATA
    DELETE BOOK_HIERARCHY_RPL_QV_EXPANDED
    WHERE ASOFDATE = IDATE;
	
    --INSERT THE BOOK HIERARCHY ROWS THAT DO HAVE SOURCE_SYSTEM INFORMED
    INSERT
    INTO BOOK_HIERARCHY_RPL_QV_EXPANDED
      ( ASOFDATE,
        BOOK_ID,
        VOLCKER_TRADING_DESK,
        VOLCKER_TRADING_DESK_FULL,
        LOWEST_LEVEL_RPL_CODE,
        LOWEST_LEVEL_RPL_FULL_NAME,
        LOWEST_LEVEL_RPL,
        SOURCE_SYSTEM,
        LEGAL_ENTITY,
        GLOBAL_TRADER_BOOK_ID,
        PROFIT_CENTER_ID,
        COMMENTS,
        DATA_SOURCE,
        CREATE_DATE,
        LAST_MODIFIED_DATE,
        CHARGE_REPORTING_UNIT_CODE,
        CHARGE_REPORTING_UNIT,
        CHARGE_REPORTING_PARENT_CODE,
        CHARGE_REPORTING_PARENT,
        MI_LOCATION,
        UBR_LEVEL_1_ID,
        UBR_LEVEL_1_NAME,
        UBR_LEVEL_1_RPL_CODE,
        UBR_LEVEL_2_ID,
        UBR_LEVEL_2_NAME,
        UBR_LEVEL_2_RPL_CODE,
        UBR_LEVEL_3_ID,
        UBR_LEVEL_3_NAME,
        UBR_LEVEL_3_RPL_CODE,
        UBR_LEVEL_4_ID,
        UBR_LEVEL_4_NAME,
        UBR_LEVEL_4_RPL_CODE,
        UBR_LEVEL_5_ID,
        UBR_LEVEL_5_NAME,
        UBR_LEVEL_5_RPL_CODE,
        UBR_LEVEL_6_ID,
        UBR_LEVEL_6_NAME,
        UBR_LEVEL_6_RPL_CODE,
        UBR_LEVEL_7_ID,
        UBR_LEVEL_7_NAME,
        UBR_LEVEL_7_RPL_CODE,
        UBR_LEVEL_8_ID,
        UBR_LEVEL_8_NAME,
        UBR_LEVEL_8_RPL_CODE,
        UBR_LEVEL_9_ID,
        UBR_LEVEL_9_NAME,
        UBR_LEVEL_9_RPL_CODE,
        UBR_LEVEL_10_ID,
        UBR_LEVEL_10_NAME,
        UBR_LEVEL_10_RPL_CODE,
        UBR_LEVEL_11_ID,
        UBR_LEVEL_11_NAME,
        UBR_LEVEL_11_RPL_CODE,
        UBR_LEVEL_12_ID,
        UBR_LEVEL_12_NAME,
        UBR_LEVEL_12_RPL_CODE,
        UBR_LEVEL_13_ID,
        UBR_LEVEL_13_NAME,
        UBR_LEVEL_13_RPL_CODE,
        UBR_LEVEL_14_ID,
        UBR_LEVEL_14_NAME,
        UBR_LEVEL_14_RPL_CODE,
        DESK_LEVEL_1_ID,
        DESK_LEVEL_1_NAME,
        DESK_LEVEL_1_RPL_CODE,
        DESK_LEVEL_2_ID,
        DESK_LEVEL_2_NAME,
        DESK_LEVEL_2_RPL_CODE,
        DESK_LEVEL_3_ID,
        DESK_LEVEL_3_NAME,
        DESK_LEVEL_3_RPL_CODE,
        DESK_LEVEL_4_ID,
        DESK_LEVEL_4_NAME,
        DESK_LEVEL_4_RPL_CODE,
        DESK_LEVEL_5_ID,
        DESK_LEVEL_5_NAME,
        DESK_LEVEL_5_RPL_CODE,
        PORTFOLIO_ID,
        PORTFOLIO_NAME,
        PORTFOLIO_RPL_CODE,
        BUSINESS,
        SUB_BUSINESS,
        CREATE_USER,
        LAST_MODIFICATION_USER,
        REGION,
        SUBREGION,
        APPROVER_USER,
        APPROVAL_DATE,
        COVERED_FUND_BUS_UNIT_RPL_CODE,
        COVERED_FUND_BUS_UNIT_NAME
      )
    SELECT
	  ASOFDATE,
      BOOK_ID,
      VOLCKER_TRADING_DESK,
      VOLCKER_TRADING_DESK_FULL,
      LOWEST_LEVEL_RPL_CODE,
      LOWEST_LEVEL_RPL_FULL_NAME,
      LOWEST_LEVEL_RPL,
      SOURCE_SYSTEM,
      LEGAL_ENTITY,
      GLOBAL_TRADER_BOOK_ID,
      PROFIT_CENTER_ID,
      COMMENTS,
      DATA_SOURCE,
      CREATE_DATE,
      LAST_MODIFIED_DATE,
      CHARGE_REPORTING_UNIT_CODE,
      CHARGE_REPORTING_UNIT,
      CHARGE_REPORTING_PARENT_CODE,
      CHARGE_REPORTING_PARENT,
      MI_LOCATION,
      UBR_LEVEL_1_ID,
      UBR_LEVEL_1_NAME,
      UBR_LEVEL_1_RPL_CODE,
      UBR_LEVEL_2_ID,
      UBR_LEVEL_2_NAME,
      UBR_LEVEL_2_RPL_CODE,
      UBR_LEVEL_3_ID,
      UBR_LEVEL_3_NAME,
      UBR_LEVEL_3_RPL_CODE,
      UBR_LEVEL_4_ID,
      UBR_LEVEL_4_NAME,
      UBR_LEVEL_4_RPL_CODE,
      UBR_LEVEL_5_ID,
      UBR_LEVEL_5_NAME,
      UBR_LEVEL_5_RPL_CODE,
      UBR_LEVEL_6_ID,
      UBR_LEVEL_6_NAME,
      UBR_LEVEL_6_RPL_CODE,
      UBR_LEVEL_7_ID,
      UBR_LEVEL_7_NAME,
      UBR_LEVEL_7_RPL_CODE,
      UBR_LEVEL_8_ID,
      UBR_LEVEL_8_NAME,
      UBR_LEVEL_8_RPL_CODE,
      UBR_LEVEL_9_ID,
      UBR_LEVEL_9_NAME,
      UBR_LEVEL_9_RPL_CODE,
      UBR_LEVEL_10_ID,
      UBR_LEVEL_10_NAME,
      UBR_LEVEL_10_RPL_CODE,
      UBR_LEVEL_11_ID,
      UBR_LEVEL_11_NAME,
      UBR_LEVEL_11_RPL_CODE,
      UBR_LEVEL_12_ID,
      UBR_LEVEL_12_NAME,
      UBR_LEVEL_12_RPL_CODE,
      UBR_LEVEL_13_ID,
      UBR_LEVEL_13_NAME,
      UBR_LEVEL_13_RPL_CODE,
      UBR_LEVEL_14_ID,
      UBR_LEVEL_14_NAME,
      UBR_LEVEL_14_RPL_CODE,
      DESK_LEVEL_1_ID,
      DESK_LEVEL_1_NAME,
      DESK_LEVEL_1_RPL_CODE,
      DESK_LEVEL_2_ID,
      DESK_LEVEL_2_NAME,
      DESK_LEVEL_2_RPL_CODE,
      DESK_LEVEL_3_ID,
      DESK_LEVEL_3_NAME,
      DESK_LEVEL_3_RPL_CODE,
      DESK_LEVEL_4_ID,
      DESK_LEVEL_4_NAME,
      DESK_LEVEL_4_RPL_CODE,
      DESK_LEVEL_5_ID,
      DESK_LEVEL_5_NAME,
      DESK_LEVEL_5_RPL_CODE,
      PORTFOLIO_ID,
      PORTFOLIO_NAME,
      PORTFOLIO_RPL_CODE,
      BUSINESS,
      SUB_BUSINESS,
      CREATE_USER,
      LAST_MODIFICATION_USER,
      REGION,
      SUBREGION,
      APPROVER_USER,
      APPROVAL_DATE,
      COVERED_FUND_BUS_UNIT_RPL_CODE,
      COVERED_FUND_BUS_UNIT_NAME
    FROM BOOK_HIERARCHY_RPL
    WHERE ASOFDATE = IDATE
    AND SOURCE_SYSTEM IS NOT NULL;
    
    --INSERT THE BOOK HIERARCHY ROWS THAT DON'T HAVE SOURCE_SYSTEM INFORMED
    INSERT
    INTO BOOK_HIERARCHY_RPL_QV_EXPANDED
      ( ASOFDATE,
        BOOK_ID,
        VOLCKER_TRADING_DESK,
        VOLCKER_TRADING_DESK_FULL,
        LOWEST_LEVEL_RPL_CODE,
        LOWEST_LEVEL_RPL_FULL_NAME,
        LOWEST_LEVEL_RPL,
        SOURCE_SYSTEM,
        LEGAL_ENTITY,
        GLOBAL_TRADER_BOOK_ID,
        PROFIT_CENTER_ID,
        COMMENTS,
        DATA_SOURCE,
        CREATE_DATE,
        LAST_MODIFIED_DATE,
        CHARGE_REPORTING_UNIT_CODE,
        CHARGE_REPORTING_UNIT,
        CHARGE_REPORTING_PARENT_CODE,
        CHARGE_REPORTING_PARENT,
        MI_LOCATION,
        UBR_LEVEL_1_ID,
        UBR_LEVEL_1_NAME,
        UBR_LEVEL_1_RPL_CODE,
        UBR_LEVEL_2_ID,
        UBR_LEVEL_2_NAME,
        UBR_LEVEL_2_RPL_CODE,
        UBR_LEVEL_3_ID,
        UBR_LEVEL_3_NAME,
        UBR_LEVEL_3_RPL_CODE,
        UBR_LEVEL_4_ID,
        UBR_LEVEL_4_NAME,
        UBR_LEVEL_4_RPL_CODE,
        UBR_LEVEL_5_ID,
        UBR_LEVEL_5_NAME,
        UBR_LEVEL_5_RPL_CODE,
        UBR_LEVEL_6_ID,
        UBR_LEVEL_6_NAME,
        UBR_LEVEL_6_RPL_CODE,
        UBR_LEVEL_7_ID,
        UBR_LEVEL_7_NAME,
        UBR_LEVEL_7_RPL_CODE,
        UBR_LEVEL_8_ID,
        UBR_LEVEL_8_NAME,
        UBR_LEVEL_8_RPL_CODE,
        UBR_LEVEL_9_ID,
        UBR_LEVEL_9_NAME,
        UBR_LEVEL_9_RPL_CODE,
        UBR_LEVEL_10_ID,
        UBR_LEVEL_10_NAME,
        UBR_LEVEL_10_RPL_CODE,
        UBR_LEVEL_11_ID,
        UBR_LEVEL_11_NAME,
        UBR_LEVEL_11_RPL_CODE,
        UBR_LEVEL_12_ID,
        UBR_LEVEL_12_NAME,
        UBR_LEVEL_12_RPL_CODE,
        UBR_LEVEL_13_ID,
        UBR_LEVEL_13_NAME,
        UBR_LEVEL_13_RPL_CODE,
        UBR_LEVEL_14_ID,
        UBR_LEVEL_14_NAME,
        UBR_LEVEL_14_RPL_CODE,
        DESK_LEVEL_1_ID,
        DESK_LEVEL_1_NAME,
        DESK_LEVEL_1_RPL_CODE,
        DESK_LEVEL_2_ID,
        DESK_LEVEL_2_NAME,
        DESK_LEVEL_2_RPL_CODE,
        DESK_LEVEL_3_ID,
        DESK_LEVEL_3_NAME,
        DESK_LEVEL_3_RPL_CODE,
        DESK_LEVEL_4_ID,
        DESK_LEVEL_4_NAME,
        DESK_LEVEL_4_RPL_CODE,
        DESK_LEVEL_5_ID,
        DESK_LEVEL_5_NAME,
        DESK_LEVEL_5_RPL_CODE,
        PORTFOLIO_ID,
        PORTFOLIO_NAME,
        PORTFOLIO_RPL_CODE,
        BUSINESS,
        SUB_BUSINESS,
        CREATE_USER,
        LAST_MODIFICATION_USER,
        REGION,
        SUBREGION,
        APPROVER_USER,
        APPROVAL_DATE,
        COVERED_FUND_BUS_UNIT_RPL_CODE,
        COVERED_FUND_BUS_UNIT_NAME
      )
    SELECT
	  ASOFDATE,
      BHR.BOOK_ID,
      VOLCKER_TRADING_DESK,
      VOLCKER_TRADING_DESK_FULL,
      LOWEST_LEVEL_RPL_CODE,
      LOWEST_LEVEL_RPL_FULL_NAME,
      LOWEST_LEVEL_RPL,
      SOURCE_SYSTEM_ID,
      LEGAL_ENTITY,
      GLOBAL_TRADER_BOOK_ID,
      PROFIT_CENTER_ID,
      COMMENTS,
      DATA_SOURCE,
      CREATE_DATE,
      LAST_MODIFIED_DATE,
      CHARGE_REPORTING_UNIT_CODE,
      CHARGE_REPORTING_UNIT,
      CHARGE_REPORTING_PARENT_CODE,
      CHARGE_REPORTING_PARENT,
      MI_LOCATION,
      UBR_LEVEL_1_ID,
      UBR_LEVEL_1_NAME,
      UBR_LEVEL_1_RPL_CODE,
      UBR_LEVEL_2_ID,
      UBR_LEVEL_2_NAME,
      UBR_LEVEL_2_RPL_CODE,
      UBR_LEVEL_3_ID,
      UBR_LEVEL_3_NAME,
      UBR_LEVEL_3_RPL_CODE,
      UBR_LEVEL_4_ID,
      UBR_LEVEL_4_NAME,
      UBR_LEVEL_4_RPL_CODE,
      UBR_LEVEL_5_ID,
      UBR_LEVEL_5_NAME,
      UBR_LEVEL_5_RPL_CODE,
      UBR_LEVEL_6_ID,
      UBR_LEVEL_6_NAME,
      UBR_LEVEL_6_RPL_CODE,
      UBR_LEVEL_7_ID,
      UBR_LEVEL_7_NAME,
      UBR_LEVEL_7_RPL_CODE,
      UBR_LEVEL_8_ID,
      UBR_LEVEL_8_NAME,
      UBR_LEVEL_8_RPL_CODE,
      UBR_LEVEL_9_ID,
      UBR_LEVEL_9_NAME,
      UBR_LEVEL_9_RPL_CODE,
      UBR_LEVEL_10_ID,
      UBR_LEVEL_10_NAME,
      UBR_LEVEL_10_RPL_CODE,
      UBR_LEVEL_11_ID,
      UBR_LEVEL_11_NAME,
      UBR_LEVEL_11_RPL_CODE,
      UBR_LEVEL_12_ID,
      UBR_LEVEL_12_NAME,
      UBR_LEVEL_12_RPL_CODE,
      UBR_LEVEL_13_ID,
      UBR_LEVEL_13_NAME,
      UBR_LEVEL_13_RPL_CODE,
      UBR_LEVEL_14_ID,
      UBR_LEVEL_14_NAME,
      UBR_LEVEL_14_RPL_CODE,
      DESK_LEVEL_1_ID,
      DESK_LEVEL_1_NAME,
      DESK_LEVEL_1_RPL_CODE,
      DESK_LEVEL_2_ID,
      DESK_LEVEL_2_NAME,
      DESK_LEVEL_2_RPL_CODE,
      DESK_LEVEL_3_ID,
      DESK_LEVEL_3_NAME,
      DESK_LEVEL_3_RPL_CODE,
      DESK_LEVEL_4_ID,
      DESK_LEVEL_4_NAME,
      DESK_LEVEL_4_RPL_CODE,
      DESK_LEVEL_5_ID,
      DESK_LEVEL_5_NAME,
      DESK_LEVEL_5_RPL_CODE,
      PORTFOLIO_ID,
      PORTFOLIO_NAME,
      PORTFOLIO_RPL_CODE,
      BUSINESS,
      SUB_BUSINESS,
      CREATE_USER,
      LAST_MODIFICATION_USER,
      REGION,
      SUBREGION,
      APPROVER_USER,
      APPROVAL_DATE,
      COVERED_FUND_BUS_UNIT_RPL_CODE,
      COVERED_FUND_BUS_UNIT_NAME
    FROM BOOK_HIERARCHY_RPL BHR,
       (  SELECT DISTINCT SOURCE_SYSTEM_ID, BOOK_ID
          FROM SOURCE_SYSTEM, BOOK_HIERARCHY_RPL BHR
          WHERE BHR.ASOFDATE = IDATE
          AND BHR.SOURCE_SYSTEM IS NULL         
          MINUS
          SELECT DISTINCT SOURCE_SYSTEM, BOOK_ID
          FROM BOOK_HIERARCHY_RPL BHR
          WHERE ASOFDATE = IDATE
          AND BHR.SOURCE_SYSTEM IS NOT NULL) SS
    WHERE BHR.ASOFDATE = IDATE
      AND BHR.SOURCE_SYSTEM IS NULL
      AND BHR.BOOK_ID = SS.BOOK_ID;
  
  EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
  END REFRESH_BH_RPL_QV_EXPANDED;

END PKG_REPORTS;
